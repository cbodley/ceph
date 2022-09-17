// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <list>
#include <memory>
#include <boost/context/protected_fixedsize_stack.hpp>
#include <spawn/spawn.hpp>
#include <quiche.h>

#include "common/async/shared_mutex.h"
#include "common/dout.h"

#include "rgw_dmclock_async_scheduler.h"
#include "rgw_frontend.h"
#include "rgw_process.h"
#include "rgw_request.h"
#include "rgw_tools.h"

#include "client_io.h"
#include "connection.h"
#include "frontend.h"
#include "listener.h"
#include "message.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::h3 {

// use mmap/mprotect to allocate 512k coroutine stacks
auto make_stack_allocator() {
  return boost::context::protected_fixedsize_stack{512*1024};
}

/// Synchronization primitive for Frontend::pause_for_new_config()
using SharedMutex = ceph::async::SharedMutex<default_executor>;

void config_deleter::operator()(quiche_config* config)
{
  ::quiche_config_free(config);
}
void h3_config_deleter::operator()(quiche_h3_config* h3)
{
  ::quiche_h3_config_free(h3);
}

class StreamHandler {
  const RGWProcessEnv& env;
  const RGWFrontendConfig* conf;
  asio::io_context& context;
  SharedMutex& pause_mutex;
  rgw::dmclock::Scheduler* scheduler;
  std::string uri_prefix;

  struct Prefix : DoutPrefixPipe {
    uint64_t stream_id;
    Prefix(const DoutPrefixProvider& dpp, uint64_t stream_id)
        : DoutPrefixPipe(dpp), stream_id(stream_id) {}
    void add_prefix(std::ostream& out) const override {
      out << "stream " << stream_id << ": ";
    }
  };
 public:
  StreamHandler(const RGWProcessEnv& env, const RGWFrontendConfig* conf,
                asio::io_context& context, SharedMutex& pause_mutex,
                rgw::dmclock::Scheduler* scheduler)
    : env(env), context(context), pause_mutex(pause_mutex), scheduler(scheduler)
  {
    auto& config = conf->get_config_map();
    if (auto i = config.find("prefix"); i != config.end()) {
      uri_prefix = i->second;
    }
  }

  // stackful request coroutine
  void process(boost::intrusive_ptr<Connection> conn, uint64_t stream_id,
               http::fields request, ip::udp::endpoint self,
               ip::udp::endpoint peer, yield_context yield)
  {
    auto dpp = Prefix{*conn, stream_id};
    auto cct = dpp.get_cct();

    // wait if paused
    error_code ec;
    auto lock = pause_mutex.async_lock_shared(yield[ec]);
    if (ec == asio::error::operation_aborted) {
      return;
    } else if (ec) {
      ldpp_dout(conn.get(), 1) << "failed to lock: " << ec.message() << dendl;
      return;
    }

    // process the request
    RGWRequest req{env.driver->get_new_req_id()};

    auto io = ClientIO{context, yield, conn.get(), stream_id,
        std::move(request), std::move(self), std::move(peer)};
    RGWRestfulIO client(cct, &io);
    optional_yield y = null_yield;
    if (cct->_conf->rgw_beast_enable_async) {
      y = optional_yield{context, yield};
    }
    int http_ret = 0;
    std::string user = "-";
    //const auto started = ceph::coarse_real_clock::now();
    ceph::coarse_real_clock::duration latency{};
    process_request(env, &req, uri_prefix, &client, y,
                    scheduler, &user, &latency, &http_ret);
#if 0
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw_access, 1)) {
      // access log line elements begin per Apache Combined Log Format with additions following
      lsubdout(cct, rgw_access, 1) << "beast: " << std::hex << &req << std::dec << ": "
          << remote_endpoint.address() << " - " << user << " [" << log_apache_time{started} << "] \""
          << message.method_string() << ' ' << message.target() << ' '
          << http_version{message.version()} << "\" " << http_ret << ' '
          << client.get_bytes_sent() + client.get_bytes_received() << ' '
          << log_header{message, http::field::referer, "\""} << ' '
          << log_header{message, http::field::user_agent, "\""} << ' '
          << log_header{message, http::field::range} << " latency="
          << latency << dendl;
    }
#endif
  }

  void operator()(boost::intrusive_ptr<Connection> c, uint64_t s,
                  http::fields r, ip::udp::endpoint self,
                  ip::udp::endpoint peer)
  {
    // spawn the stackful coroutine on the default io_context executor, not the
    // Connection's strand
    spawn::spawn(context,
        [this, c=std::move(c), s, r=std::move(r), self=std::move(self),
        peer=std::move(peer)] (yield_context yield) mutable {
          process(std::move(c), s, std::move(r),
                  std::move(self), std::move(peer), yield);
        }, make_stack_allocator());
  }
};

class Frontend : public RGWFrontend, public DoutPrefix {
  const RGWFrontendConfig* conf;

  config_ptr config;
  h3_config_ptr h3config;

  asio::io_context context;
  std::vector<std::thread> threads;

  SharedMutex pause_mutex;
  rgw::dmclock::SimpleThrottler scheduler;
  stream_handler on_new_stream;

  std::list<Listener> listeners;

 public:
  Frontend(CephContext* cct, const RGWProcessEnv& env,
           const RGWFrontendConfig* conf)
    : DoutPrefix(cct, dout_subsys, "h3: "), conf(conf),
      pause_mutex(context.get_executor()), scheduler(cct),
      on_new_stream(StreamHandler{env, conf, context, pause_mutex, &scheduler})
  {}

  int init() override;

  int run() override;
  void stop() override;
  void join() override;

  void pause_for_new_config() override;
  void unpause_with_new_config() override;
};

static void log_callback(const char* message, void* argp)
{
  auto cct = reinterpret_cast<CephContext*>(argp);
  ldout(cct, 1) << message << dendl;
}

static uint16_t parse_port(std::string_view input,
                           error_code& ec)
{
  uint16_t port = 0;
  auto [p, errc] = std::from_chars(input.begin(), input.end(), port);
  if (errc != std::errc{}) {
    ec = make_error_code(errc);
  }
  if (port == 0 || p != input.end()) {
    ec = make_error_code(std::errc::invalid_argument);
  }
  return port;
}

int Frontend::init()
{
  config.reset(::quiche_config_new(QUICHE_PROTOCOL_VERSION));
  h3config.reset(::quiche_h3_config_new());

  static constexpr std::string_view alpn = "\x02h3";
  ::quiche_config_set_application_protos(config.get(),
      (uint8_t *) alpn.data(), alpn.size());

  if (auto d = conf->get_val("debug"); d) {
    ::quiche_enable_debug_logging(log_callback, get_cct());
    ldpp_dout(this, 1) << "enabled quiche debug logging" << dendl;
  }
  quiche_config_set_max_idle_timeout(config.get(), 5000); // in ms
  quiche_config_set_max_recv_udp_payload_size(config.get(), max_datagram_size);
  quiche_config_set_max_send_udp_payload_size(config.get(), max_datagram_size);
  quiche_config_set_initial_max_data(config.get(), 10'000'000);
  quiche_config_set_initial_max_stream_data_bidi_local(config.get(), 1'000'000);
  quiche_config_set_initial_max_stream_data_bidi_remote(config.get(), 1'000'000);
  quiche_config_set_initial_max_stream_data_uni(config.get(), 1'000'000);
  quiche_config_set_initial_max_streams_bidi(config.get(), 100);
  quiche_config_set_initial_max_streams_uni(config.get(), 100);
  quiche_config_set_disable_active_migration(config.get(), true);

  // ssl configuration
  auto certs = conf->get_config_map().equal_range("cert");
  if (certs.first == certs.second) {
    ldpp_dout(this, -1) << "frontend config requires at least one 'cert'" << dendl;
    return -EINVAL;
  }
  for (auto i = certs.first; i != certs.second; ++i) {
    // TODO: quiche has no interfaces for loading certs/keys from memory, so
    // can't support the mon config keys. use asio::ssl wrappers like the
    // beast frontend, and pass its ssl context into quiche_conn_new_with_tls()
    int r = ::quiche_config_load_cert_chain_from_pem_file(
        config.get(), i->second.c_str());
    if (r < 0) {
      ldpp_dout(this, -1) << "failed to load cert chain from " << i->second << dendl;
      return r;
    }
  }
  auto keys = conf->get_config_map().equal_range("key");
  if (keys.first == keys.second) {
    ldpp_dout(this, -1) << "frontend config requires at least one 'key'" << dendl;
    return -EINVAL;
  }
  for (auto i = keys.first; i != keys.second; ++i) {
    int r = ::quiche_config_load_priv_key_from_pem_file(
        config.get(), i->second.c_str());
    if (r < 0) {
      ldpp_dout(this, -1) << "failed to load private key from " << i->second << dendl;
      return r;
    }
  }

  // parse endpoints
  auto ports = conf->get_config_map().equal_range("port");
  if (ports.first == ports.second) {
    ldpp_dout(this, -1) << "frontend config requires at least one 'port'" << dendl;
    return -EINVAL;
  }
  for (auto i = ports.first; i != ports.second; ++i) {
    error_code ec;
    uint16_t port = parse_port(i->second.c_str(), ec);
    if (ec) {
      ldpp_dout(this, -1) << "failed to parse port=" << i->second << dendl;
      return -ec.value();
    }

    // bind a nonblocking udp socket for both v4/v6
    const auto endpoint = ip::udp::endpoint{ip::udp::v6(), port};
    auto socket = socket_type{context.get_executor()};
    socket.open(endpoint.protocol(), ec);
    if (ec) {
      ldpp_dout(this, -1) << "failed to open socket with " << ec.message() << dendl;
      return -ec.value();
    }
    socket.non_blocking(true, ec);
    socket.set_option(ip::v6_only{false}, ec);
    socket.set_option(socket_type::reuse_address{true}, ec);
    socket.bind(endpoint, ec);
    if (ec) {
      ldpp_dout(this, -1) << "failed to bind address " << endpoint
          << " with " << ec.message() << dendl;
      return -ec.value();
    }
    ldpp_dout(this, 20) << "bound " << endpoint << dendl;

    // construct the Listener and start accepting connections
    listeners.emplace_back(get_cct(), config.get(), h3config.get(),
                           std::move(socket), on_new_stream);
    listeners.back().async_listen(asio::detached);
  }
  return 0;
}

int Frontend::run()
{
  const int thread_count = get_cct()->_conf->rgw_thread_pool_size;
  threads.reserve(thread_count);

  ldpp_dout(this, 4) << "frontend spawning " << thread_count << " threads" << dendl;

  for (int i = 0; i < thread_count; i++) {
    threads.emplace_back([this]() noexcept {
      // request warnings on synchronous librados calls in this thread
      is_asio_thread = true;
      context.run();
    });
  }
  return 0;
}

void Frontend::stop()
{
  error_code ec;
  // close the listeners and their connections
  for (auto& l : listeners) {
    l.close(ec);
  }
}

void Frontend::join()
{
  ldpp_dout(this, 4) << "frontend joining threads..." << dendl;
  for (auto& thread : threads) {
    thread.join();
  }
  ldpp_dout(this, 4) << "frontend done" << dendl;
}

void Frontend::pause_for_new_config()
{
  ldpp_dout(this, 4) << "frontend pausing connections..." << dendl;

  // TODO: cancel pending calls to accept(), but don't close the sockets

  // pause and wait for outstanding requests to complete
  error_code ec;
  pause_mutex.lock(ec);

  if (ec) {
    ldpp_dout(this, 1) << "frontend failed to pause: " << ec.message() << dendl;
  } else {
    ldpp_dout(this, 4) << "frontend paused" << dendl;
  }
}

void Frontend::unpause_with_new_config()
{
  // unpause to unblock connections
  pause_mutex.unlock();

  // TODO: start accepting connections again

  ldpp_dout(this, 4) << "frontend unpaused" << dendl;
}

std::unique_ptr<RGWFrontend> create_frontend(CephContext* cct,
                                             const RGWProcessEnv& env,
                                             const RGWFrontendConfig* conf)
{
  return std::make_unique<Frontend>(cct, env, conf);
}

} // namespace rgw::h3
