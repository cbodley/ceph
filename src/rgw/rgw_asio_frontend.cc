// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

#include "rgw_asio_client.h"
#include "rgw_asio_frontend.h"

#define dout_subsys ceph_subsys_rgw

namespace {

class Pauser {
  std::mutex mutex;
  std::condition_variable cond_ready; // signaled on ready==true
  std::condition_variable cond_paused; // signaled on waiters==thread_count
  bool ready{false};
  int waiters{0};
 public:
  template <typename Func>
  void pause(int thread_count, Func&& func);
  void unpause();
  void wait();
};

template <typename Func>
void Pauser::pause(int thread_count, Func&& func)
{
  std::unique_lock<std::mutex> lock(mutex);
  ready = false;
  lock.unlock();

  func();

  // wait for all threads to pause
  lock.lock();
  cond_paused.wait(lock, [=] { return waiters == thread_count; });
}

void Pauser::unpause()
{
  std::lock_guard<std::mutex> lock(mutex);
  ready = true;
  cond_ready.notify_all();
}

void Pauser::wait()
{
  std::unique_lock<std::mutex> lock(mutex);
  ++waiters;
  cond_paused.notify_one(); // notify pause() that we're waiting
  cond_ready.wait(lock, [this] { return ready; }); // wait for unpause()
  --waiters;
}

using tcp = boost::asio::ip::tcp;
namespace beast = boost::beast;

void handle_connection(RGWProcessEnv& env, tcp::socket& socket,
                       boost::asio::yield_context yield)
{
  // limit header to 4k, since we read it all into a single flat_buffer
  static constexpr size_t header_limit = 4096;
  // don't impose a limit on the body, since we read it in pieces
  static constexpr size_t body_limit = std::numeric_limits<size_t>::max();

  auto cct = env.store->ctx();
  boost::system::error_code ec;
  beast::flat_buffer buffer;

  // read messages from the socket until eof
  for (;;) {
    // configure the parser
    rgw::asio::parser_type parser;
    parser.header_limit(header_limit);
    parser.body_limit(body_limit);

    // parse the header
    beast::http::async_read_header(socket, buffer, parser, yield[ec]);
    if (ec == boost::asio::error::connection_reset ||
        ec == beast::http::error::end_of_stream) {
      return;
    }
    if (ec) {
      ldout(cct, 1) << "failed to read header: " << ec.message() << dendl;
      auto& message = parser.get();
      beast::http::response<beast::http::empty_body> response;
      response.result(beast::http::status::bad_request);
      response.version(message.version() == 10 ? 10 : 11);
      response.prepare_payload();
      beast::http::async_write(socket, response, yield[ec]);
      if (ec) {
        ldout(cct, 5) << "failed to write response: " << ec.message() << dendl;
      }
      ldout(cct, 1) << "====== req done http_status=400 ======" << dendl;
      return;
    }

    // process the request
    RGWRequest req{env.store->get_new_req_id()};

    rgw::asio::ClientIO real_client{socket, parser, buffer};

    auto real_client_io = rgw::io::add_reordering(
                            rgw::io::add_buffering(cct,
                              rgw::io::add_chunking(
                                rgw::io::add_conlen_controlling(
                                  &real_client))));
    RGWRestfulIO client(cct, &real_client_io);
    process_request(env.store, env.rest, &req, env.uri_prefix,
                    *env.auth_registry, &client, env.olog, &yield);

    if (!parser.is_keep_alive()) {
      return;
    }

    // if we failed before reading the entire message, discard any remaining
    // bytes before reading the next
    while (!parser.is_done()) {
      static std::array<char, 1024> discard_buffer;

      auto& body = parser.get().body();
      body.size = discard_buffer.size();
      body.data = discard_buffer.data();

      beast::http::async_read_some(socket, buffer, parser, yield[ec]);
      if (ec == boost::asio::error::connection_reset) {
        return;
      }
      if (ec) {
        ldout(cct, 5) << "failed to discard unread message: "
            << ec.message() << dendl;
        return;
      }
    }
  }
}

class AsioFrontend {
  RGWProcessEnv env;
  boost::asio::io_service service;

  tcp::acceptor acceptor;
  tcp::socket peer_socket;

  std::vector<std::thread> threads;
  Pauser pauser;
  std::atomic<bool> going_down{false};

  CephContext* ctx() const { return env.store->ctx(); }

  void accept(boost::system::error_code ec);

 public:
  AsioFrontend(const RGWProcessEnv& env)
    : env(env), acceptor(service), peer_socket(service) {}

  int init();
  int run();
  void stop();
  void join();
  void pause();
  void unpause(RGWRados* store, rgw_auth_registry_ptr_t);
};

int AsioFrontend::init()
{
  auto ep = tcp::endpoint{tcp::v4(), static_cast<unsigned short>(env.port)};
  ldout(ctx(), 4) << "frontend listening on " << ep << dendl;

  boost::system::error_code ec;
  acceptor.open(ep.protocol(), ec);
  if (ec) {
    lderr(ctx()) << "failed to open socket: " << ec.message() << dendl;
    return -ec.value();
  }
  acceptor.set_option(tcp::acceptor::reuse_address(true));
  acceptor.bind(ep, ec);
  if (ec) {
    lderr(ctx()) << "failed to bind address " << ep <<
        ": " << ec.message() << dendl;
    return -ec.value();
  }
  acceptor.listen(boost::asio::socket_base::max_connections);
  acceptor.async_accept(peer_socket,
                        [this] (boost::system::error_code ec) {
                          return accept(ec);
                        });
  return 0;
}

void AsioFrontend::accept(boost::system::error_code ec)
{
  if (!acceptor.is_open()) {
    return;
  } else if (ec == boost::asio::error::operation_aborted) {
    return;
  } else if (ec) {
    throw ec;
  }
  auto socket = std::move(peer_socket);
  acceptor.async_accept(peer_socket,
                        [this] (boost::system::error_code ec) {
                          return accept(ec);
                        });

  // spawn a coroutine to handle the connection
  // TODO: c++14 lambda capture for the socket&&
  boost::asio::spawn(service,
                     std::bind(handle_connection, std::ref(env),
                               std::move(socket), std::placeholders::_1));
}

int AsioFrontend::run()
{
  auto cct = ctx();
  const int thread_count = cct->_conf->rgw_thread_pool_size;
  threads.reserve(thread_count);

  ldout(cct, 4) << "frontend spawning " << thread_count << " threads" << dendl;

  for (int i = 0; i < thread_count; i++) {
    threads.emplace_back([=] {
      for (;;) {
        service.run();
        if (going_down) {
          break;
        }
        pauser.wait();
      }
    });
  }
  return 0;
}

void AsioFrontend::stop()
{
  ldout(ctx(), 4) << "frontend initiating shutdown..." << dendl;

  going_down = true;

  boost::system::error_code ec;
  acceptor.close(ec);

  // unblock the run() threads
  service.stop();
}

void AsioFrontend::join()
{
  if (!going_down) {
    stop();
  }
  ldout(ctx(), 4) << "frontend joining threads..." << dendl;
  for (auto& thread : threads) {
    thread.join();
  }
  ldout(ctx(), 4) << "frontend done" << dendl;
}

void AsioFrontend::pause()
{
  ldout(ctx(), 4) << "frontend pausing threads..." << dendl;
  pauser.pause(threads.size(), [=] {
    // unblock the run() threads
    service.stop();
  });
  ldout(ctx(), 4) << "frontend paused" << dendl;
}

void AsioFrontend::unpause(RGWRados* const store,
                           rgw_auth_registry_ptr_t auth_registry)
{
  env.store = store;
  env.auth_registry = std::move(auth_registry);
  ldout(ctx(), 4) << "frontend unpaused" << dendl;
  service.reset();
  pauser.unpause();
}

} // anonymous namespace

class RGWAsioFrontend::Impl : public AsioFrontend {
 public:
  Impl(const RGWProcessEnv& env) : AsioFrontend(env) {}
};

RGWAsioFrontend::RGWAsioFrontend(const RGWProcessEnv& env)
  : impl(new Impl(env))
{
}

RGWAsioFrontend::~RGWAsioFrontend() = default;

int RGWAsioFrontend::init()
{
  return impl->init();
}

int RGWAsioFrontend::run()
{
  return impl->run();
}

void RGWAsioFrontend::stop()
{
  impl->stop();
}

void RGWAsioFrontend::join()
{
  impl->join();
}

void RGWAsioFrontend::pause_for_new_config()
{
  impl->pause();
}

void RGWAsioFrontend::unpause_with_new_config(
  RGWRados* const store,
  rgw_auth_registry_ptr_t auth_registry
) {
  impl->unpause(store, std::move(auth_registry));
}
