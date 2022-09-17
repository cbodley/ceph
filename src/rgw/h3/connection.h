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

#pragma once

#include <memory>
#include <optional>
#include <span>
#include <tuple>

#include <boost/asio/strand.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <quiche.h>

#include "common/async/yield_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"

#include "h3/types.h"

namespace rgw::h3 {

struct conn_deleter {
  void operator()(quiche_conn* conn) { ::quiche_conn_free(conn); }
};
using conn_ptr = std::unique_ptr<quiche_conn, conn_deleter>;

struct h3_conn_deleter {
  void operator()(quiche_h3_conn* h3) { ::quiche_h3_conn_free(h3); }
};
using h3_conn_ptr = std::unique_ptr<quiche_h3_conn, h3_conn_deleter>;


/// Each Connection uses a strand executor to ensure thread-safe access to its
/// state.
using connection_executor = asio::strand<default_executor>;


/// A stream handle used to coordinate i/o between ClientIO and Connection.
struct StreamIO : boost::intrusive::set_base_hook<> {
  uint64_t id;
  std::span<uint8_t> data;
  bool fin = false;

  template <typename T>
  using awaitable = asio::awaitable<T, connection_executor>;
  using use_awaitable_t = asio::use_awaitable_t<connection_executor>;

  using wait_signature = void(std::exception_ptr, error_code);
  using wait_handler_type = typename asio::async_result<
      use_awaitable_t, wait_signature>::handler_type;
  std::optional<wait_handler_type> wait_handler;

  StreamIO(uint64_t id) : id(id) {}

  error_code read_some(const DoutPrefixProvider* dpp,
                       quiche_h3_conn* h3conn, quiche_conn* conn);
  error_code write_some(const DoutPrefixProvider* dpp,
                        quiche_h3_conn* h3conn, quiche_conn* conn, bool fin);

  awaitable<error_code> async_wait();
  void wake(error_code ec);
};

struct streamid_key {
  using type = uint64_t;
  type operator()(const StreamIO& s) { return s.id; }
};

struct streamid_less {
  using K = streamid_key::type;
  using V = const StreamIO&;
  bool operator()(V lhs, V rhs) const { return lhs.id < rhs.id; }
  bool operator()(K lhs, V rhs) const { return lhs < rhs.id; }
  bool operator()(V lhs, K rhs) const { return lhs.id < rhs; }
  bool operator()(K lhs, K rhs) const { return lhs < rhs; }
};

using streamio_set = boost::intrusive::set<StreamIO,
      boost::intrusive::key_of_value<streamid_key>,
      boost::intrusive::compare<streamid_less>>;


/// A server-side h3 connection accepted by a Listener. When incoming packets
/// indicate new client-initiated streams, the stream handler is called to
/// process their requests. When the connection has packets to send, they're
/// written directly to the udp socket instead of going through Listener.
class Connection : public DoutPrefixProvider,
                   public boost::intrusive_ref_counter<Connection>,
                   public boost::intrusive::set_base_hook<> {
 public:
  using executor_type = connection_executor;

  Connection(CephContext* cct, quiche_h3_config* h3config,
             socket_type& socket, stream_handler& on_new_stream,
             conn_ptr conn, connection_id cid);
  ~Connection();

  executor_type get_executor() const { return ex; }

 private:
  /// Dispatch a completion handler on its associated executor.
  template <typename Handler, typename... Ts>
  static void complete(Handler&& h, std::tuple<Ts...>&& args)
  {
    auto ex = asio::get_associated_executor(h);
    auto alloc = asio::get_associated_allocator(
        h, asio::recycling_allocator<void>());
    auto f = [h=std::move(h), args=std::move(args)] () mutable {
      std::apply(std::move(h), std::move(args));
    };
    asio::execution::execute(
        asio::prefer(ex,
            asio::execution::allocator(alloc),
            asio::execution::blocking.possibly,
            asio::execution::outstanding_work.tracked),
        std::move(f));
  }

 public:
  /// Accept and service a new connection on its own executor. The completion
  /// handler does not complete until the connection closes.
  template <typename CompletionToken>
  auto async_accept(CompletionToken&& token)
  {
    using Signature = void(error_code);
    auto self = boost::intrusive_ptr{this};
    return asio::async_initiate<CompletionToken, Signature>(
        [this, self=std::move(self)] (auto&& handler) mutable {
          // spawn accept() on the connection's executor and enable its
          // cancellation via 'cancel_accept'
          asio::co_spawn(get_executor(), accept(),
              asio::bind_cancellation_slot(cancel_accept.slot(),
                  [self=std::move(self), h=std::move(handler)]
                  (std::exception_ptr eptr, error_code ec) mutable {
                    if (eptr) {
                      std::rethrow_exception(std::move(eptr));
                    } else {
                      complete(std::move(h), std::make_tuple(ec));
                    }
                  }));
        }, token);
  }

  /// process an input packet on the connection's executor
  template <typename CompletionToken>
  auto async_handle_packet(std::span<uint8_t> data, ip::udp::endpoint peer,
                           ip::udp::endpoint self, CompletionToken&& token)
  {
    using Signature = void(error_code);
    return asio::async_initiate<CompletionToken, Signature>(
        [this, data, peer=std::move(peer),
         self=std::move(self)] (auto&& handler) mutable {
          asio::dispatch(get_executor(),
              [this, data, peer=std::move(peer), self=std::move(self),
               h=std::move(handler)] () mutable {
                auto ec = handle_packet(data, std::move(peer), std::move(self));
                complete(std::move(h), std::make_tuple(ec));
              });
        }, token);
  }

  /// read request body from the given stream
  template <typename CompletionToken>
  auto async_read_body(StreamIO& stream, std::span<uint8_t> data,
                       CompletionToken&& token)
  {
    using Signature = void(error_code, size_t);
    return asio::async_initiate<CompletionToken, Signature>(
        [this, &stream, data] (auto&& handler) {
          using result_type = std::tuple<error_code, size_t>;
          asio::co_spawn(get_executor(), read_body(stream, data),
              [h = std::move(handler)] (std::exception_ptr eptr,
                                        result_type result) mutable {
                if (eptr) {
                  std::rethrow_exception(std::move(eptr));
                } else {
                  complete(std::move(h), std::move(result));
                }
              });
        }, token);
  }

  /// write reponse headers to the given stream
  template <typename CompletionToken>
  auto async_write_response(StreamIO& stream,
                            std::span<quiche_h3_header> headers,
                            bool fin, CompletionToken&& token)
  {
    using Signature = void(error_code);
    return asio::async_initiate<CompletionToken, Signature>(
        [this, &stream, headers, fin] (auto&& handler) {
          asio::co_spawn(get_executor(),
                                write_response(stream, headers, fin),
              [h = std::move(handler)] (std::exception_ptr eptr,
                                        error_code ec) mutable {
                if (eptr) {
                  std::rethrow_exception(std::move(eptr));
                } else {
                  complete(std::move(h), std::make_tuple(ec));
                }
              });
        }, token);
  }

  /// write reponse body to the given stream
  template <typename CompletionToken>
  auto async_write_body(StreamIO& stream, std::span<uint8_t> data,
                        bool fin, CompletionToken&& token)
  {
    using Signature = void(error_code, size_t);
    return asio::async_initiate<CompletionToken, Signature>(
        [this, &stream, data, fin] (auto&& handler) {
          using result_type = std::tuple<error_code, size_t>;
          asio::co_spawn(get_executor(), write_body(stream, data, fin),
              [h = std::move(handler)] (std::exception_ptr eptr,
                                        result_type result) mutable {
                if (eptr) {
                  std::rethrow_exception(std::move(eptr));
                } else {
                  complete(std::move(h), std::move(result));
                }
              });
        }, token);
  }

  void cancel();

  // DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext* get_cct() const override;
  unsigned get_subsys() const override;

 public:
  CephContext* cct;
  quiche_h3_config* h3config;
  executor_type ex;
  // socket shared with Listener, but Connection only uses it to write packets
  socket_type& socket;
  stream_handler& on_new_stream;

  // cancellation signal attached to async_accept()
  asio::cancellation_signal cancel_accept;

  timer_type<ceph::coarse_mono_clock> timeout_timer;

  // awaitable type associated with our explicit connection executor instead of
  // the polymorphic any_io_executor
  template <typename T>
  using awaitable = asio::awaitable<T, executor_type>;
  using use_awaitable_t = asio::use_awaitable_t<executor_type>;
  static constexpr use_awaitable_t use_awaitable{};

  using writer_signature = void(std::exception_ptr, error_code);
  using writer_handler_type = typename asio::async_result<
      use_awaitable_t, writer_signature>::handler_type;
  std::optional<writer_handler_type> writer_handler;

  using pacing_clock = ceph::mono_clock;
  timer_type<pacing_clock> pacing_timer;
  // we get pacing hints with a higher resolution than our timer. accumulate
  // pacing delays until they're large enough to wait on
  static constexpr auto pacing_threshold = std::chrono::milliseconds(10);
  ceph::timespan pacing_remainder = ceph::timespan::zero();

  conn_ptr conn;
  h3_conn_ptr h3conn;
  connection_id cid;

  // streams waiting on reads/writes
  streamio_set readers;
  streamio_set writers;

  // long-lived coroutine that completes when the connection closes
  awaitable<error_code> accept();

  // arm the timeout timer with quiche_conn_timeout_as_nanos()
  void reset_timeout();
  // timeout handler
  void on_timeout(error_code ec);

  // send a batch of outgoing packets
  awaitable<error_code> flush_some();
  // flush all outgoing packets
  awaitable<error_code> flush();
  // continuous writer that wakes up to flush when writer_wait() is called
  awaitable<error_code> writer();
  // completes when writer_wake() is called
  awaitable<error_code> writer_wait();
  // wake writer() to flush data
  void writer_wake(error_code ec = {});

  // poll the connection for request headers on a new stream
  error_code poll_events(ip::udp::endpoint peer, ip::udp::endpoint self);

  // handle an incoming packet from the Listener
  error_code handle_packet(std::span<uint8_t> data, ip::udp::endpoint peer,
                           ip::udp::endpoint self);

  // add a stream to the given set and wait for its io to be ready
  auto streamio_wait(StreamIO& stream, streamio_set& blocked)
      -> awaitable<error_code>;

  // cancel blocked streams with the given error
  void streamio_reset(error_code ec);

  // receive body bytes from the given stream
  auto read_body(StreamIO& stream, std::span<uint8_t> data)
      -> awaitable<std::tuple<error_code, size_t>>;

  // write response headers to the given stream
  auto write_response(StreamIO& stream, std::span<quiche_h3_header> headers,
                      bool fin)
      -> awaitable<error_code>;

  // write response body to the given stream
  auto write_body(StreamIO& stream, std::span<uint8_t> data, bool fin)
      -> awaitable<std::tuple<error_code, size_t>>;

  // return an error code that best describes why the connection was closed
  error_code on_closed();
};

} // namespace rgw::h3
