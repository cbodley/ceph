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

#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <span>

#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <h3/types.h>

namespace rgw::h3 {

class Observer;

/// Configuration options.
struct Options {
  std::chrono::milliseconds conn_idle_timeout{5000};
  size_t conn_max_streams_bidi = 100;
  size_t conn_max_streams_uni = 10;
  size_t conn_max_data = 10'000'000;
  size_t stream_max_data_bidi_local = 1'000'000;
  size_t stream_max_data_bidi_remote = 1'000'000;
  size_t stream_max_data_uni = 1'000'000;
  const char* ssl_certificate_path = nullptr;
  const char* ssl_private_key_path = nullptr;
  void (*log_callback)(const char*, void*) = nullptr;
  void* log_arg = nullptr;
};

/// Opaque library configuration shared between Listeners.
class Config {
 public:
  virtual ~Config() {}
};

/// Abstract listener interface.
class Listener {
 public:
  virtual ~Listener() {}

  /// Each Listener uses a strand executor to ensure thread-safe access to its
  /// state.
  using executor_type = asio::strand<default_executor>;

  /// Return the associated executor.
  virtual executor_type get_executor() const = 0;

  /// Spawn a coroutine that continuously receives packets and accepts incoming
  /// connections.
  virtual void async_listen() = 0;

  /// Close the socket and abort the coroutine from async_listen().
  virtual void close() = 0;
};

struct StreamIO;

/// Reference-counted abstract connection interface.
class Connection : public boost::intrusive_ref_counter<Connection> {
 public:
  virtual ~Connection() {}

  /// Each Connection uses a strand executor to ensure thread-safe access to its
  /// state.
  using executor_type = asio::strand<default_executor>;

  /// Return the associated executor.
  virtual executor_type get_executor() const = 0;

  /// Return the negotiated connection id.
  virtual connection_id get_cid() const = 0;

  /// Write response headers to the given stream. Errors are thrown as
  /// boost::system::system_error exceptions.
  virtual auto write_response(StreamIO& stream, const http::fields& response,
                              bool fin)
      -> asio::awaitable<void, executor_type> = 0;

  /// Write response body to the given stream. Return the number of bytes
  /// written. Errors are thrown as boost::system::system_error exceptions.
  virtual auto write_body(StreamIO& stream, std::span<uint8_t> data, bool fin)
      -> asio::awaitable<size_t, executor_type> = 0;

  /// Receive body bytes from the given stream. Return the number of bytes
  /// read. Errors are thrown as boost::system::system_error exceptions.
  virtual auto read_body(StreamIO& stream, std::span<uint8_t> data)
      -> asio::awaitable<size_t, executor_type> = 0;
};

/// A stream handle used to coordinate i/o between ClientIO and Connection.
struct StreamIO : boost::intrusive::set_base_hook<> {
  uint64_t id;
  std::span<uint8_t> data;
  bool fin = false;

  using Signature = void(std::exception_ptr, error_code);
  using Handler = typename asio::async_result<
      asio::use_awaitable_t<Connection::executor_type>,
      Signature>::handler_type;
  std::optional<Handler> handler;

  StreamIO(uint64_t id) : id(id) {}
};

/// Function signature for the callback that handles incoming request streams.
using StreamHandlerSignature = void(
    boost::intrusive_ptr<Connection> conn,
    uint64_t stream_id,
    http::fields request_headers,
    ip::udp::endpoint myaddr,
    ip::udp::endpoint peeraddr);

using StreamHandler = std::function<StreamHandlerSignature>;


/// Function pointer type for the create_h3_config() entrypoint.
using create_config_fn = std::unique_ptr<Config> (*)(const Options&);

/// Function pointer type for the create_h3_listener() entrypoint.
using create_listener_fn = std::unique_ptr<Listener> (*)(
    Config& config, Observer& observer, Listener::executor_type ex,
    StreamHandler& on_new_stream, udp_socket socket);

} // namespace rgw::h3
