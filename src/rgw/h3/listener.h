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

#include <random>
#include <span>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/strand.hpp>
#include <quiche.h>

#include "common/dout.h"

#include "h3/connection_set.h"
#include "h3/types.h"

namespace rgw::h3 {

struct message;

/// Listener reads packets from a bound udp socket, manages the set of accepted
/// connections, and delivers their incoming packets.
class Listener : public DoutPrefixProvider {
 public:
  using executor_type = asio::strand<default_executor>;

  /// Construct a listener on the given socket.
  explicit Listener(CephContext* cct, quiche_config* config,
                    quiche_h3_config* h3config, socket_type socket,
                    stream_handler& on_new_stream);

  /// Start reading packets from the socket, accepting new connections and
  /// their streams, and dispatching requests to the stream handler.
  template <typename CompletionToken>
  auto async_listen(CompletionToken&& token)
  {
    using Signature = void();
    return asio::async_initiate<CompletionToken, Signature>(
        [this] (auto&& handler) {
          // spawn listen() on the listener's executor and enable its
          // cancellation via 'cancel_listen'
          asio::co_spawn(get_executor(), listen(),
              asio::bind_cancellation_slot(cancel_listen.slot(),
                  [h=std::move(handler)] (std::exception_ptr eptr) mutable {
                    if (eptr) {
                      std::rethrow_exception(std::move(eptr));
                    } else {
                      asio::dispatch(std::move(h));
                    }
                  }));
        }, token);
  }

  executor_type get_executor() const { return ex; }

  /// Close the socket and its associated connections/streams
  void close(error_code& ec);

  // DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext* get_cct() const override;
  unsigned get_subsys() const override;

 private:
  CephContext* cct;
  quiche_config* config;
  quiche_h3_config* h3config;
  executor_type ex;
  socket_type socket;
  stream_handler& on_new_stream;

  asio::cancellation_signal cancel_listen;
  connection_set connections_by_id;

  template <typename T>
  using awaitable = asio::awaitable<T, executor_type>;
  static constexpr asio::use_awaitable_t<executor_type> use_awaitable{};

  awaitable<void> listen();
  awaitable<error_code> on_packet(std::default_random_engine& rng,
                                  message* packet, ip::udp::endpoint self);
};

} // namespace rgw::h3
