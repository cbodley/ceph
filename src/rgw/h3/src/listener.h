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

#include <boost/asio/detached.hpp>
#include <quiche.h>

#include <h3/h3.h>
#include "connection_set.h"

namespace rgw::h3 {

struct message;

/// Listener reads packets from a bound udp socket, manages the set of accepted
/// connections, and delivers their incoming packets.
class ListenerImpl : public Listener {
 public:
  /// Construct a listener on the given socket.
  explicit ListenerImpl(Observer& observer, executor_type ex, Config& cfg,
                        udp_socket socket, StreamHandler& on_new_stream);

  executor_type get_executor() const override { return ex; }

  /// Start reading packets from the socket, accepting new connections and
  /// their streams, and dispatching requests to the stream handler.
  void async_listen() override
  {
    // spawn listen() on the listener's executor and enable its
    // cancellation via 'cancel_listen'
    asio::co_spawn(get_executor(), listen(),
        asio::bind_cancellation_slot(cancel_listen.slot(), asio::detached));
  }

  /// Close the socket and its associated connections/streams
  void close() override;

 private:
  Observer& observer;
  quiche_config* config;
  quiche_h3_config* h3config;
  executor_type ex;
  udp_socket socket;
  StreamHandler& on_new_stream;

  asio::cancellation_signal cancel_listen;
  connection_set connections_by_id;

  using use_awaitable_t = asio::use_awaitable_t<executor_type>;
  static constexpr use_awaitable_t use_awaitable{};

  auto listen()
      -> asio::awaitable<void, executor_type>;
  auto on_packet(std::default_random_engine& rng,
                 message* packet, ip::udp::endpoint self)
      -> asio::awaitable<error_code, executor_type>;
};

} // namespace rgw::h3
