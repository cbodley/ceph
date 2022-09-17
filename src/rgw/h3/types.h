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

#include <functional>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/beast/http/fields.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/system/error_code.hpp>

struct quiche_config;
struct quiche_h3_config;

namespace rgw::h3 {

namespace asio = boost::asio;
using boost::system::error_code;
namespace ip = asio::ip;
namespace http = boost::beast::http;

struct config_deleter { void operator()(quiche_config* config); };
using config_ptr = std::unique_ptr<quiche_config, config_deleter>;

struct h3_config_deleter { void operator()(quiche_h3_config* h3); };
using h3_config_ptr = std::unique_ptr<quiche_h3_config, h3_config_deleter>;


class Connection;

/// Use explicit executor types to avoid the polymorphic any_io_executor.
/// The frontend's default executor uses the io_context directly.
using default_executor = asio::io_context::executor_type;

using socket_type = asio::basic_datagram_socket<ip::udp, default_executor>;

template <typename Clock>
using timer_type = asio::basic_waitable_timer<
    Clock, asio::wait_traits<Clock>, default_executor>;

/// An opaque array of up to QUICHE_MAX_CONN_ID_LEN=20 bytes.
using connection_id = boost::container::static_vector<uint8_t, 20>;

/// Token generated for the purpose of address validation.
using address_validation_token = boost::container::static_vector<uint8_t, 128>;


/// Function signature for the callback that handles incoming request streams.
using StreamHandlerSignature = void(
    boost::intrusive_ptr<Connection> conn,
    uint64_t stream_id,
    http::fields request_headers,
    ip::udp::endpoint myaddr,
    ip::udp::endpoint peeraddr);

using stream_handler = std::function<StreamHandlerSignature>;

} // namespace rgw::h3
