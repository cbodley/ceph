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

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/beast/http/fields.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/system/error_code.hpp>

namespace rgw::h3 {

// aliases
namespace asio = boost::asio;
using boost::system::error_code;
namespace ip = asio::ip;
namespace http = boost::beast::http;


/// An opaque array of up to QUICHE_MAX_CONN_ID_LEN=20 bytes.
using connection_id = boost::container::static_vector<uint8_t, 20>;

/// Token generated for the purpose of address validation.
using address_validation_token = boost::container::static_vector<uint8_t, 128>;

/// Use explicit executor types to avoid the polymorphic any_io_executor.
/// The frontend's default executor uses the io_context directly.
using default_executor = asio::io_context::executor_type;

/// UDP socket on the default executor.
using udp_socket = asio::basic_datagram_socket<ip::udp, default_executor>;

} // namespace rgw::h3
