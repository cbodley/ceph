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

#include <boost/container/small_vector.hpp>
#include <quiche.h>
#include "client_io.h"

namespace rgw::h3 {

ClientIO::ClientIO(asio::io_context& context, yield_context yield,
                   Connection* conn, uint64_t stream_id, http::fields req,
                   ip::udp::endpoint local_endpoint,
                   ip::udp::endpoint remote_endpoint)
  : yield(yield), conn(conn),
    stream(stream_id), request(std::move(req)),
    local_endpoint(std::move(local_endpoint)),
    remote_endpoint(std::move(remote_endpoint))
{
}

int ClientIO::init_env(CephContext* cct)
{
  env.init(cct);

  //perfcounter->inc(l_rgw_qlen);
  //perfcounter->inc(l_rgw_qactive);

  for (const auto& header : request) {
    const auto field = header.name(); // enum type for known headers
    const auto name = header.name_string();
    const auto value = header.value();

    // h3 request pseudo-headers
    if (name == ":method") {
      env.set("REQUEST_METHOD", value.to_string());
      continue;
    }
    if (name == ":scheme") { // unused
      continue;
    }
    if (name == ":authority") {
      env.set("HTTP_HOST", value.to_string());
      continue;
    }
    if (name == ":path") {
      env.set("REQUEST_URI", value.to_string());
      // split uri from query
      auto uri = value;
      auto pos = uri.find('?');
      if (pos != uri.npos) {
        auto query = uri.substr(pos + 1);
        env.set("QUERY_STRING", query.to_string());
        uri = uri.substr(0, pos);
      }
      env.set("SCRIPT_URI", uri.to_string());
      continue;
    }

    if (field == http::field::content_length) {
      env.set("CONTENT_LENGTH", value.to_string());
      continue;
    }
    if (field == http::field::content_type) {
      env.set("CONTENT_TYPE", value.to_string());
      continue;
    }

    static const std::string_view HTTP_{"HTTP_"};
    std::string buf;
    buf.resize(name.size() + HTTP_.size());
    auto dest = std::copy(std::begin(HTTP_), std::end(HTTP_), buf.data());
    for (auto src = name.begin(); src != name.end(); ++src, ++dest) {
      if (*src == '-') {
        *dest = '_';
      } else if (*src == '_') {
        *dest = '-';
      } else {
        *dest = std::toupper(*src);
      }
    }
    env.set(std::move(buf), value.to_string());
  }

  env.set("HTTP_VERSION", "3.0");

  auto port = std::to_string(local_endpoint.port());
  env.set("SERVER_PORT", port);
  env.set("SERVER_PORT_SECURE", std::move(port));
  env.set("REMOTE_ADDR", remote_endpoint.address().to_string());
  return 0;
}


size_t ClientIO::send_status(int status, const char* status_name)
{
  // add ":status" pseudo-header
  static constexpr std::string_view name = ":status";
  std::string value = std::to_string(status);
  response.insert({name.data(), name.size()},
                  {value.data(), value.size()});
  return 0;
}

size_t ClientIO::send_100_continue()
{
  static constexpr std::string_view name = ":status";
  static constexpr std::string_view value = "100";
  auto header = quiche_h3_header{
    reinterpret_cast<const uint8_t*>(name.data()), name.size(),
    reinterpret_cast<const uint8_t*>(value.data()), value.size()
  };
  auto headers = std::span{&header, 1};

  static constexpr bool fin = false;
  conn->async_write_response(stream, headers, fin, yield); // throw on error
  return 0;
}

size_t ClientIO::send_header(const std::string_view& name,
                             const std::string_view& value)
{
  response.insert({name.data(), name.size()},
                  {value.data(), value.size()});
  return 0;
}

size_t ClientIO::send_content_length(uint64_t len)
{
  boost::beast::string_view name = to_string(http::field::content_length);
  std::string value = std::to_string(len);
  response.insert(name, value);
  return 0;
}

size_t ClientIO::complete_header()
{
  static constexpr size_t static_count = 32;
  using vector_type = boost::container::small_vector<
      quiche_h3_header, static_count>;
  vector_type headers;

  for (const auto& f : response) {
    auto& h = headers.emplace_back();
    const auto name = f.name_string();
    h.name = reinterpret_cast<const uint8_t*>(name.data());
    h.name_len = name.size();
    const auto value = f.value();
    h.value = reinterpret_cast<const uint8_t*>(value.data());
    h.value_len = value.size();
  }

  static constexpr bool fin = false;
  conn->async_write_response(stream, headers, fin, yield); // throw on error
  return 0;
}

size_t ClientIO::recv_body(char* buf, size_t len)
{
  auto data = std::span{reinterpret_cast<uint8_t*>(buf), len};
  return conn->async_read_body(stream, data, yield); // throw on error
}

size_t ClientIO::send_body(const char* buf, size_t len)
{
  char* tmp = const_cast<char*>(buf); // for quiche_h3_send_body(uint8_t *body)
  auto data = std::span{reinterpret_cast<uint8_t*>(tmp), len};
  static constexpr bool fin = false;
  return conn->async_write_body(stream, data, fin, yield); // throw on error
}

size_t ClientIO::complete_request()
{
  static constexpr std::span<uint8_t> empty{};
  static constexpr bool fin = true;
  return conn->async_write_body(stream, empty, fin, yield); // throw on error
}

} // namespace rgw::h3
