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

#include <sys/socket.h>
#include <sys/uio.h>
#include <array>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/intrusive_ptr.hpp>
#include "address_validation.h"
#include "error.h"
#include "listener.h"
#include "message.h"
#include "ostream.h"

namespace rgw::h3 {

Listener::Listener(CephContext* cct, quiche_config* config,
                   quiche_h3_config* h3config, socket_type socket,
                   stream_handler& on_new_stream)
    : cct(cct), config(config), h3config(h3config),
      ex(socket.get_executor()), socket(std::move(socket)),
      on_new_stream(on_new_stream)
{
}

auto Listener::listen() -> awaitable<void>
{
  // read and dispatch packets until the socket closes
  ldpp_dout(this, 20) << "listening" << dendl;

  // generator for random connection ids
  std::default_random_engine rng{std::random_device{}()};

  // receive up to 16 packets at a time with recvmmsg()
  static constexpr size_t max_mmsg = 16;
  std::array<message, max_mmsg> messages;
  std::array<iovec, max_mmsg> iovs;
  std::array<mmsghdr, max_mmsg> headers;

  for (size_t i = 0; i < max_mmsg; i++) {
    auto& m = messages[i];
    iovs[i] = iovec{m.buffer.data(), m.buffer.max_size()};
    auto& h = headers[i];
    h.msg_hdr.msg_name = m.peer.data();
    h.msg_hdr.msg_namelen = m.peer.size();
    h.msg_hdr.msg_iov = &iovs[i];
    h.msg_hdr.msg_iovlen = 1;
    h.msg_hdr.msg_control = nullptr;
    h.msg_hdr.msg_controllen = 0;
    h.msg_hdr.msg_flags = 0;
  }

  error_code ec;
  while (!ec) {
    for (size_t i = 0; i < max_mmsg; i++) {
      messages[i].buffer.resize(messages[i].buffer.max_size(),
                                boost::container::default_init);
      headers[i].msg_len = 0;
    }

    const int count = ::recvmmsg(socket.native_handle(), headers.data(),
                                 headers.size(), 0, nullptr);
    if (count == -1) {
      auto ec = error_code{errno, boost::system::system_category()};

      if (ec == std::errc::operation_would_block ||
          ec == std::errc::resource_unavailable_try_again) {
        // wait until the socket is readable
        co_await socket.async_wait(ip::udp::socket::wait_read,
            asio::redirect_error(use_awaitable, ec));
        continue;
      }

      ldpp_dout(this, 1) << "recvmmsg() failed: " << ec.message() << dendl;
      break;
    }
    ldpp_dout(this, 30) << "recvmmsg() got " << count << " packets" << dendl;

    auto self = socket.local_endpoint(ec);
    if (ec) {
      break;
    }

    auto m = messages.begin();
    auto end = std::next(m, count);
    for (auto h = headers.begin(); m != end; ++m, ++h) {
      // set the message size to match the bytes received
      m->buffer.resize(h->msg_len, boost::container::default_init);

      ec = co_await on_packet(rng, &*m, self);
      if (ec) {
        break;
      }
    }
#if 0
    // handle the packets in parallel. the number of coroutines we spawn with
    // co_await here must be known at compile-time, so we pass an empty span for
    // any packets with i >= count and skip them
    using namespace asio::experimental::awaitable_operators;
    co_await (
        on_packet(rng, &messages[0], end) &&
        on_packet(rng, &messages[1], end) &&
        on_packet(rng, &messages[2], end) &&
        on_packet(rng, &messages[3], end) &&
        on_packet(rng, &messages[4], end) &&
        on_packet(rng, &messages[5], end) &&
        on_packet(rng, &messages[6], end) &&
        on_packet(rng, &messages[7], end) &&
        on_packet(rng, &messages[8], end) &&
        on_packet(rng, &messages[9], end) &&
        on_packet(rng, &messages[10], end) &&
        on_packet(rng, &messages[11], end) &&
        on_packet(rng, &messages[12], end) &&
        on_packet(rng, &messages[13], end) &&
        on_packet(rng, &messages[14], end) &&
        on_packet(rng, &messages[15], end)
      );
#endif
  }

  ldpp_dout(this, 20) << "done listening" << dendl;
  co_return;
}

auto Listener::on_packet(std::default_random_engine& rng,
                         message* packet, ip::udp::endpoint self)
  -> awaitable<error_code>
{
  auto data = std::span(packet->buffer);
  ip::udp::endpoint& peer = packet->peer;

  // parse the packet header
  uint32_t version = 0;
  uint8_t type = 0;
  auto scid = connection_id{connection_id::max_size(),
                            boost::container::default_init};
  auto dcid = connection_id{connection_id::max_size(),
                            boost::container::default_init};
  size_t scid_len = scid.max_size();
  size_t dcid_len = dcid.max_size();
  auto token = address_validation_token{address_validation_token::max_size(),
                                        boost::container::default_init};
  size_t token_len = token.max_size();

  int rc = ::quiche_header_info(
      data.data(), data.size(),
      QUICHE_MAX_CONN_ID_LEN, &version, &type,
      scid.data(), &scid_len,
      dcid.data(), &dcid_len,
      token.data(), &token_len);
  if (rc < 0) {
    auto ec = error_code{rc, quic_category()};
    ldpp_dout(this, 20) << "failed to parse packet header " << ec.message() << dendl;
    co_return error_code{}; // not fatal
  }
  scid.resize(scid_len);
  dcid.resize(dcid_len);
  token.resize(token_len);

  ldpp_dout(this, 30) << "received packet type " << PacketType{type}
      << " of " << data.size() << " bytes from " << peer
      << " with scid=" << scid << " dcid=" << dcid << " token=" << token << dendl;

  // look up connection by dcid
  boost::intrusive_ptr<Connection> connection;
  {
    connection_set::insert_commit_data commit_data;
    auto insert = connections_by_id.insert_check(dcid, commit_data);

    if (!insert.second) {
      // connection existed, take a reference while we deliver the packet
      connection = boost::intrusive_ptr<Connection>(&*insert.first);
    } else {
      // dcid not found, can we accept the connection?

      if (!::quiche_version_is_supported(version)) {
        // send a version negotiation packet
        std::array<uint8_t, 2048> outbuf;
        ssize_t bytes = ::quiche_negotiate_version(
            scid.data(), scid.size(),
            dcid.data(), dcid.size(),
            outbuf.data(), outbuf.size());
        if (bytes <= 0) {
          auto ec = error_code{static_cast<int>(bytes), quic_category()};
          ldpp_dout(this, 20) << "quiche_negotiate_version failed with "
              << ec.message() << dendl;
          co_return error_code{}; // not fatal
        }

        error_code ec;
        co_await socket.async_send_to(
            asio::buffer(outbuf.data(), bytes), peer, 0,
            asio::redirect_error(use_awaitable, ec));
        if (ec) {
          ldpp_dout(this, 20) << "send to " << peer
              << " failed with " << ec.message() << dendl;
        } else {
          ldpp_dout(this, 20) << "sent version negotitation packet of "
              << bytes << " bytes to " << peer
              << " that requested version=" << version << dendl;
        }
        co_return ec;
      }

      if (token_len == 0) {
        // stateless retry
        token_len = write_token(dcid, peer, token);
        if (!token_len) {
          ldpp_dout(this, 20) << "write_token failed" << dendl;
          co_return error_code{};
        }

        // generate a random cid
        connection_id cid;
        cid.resize(QUICHE_MAX_CONN_ID_LEN);
        std::generate(cid.begin(), cid.end(), rng);

        std::array<uint8_t, 2048> outbuf;
        ssize_t bytes = ::quiche_retry(scid.data(), scid.size(),
                                       dcid.data(), dcid.size(),
                                       cid.data(), cid.size(),
                                       token.data(), token.size(), version,
                                       outbuf.data(), outbuf.size());
        if (bytes <= 0) {
          auto ec = error_code{static_cast<int>(bytes), quic_category()};
          ldpp_dout(this, 20) << "quiche_retry failed with "
              << ec.message() << dendl;
          co_return error_code{}; // not fatal
        }

        error_code ec;
        const size_t sent = co_await socket.async_send_to(
            asio::buffer(outbuf.data(), bytes), peer, 0,
            asio::redirect_error(use_awaitable, ec));
        if (ec) {
          ldpp_dout(this, 20) << "send to " << peer
              << " failed with " << ec.message() << dendl;
        } else {
          ldpp_dout(this, 20) << "sent retry packet of " << sent
              << " bytes with token=" << token << " cid=" << cid
              << " to " << peer << dendl;
        }
        co_return ec;
      }

      // token validation
      connection_id odcid;
      const size_t odcid_len = validate_token(token, peer, odcid);
      if (odcid_len == 0) {
        ldpp_dout(this, 20) << "token validation failed" << dendl;
        co_return error_code{}; // not fatal
      }

      auto conn = conn_ptr{::quiche_accept(dcid.data(), dcid.size(),
                                           odcid.data(), odcid.size(),
                                           self.data(), self.size(),
                                           peer.data(), peer.size(),
                                           config)};
      if (!conn) {
        ldpp_dout(this, 20) << "quiche_accept failed" << dendl;
        co_return error_code{}; // not fatal
      }

      // allocate the Connection and commit its set insertion
      connection = new Connection(cct, h3config, socket, on_new_stream,
                                  std::move(conn), std::move(dcid));
      insert.first = connections_by_id.insert_commit(*connection, commit_data);

      // accept the connection for processing. once the connection closes,
      // remove it from the connection set. this completion handler holds a
      // reference to the Connection while it's in the set
      connection->async_accept(
          asio::bind_executor(get_executor(),
              [this, connection] (error_code ec) {
                if (connection->is_linked()) {
                  auto c = connections_by_id.iterator_to(*connection);
                  connections_by_id.erase(c);
                }
              }));
    }
  }

  // handle the packet under Connection's executor
  error_code ec;
  co_await connection->async_handle_packet(data, peer, self,
      asio::redirect_error(use_awaitable, ec));

  co_return error_code{};
}

void Listener::close(error_code& ec)
{
  ldpp_dout(this, 20) << "listener closing" << dendl;

  asio::dispatch(get_executor(), [this] {
      // cancel the connections and remove them from the set
      auto c = connections_by_id.begin();
      while (c != connections_by_id.end()) {
        c->cancel();
        c = connections_by_id.erase(c);
      }
    });

  // cancel listen()
  cancel_listen.emit(asio::cancellation_type::terminal);

  // close the socket
  socket.close(ec);
}


std::ostream& Listener::gen_prefix(std::ostream& out) const
{
  error_code ec;
  return out << "h3 " << socket.local_endpoint(ec) << ": ";
}

CephContext* Listener::get_cct() const
{
  return cct;
}

unsigned Listener::get_subsys() const
{
  return ceph_subsys_rgw;
}

} // namespace rgw::h3
