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

#include <boost/asio/experimental/append.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include "connection.h"
#include "error.h"
#include "message.h"
#include "ostream.h"

namespace rgw::h3 {

Connection::Connection(CephContext* cct, quiche_h3_config* h3config,
                       socket_type& socket, stream_handler& on_new_stream,
                       conn_ptr _conn, connection_id cid)
    : cct(cct), h3config(h3config), ex(socket.get_executor()),
      socket(socket), on_new_stream(on_new_stream),
      timeout_timer(socket.get_executor()),
      pacing_timer(socket.get_executor()),
      conn(std::move(_conn)), cid(std::move(cid))
{
}

Connection::~Connection()
{
  ldpp_dout(this, 20) << "~Connection" << dendl;
}

auto Connection::accept() -> awaitable<error_code>
{
  ldpp_dout(this, 20) << "accepted" << dendl;

  reset_timeout(); // schedule the initial timeout

  auto ec = co_await writer();
  ldpp_dout(this, 20) << "connection exiting with " << ec.message() << dendl;
  co_return ec;
}

void Connection::reset_timeout()
{
  auto ns = std::chrono::nanoseconds{::quiche_conn_timeout_as_nanos(conn.get())};
  ldpp_dout(this, 30) << "timeout scheduled in " << ns << dendl;
  timeout_timer.expires_after(ns);
  if (ns.count()) {
    timeout_timer.async_wait([this] (error_code ec) { on_timeout(ec); });
  }
}

void Connection::on_timeout(error_code ec)
{
  if (ec) {
    return;
  }
  ldpp_dout(this, 30) << "timeout handler" << dendl;

  ::quiche_conn_on_timeout(conn.get());

  if (::quiche_conn_is_closed(conn.get())) {
    ec = on_closed();
    ldpp_dout(this, 20) << "timer detected close, returning "
        << ec.message() << dendl;
  }

  writer_wake(ec);
}

auto Connection::flush_some() -> awaitable<error_code>
{
  // send up to 8 packets at a time with sendmmsg()
  static constexpr size_t max_mmsg = 8;
  std::array<message, max_mmsg> messages;
  std::array<iovec, max_mmsg> iovs;
  std::array<mmsghdr, max_mmsg> headers;
  std::array<quiche_send_info, max_mmsg> sendinfos;

  error_code ec;
  size_t count = 0;
  for (; count < max_mmsg; count++) {
    auto& m = messages[count];
    auto& sendinfo = sendinfos[count];

    // serialize the next packet for sending
    const ssize_t bytes = ::quiche_conn_send(
        conn.get(), m.buffer.data(), m.buffer.max_size(), &sendinfo);

    if (bytes < 0) {
      ec.assign(bytes, quic_category());
      if (ec == quic_errc::done) {
        if (count == 0) {
          co_return ec;
        }
        break; // send the packets we've already prepared
      }
      ldpp_dout(this, 20) << "quiche_conn_send() failed: "
          << ec.message() << dendl;
      co_return ec;
    }

    auto& iov = iovs[count];
    iov = iovec{m.buffer.data(), static_cast<size_t>(bytes)};

    auto& h = headers[count];
    h.msg_hdr.msg_name = &sendinfo.to;
    h.msg_hdr.msg_namelen = sendinfo.to_len;
    h.msg_hdr.msg_iov = &iov;
    h.msg_hdr.msg_iovlen = 1;
    h.msg_hdr.msg_control = nullptr;
    h.msg_hdr.msg_controllen = 0;
    h.msg_hdr.msg_flags = 0;
    h.msg_len = 0;
  }

  // quiche's pacing hints have undefined behavior and can return tv_nsec
  // outside the specified range, leading to huge pacing delays:
  // https://github.com/cloudflare/quiche/pull/1403
#if 0
  // apply the pacing delay from the last packet
  const auto& last = sendinfos[count-1];
  const auto send_at = pacing_clock::time_point{
      std::chrono::seconds(last.at.tv_sec) +
      std::chrono::nanoseconds(last.at.tv_nsec)};

  if (send_at > pacing_clock::zero()) {
    const auto now = pacing_clock::now();
    if (send_at > now) {
      const auto delay = send_at - now;
      pacing_remainder += delay;
      if (pacing_remainder >= pacing_threshold) {
        ldpp_dout(this, 20) << "pacing delay " << pacing_remainder << dendl;

        pacing_timer.expires_after(pacing_remainder);
        co_await pacing_timer.async_wait(
            asio::redirect_error(use_awaitable, ec));
        if (ec) {
          ldpp_dout(this, 20) << "pacing timer failed with "
              << ec.message()<< dendl;
          co_return ec;
        }
        pacing_remainder = ceph::timespan::zero();
      }
    }
  }
#endif
  int sent = ::sendmmsg(socket.native_handle(), headers.data(), count, 0);
  if (sent == -1) {
    ec.assign(errno, boost::system::system_category());
    ldpp_dout(this, 1) << "sendmmsg() failed: " << ec.message() << dendl;
  } else {
    ldpp_dout(this, 30) << "sendmmsg() sent " << sent << " packets" << dendl;
  }
  co_return ec;
}

auto Connection::flush() -> awaitable<error_code>
{
  error_code ec;
  while (!ec) {
    ec = co_await flush_some();
  }
  co_return ec;
}

auto Connection::writer() -> awaitable<error_code>
{
  for (;;) {
    error_code ec = co_await writer_wait();
    if (ec) {
      co_return ec;
    }

    ec = co_await flush();
    if (ec != quic_errc::done) {
      ldpp_dout(this, 20) << "flush failed: " << ec.message() << dendl;
      co_return ec;
    }

    if (::quiche_conn_is_closed(conn.get())) {
      ec = on_closed();
      ldpp_dout(this, 20) << "writer detected close, returning "
          << ec.message() << dendl;
      co_return ec;
    }
    reset_timeout(); // reschedule the connection timeout
  }
  // unreachable
}

auto Connection::writer_wait() -> awaitable<error_code>
{
  ceph_assert(!writer_handler); // one waiter at a time

  use_awaitable_t token;
  return asio::async_initiate<use_awaitable_t, writer_signature>(
      [this] (writer_handler_type&& h) {
        writer_handler.emplace(std::move(h));
      }, token);
}

void Connection::writer_wake(error_code ec)
{
  if (!writer_handler) {
    return;
  }
  auto c = asio::experimental::append(std::move(*writer_handler), nullptr, ec);
  writer_handler.reset();

  asio::post(std::move(c));
}

struct h3event_deleter {
  void operator()(quiche_h3_event* ev) { ::quiche_h3_event_free(ev); }
};
using h3event_ptr = std::unique_ptr<quiche_h3_event, h3event_deleter>;


int header_cb(uint8_t *name, size_t name_len,
              uint8_t *value, size_t value_len,
              void *argp)
{
  auto& headers = *reinterpret_cast<http::fields*>(argp);
  headers.insert({reinterpret_cast<char*>(name), name_len},
                 {reinterpret_cast<char*>(value), value_len});
  return 0;
}

error_code Connection::poll_events(ip::udp::endpoint peer,
                                   ip::udp::endpoint self)
{
  for (;;) {
    quiche_h3_event* pevent = nullptr;
    int64_t stream_id = ::quiche_h3_conn_poll(h3conn.get(), conn.get(), &pevent);
    if (stream_id < 0) {
      break;
    }
    auto ev = h3event_ptr{pevent};
    const auto type = ::quiche_h3_event_type(ev.get());
    if (type == QUICHE_H3_EVENT_HEADERS) {
      // read request headers
      http::fields headers;
      int r = ::quiche_h3_event_for_each_header(ev.get(), header_cb, &headers);
      if (r < 0) {
        auto ec = error_code{r, h3_category()};
        ldpp_dout(this, 20) << "stream=" << stream_id
            << " poll_events() failed with " << ec.message() << dendl;
        return ec;
      }
      ldpp_dout(this, 20) << "stream=" << stream_id << " got headers" << dendl;

      on_new_stream(boost::intrusive_ptr{this}, stream_id, std::move(headers),
                    std::move(self), std::move(peer));
    } else if (type == QUICHE_H3_EVENT_DATA) {
      if (auto r = readers.find(stream_id); r != readers.end()) {
        auto ec = r->read_some(this, h3conn.get(), conn.get());
        if (ec == h3_errc::done) {
          // wait for the next event
        } else if (ec) {
          return ec;
        } else if (r->data.empty()) {
          // wake the reader
          auto& reader = *r;
          readers.erase(r);
          reader.wake(ec);
        }
      }
    }
    // TODO: do we care about any other h3 events?
  }
  return {};
}

error_code Connection::handle_packet(std::span<uint8_t> data,
                                     ip::udp::endpoint peer,
                                     ip::udp::endpoint self)
{
  error_code ec;

  const auto recvinfo = quiche_recv_info{
    peer.data(), static_cast<socklen_t>(peer.size()),
    self.data(), static_cast<socklen_t>(self.size())
  };

  const ssize_t bytes = ::quiche_conn_recv(
      conn.get(), data.data(), data.size(), &recvinfo);
  if (bytes < 0) {
    ec.assign(bytes, quic_category());
    ldpp_dout(this, 20) << "quiche_conn_recv() failed: "
        << ec.message() << dendl;
    return ec;
  }

  if (::quiche_conn_is_established(conn.get())) {
    if (!h3conn) {
      // create the quiche_h3_conn() on first use
      h3conn.reset(::quiche_h3_conn_new_with_transport(conn.get(), h3config));
    }

    ec = poll_events(std::move(peer), std::move(self));
    if (ec) {
      return ec;
    }

    // try to wake writers
    auto w = writers.begin();
    while (w != writers.end()) {
      // try to write some more
      auto ec = w->write_some(this, h3conn.get(), conn.get(), w->fin);
      if (ec == h3_errc::done) {
        ++w; // wait for more packets
      } else if (ec) {
        return ec;
      } else if (w->data.empty()) {
        auto& writer = *w;
        w = writers.erase(w);
        writer.wake(ec);
      } else {
        ++w; // wait for more packets
      }
    }
  }

  writer_wake();

  return error_code{};
}

auto Connection::streamio_wait(StreamIO& stream, streamio_set& blocked)
    -> awaitable<error_code>
{
  blocked.push_back(stream);
  writer_wake();

  return stream.async_wait();
}

void Connection::streamio_reset(error_code ec)
{
  // cancel any readers/writers
  auto r = readers.begin();
  while (r != readers.end()) {
    auto& reader = *r;
    r = readers.erase(r);
    reader.wake(ec);
  }
  auto w = writers.begin();
  while (w != writers.end()) {
    auto& writer = *w;
    w = writers.erase(w);
    writer.wake(ec);
  }
}

error_code StreamIO::read_some(const DoutPrefixProvider* dpp,
                               quiche_h3_conn* h3conn, quiche_conn* conn)
{
  error_code ec;
  const ssize_t bytes = ::quiche_h3_recv_body(
        h3conn, conn, id, data.data(), data.size());
  if (bytes < 0) {
    ec.assign(static_cast<int>(bytes), h3_category());
  } else {
    data = data.subspan(bytes);
    ldpp_dout(dpp, 30) << "stream " << id << " read " << bytes << " bytes, "
        << data.size() << " remain" << dendl;
  }
  return ec;
}

error_code StreamIO::write_some(const DoutPrefixProvider* dpp,
                                quiche_h3_conn* h3conn, quiche_conn* conn,
                                bool fin)
{
  error_code ec;
  const ssize_t bytes = ::quiche_h3_send_body(
      h3conn, conn, id, data.data(), data.size(), fin);
  if (bytes < 0) {
    ec.assign(static_cast<int>(bytes), h3_category());
  } else {
    data = data.subspan(bytes);
    ldpp_dout(dpp, 30) << "stream " << id << " wrote " << bytes << " bytes, "
        << data.size() << " remain" << dendl;
  }
  return ec;
}

auto StreamIO::async_wait()
    -> awaitable<error_code>
{
  ceph_assert(!wait_handler); // one waiter at a time

  use_awaitable_t token;
  return asio::async_initiate<use_awaitable_t, wait_signature>(
      [this] (wait_handler_type&& h) {
        wait_handler.emplace(std::move(h));
      }, token);
}

void StreamIO::wake(error_code ec)
{
  ceph_assert(wait_handler);

  // bind arguments to the handler for dispatch
  auto c = asio::experimental::append(std::move(*wait_handler), nullptr, ec);
  wait_handler.reset();

  asio::post(std::move(c));
}

auto Connection::read_body(StreamIO& stream, std::span<uint8_t> data)
    -> awaitable<std::tuple<error_code, size_t>>
{
  stream.data = data;

  while (!stream.data.empty()) {
    auto ec = stream.read_some(this, h3conn.get(), conn.get());
    if (ec == h3_errc::done) {
      ldpp_dout(this, 30) << "stream " << stream.id << " read_body "
          "waiting to read " << stream.data.size() << " more bytes" << dendl;

      ec = co_await streamio_wait(stream, readers);
      if (ec) {
        ldpp_dout(this, 20) << "stream " << stream.id
            << " read_body canceled: " << ec.message() << dendl;
        co_return std::make_tuple(ec, 0);
      }
    } else if (ec) {
      ldpp_dout(this, 20) << "stream " << stream.id
          << " quiche_h3_recv_body() failed with " << ec.message() << dendl;
      co_return std::make_tuple(ec, 0);
    }
  }

  ldpp_dout(this, 30) << "stream " << stream.id
      << " read_body read " << data.size() << " bytes" << dendl;
  co_return std::make_tuple(error_code{}, data.size() - stream.data.size());
}

auto Connection::write_response(StreamIO& stream,
                                std::span<quiche_h3_header> headers, bool fin)
    -> awaitable<error_code>
{
  for (;;) { // retry on h3_errc::done
    const int result = ::quiche_h3_send_response(
        h3conn.get(), conn.get(), stream.id,
        headers.data(), headers.size(), fin);

    if (result < 0) {
      auto ec = error_code{result, h3_category()};
      ldpp_dout(this, 20) << "quiche_h3_send_response() failed with "
          << ec.message() << dendl;
      if (ec == h3_errc::done) {
        ldpp_dout(this, 30) << "stream " << stream.id
            << " write_response waiting" << dendl;

        ec = co_await streamio_wait(stream, writers);
        if (ec) {
          ldpp_dout(this, 20) << "stream " << stream.id
              << " write_response canceled: " << ec.message() << dendl;
          co_return ec;
        }

        ldpp_dout(this, 30) << "stream " << stream.id
            << " write_response retrying" << dendl;
        continue;
      }
      co_return ec;
    }

    ldpp_dout(this, 30) << "stream " << stream.id
        << " write_response done" << dendl;
    writer_wake();
    co_return error_code{};
  }
  // unreachable
}

auto Connection::write_body(StreamIO& stream, std::span<uint8_t> data, bool fin)
    -> awaitable<std::tuple<error_code, size_t>>
{
  stream.data = data;
  stream.fin = fin;

  // if fin is set, call quiche_h3_send_body() even if there's no data
  bool fin_flag = fin;

  while (!stream.data.empty() || fin_flag) {
    auto ec = stream.write_some(this, h3conn.get(), conn.get(), fin);
    if (ec == h3_errc::done) {
      ldpp_dout(this, 30) << "stream " << stream.id << " write_body "
          "waiting to write " << stream.data.size() << " more bytes" << dendl;

      ec = co_await streamio_wait(stream, writers);
      if (ec) {
        ldpp_dout(this, 20) << "stream " << stream.id
            << " write_body canceled: " << ec.message() << dendl;
        co_return std::make_tuple(ec, 0);
      }
    } else if (ec) {
      ldpp_dout(this, 20) << "stream " << stream.id
          << " quiche_h3_send_body() failed with " << ec.message() << dendl;
      co_return std::make_tuple(ec, 0);
    } else {
      fin_flag = false;
    }
  }

  ldpp_dout(this, 30) << "stream " << stream.id
      << " write_body wrote " << data.size() << " bytes" << dendl;
  writer_wake();
  co_return std::make_tuple(error_code{}, data.size() - stream.data.size());
}

error_code Connection::on_closed()
{
  bool is_app;
  uint64_t code;
  const uint8_t* reason_buf;
  size_t reason_len;
  if (::quiche_conn_peer_error(conn.get(), &is_app, &code,
                               &reason_buf, &reason_len)) {
    auto reason = std::string_view{
      reinterpret_cast<const char*>(reason_buf), reason_len};
    // TODO: interpret code
    ldpp_dout(this, 20) << "peer closed the connection with code " << code
        << " reason: " << reason << dendl;
    return make_error_code(std::errc::connection_reset);
  }
  if (::quiche_conn_local_error(conn.get(), &is_app, &code,
                                &reason_buf, &reason_len)) {
    auto reason = std::string_view{
      reinterpret_cast<const char*>(reason_buf), reason_len};
    ldpp_dout(this, 20) << "connection closed with code " << code
        << " reason: " << reason << dendl;
    return make_error_code(std::errc::connection_reset);
  }
  if (::quiche_conn_is_timed_out(conn.get())) {
    ldpp_dout(this, 20) << "connection timed out" << dendl;
    return make_error_code(std::errc::timed_out);
  }
  ldpp_dout(this, 20) << "connection closed" << dendl;
  return error_code{};
}

void Connection::cancel()
{
  auto self = boost::intrusive_ptr{this};
  asio::dispatch(get_executor(), [this, self=std::move(self)] {
      // cancel accept() so it returns control to Listener for removal from its
      // connection set
      cancel_accept.emit(asio::cancellation_type::terminal);

      // cancel any pending stream io to unblock process_request()
      const auto ec = make_error_code(asio::error::operation_aborted);
      streamio_reset(ec);

      // cancel timers
      timeout_timer.cancel();
      writer_wake(ec);
      pacing_timer.cancel();
    });
}


std::ostream& Connection::gen_prefix(std::ostream& out) const
{
  error_code ec;
  return out << "h3 " << socket.local_endpoint(ec) << " conn " << cid << ": ";
}

CephContext* Connection::get_cct() const
{
  return cct;
}

unsigned Connection::get_subsys() const
{
  return ceph_subsys_rgw;
}

} // namespace rgw::h3
