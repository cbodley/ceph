// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw/http/client.h"
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#include <boost/asio/spawn.hpp>

#include <gtest/gtest.h>

namespace rgw::http {

using namespace std::chrono_literals;

using Clock = ceph::mono_clock;
using Timer = boost::asio::basic_waitable_timer<Clock>;

// an AsyncReadStream/AsyncWriteStream that does nothing until cancelation
class NullAsyncStream {
  boost::asio::io_context& context;
  using Signature = void(boost::system::error_code, size_t);
  using Completion = ceph::async::Completion<Signature>;
  std::unique_ptr<Completion> completion;
 public:
  NullAsyncStream(boost::asio::io_context& context) : context(context) {}

  auto get_executor() { return context.get_executor(); }

  using lowest_layer_type = NullAsyncStream;
  lowest_layer_type& lowest_layer() { return *this; }

  template <typename MutableBufferSequence, typename CompletionToken>
  auto async_read_some(const MutableBufferSequence& buffers,
                       CompletionToken&& token) {
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    ceph_assert(!completion);
    completion = Completion::create(context.get_executor(),
                                    std::move(init.completion_handler));
    return init.result.get();
  }
  template <typename ConstBufferSequence, typename CompletionToken>
  auto async_write_some(const ConstBufferSequence& buffers,
                        CompletionToken&& token) {
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    ceph_assert(!completion);
    completion = Completion::create(context.get_executor(),
                                    std::move(init.completion_handler));
    return init.result.get();
  }
  void cancel() {
    boost::system::error_code ec = boost::asio::error::operation_aborted;
    if (completion) {
      ceph::async::dispatch(std::move(completion), ec, 0);
    }
  }
};

auto capture(std::optional<boost::system::error_code>& ec) {
  return [&] (boost::system::error_code e, size_t b) { ec = e; };
}

TEST(TimedStream, async_read_timeout)
{
  boost::asio::io_context context;
  TimedStream stream(NullAsyncStream{context}, Timer{context}, 1ms);

  std::array<char, 4> buffer;
  std::optional<boost::system::error_code> ec;
  stream.async_read_some(boost::asio::buffer(buffer), capture(ec));

  EXPECT_NE(0u, context.run_for(10ms));
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec);
}

TEST(TimedStream, async_write_timeout)
{
  boost::asio::io_context context;
  TimedStream stream(NullAsyncStream{context}, Timer{context}, 1ms);

  std::vector<char> buffer(16, 'a');
  std::optional<boost::system::error_code> ec;
  stream.async_write_some(boost::asio::buffer(buffer), capture(ec));

  EXPECT_NE(0u, context.run_for(10ms));
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec);
}

using TcpPool = ConnectionPool<tcp::socket, Clock>;

auto capture(std::optional<boost::system::error_code>& ec,
             std::optional<TcpPool::Connection>& conn) {
  return [&] (boost::system::error_code e,
              std::optional<TcpPool::Connection> c) {
    ec = e;
    conn = std::move(c);
  };
}

TEST(ConnectionPool, get_resolve_error)
{
  boost::asio::io_context context;
  TcpPool connections(context, "!@#$%^&*", "", 20, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec;
  std::optional<TcpPool::Connection> conn;
  connections.async_get(capture(ec, conn));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec);
  EXPECT_EQ(boost::asio::error::host_not_found, *ec);
  EXPECT_FALSE(conn);
}

TEST(ConnectionPool, get_resolve_close)
{
  boost::asio::io_context context;
  TcpPool connections(context, "localhost", "", 20, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec;
  std::optional<TcpPool::Connection> conn;
  connections.async_get(capture(ec, conn));

  connections.close();

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec);
  EXPECT_FALSE(conn);
}

TEST(ConnectionPool, get_connect_error)
{
  boost::asio::io_context context;
  TcpPool connections(context, "localhost", "", 20, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec;
  std::optional<TcpPool::Connection> conn;
  connections.async_get(capture(ec, conn));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec);
  EXPECT_EQ(boost::asio::error::connection_refused, *ec);
  EXPECT_FALSE(conn);
}

TEST(ConnectionPool, get_connect_timeout)
{
  boost::asio::io_context context;
  TcpPool connections(context, "10.0.0.0", "", 20, 0, 1ms, 0s);

  std::optional<boost::system::error_code> ec;
  std::optional<TcpPool::Connection> conn;
  connections.async_get(capture(ec, conn));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec);
  EXPECT_EQ(boost::asio::error::operation_aborted, ec);
  EXPECT_FALSE(conn);
}

TEST(ConnectionPool, get_connect_close)
{
  boost::asio::io_context context;
  TcpPool connections(context, "10.0.0.0", "", 20, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec;
  std::optional<TcpPool::Connection> conn;
  connections.async_get(capture(ec, conn));

  EXPECT_NE(0u, context.run_for(1ms));
  EXPECT_FALSE(context.stopped());

  ASSERT_FALSE(ec);
  connections.close();

  EXPECT_NE(0u, context.poll());
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec);
  EXPECT_EQ(boost::asio::error::operation_aborted, ec);
  EXPECT_FALSE(conn);
}

tcp::acceptor start_listener(boost::asio::io_context& context)
{
  tcp::acceptor acceptor(context);
  tcp::endpoint endpoint(tcp::v4(), 0);
  acceptor.open(endpoint.protocol());
  acceptor.bind(endpoint);
  acceptor.listen();
  return acceptor;
}

TEST(ConnectionPool, close_over_max)
{
  boost::asio::io_context context;
  auto acceptor = start_listener(context);
  const auto port = acceptor.local_endpoint().port();

  constexpr uint32_t max_connections = 1;
  TcpPool connections(context, "localhost", std::to_string(port),
                      max_connections, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec1;
  std::optional<TcpPool::Connection> conn1;
  connections.async_get(capture(ec1, conn1));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());
  context.restart();

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(conn1);

  std::optional<boost::system::error_code> ec2;
  std::optional<TcpPool::Connection> conn2;
  connections.async_get(capture(ec2, conn2));

  EXPECT_EQ(0u, context.poll());
  EXPECT_FALSE(context.stopped());

  connections.close();

  EXPECT_NE(0u, context.poll());
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec2);
  ASSERT_FALSE(conn2);
}

TEST(ConnectionPool, put_get_idle)
{
  boost::asio::io_context context;
  auto acceptor = start_listener(context);
  const auto port = acceptor.local_endpoint().port();

  TcpPool connections(context, "localhost", std::to_string(port),
                      20, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec1;
  std::optional<TcpPool::Connection> conn1;
  connections.async_get(capture(ec1, conn1));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());
  context.restart();

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(conn1);

  conn1.reset();

  acceptor.close(); // so a reconnect would fail

  std::optional<boost::system::error_code> ec2;
  std::optional<TcpPool::Connection> conn2;
  connections.async_get(capture(ec2, conn2));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(conn2);
}

TEST(ConnectionPool, put_get_reconnect)
{
  boost::asio::io_context context;
  auto acceptor = start_listener(context);
  const auto port = acceptor.local_endpoint().port();

  TcpPool connections(context, "localhost", std::to_string(port),
                      20, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec1;
  std::optional<TcpPool::Connection> conn1;
  connections.async_get(capture(ec1, conn1));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());
  context.restart();

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(conn1);

  {
    // accept the connection and close it
    tcp::socket socket(context);
    acceptor.accept(socket);
    socket.close();

    // try to read and get an error
    std::array<char, 4> buffer;
    std::optional<boost::system::error_code> ec;
    conn1->async_read_some(boost::asio::buffer(buffer), capture(ec));

    EXPECT_NE(0u, context.run_for(100ms));
    EXPECT_TRUE(context.stopped());
    context.restart();

    ASSERT_TRUE(ec);
    EXPECT_EQ(boost::asio::error::eof, *ec);
  }

  acceptor.close(); // so the reconnect fails with connection_refused

  std::optional<boost::system::error_code> ec2;
  std::optional<TcpPool::Connection> conn2;
  connections.async_get(capture(ec2, conn2));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::asio::error::connection_refused, *ec2);
  ASSERT_FALSE(conn2);
}

TEST(ConnectionPool, put_over_max_reuse)
{
  boost::asio::io_context context;
  auto acceptor = start_listener(context);
  const auto port = acceptor.local_endpoint().port();

  constexpr uint32_t max_connections = 1;
  TcpPool connections(context, "localhost", std::to_string(port),
                      max_connections, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec1;
  std::optional<TcpPool::Connection> conn1;
  connections.async_get(capture(ec1, conn1));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());
  context.restart();

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(conn1);

  acceptor.close(); // so a reconnect would fail

  std::optional<boost::system::error_code> ec2;
  std::optional<TcpPool::Connection> conn2;
  connections.async_get(capture(ec2, conn2));

  EXPECT_NE(0u, context.run_for(1ms));
  EXPECT_FALSE(context.stopped());

  conn1.reset();

  EXPECT_NE(0u, context.poll());
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(conn2);
}

TEST(ConnectionPool, put_over_max_reconnect)
{
  boost::asio::io_context context;
  auto acceptor = start_listener(context);
  const auto port = acceptor.local_endpoint().port();

  constexpr uint32_t max_connections = 1;
  TcpPool connections(context, "localhost", std::to_string(port),
                      max_connections, 0, 0s, 0s);

  std::optional<boost::system::error_code> ec1;
  std::optional<TcpPool::Connection> conn1;
  connections.async_get(capture(ec1, conn1));

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());
  context.restart();

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(conn1);

  std::optional<boost::system::error_code> ec2;
  std::optional<TcpPool::Connection> conn2;
  connections.async_get(capture(ec2, conn2));

  EXPECT_EQ(0u, context.run_for(1ms));
  EXPECT_FALSE(context.stopped());

  {
    // accept the connection and close it
    tcp::socket socket(context);
    acceptor.accept(socket);
    socket.close();
  }
  acceptor.close(); // so the reconnect fails with connection_refused
  {
    // try to read and get an error
    std::array<char, 4> buffer;
    std::optional<boost::system::error_code> ec;
    conn1->async_read_some(boost::asio::buffer(buffer), capture(ec));

    EXPECT_NE(0u, context.run_for(100ms));
    EXPECT_TRUE(context.stopped());
    context.restart();

    ASSERT_TRUE(ec);
    EXPECT_EQ(boost::asio::error::eof, *ec);
  }

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::asio::error::connection_refused, *ec2);
  ASSERT_FALSE(conn2);
}

#ifdef HAVE_BOOST_CONTEXT
TEST(ConnectionPool, get_spawn)
{
  boost::asio::io_context context;
  auto acceptor = start_listener(context);
  const auto port = acceptor.local_endpoint().port();

  TcpPool connections(context, "localhost", std::to_string(port),
                      20, 0, 0s, 0s);

  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
      boost::system::error_code ec;
      auto conn = connections.async_get(yield[ec]);
      ASSERT_EQ(boost::system::errc::success, ec);
      ASSERT_TRUE(conn);
    });

  EXPECT_NE(0u, context.run_for(100ms));
  EXPECT_TRUE(context.stopped());
}
#endif // HAVE_BOOST_CONTEXT

} // namespace rgw::http
