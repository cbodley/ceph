// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw/rgw_asio_dmclock.h"
#include <optional>
#include <gtest/gtest.h>

enum class ClientId {
  client1,
  client2
};

crimson::dmclock::ClientInfo info1{1, 1, 1};
crimson::dmclock::ClientInfo info2{1, 1, 1};

auto get_client_info = [] (ClientId client) {
  switch (client) {
    case ClientId::client1: return &info1;
    case ClientId::client2: return &info2;
  }
  std::abort();
};

struct Request {};

using Queue = rgw::dmclock::Service<ClientId, Request>;

// wrap results in optional<> so we can test whether the callback fired yet
using optional_error = std::optional<boost::system::error_code>;
using optional_result = std::optional<Queue::result_type>;

// return a lambda that can be used as a callback to capture its arguments
auto capture(optional_error& opt_ec, optional_result& opt_res)
{
  return [&] (boost::system::error_code ec, Queue::result_type& res) {
    opt_ec = ec;
    opt_res = std::move(res);
  };
}

TEST(dmClockService, AsyncRequest)
{
  boost::asio::io_context context;
  Queue queue(context, get_client_info);

  optional_error ec1, ec2;
  optional_result r1, r2;

  queue.async_request(Request{}, ClientId::client1,
                      crimson::dmclock::ReqParams{},
                      crimson::dmclock::get_time(), 0,
                      capture(ec1, r1));
  queue.async_request(Request{}, ClientId::client2,
                      crimson::dmclock::ReqParams{},
                      crimson::dmclock::get_time(), 1,
                      capture(ec2, r2));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(r1);
  EXPECT_EQ(ClientId::client1, r1->client);
  EXPECT_TRUE(r1->request);
  EXPECT_EQ(crimson::dmclock::PhaseType::reservation, r1->phase);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(r2);
  EXPECT_EQ(ClientId::client2, r2->client);
  EXPECT_TRUE(r2->request);
  EXPECT_EQ(crimson::dmclock::PhaseType::priority, r2->phase);
}

TEST(dmClockService, Cancel)
{
  boost::asio::io_context context;
  Queue queue(context, get_client_info);

  optional_error ec1, ec2;
  optional_result r1, r2;

  queue.async_request(Request{}, ClientId::client1,
                      crimson::dmclock::ReqParams{},
                      crimson::dmclock::get_time(), 1,
                      capture(ec1, r1));
  queue.async_request(Request{}, ClientId::client2,
                      crimson::dmclock::ReqParams{},
                      crimson::dmclock::get_time(), 1,
                      capture(ec2, r2));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  queue.cancel();

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);
  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec2);
}

TEST(dmClockService, CancelClient)
{
  boost::asio::io_context context;
  Queue queue(context, get_client_info);

  optional_error ec1, ec2;
  optional_result r1, r2;

  queue.async_request(Request{}, ClientId::client1,
                      crimson::dmclock::ReqParams{},
                      crimson::dmclock::get_time(), 1,
                      capture(ec1, r1));
  queue.async_request(Request{}, ClientId::client2,
                      crimson::dmclock::ReqParams{},
                      crimson::dmclock::get_time(), 1,
                      capture(ec2, r2));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  queue.cancel(ClientId::client1);

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(r2);
  EXPECT_EQ(ClientId::client2, r2->client);
  EXPECT_TRUE(r2->request);
  EXPECT_EQ(crimson::dmclock::PhaseType::priority, r2->phase);
}

TEST(dmClockService, CancelOnDestructor)
{
  boost::asio::io_context context;

  optional_error ec1, ec2;
  optional_result r1, r2;

  {
    Queue queue(context, get_client_info);

    queue.async_request(Request{}, ClientId::client1,
                        crimson::dmclock::ReqParams{},
                        crimson::dmclock::get_time(), 1,
                        capture(ec1, r1));
    queue.async_request(Request{}, ClientId::client2,
                        crimson::dmclock::ReqParams{},
                        crimson::dmclock::get_time(), 1,
                        capture(ec2, r2));
  }

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);
  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec2);
}

// return a lambda from capture() that's bound to run on the given executor
template <typename Executor>
auto capture(const Executor& ex, optional_error& err, optional_result& res)
{
  return boost::asio::bind_executor(ex, capture(err, res));
}

TEST(dmClockService, CrossExecutorRequest)
{
  boost::asio::io_context queue_context;
  Queue queue(queue_context, get_client_info);

  // create a separate execution context to use for all callbacks to test that
  // pending requests maintain executor work guards on both executors
  boost::asio::io_context callback_context;
  auto ex2 = callback_context.get_executor();

  optional_error ec1, ec2;
  optional_result r1, r2;

  queue.async_request(Request{}, ClientId::client1,
                      crimson::dmclock::ReqParams{},
                      crimson::dmclock::get_time(), 0,
                      capture(ex2, ec1, r1));
  queue.async_request(Request{}, ClientId::client2,
                      crimson::dmclock::ReqParams{},
                      crimson::dmclock::get_time(), 0,
                      capture(ex2, ec2, r2));

  callback_context.poll();
  // maintains work on callback executor while in queue
  EXPECT_FALSE(callback_context.stopped());

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  queue_context.poll();
  EXPECT_TRUE(queue_context.stopped());

  EXPECT_FALSE(ec1); // no callbacks until callback executor runs
  EXPECT_FALSE(ec2);

  callback_context.poll();
  EXPECT_TRUE(callback_context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(r1);
  EXPECT_EQ(ClientId::client1, r1->client);
  EXPECT_TRUE(r1->request);
  EXPECT_EQ(crimson::dmclock::PhaseType::reservation, r1->phase);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(r2);
  EXPECT_EQ(ClientId::client2, r2->client);
  EXPECT_TRUE(r2->request);
  EXPECT_EQ(crimson::dmclock::PhaseType::reservation, r2->phase);
}
