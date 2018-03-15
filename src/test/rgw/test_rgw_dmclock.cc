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

//#define BOOST_ASIO_ENABLE_HANDLER_TRACKING

#include "rgw/rgw_dmclock.h"
#include <optional>
#include <boost/asio/spawn.hpp>
#include <gtest/gtest.h>
#include "acconfig.h"

namespace rgw::dmclock {

enum {
  client1,
  client2
};

dmc::ClientInfo info1{1, 1, 1};
dmc::ClientInfo info2{1, 1, 1};

auto get_client_info = [] (ClientId client) {
  switch (client) {
    case client1: return &info1;
    case client2: return &info2;
  }
  std::abort();
};

using boost::system::error_code;

// return a lambda that can be used as a callback to capture its arguments
auto capture(std::optional<error_code>& opt_ec,
             std::optional<dmc::PhaseType>& opt_phase)
{
  return [&] (error_code ec, dmc::PhaseType phase) {
    opt_ec = ec;
    opt_phase = phase;
  };
}

TEST(dmclock, AsyncRequest)
{
  boost::asio::io_context context;
  PriorityQueue queue(context, get_client_info);

  std::optional<error_code> ec1, ec2;
  std::optional<dmc::PhaseType> p1, p2;

  queue.async_request(client1, {}, dmc::get_time(), 0, capture(ec1, p1));
  queue.async_request(client2, {}, dmc::get_time(), 1, capture(ec2, p2));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(p1);
  EXPECT_EQ(dmc::PhaseType::reservation, *p1);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(p2);
  EXPECT_EQ(dmc::PhaseType::priority, *p2);
}

TEST(dmclock, Cancel)
{
  boost::asio::io_context context;
  PriorityQueue queue(context, get_client_info);

  std::optional<error_code> ec1, ec2;
  std::optional<dmc::PhaseType> p1, p2;

  queue.async_request(client1, {}, dmc::get_time(), 1, capture(ec1, p1));
  queue.async_request(client2, {}, dmc::get_time(), 1, capture(ec2, p2));
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

TEST(dmclock, CancelClient)
{
  boost::asio::io_context context;
  PriorityQueue queue(context, get_client_info);

  std::optional<error_code> ec1, ec2;
  std::optional<dmc::PhaseType> p1, p2;

  queue.async_request(client1, {}, dmc::get_time(), 1, capture(ec1, p1));
  queue.async_request(client2, {}, dmc::get_time(), 1, capture(ec2, p2));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  queue.cancel(client1);

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(p2);
  EXPECT_EQ(dmc::PhaseType::priority, *p2);
}

TEST(dmclock, CancelOnDestructor)
{
  boost::asio::io_context context;

  std::optional<error_code> ec1, ec2;
  std::optional<dmc::PhaseType> p1, p2;

  {
    PriorityQueue queue(context, get_client_info);

    queue.async_request(client1, {}, dmc::get_time(), 1, capture(ec1, p1));
    queue.async_request(client2, {}, dmc::get_time(), 1, capture(ec2, p2));
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
auto capture(const Executor& ex, std::optional<error_code>& opt_ec,
             std::optional<dmc::PhaseType>& opt_res)
{
  return boost::asio::bind_executor(ex, capture(opt_ec, opt_res));
}

TEST(dmclock, CrossExecutorRequest)
{
  boost::asio::io_context queue_context;
  PriorityQueue queue(queue_context, get_client_info);

  // create a separate execution context to use for all callbacks to test that
  // pending requests maintain executor work guards on both executors
  boost::asio::io_context callback_context;
  auto ex2 = callback_context.get_executor();

  std::optional<error_code> ec1, ec2;
  std::optional<dmc::PhaseType> p1, p2;

  queue.async_request(client1, {}, dmc::get_time(), 0, capture(ex2, ec1, p1));
  queue.async_request(client2, {}, dmc::get_time(), 0, capture(ex2, ec2, p2));

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
  ASSERT_TRUE(p1);
  EXPECT_EQ(dmc::PhaseType::reservation, *p1);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(p2);
  EXPECT_EQ(dmc::PhaseType::reservation, *p2);
}

#ifdef HAVE_BOOST_CONTEXT

TEST(dmclock, SpawnAsyncRequest)
{
  boost::asio::io_context context;

  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
    PriorityQueue queue(context, get_client_info);

    error_code ec1, ec2;
    auto p1 = queue.async_request(client1, {}, dmc::get_time(), 0, yield[ec1]);
    EXPECT_EQ(boost::system::errc::success, ec1);
    EXPECT_EQ(dmc::PhaseType::reservation, p1);

    auto p2 = queue.async_request(client2, {}, dmc::get_time(), 1, yield[ec2]);
    EXPECT_EQ(boost::system::errc::success, ec2);
    EXPECT_EQ(dmc::PhaseType::priority, p2);
  });

  context.poll();
  EXPECT_TRUE(context.stopped());
}

#endif

} // namespace rgw::dmclock
