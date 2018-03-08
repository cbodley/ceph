// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "common/async_throttle.h"

#include <optional>
#include <type_traits>
#include <vector>
#include <gtest/gtest.h>

TEST(AsyncThrottle, Deduction)
{
  boost::asio::io_context context;
  {
    ceph::async::Throttle t{context.get_executor(), 0};
    static_assert(std::is_same_v<decltype(t.get_executor()),
                                 boost::asio::strand<boost::asio::io_context::executor_type>>);
  }
  {
    boost::asio::strand ex{context.get_executor()};
    ceph::async::Throttle t{ex, 0};
    // must not wrap the strand in another strand
    static_assert(std::is_same_v<decltype(t.get_executor()),
                                 boost::asio::strand<boost::asio::io_context::executor_type>>);
    // must copy the existing strand, not construct a separate one
    EXPECT_EQ(ex, t.get_executor());
  }
}

using optional_error_code = std::optional<boost::system::error_code>;

// return a lambda that can be used as a callback to capture its error code
auto capture_ec(optional_error_code& opt_ec)
{
  return [&] (boost::system::error_code ec) { opt_ec = ec; };
}

TEST(AsyncThrottle, AsyncGet)
{
  boost::asio::io_context context;
  ceph::async::Throttle throttle(context.get_executor(), 1);

  optional_error_code ec1, ec2, ec3;
  throttle.async_get(1, capture_ec(ec1));
  throttle.async_get(1, capture_ec(ec2));
  throttle.async_get(0, capture_ec(ec3));

  EXPECT_FALSE(ec1); // no callbacks until poll()
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);

  context.poll();
  EXPECT_FALSE(context.stopped());

  ASSERT_TRUE(ec1); // only the first callback
  EXPECT_EQ(boost::system::errc::success, *ec1);
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3); // third request for 0 waits behind second

  throttle.put(1); // unblock the second and third requests
  EXPECT_FALSE(ec2); // no callbacks until poll()
  EXPECT_FALSE(ec3);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(ec3);
  EXPECT_EQ(boost::system::errc::success, *ec3);
}

TEST(AsyncThrottle, AsyncGetMany)
{
  constexpr size_t max_concurrent = 10;
  constexpr size_t total = 100;
  size_t processed = 0;

  boost::asio::io_context context;
  ceph::async::Throttle throttle(context.get_executor(), max_concurrent);

  for (size_t i = 0; i < total; i++) {
    throttle.async_get(1, [&] (boost::system::error_code ec) {
        ASSERT_EQ(boost::system::errc::success, ec);
        processed++;
        throttle.put(1);
      });
  }

  context.poll();
  EXPECT_TRUE(context.stopped());

  EXPECT_EQ(total, processed);
}

TEST(AsyncThrottle, AsyncSetMaximum)
{
  boost::asio::io_context context;
  ceph::async::Throttle throttle(context.get_executor(), 0);

  optional_error_code ec1, ec2, ec3;
  throttle.async_get(1, capture_ec(ec1));
  throttle.async_get(1, capture_ec(ec2));
  throttle.async_get(1, capture_ec(ec3));

  optional_error_code ecmax1;
  throttle.async_set_maximum(2, capture_ec(ecmax1));

  // no callbacks before poll()
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);
  EXPECT_FALSE(ecmax1);

  context.poll();
  EXPECT_FALSE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  EXPECT_FALSE(ec3);
  ASSERT_TRUE(ecmax1);
  EXPECT_EQ(boost::system::errc::success, *ecmax1);

  optional_error_code ecmax2;
  throttle.async_set_maximum(0, capture_ec(ecmax2));
  EXPECT_FALSE(ecmax2);

  context.poll();
  EXPECT_FALSE(context.stopped());

  EXPECT_FALSE(ecmax2);

  throttle.put(2);
  EXPECT_FALSE(ecmax2);

  context.poll();
  EXPECT_FALSE(context.stopped());

  ASSERT_TRUE(ecmax2);
  EXPECT_EQ(boost::system::errc::success, *ecmax2);
  EXPECT_FALSE(ec3);
}

TEST(AsyncThrottle, Cancel)
{
  boost::asio::io_context context;
  ceph::async::Throttle throttle(context.get_executor(), 1);

  optional_error_code ec1, ec2, ec3;
  throttle.async_get(1, capture_ec(ec1));
  throttle.async_get(1, capture_ec(ec2));
  throttle.async_get(1, capture_ec(ec3));

  EXPECT_FALSE(ec1); // no callbacks until poll()
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);

  context.poll();
  EXPECT_FALSE(context.stopped());

  ASSERT_TRUE(ec1); // only the first callback
  EXPECT_EQ(boost::system::errc::success, *ec1);
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);

  optional_error_code ecmax;
  throttle.async_set_maximum(0, capture_ec(ecmax));
  EXPECT_FALSE(ecmax);

  context.poll();
  EXPECT_FALSE(context.stopped());

  EXPECT_FALSE(ecmax);

  throttle.cancel();
  EXPECT_FALSE(ec2); // no callbacks until poll()
  EXPECT_FALSE(ec3);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ec2);
  ASSERT_TRUE(ec3);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ec3);
  ASSERT_TRUE(ecmax);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ecmax);
}

// TODO: TEST(AsyncThrottle, CancelOnDestruct)

// return a lambda from capture_ec() that's bound to run on the given executor
template <typename Executor>
auto bind_capture_ec(const Executor& ex, optional_error_code& opt_ec)
{
  return boost::asio::bind_executor(ex, capture_ec(opt_ec));
}

TEST(AsyncThrottle, CrossExecutor)
{
  boost::asio::io_context throttle_context;
  ceph::async::Throttle throttle(throttle_context.get_executor(), 1);

  // create a separate execution context to use for all callbacks to test that
  // pending requests maintain executor work guards on both executors
  boost::asio::io_context callback_context;
  auto ex2 = callback_context.get_executor();

  optional_error_code ec1, ec2;
  throttle.async_get(1, bind_capture_ec(ex2, ec1));
  throttle.async_get(1, bind_capture_ec(ex2, ec2));

  EXPECT_FALSE(ec1);

  callback_context.poll();
  // maintains work while on callback_context before it runs on throttle_context
  EXPECT_FALSE(callback_context.stopped());

  EXPECT_FALSE(ec1);

  throttle_context.poll();
  EXPECT_FALSE(throttle_context.stopped());

  EXPECT_FALSE(ec1);

  callback_context.poll();
  EXPECT_FALSE(callback_context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  EXPECT_FALSE(ec2);

  throttle.cancel(); // cancel second request

  EXPECT_FALSE(ec2);

  callback_context.poll();
  EXPECT_FALSE(callback_context.stopped());

  EXPECT_FALSE(ec2);

  throttle_context.poll();
  EXPECT_TRUE(throttle_context.stopped());

  EXPECT_FALSE(ec2);

  callback_context.poll();
  EXPECT_TRUE(callback_context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ec2);

  optional_error_code ecmax;
  throttle.async_set_maximum(0, bind_capture_ec(ex2, ecmax));

  EXPECT_FALSE(ecmax);

  callback_context.restart();
  callback_context.poll();
  EXPECT_FALSE(callback_context.stopped());

  EXPECT_FALSE(ecmax);

  throttle_context.restart();
  throttle_context.poll();
  EXPECT_FALSE(throttle_context.stopped());

  throttle.put(1);

  EXPECT_FALSE(ecmax);

  callback_context.poll();
  EXPECT_FALSE(callback_context.stopped());

  EXPECT_FALSE(ecmax);

  throttle_context.poll();
  EXPECT_TRUE(throttle_context.stopped());

  EXPECT_FALSE(ecmax);

  callback_context.poll();
  EXPECT_TRUE(callback_context.stopped());

  EXPECT_TRUE(ecmax);
}
