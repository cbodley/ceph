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

#include "rgw/rgw_putobj.h" // include first to test that it's self-contained
#include "rgw/rgw_putobj.h" // include again to test include guard

#include "include/rados/librados.hpp"

#include <gtest/gtest.h>

// test fixture for global setup/teardown
class PutObjThrottle : public ::testing::Test {
  static constexpr auto poolname = "ceph_test_rgw_putobj";

 protected:
  static librados::Rados rados;
  static librados::IoCtx io;

  static rgw_raw_obj make_obj(const std::string& oid) {
    return {{poolname}, oid};
  }
 public:
  static void SetUpTestCase() {
    ASSERT_EQ(0, rados.init_with_context(g_ceph_context));
    ASSERT_EQ(0, rados.connect());
    // open/create test pool
    int r = rados.ioctx_create(poolname, io);
    if (r == -ENOENT) {
      r = rados.pool_create(poolname);
      if (r == -EEXIST) {
        r = 0;
      } else if (r == 0) {
        r = rados.ioctx_create(poolname, io);
      }
    }
    ASSERT_EQ(0, r);
  }

  static void TearDownTestCase() {
    rados.shutdown();
  }
};
librados::Rados PutObjThrottle::rados;
librados::IoCtx PutObjThrottle::io;

namespace rgw::putobj {

TEST_F(PutObjThrottle, Cancel)
{
  AioThrottle throttle(1);
  auto obj = make_obj(__PRETTY_FUNCTION__);
  {
    auto h = throttle.get({obj, 1});
    // cancels on destruction before aio_operate()
  }
  // drain doesn't block and returns no completions
  auto completed = throttle.drain();
  EXPECT_EQ(0u, completed.size());
}

TEST_F(PutObjThrottle, NoThrottleUpToMax)
{
  AioThrottle throttle(4);
  auto obj = make_obj(__PRETTY_FUNCTION__);
  {
    auto h1 = throttle.get({obj, 1});
    EXPECT_TRUE(h1.completed.empty());
    auto h2 = throttle.get({obj, 1});
    EXPECT_TRUE(h2.completed.empty());
    auto h3 = throttle.get({obj, 1});
    EXPECT_TRUE(h3.completed.empty());
    auto h4 = throttle.get({obj, 1});
    EXPECT_TRUE(h4.completed.empty());
  }
  auto completed = throttle.drain();
  ASSERT_EQ(0u, completed.size());
}

TEST_F(PutObjThrottle, ThrottleOverMax)
{
  constexpr uint64_t window = 4;
  AioThrottle throttle(window);
  auto obj = make_obj(__PRETTY_FUNCTION__);

  // issue 32 writes, and verify that max_outstanding <= window
  uint64_t max_outstanding = 0;
  uint64_t outstanding = 0;

  for (uint64_t i = 0; i < 32; i++) {
    auto h = throttle.get({obj, 1});

    librados::ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, io.aio_operate(obj.oid, h.completion, &op));
    h.release();

    outstanding++;
    outstanding -= h.completed.size();

    if (max_outstanding < outstanding) {
      max_outstanding = outstanding;
    }
  }
  auto completed = throttle.drain();
  EXPECT_EQ(window, max_outstanding);
}

TEST_F(PutObjThrottle, OutOfOrder)
{
  AioThrottle throttle(3);
  auto obj = make_obj(__PRETTY_FUNCTION__);
  {
    // schedule up to window
    auto h1 = throttle.get({obj, 1});
    auto h2 = throttle.get({obj, 1});
    auto h3 = throttle.get({obj, 1});

    // submit a later write
    librados::ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, io.aio_operate(obj.oid, h3.completion, &op));
    h3.release();

    auto h4 = throttle.get({obj, 1});
    EXPECT_EQ(1u, h4.completed.size()); // completion from h3

    // remaining handles are canceled on destruction
  }
  auto completed = throttle.drain();
  EXPECT_EQ(0u, completed.size());
}

} // namespace rgw::putobj
