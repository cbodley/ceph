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

#include "common/async/block.h"
#include "common/async/completion.h"
#include "common/ceph_time.h"

#include <gtest/gtest.h>

namespace ceph::async {

using boost::system::error_code;
const error_code ok;
const error_code aborted = boost::asio::error::operation_aborted;

// mock async initiator that posts the handler with extra arguments bound
template <typename Signature, typename ExecutionContext,
          typename CompletionToken, typename ...Args>
auto async_test(ExecutionContext& context, CompletionToken&& token,
                Args&& ...args)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto& handler = init.completion_handler;
  auto alloc = boost::asio::get_associated_allocator(handler);
  auto ex1 = context.get_executor();
  auto ex2 = boost::asio::get_associated_executor(handler, ex1);
  auto b = bind_handler(std::move(handler), std::forward<Args>(args)...);
  ex2.post(forward_handler(std::move(b)), alloc);
  return init.result.get();
}

TEST(BlockOn, capture_errors)
{
  boost::asio::io_context ctx;
  auto block = block_on(ctx);
  error_code ec;
  async_test<void()>(ctx, block[ec]);
  ASSERT_EQ(ok, ec);
  async_test<void(error_code)>(ctx, block[ec], aborted);
  ASSERT_EQ(aborted, ec);
  EXPECT_EQ(10, async_test<void(int)>(ctx, block[ec], 10));
  ASSERT_EQ(ok, ec);
  EXPECT_EQ(10, async_test<void(error_code, int)>(ctx, block[ec], aborted, 10));
  ASSERT_EQ(aborted, ec);
}

TEST(BlockOn, throw_errors)
{
  boost::asio::io_context ctx;
  auto block = block_on(ctx);
  EXPECT_NO_THROW(async_test<void(error_code)>(ctx, block, ok));
  EXPECT_THROW(async_test<void(error_code)>(ctx, block, aborted),
               boost::system::system_error);
  EXPECT_NO_THROW(async_test<void(error_code, int)>(ctx, block, ok, 10));
  EXPECT_THROW(async_test<void(error_code, int)>(ctx, block, aborted, 10),
               boost::system::system_error);
}

TEST(BlockOn, return_type)
{
  boost::asio::io_context ctx;
  auto block = block_on(ctx);
  async_test<void()>(ctx, block);
  EXPECT_EQ(10, async_test<void(int)>(ctx, block, 10));
  EXPECT_EQ(10, async_test<void(error_code, int)>(ctx, block, ok, 10));
}

TEST(BlockOn, return_move_only)
{
  boost::asio::io_context ctx;
  auto block = block_on(ctx);
  auto p = async_test<void(std::unique_ptr<int>)>(
      ctx, block, std::make_unique<int>(10));
  ASSERT_TRUE(p);
  EXPECT_EQ(10, *p);
  auto q = async_test<void(error_code, std::unique_ptr<int>)>(
      ctx, block, ok, std::make_unique<int>(10));
  ASSERT_TRUE(q);
  EXPECT_EQ(10, *q);
}

TEST(BlockOn, return_tuple)
{
  boost::asio::io_context ctx;
  auto block = block_on(ctx);
  {
    auto p = async_test<void(int, std::string)>(ctx, block, 10, "hello");
    static_assert(std::is_same_v<decltype(p), std::tuple<int, std::string>>);
    EXPECT_EQ(10, std::get<0>(p));
    EXPECT_EQ("hello", std::get<1>(p));
  }
  {
    const std::string hello = "hello";
    auto p = async_test<void(int, const std::string&)>(ctx, block, 10, std::ref(hello));
    static_assert(std::is_same_v<decltype(p), std::tuple<int, const std::string&>>);
    EXPECT_EQ(10, std::get<0>(p));
    EXPECT_EQ("hello", std::get<1>(p));
  }
  {
    auto p = async_test<void(int, std::unique_ptr<int>)>(ctx, block, 10, std::make_unique<int>(42));
    static_assert(std::is_same_v<decltype(p), std::tuple<int, std::unique_ptr<int>>>);
    EXPECT_EQ(10, std::get<0>(p));
    EXPECT_EQ(42, *std::get<1>(p));
  }
}

class Waiter {
  boost::asio::io_context& ctx;
  using Signature = void(error_code);
  std::unique_ptr<Completion<Signature>> c;
 public:
  Waiter(boost::asio::io_context& ctx) : ctx(ctx) {}

  using executor_type = boost::asio::io_context::executor_type;
  executor_type get_executor() noexcept { return ctx.get_executor(); }

  template <typename CompletionToken>
  auto async_wait(CompletionToken&& token) {
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    c = Completion<Signature>::create(get_executor(),
                                      std::move(init.completion_handler));
    return init.result.get();
  }
  void cancel() {
    if (c) {
      defer(std::move(c), aborted);
    }
  }
};

using namespace std::chrono_literals;
using Clock = ceph::coarse_mono_clock;
using Timer = boost::asio::basic_waitable_timer<Clock>;

TEST(BlockOn, with_timeout)
{
  boost::asio::io_context ctx;
  Waiter waiter{ctx};

  Timer timer{ctx};
  timer.expires_after(1ms);
  timer.async_wait([&waiter] (error_code ec) {
      if (ec != aborted) { waiter.cancel(); }
    });

  error_code ec;
  waiter.async_wait(block_on(ctx)[ec]);
  EXPECT_EQ(aborted, ec);
}

} // namespace ceph::async
