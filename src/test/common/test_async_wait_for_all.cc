// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/async/wait_for_all.h"

#include <algorithm>
#include <functional>
#include <optional>
#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>
#include "common/async/co_waiter.h"

namespace ceph::async {

namespace asio = boost::asio;
namespace errc = boost::system::errc;
using boost::system::error_code;

using executor_type = asio::io_context::executor_type;

template <typename T>
using awaitable = asio::awaitable<T, executor_type>;

template <typename T>
auto capture(std::optional<T>& opt)
{
  return [&opt] (T value) {
    opt = std::move(value);
  };
}

template <typename T>
auto capture(asio::cancellation_signal& signal, std::optional<T>& opt)
{
  return asio::bind_cancellation_slot(signal.slot(), capture(opt));
}

TEST(wait_for_all, empty_return_void)
{
  asio::io_context ctx;

  awaitable<void>* end = nullptr;
  std::exception_ptr* out = nullptr;

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(end, end, out), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(wait_for_all, empty_return_value)
{
  asio::io_context ctx;

  awaitable<std::unique_ptr<int>>* end = nullptr;
  std::pair<std::exception_ptr, std::unique_ptr<int>>* out = nullptr;

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(end, end, out), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(wait_for_all, single_return_void)
{
  asio::io_context ctx;

  co_waiter<void, executor_type> waiter;
  awaitable<void> begin = waiter.get();
  std::exception_ptr out;

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(&begin, &begin + 1, &out), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(nullptr);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_FALSE(out);
}

TEST(wait_for_all, single_return_value)
{
  asio::io_context ctx;

  co_waiter<std::unique_ptr<int>, executor_type> waiter;
  awaitable<std::unique_ptr<int>> begin = waiter.get();
  std::pair<std::exception_ptr, std::unique_ptr<int>> out;

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(&begin, &begin + 1, &out), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(nullptr, std::make_unique<int>(42));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_FALSE(out.first);
  ASSERT_TRUE(out.second);
  EXPECT_EQ(42, *out.second);
}

TEST(wait_for_all, single_exception_void)
{
  asio::io_context ctx;

  co_waiter<void, executor_type> waiter;
  awaitable<void> begin = waiter.get();
  std::exception_ptr out;

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(&begin, &begin + 1, &out), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  ASSERT_TRUE(out);
  EXPECT_THROW(std::rethrow_exception(out), std::runtime_error);
}

TEST(wait_for_all, single_exception_value)
{
  asio::io_context ctx;

  co_waiter<std::unique_ptr<int>, executor_type> waiter;
  awaitable<std::unique_ptr<int>> begin = waiter.get();
  std::pair<std::exception_ptr, std::unique_ptr<int>> out;

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(&begin, &begin + 1, &out), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(std::make_exception_ptr(std::runtime_error{"oops"}), {});

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  ASSERT_TRUE(out.first);
  EXPECT_THROW(std::rethrow_exception(out.first), std::runtime_error);
  EXPECT_FALSE(out.second);
}

TEST(wait_for_all, spawn_shutdown_void)
{
  asio::io_context ctx;

  co_waiter<void, executor_type> waiter1;
  co_waiter<void, executor_type> waiter2;
  awaitable<void> crs[] = {waiter1.get(), waiter2.get()};
  std::exception_ptr out[2];

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  // shut down before waiters complete
}

TEST(wait_for_all, spawn_shutdown_value)
{
  asio::io_context ctx;

  co_waiter<std::unique_ptr<int>, executor_type> waiter1;
  co_waiter<std::unique_ptr<int>, executor_type> waiter2;
  awaitable<std::unique_ptr<int>> crs[] = {waiter1.get(), waiter2.get()};
  std::pair<std::exception_ptr, std::unique_ptr<int>> out[2];

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  // shut down before waiters complete
}

TEST(wait_for_all, spawn_cancel_void)
{
  asio::io_context ctx;

  co_waiter<void, executor_type> waiter1;
  co_waiter<void, executor_type> waiter2;
  awaitable<void> crs[] = {waiter1.get(), waiter2.get()};
  std::exception_ptr out[2];

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  // cancel before waiters complete
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
  // no exceptions written to output after cancellation
  EXPECT_TRUE(std::none_of(std::begin(out), std::end(out), std::identity{}));
}

TEST(wait_for_all, spawn_cancel_value)
{
  asio::io_context ctx;

  co_waiter<std::unique_ptr<int>, executor_type> waiter1;
  co_waiter<std::unique_ptr<int>, executor_type> waiter2;
  awaitable<std::unique_ptr<int>> crs[] = {waiter1.get(), waiter2.get()};
  std::pair<std::exception_ptr, std::unique_ptr<int>> out[2];

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0].second);

  // cancel before waiters complete
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
  // no exceptions written to output after cancellation
  EXPECT_TRUE(std::none_of(std::begin(out), std::end(out),
                           [] (auto& o) { return o.first; }));
}

TEST(wait_for_all, partial_shutdown_void)
{
  asio::io_context ctx;

  co_waiter<void, executor_type> waiter1;
  co_waiter<void, executor_type> waiter2;
  awaitable<void> crs[] = {waiter1.get(), waiter2.get()};
  std::exception_ptr out[2];

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0]);

  waiter1.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_TRUE(out[0]);
  EXPECT_FALSE(out[1]);
  // shut down before waiter2 completes
}

TEST(wait_for_all, partial_shutdown_value)
{
  asio::io_context ctx;

  co_waiter<std::unique_ptr<int>, executor_type> waiter1;
  co_waiter<std::unique_ptr<int>, executor_type> waiter2;
  awaitable<std::unique_ptr<int>> crs[] = {waiter1.get(), waiter2.get()};
  std::pair<std::exception_ptr, std::unique_ptr<int>> out[2];

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0].second);

  waiter1.complete(nullptr, std::make_unique<int>(1));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_TRUE(out[0].second);
  EXPECT_EQ(1, *out[0].second);
  EXPECT_FALSE(out[1].second);
  // shut down before waiter2 completes
}

TEST(wait_for_all, partial_cancel_void)
{
  asio::io_context ctx;

  co_waiter<void, executor_type> waiter1;
  co_waiter<void, executor_type> waiter2;
  awaitable<void> crs[] = {waiter1.get(), waiter2.get()};
  std::exception_ptr out[2];

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0]);

  waiter1.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_TRUE(out[0]);
  EXPECT_FALSE(out[1]);

  // cancel before waiter2 completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
  // no exceptions written to output after cancellation
  EXPECT_FALSE(out[1]);
}

TEST(wait_for_all, partial_cancel_value)
{
  asio::io_context ctx;

  co_waiter<std::unique_ptr<int>, executor_type> waiter1;
  co_waiter<std::unique_ptr<int>, executor_type> waiter2;
  awaitable<std::unique_ptr<int>> crs[] = {waiter1.get(), waiter2.get()};
  std::pair<std::exception_ptr, std::unique_ptr<int>> out[2];

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0].second);

  waiter1.complete(nullptr, std::make_unique<int>(1));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_TRUE(out[0].second);
  EXPECT_EQ(1, *out[0].second);
  EXPECT_FALSE(out[1].second);

  // cancel before waiter2 completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
  // no exceptions written to output after cancellation
  EXPECT_FALSE(out[1].first);
}

TEST(wait_for_all, triple_ascending_void)
{
  asio::io_context ctx;

  co_waiter<void, executor_type> waiter1;
  co_waiter<void, executor_type> waiter2;
  co_waiter<void, executor_type> waiter3;
  awaitable<void> crs[] = {waiter1.get(), waiter2.get(), waiter3.get()};
  std::exception_ptr out[3];

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0]);

  waiter1.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_TRUE(out[0]);
  EXPECT_FALSE(out[1]);

  waiter2.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_TRUE(out[1]);
  EXPECT_FALSE(out[2]);

  waiter3.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_TRUE(out[2]);
}

TEST(wait_for_all, triple_ascending_value)
{
  asio::io_context ctx;

  co_waiter<std::unique_ptr<int>, executor_type> waiter1;
  co_waiter<std::unique_ptr<int>, executor_type> waiter2;
  co_waiter<std::unique_ptr<int>, executor_type> waiter3;
  awaitable<std::unique_ptr<int>> crs[] = {
      waiter1.get(), waiter2.get(), waiter3.get()};
  std::pair<std::exception_ptr, std::unique_ptr<int>> out[3];

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0].second);

  waiter1.complete(nullptr, std::make_unique<int>(1));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_TRUE(out[0].second);
  EXPECT_EQ(1, *out[0].second);
  EXPECT_FALSE(out[1].second);

  waiter2.complete(nullptr, std::make_unique<int>(2));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_TRUE(out[1].second);
  EXPECT_EQ(2, *out[1].second);
  EXPECT_FALSE(out[2].second);

  waiter3.complete(nullptr, std::make_unique<int>(3));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  ASSERT_TRUE(out[2].second);
  EXPECT_EQ(3, *out[2].second);
}

TEST(wait_for_all, triple_descending_void)
{
  asio::io_context ctx;

  co_waiter<void, executor_type> waiter1;
  co_waiter<void, executor_type> waiter2;
  co_waiter<void, executor_type> waiter3;
  awaitable<void> crs[] = {waiter1.get(), waiter2.get(), waiter3.get()};
  std::exception_ptr out[3];

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0]);

  waiter3.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0]);

  waiter2.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0]);

  waiter1.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_TRUE(out[0]);
  EXPECT_TRUE(out[1]);
  EXPECT_TRUE(out[2]);
}

TEST(wait_for_all, triple_decending_value)
{
  asio::io_context ctx;

  co_waiter<std::unique_ptr<int>, executor_type> waiter1;
  co_waiter<std::unique_ptr<int>, executor_type> waiter2;
  co_waiter<std::unique_ptr<int>, executor_type> waiter3;
  awaitable<std::unique_ptr<int>> crs[] = {
      waiter1.get(), waiter2.get(), waiter3.get()};
  std::pair<std::exception_ptr, std::unique_ptr<int>> out[3];

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, wait_for_all(std::begin(crs), std::end(crs),
                                   std::begin(out)), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0].second);

  waiter3.complete(nullptr, std::make_unique<int>(3));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0].second);

  waiter2.complete(nullptr, std::make_unique<int>(2));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_FALSE(out[0].second);

  waiter1.complete(nullptr, std::make_unique<int>(1));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  ASSERT_TRUE(out[0].second);
  EXPECT_EQ(1, *out[0].second);
  ASSERT_TRUE(out[1].second);
  EXPECT_EQ(2, *out[1].second);
  ASSERT_TRUE(out[2].second);
  EXPECT_EQ(3, *out[2].second);
}

} // namespace ceph::async
