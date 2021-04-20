// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/async/lease.h"
#include <gtest/gtest.h>
#include <optional>
#include "common/ceph_time.h"

namespace ceph::async {

using error_code = boost::system::error_code;
using clock_type = ceph::mono_clock;
using executor_type = boost::asio::io_context::executor_type;

// returns a callback function that captures the resulting error_code
auto capture(std::optional<error_code>& opt_ec) {
  return [&opt_ec] (error_code ec) { opt_ec = ec; };
}

// noop Locker that immediately returns predefined error codes
struct MockLocker {
  error_code lock_ec;
  error_code renew_ec;
  error_code unlock_ec;

  template <typename Rep, typename Period, typename Handler>
  void async_lock(std::chrono::duration<Rep, Period> duration, Handler&& h) {
    auto ex = boost::asio::get_associated_executor(h);
    auto alloc = boost::asio::get_associated_allocator(h);
    ex.post(ceph::async::bind_handler(std::move(h), lock_ec), alloc);
  }

  template <typename Rep, typename Period, typename Handler>
  void async_renew(std::chrono::duration<Rep, Period> duration, Handler&& h) {
    auto ex = boost::asio::get_associated_executor(h);
    auto alloc = boost::asio::get_associated_allocator(h);
    ex.post(ceph::async::bind_handler(std::move(h), renew_ec), alloc);
  }

  template <typename Handler>
  void async_unlock(Handler&& h) {
    auto ex = boost::asio::get_associated_executor(h);
    auto alloc = boost::asio::get_associated_allocator(h);
    ex.post(ceph::async::bind_handler(std::move(h), unlock_ec), alloc);
  }
};

// wrapper that counts the calls to each api
template <typename Locker>
struct CountingLocker : Locker {
  uint32_t lock_calls = 0;
  uint32_t renew_calls = 0;
  uint32_t unlock_calls = 0;

  template <typename Rep, typename Period, typename Handler>
  void async_lock(std::chrono::duration<Rep, Period> duration, Handler&& h) {
    ++lock_calls;
    Locker::async_lock(duration, std::move(h));
  }

  template <typename Rep, typename Period, typename Handler>
  void async_renew(std::chrono::duration<Rep, Period> duration, Handler&& h) {
    ++renew_calls;
    Locker::async_renew(duration, std::move(h));
  }

  template <typename Handler>
  void async_unlock(Handler&& h) {
    ++unlock_calls;
    Locker::async_unlock(std::move(h));
  }
};

TEST(lease, lock_unlock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};
  const auto duration = std::chrono::milliseconds(20);

  // acquire the lock
  std::optional<error_code> lock_ec;
  lease.async_lock(duration, capture(lock_ec));
  ASSERT_FALSE(lock_ec);
  EXPECT_FALSE(lease.locked());

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec); // initial lock acquired
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);

  // unlock
  std::optional<error_code> unlock_ec;
  lease.async_unlock(capture(unlock_ec));
  ASSERT_FALSE(unlock_ec);
  EXPECT_FALSE(lease.locked());

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // should finish without any more waits
  ASSERT_TRUE(unlock_ec); // async_unlock() handler was called
  EXPECT_FALSE(*unlock_ec);
  EXPECT_FALSE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(1, locker.unlock_calls);
}

// wrapper that can add delays to the wrapped calls
template <typename Locker>
struct DelayedLocker : Locker {
  using timer_type = boost::asio::basic_waitable_timer<clock_type,
        boost::asio::wait_traits<clock_type>, executor_type>;
  timer_type lock_timer, renew_timer, unlock_timer;
  std::optional<ceph::timespan> lock_delay, renew_delay, unlock_delay;

  DelayedLocker(executor_type ex)
    : lock_timer(ex), renew_timer(ex), unlock_timer(ex)
  {}

  template <typename Rep, typename Period, typename Handler>
  void async_lock(std::chrono::duration<Rep, Period> duration, Handler&& h) {
    if (!lock_delay) {
      Locker::async_lock(duration, std::move(h));
      return;
    }
    lock_timer.expires_after(*lock_delay);
    auto ex = boost::asio::get_associated_executor(h);
    auto handler = [this, duration, h=std::move(h)] (error_code ec) {
      if (ec) {
        std::move(h)(ec);
      } else {
        Locker::async_lock(duration, std::move(h));
      }
    };
    lock_timer.async_wait(bind_executor(ex, std::move(handler)));
  }

  template <typename Rep, typename Period, typename Handler>
  void async_renew(std::chrono::duration<Rep, Period> duration, Handler&& h) {
    if (!renew_delay) {
      Locker::async_renew(duration, std::move(h));
      return;
    }
    renew_timer.expires_after(*renew_delay);
    auto ex = boost::asio::get_associated_executor(h);
    auto handler = [this, duration, h=std::move(h)] (error_code ec) {
      if (ec) {
        std::move(h)(ec);
      } else {
        Locker::async_renew(duration, std::move(h));
      }
    };
    renew_timer.async_wait(bind_executor(ex, std::move(handler)));
  }

  template <typename Handler>
  void async_unlock(Handler&& h) {
    if (!unlock_delay) {
      Locker::async_unlock(std::move(h));
      return;
    }
    unlock_timer.expires_after(*unlock_delay);
    auto ex = boost::asio::get_associated_executor(h);
    auto handler = [this, h=std::move(h)] (error_code ec) {
      if (ec) {
        std::move(h)(ec);
      } else {
        Locker::async_unlock(std::move(h));
      }
    };
    unlock_timer.async_wait(bind_executor(ex, std::move(handler)));
  }
};

TEST(lease, lock_cancel)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<DelayedLocker<MockLocker>>;
  auto locker = locker_type{ex};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};

  // delay the lock so we can cancel before it completes
  locker.lock_delay = std::chrono::milliseconds(10);

  std::optional<error_code> lock_ec;
  lease.async_lock(std::chrono::milliseconds(20), capture(lock_ec));
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(lock_ec);
  EXPECT_FALSE(lease.locked());

  lease.cancel();

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(lock_ec);

  ctx.run_for(std::chrono::milliseconds(15));
  ASSERT_TRUE(ctx.stopped());
  EXPECT_FALSE(lease.locked());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);
}

// time each call to async_renew() to verify that it happens within
// duration/2 and duration after the last lock response
template <typename Locker>
struct RenewTimingLocker : Locker {
  std::optional<clock_type::time_point> locked_at;

  template <typename Rep, typename Period, typename Handler>
  void async_lock(std::chrono::duration<Rep, Period> duration, Handler&& h) {
    ASSERT_FALSE(locked_at);
    auto ex = boost::asio::get_associated_executor(h);
    auto handler = [this, h=std::move(h)] (error_code ec) {
      locked_at = clock_type::now();
      std::move(h)(ec);
    };
    Locker::async_lock(duration, bind_executor(ex, std::move(handler)));
  }

  template <typename Rep, typename Period, typename Handler>
  void async_renew(std::chrono::duration<Rep, Period> duration, Handler&& h) {
    ASSERT_TRUE(locked_at);
    const auto now = clock_type::now();
    EXPECT_GE(now, *locked_at + duration / 2);
    EXPECT_LT(now, *locked_at + duration);
    auto ex = boost::asio::get_associated_executor(h);
    auto handler = [this, h=std::move(h)] (error_code ec) {
      locked_at = clock_type::now();
      std::move(h)(ec);
    };
    Locker::async_renew(duration, bind_executor(ex, std::move(handler)));
  }
};

TEST(lease, lock_renew_unlock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = RenewTimingLocker<CountingLocker<MockLocker>>;
  auto locker = locker_type{};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};
  const auto duration = std::chrono::milliseconds(20);

  // acquire the lock
  std::optional<error_code> lock_ec;
  lease.async_lock(duration, capture(lock_ec));
  ASSERT_FALSE(lock_ec);
  EXPECT_FALSE(lease.locked());

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec); // initial lock acquired
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);

  // let renew run for a while
  ctx.run_for(std::chrono::milliseconds(65));
  ASSERT_FALSE(ctx.stopped());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(6, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);

  // unlock
  std::optional<error_code> unlock_ec;
  lease.async_unlock(capture(unlock_ec));
  ASSERT_FALSE(unlock_ec);
  EXPECT_FALSE(lease.locked());

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // should finish without any more waits
  ASSERT_TRUE(unlock_ec); // async_unlock() handler was called
  EXPECT_FALSE(*unlock_ec);
  EXPECT_FALSE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(6, locker.renew_calls);
  EXPECT_EQ(1, locker.unlock_calls);
}

TEST(lease, lock_renew_destruct)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = RenewTimingLocker<CountingLocker<MockLocker>>;
  auto locker = locker_type{};
  {
    using lease_type = Lease<clock_type, locker_type&, executor_type>;
    auto lease = lease_type{ex, locker};
    const auto duration = std::chrono::milliseconds(20);

    // acquire the lock
    std::optional<error_code> lock_ec;
    lease.async_lock(duration, capture(lock_ec));
    ASSERT_FALSE(lock_ec);
    EXPECT_FALSE(lease.locked());

    ctx.poll();
    ASSERT_FALSE(ctx.stopped());
    ASSERT_TRUE(lock_ec); // initial lock acquired
    EXPECT_FALSE(*lock_ec);
    EXPECT_TRUE(lease.locked());
    EXPECT_EQ(1, locker.lock_calls);
    EXPECT_EQ(0, locker.renew_calls);
    EXPECT_EQ(0, locker.unlock_calls);

    // let renew run for a while
    ctx.run_for(std::chrono::milliseconds(65));
    ASSERT_FALSE(ctx.stopped());
    EXPECT_TRUE(lease.locked());
    EXPECT_EQ(1, locker.lock_calls);
    EXPECT_EQ(6, locker.renew_calls);
    EXPECT_EQ(0, locker.unlock_calls);
    // lease goes out of scope
  }
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(6, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, lock_error)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  // inject EBUSY on async_lock()
  locker.lock_ec = make_error_code(boost::system::errc::device_or_resource_busy);
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};
  const auto duration = std::chrono::milliseconds(20);

  std::optional<error_code> ec;
  lease.async_lock(duration, capture(ec));
  ASSERT_FALSE(ec);
  EXPECT_FALSE(lease.locked());

  // run handlers that are ready, but don't wait on any timers
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(ec); // initial lock acquired
  EXPECT_EQ(boost::system::errc::device_or_resource_busy, *ec);
  EXPECT_FALSE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, renew_error)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  // inject ENOENT on async_renew()
  locker.renew_ec = make_error_code(boost::system::errc::no_such_file_or_directory);
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};
  const auto duration = std::chrono::milliseconds(20);

  // acquire the lock
  std::optional<error_code> ec;
  lease.async_lock(duration, capture(ec));
  ASSERT_FALSE(ec);
  EXPECT_FALSE(lease.locked());

  // run handlers that are ready, but don't wait on any timers
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);

  // wait up to the lock duration for the renewal to fail
  ctx.run_for(duration);
  ASSERT_TRUE(ctx.stopped());
  EXPECT_FALSE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(1, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, unlock_error)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  // inject ENOENT on async_unlock()
  locker.unlock_ec = make_error_code(boost::system::errc::no_such_file_or_directory);
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};
  const auto duration = std::chrono::milliseconds(20);

  // acquire the lock
  std::optional<error_code> lock_ec;
  lease.async_lock(duration, capture(lock_ec));
  ASSERT_FALSE(lock_ec);
  EXPECT_FALSE(lease.locked());

  // run handlers that are ready, but don't wait on any timers
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);

  // unlock
  std::optional<error_code> unlock_ec;
  lease.async_unlock(capture(unlock_ec));
  EXPECT_FALSE(unlock_ec);
  EXPECT_FALSE(lease.locked());

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(unlock_ec);
  EXPECT_EQ(boost::system::errc::no_such_file_or_directory, *unlock_ec);
  EXPECT_FALSE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(1, locker.unlock_calls);
}

TEST(lease, delayed_renew)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<RenewTimingLocker<DelayedLocker<MockLocker>>>;
  auto locker = locker_type{ex};
  // delay the first renewal by more than the lease timer
  locker.renew_delay = std::chrono::milliseconds(40);
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};
  const auto duration = std::chrono::milliseconds(20);

  // acquire the lock
  std::optional<error_code> lock_ec;
  lease.async_lock(duration, capture(lock_ec));
  ASSERT_FALSE(lock_ec);
  EXPECT_FALSE(lease.locked());

  // let the lock complete and start the renewal timer
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);

  // wait for the renewal timer
  ctx.run_one();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(1, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);

  locker.renew_delay = std::nullopt; // no delay on future renews

  // let renew run for a while. while async_renew() is delayed, we shouldn't see
  // any extra calls to async_renew()
  ctx.run_for(std::chrono::milliseconds(65));
  ASSERT_FALSE(ctx.stopped());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(3, locker.renew_calls); // delayed til 40ms then renew at 50ms 60ms
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, destruct_during_lock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<DelayedLocker<MockLocker>>;
  auto locker = locker_type{ex};
  locker.lock_delay = std::chrono::hours(1);

  std::optional<error_code> lock_ec;
  {
    using lease_type = Lease<clock_type, locker_type&, executor_type>;
    auto lease = lease_type{ex, locker};
    const auto duration = std::chrono::milliseconds(20);

    lease.async_lock(duration, capture(lock_ec));
    ASSERT_FALSE(lock_ec);
    EXPECT_FALSE(lease.locked());
    EXPECT_EQ(1, locker.lock_calls);
    EXPECT_EQ(0, locker.renew_calls);
    EXPECT_EQ(0, locker.unlock_calls);

    ctx.poll();
    ASSERT_FALSE(ctx.stopped());
    EXPECT_FALSE(lock_ec);
    EXPECT_FALSE(lease.locked());
    EXPECT_EQ(1, locker.lock_calls);
    EXPECT_EQ(0, locker.renew_calls);
    EXPECT_EQ(0, locker.unlock_calls);
    // lease goes out of scope
  }
  locker.lock_timer.cancel();
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_EQ(boost::asio::error::operation_aborted, *lock_ec);
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, destruct_during_renew)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<DelayedLocker<MockLocker>>;
  auto locker = locker_type{ex};
  locker.renew_delay = std::chrono::hours(1);

  {
    using lease_type = Lease<clock_type, locker_type&, executor_type>;
    auto lease = lease_type{ex, locker};
    const auto duration = std::chrono::milliseconds(20);

    std::optional<error_code> lock_ec;
    lease.async_lock(duration, capture(lock_ec));
    ASSERT_FALSE(lock_ec);
    EXPECT_FALSE(lease.locked());

    ctx.poll();
    ASSERT_FALSE(ctx.stopped());
    ASSERT_TRUE(lock_ec);
    EXPECT_FALSE(*lock_ec);
    EXPECT_TRUE(lease.locked());
    EXPECT_EQ(1, locker.lock_calls);
    EXPECT_EQ(0, locker.renew_calls);
    EXPECT_EQ(0, locker.unlock_calls);

    ctx.run_one(); // wait for the renewal timer
    ASSERT_FALSE(ctx.stopped());
    EXPECT_TRUE(lease.locked());
    EXPECT_EQ(1, locker.lock_calls);
    EXPECT_EQ(1, locker.renew_calls);
    EXPECT_EQ(0, locker.unlock_calls);
    // lease goes out of scope
  }
  locker.renew_timer.cancel();
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(1, locker.renew_calls);
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, destruct_during_unlock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<DelayedLocker<MockLocker>>;
  auto locker = locker_type{ex};
  locker.unlock_delay = std::chrono::hours(1);

  {
    using lease_type = Lease<clock_type, locker_type&, executor_type>;
    auto lease = lease_type{ex, locker};
    const auto duration = std::chrono::milliseconds(20);

    std::optional<error_code> lock_ec;
    lease.async_lock(duration, capture(lock_ec));
    ASSERT_FALSE(lock_ec);
    EXPECT_FALSE(lease.locked());

    ctx.poll();
    ASSERT_FALSE(ctx.stopped());
    ASSERT_TRUE(lock_ec);
    EXPECT_FALSE(*lock_ec);
    EXPECT_TRUE(lease.locked());
    EXPECT_EQ(1, locker.lock_calls);
    EXPECT_EQ(0, locker.renew_calls);
    EXPECT_EQ(0, locker.unlock_calls);

    std::optional<error_code> unlock_ec;
    lease.async_unlock(capture(unlock_ec));
    ASSERT_FALSE(unlock_ec);

    ctx.poll();
    ASSERT_FALSE(ctx.stopped());
    EXPECT_FALSE(lease.locked());
    EXPECT_EQ(1, locker.lock_calls);
    EXPECT_EQ(0, locker.renew_calls);
    EXPECT_EQ(1, locker.unlock_calls);
    // lease goes out of scope
  }
  locker.unlock_timer.cancel();
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  EXPECT_EQ(1, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
  EXPECT_EQ(1, locker.unlock_calls);
}

TEST(lease, async_lock_relock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};

  std::optional<error_code> lock_ec;
  lease.async_lock(std::chrono::milliseconds(60), capture(lock_ec));
  // relock before lock completes
  std::optional<error_code> relock_ec;
  lease.async_lock(std::chrono::milliseconds(20), capture(relock_ec));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  ASSERT_TRUE(relock_ec);
  EXPECT_FALSE(*relock_ec);
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(2, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);

  ctx.run_for(std::chrono::milliseconds(65));
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(6, locker.renew_calls); // 10ms renewal instead of the original 30ms
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, lock_cancel_relock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<DelayedLocker<MockLocker>>;
  auto locker = locker_type{ex};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};

  // delay the first lock so we can cancel and re-lock before it completes
  locker.lock_delay = std::chrono::milliseconds(10);

  std::optional<error_code> lock_ec;
  lease.async_lock(std::chrono::milliseconds(60), capture(lock_ec));
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(lock_ec);
  EXPECT_FALSE(lease.locked());

  lease.cancel();

  locker.lock_delay = std::nullopt; // no delay for the relock

  std::optional<error_code> relock_ec;
  lease.async_lock(std::chrono::milliseconds(20), capture(relock_ec));

  ctx.poll();
  ASSERT_TRUE(relock_ec);
  EXPECT_FALSE(*relock_ec);
  EXPECT_EQ(2, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);

  ctx.run_for(std::chrono::milliseconds(65));
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(lease.locked());
  ASSERT_TRUE(lock_ec); // first lock completed at some point
  EXPECT_FALSE(*lock_ec);
  EXPECT_EQ(6, locker.renew_calls); // 10ms renewal instead of the original 30ms
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, lock_relock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};

  std::optional<error_code> lock_ec;
  lease.async_lock(std::chrono::milliseconds(60), capture(lock_ec));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());

  std::optional<error_code> relock_ec;
  lease.async_lock(std::chrono::milliseconds(20), capture(relock_ec));

  ctx.poll();
  ASSERT_TRUE(relock_ec);
  EXPECT_FALSE(*relock_ec);
  EXPECT_EQ(2, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);

  ctx.run_for(std::chrono::milliseconds(65));
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(6, locker.renew_calls); // 10ms renewal instead of the original 30ms
  EXPECT_EQ(0, locker.unlock_calls);
}

TEST(lease, lock_relock_fails)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};

  std::optional<error_code> lock_ec;
  lease.async_lock(std::chrono::milliseconds(60), capture(lock_ec));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());

  // inject EIO for second lock request
  locker.lock_ec = make_error_code(boost::system::errc::io_error);

  std::optional<error_code> relock_ec;
  lease.async_lock(std::chrono::milliseconds(20), capture(relock_ec));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(relock_ec);
  EXPECT_EQ(boost::system::errc::io_error, *relock_ec);
  EXPECT_FALSE(lease.locked());
  EXPECT_EQ(2, locker.lock_calls);
  EXPECT_EQ(0, locker.renew_calls);
}

TEST(lease, lock_renew_relock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};

  std::optional<error_code> lock_ec;
  lease.async_lock(std::chrono::milliseconds(60), capture(lock_ec));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());

  ctx.run_for(std::chrono::milliseconds(35));
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(1, locker.renew_calls); // 30ms renewal

  std::optional<error_code> relock_ec;
  lease.async_lock(std::chrono::milliseconds(20), capture(relock_ec));

  ctx.poll();
  ASSERT_TRUE(relock_ec);
  EXPECT_FALSE(*relock_ec);
  EXPECT_EQ(2, locker.lock_calls);

  ctx.run_for(std::chrono::milliseconds(45));
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(5, locker.renew_calls); // 10ms renewal instead of the original 30ms
}

TEST(lease, unlock_relock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<MockLocker>;
  auto locker = locker_type{};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};

  std::optional<error_code> lock_ec;
  lease.async_lock(std::chrono::milliseconds(60), capture(lock_ec));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());

  std::optional<error_code> unlock_ec;
  lease.async_unlock(capture(unlock_ec));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ctx.restart();
  ASSERT_TRUE(unlock_ec);
  EXPECT_FALSE(*unlock_ec);
  EXPECT_FALSE(lease.locked());

  std::optional<error_code> relock_ec;
  lease.async_lock(std::chrono::milliseconds(20), capture(relock_ec));

  ctx.poll();
  ASSERT_TRUE(relock_ec);
  EXPECT_FALSE(*relock_ec);
  EXPECT_TRUE(lease.locked());

  ctx.run_for(std::chrono::milliseconds(45));
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(4, locker.renew_calls); // 10ms renewal instead of the original 30ms
}

TEST(lease, unlock_cancel_relock)
{
  boost::asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  using locker_type = CountingLocker<DelayedLocker<MockLocker>>;
  auto locker = locker_type{ex};
  using lease_type = Lease<clock_type, locker_type&, executor_type>;
  auto lease = lease_type{ex, locker};

  std::optional<error_code> lock_ec;
  lease.async_lock(std::chrono::milliseconds(60), capture(lock_ec));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(lock_ec);
  EXPECT_FALSE(*lock_ec);
  EXPECT_TRUE(lease.locked());

  // delay the unlock so we can cancel and relock in the meantime
  locker.unlock_delay = std::chrono::milliseconds(10);

  std::optional<error_code> unlock_ec;
  lease.async_unlock(capture(unlock_ec));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(unlock_ec);
  EXPECT_FALSE(lease.locked());

  std::optional<error_code> relock_ec;
  lease.async_lock(std::chrono::milliseconds(20), capture(relock_ec));

  ctx.poll();
  ASSERT_TRUE(relock_ec);
  EXPECT_FALSE(*relock_ec);
  EXPECT_TRUE(lease.locked());

  ctx.run_for(std::chrono::milliseconds(45));
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(unlock_ec); // unlock completed at some point
  EXPECT_FALSE(*unlock_ec);
  EXPECT_TRUE(lease.locked());
  EXPECT_EQ(4, locker.renew_calls); // 10ms renewal instead of the original 30ms
}

} // namespace ceph::async
