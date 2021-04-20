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

#pragma once

#include <chrono>
#include "detail/lease.h"

namespace ceph::async {

// example interface for Locker template param
struct LockerModel final {
  template <typename Rep, typename Period, typename Handler>
  void async_lock(std::chrono::duration<Rep, Period> duration,
                  Handler&& handler);

  template <typename Rep, typename Period, typename Handler>
  void async_renew(std::chrono::duration<Rep, Period> duration,
                   Handler&& handler);

  template <typename Handler>
  void async_unlock(Handler&& handler);
};


/**
 * Lease
 *
 * A lease is an exclusive lock that expires after a given duration, and must be
 * continuously renewed by the owner to avoid expiration.
 *
 * Lease starts unlocked. Once locked successfully with async_lock(), the lease
 * will be renewed automatically until the Lease destructs or is unlocked with
 * async_unlock(). After each successful lock response, another renewal request
 * will be made once half of the lease duration expires. An unsuccessful lock
 * response will cause locked() to return false and renewal will cease.
 *
 * Lease can be moved but not copied.
 *
 * Lease is not thread-safe by default. If necessary, thread-safety can be
 * achieved by providing a strand executor, and only calling Lease member
 * functions within the context of that same executor.
 *
 * @tparam Clock     type of clock for the renewal timer
 * @tparam Locker    type that implements the lock requests
 * @tparam Executor  executor for the renewal timer
 */
template <typename Clock, typename Locker, typename Executor>
class Lease {
 public:
  using clock_type = Clock;
  using duration_type = typename Clock::duration;
  using time_point = typename Clock::time_point;
  using executor_type = Executor;
 private:
  using impl_type = detail::LeaseImpl<clock_type, Locker, executor_type>;
  // impl is ref counted so its handlers can run after Lease itself destructs
  boost::intrusive_ptr<impl_type> impl;
 public:

  /// Contruct an unlocked lease.
  /// @param ex        The executor for lock renewals, and default executor for
  ///                  async_lock/unlock completion handlers without an
  ///                  associated executor.
  /// @param args      Additional arguments forwarded for Locker initialization.
  template <typename ...Args>
  Lease(executor_type ex, Args&& ...args)
    : impl(new impl_type(std::move(ex), std::forward<Args>(args)...))
  {}

  /// On destruction, lease renewal stops but unlock is not called.
  ~Lease() {
    if (impl) {
      impl->cancel();
    }
  }

  // copy disabled
  Lease(const Lease&) = delete;
  Lease& operator=(const Lease&) = delete;
  // move-only
  Lease(Lease&& o) noexcept = default;
  Lease& operator=(Lease&& o) noexcept = default;

  /// Returns the default executor used for lock renewal.
  executor_type get_executor() const {
    return impl->get_executor();
  }

  /// Returns true if the lock is held and has not expired.
  bool locked(time_point now = clock_type::now()) const {
    return impl->locked(now);
  }

  /// Requests an exclusive lock and calls the given completion handler with the
  /// resulting error_code. On success, lock renewal will continue on the
  /// default executor until lock renewal fails, async_unlock() or cancel()
  /// is called, or the Lease itself destructs. If async_lock() is called while
  /// the lease is already locked, the first renewal timer is canceled.
  template <typename CompletionToken>
  auto async_lock(duration_type duration, CompletionToken&& token) {
    return impl->async_lock(duration, std::forward<CompletionToken>(token));
  }

  /// Release a held exclusive lock and cease its renewal. A new lock can be
  /// requested with async_lock() immediately after async_unlock() returns.
  template <typename CompletionToken>
  auto async_unlock(CompletionToken&& token) {
    return impl->async_unlock(std::forward<CompletionToken>(token));
  }

  /// Cancels the renewal timer and prevents pending async_lock() calls
  /// from starting renewal on completion. A new lock can be requested with
  /// async_lock() immediately after cancel() returns.
  void cancel() {
    impl->cancel();
  }
};

} // namespace ceph::async
