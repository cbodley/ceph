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

#pragma once

#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/basic_waitable_timer.hpp>

namespace ceph::async {

// a WaitHandler that cancels another operation on timeout
template <typename Executor, typename Alloc, typename Canceler>
class CancelHandler {
  Executor ex;
  Alloc alloc;
  Canceler& c;
 public:
  CancelHandler(const Executor& ex, Alloc alloc, Canceler& c)
    : ex(ex), alloc(alloc), c(c) {}
  void operator()(boost::system::error_code ec) {
    if (ec != boost::asio::error::operation_aborted) { c.cancel(); }
  }
  using executor_type = Executor;
  executor_type get_executor() const noexcept { return ex; }
  using allocator_type = Alloc;
  allocator_type get_allocator() const noexcept { return alloc; }
};

// a handler wrapper that cancels a timer on completion
template <typename Handler, typename Timer>
struct TimedHandler {
  Handler h;
  Timer& t;
  TimedHandler(Handler&& h, Timer& t) : h(std::move(h)), t(t) {}
  template <typename ...Args>
  void operator()(Args&& ...args) {
    t.cancel();
    h(std::forward<Args>(args)...);
  }
  using allocator_type = boost::asio::associated_allocator_t<Handler>;
  allocator_type get_allocator() const noexcept {
    return boost::asio::get_associated_allocator(h);
  }
};


/// starts a timer and returns a handler wrapper that cancels it on completion
template <typename Executor1, typename Canceler,
          typename Clock, typename WaitTraits, typename Handler>
auto timed_handler(const Executor1& ex1, Canceler& canceler,
                   boost::asio::basic_waitable_timer<Clock, WaitTraits>& timer,
                   typename Clock::time_point expires_at, Handler&& handler)
{
  auto ex2 = boost::asio::get_associated_executor(handler, ex1);
  auto alloc = boost::asio::get_associated_allocator(handler);
  timer.expires_at(expires_at);
  timer.async_wait(CancelHandler(ex2, alloc, canceler));
  return TimedHandler(std::move(handler), timer);
}

/// starts a timer and returns a handler wrapper that cancels it on completion
template <typename Executor1, typename Canceler,
          typename Clock, typename WaitTraits, typename Handler>
auto timed_handler(const Executor1& ex1, Canceler& canceler,
                   boost::asio::basic_waitable_timer<Clock, WaitTraits>& timer,
                   typename Clock::duration expires_after, Handler&& handler)
{
  auto ex2 = boost::asio::get_associated_executor(handler, ex1);
  auto alloc = boost::asio::get_associated_allocator(handler);
  timer.expires_after(expires_after);
  timer.async_wait(CancelHandler(ex2, alloc, canceler));
  return TimedHandler(std::move(handler), timer);
}

} // namespace ceph::async

namespace boost::asio {

// associated_executor trait for ceph::async::TimedHandler
template <typename Handler, typename Timer, typename Executor>
struct associated_executor<ceph::async::TimedHandler<Handler, Timer>, Executor> {
  using type = associated_executor_t<Handler, Executor>;

  static type get(const ceph::async::TimedHandler<Handler, Timer>& h,
                  const Executor& ex = Executor()) noexcept {
    return get_associated_executor(h.h, ex);
  }
};

} // namespace boost::asio
