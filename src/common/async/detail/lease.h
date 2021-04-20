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
#include <optional>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "common/async/bind_handler.h"

namespace ceph::async::detail {

template <typename Clock, typename Locker, typename Executor>
class LeaseImpl : public boost::intrusive_ref_counter<LeaseImpl<Clock, Locker, Executor>> {
 public:
  using error_code = boost::system::error_code;
  using clock_type = Clock;
  using duration_type = typename Clock::duration;
  using time_point = typename Clock::time_point;
  using executor_type = Executor;
 private:
  executor_type ex;
  Locker locker;
  std::optional<time_point> expires_at;
  uint32_t current = 0; // incremented on lock/unlock/cancel to resolve races

  using Timer = boost::asio::basic_waitable_timer<clock_type,
        boost::asio::wait_traits<clock_type>, executor_type>;
  Timer timer;

  using ptr = boost::intrusive_ptr<LeaseImpl<Clock, Locker, Executor>>;

  template <typename Handler>
  static void on_acquire(error_code ec, ptr self,
                         uint32_t gen, duration_type dur,
                         Handler&& handler)
  {
    auto ex1 = self->get_executor();
    if (gen != self->current) {
      // canceled
    } else if (ec) {
      // if the lock was previously held, invalidate it
      self->expires_at = std::nullopt;
    } else {
      // update the lock timer and schedule the renewal
      self->expires_at = clock_type::now() + dur;
      auto& timer = self->timer;
      timer.expires_after(dur / 2);
      timer.async_wait([self=std::move(self), gen, dur] (error_code ec) {
            on_timer(ec, std::move(self), gen, dur);
          });
    }
    // dispatch the async_lock() completion handler
    auto ex2 = boost::asio::get_associated_executor(handler, ex1);
    auto alloc = boost::asio::get_associated_allocator(handler);
    ex2.dispatch(ceph::async::bind_handler(std::move(handler), ec), alloc);
  }

  static void on_timer(error_code ec, ptr self,
                       uint32_t gen, duration_type dur)
  {
    if (gen != self->current) {
      return; // canceled
    }
    if (ec) {
      self->expires_at = std::nullopt;
      return;
    }
    auto ex = self->get_executor();
    auto& locker = self->locker;
    auto handler = [self=std::move(self), gen, dur] (error_code ec) {
          on_renew(ec, std::move(self), gen, dur);
        };
    locker.async_renew(dur, bind_executor(ex, std::move(handler)));
  }

  static void on_renew(error_code ec, ptr self,
                       uint32_t gen, duration_type dur)
  {
    if (gen != self->current) {
      return; // canceled
    }
    if (ec) {
      self->expires_at = std::nullopt;
      return;
    }
    // update the lock timer and schedule the next renewal
    self->expires_at = clock_type::now() + dur;
    auto& timer = self->timer;
    timer.expires_after(dur / 2);
    timer.async_wait([self=std::move(self), gen, dur] (error_code ec) {
          on_timer(ec, std::move(self), gen, dur);
        });
  }

  template <typename Handler>
  static void on_unlock(error_code ec, ptr self, Handler&& handler) {
    auto ex1 = self->get_executor();
    auto ex2 = boost::asio::get_associated_executor(handler, ex1);
    auto alloc = boost::asio::get_associated_allocator(handler);
    ex2.dispatch(ceph::async::bind_handler(std::move(handler), ec), alloc);
  }

 public:
  template <typename ...Args>
  LeaseImpl(executor_type ex, Args&& ...args)
    : ex(std::move(ex)), locker(std::forward<Args>(args)...), timer(this->ex)
  {}

  LeaseImpl(const LeaseImpl&) = delete;
  LeaseImpl(LeaseImpl&&) = delete;
  LeaseImpl& operator=(const LeaseImpl&) = delete;
  LeaseImpl& operator=(LeaseImpl&&) = delete;

  executor_type get_executor() const { return ex; }

  bool locked(time_point now) const {
    return expires_at && (now < *expires_at);
  }

  template <typename CompletionToken>
  auto async_lock(duration_type dur, CompletionToken&& token) {
    const auto gen = ++current; // invalidate last gen
    timer.cancel(); // stop renewal timer

    using Signature = void(error_code);
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto& h = init.completion_handler;
    auto ex2 = boost::asio::get_associated_executor(h, get_executor());
    auto handler = [self=ptr{this}, gen, dur, h=std::move(h)] (error_code ec) {
          on_acquire(ec, std::move(self), gen, dur, std::move(h));
        };
    locker.async_lock(dur, bind_executor(ex2, std::move(handler)));
    return init.result.get();
  }

  template <typename CompletionToken>
  auto async_unlock(CompletionToken&& token) {
    expires_at = std::nullopt; // immediately flag as unlocked
    cancel(); // cancel renewal

    using Signature = void(error_code);
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto& h = init.completion_handler;
    auto ex2 = boost::asio::get_associated_executor(h, get_executor());
    auto handler = [self=ptr{this}, h=std::move(h)] (error_code ec) {
          on_unlock(ec, std::move(self), std::move(h));
        };
    locker.async_unlock(bind_executor(ex2, std::move(handler)));
    return init.result.get();
  }

  void cancel() {
    ++current; // invalidate last gen
    timer.cancel(); // stop renewal timer
  }
};

} // namespace ceph::async::detail
