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
#include "include/neorados/RADOS.hpp"
#include "neorados/cls/lock.h"
#include "lease.h"

namespace ceph::async {
namespace detail {

// implements a Locker for Lease using neorados and cls_lock
class ClsLocker {
  neorados::RADOS* rados;
  neorados::IOContext io;
  neorados::Object object;
  std::string_view name;
  std::string_view cookie;
 public:
  ClsLocker(neorados::RADOS* rados,
            neorados::IOContext io,
            neorados::Object object,
            std::string_view name,
            std::string_view cookie)
    : rados(rados), io(std::move(io)),
      object(std::move(object)),
      name(name), cookie(cookie)
  {}

  template <typename Rep, typename Period, typename Handler>
  void async_lock(std::chrono::duration<Rep, Period> dur,
                  Handler&& handler) {
    neorados::WriteOp op;
    constexpr auto type = ClsLockType::EXCLUSIVE;
    constexpr auto tag = ""; // unused
    constexpr auto description = ""; // unused
	  const auto duration = utime_t{ceph::real_clock::zero() + dur};
    constexpr int flags = LOCK_FLAG_MAY_RENEW;
    neorados::cls::lock::lock(op, name, type, cookie, tag,
                              description, duration, flags);
    rados->execute(object, io, std::move(op),
                   std::forward<Handler>(handler));
  }
  template <typename Rep, typename Period, typename Handler>
  void async_renew(std::chrono::duration<Rep, Period> dur,
                   Handler&& handler) {
    neorados::WriteOp op;
    constexpr auto type = ClsLockType::EXCLUSIVE;
    constexpr auto tag = ""; // unused
    constexpr auto description = ""; // unused
	  const auto duration = utime_t{ceph::real_clock::zero() + dur};
    constexpr int flags = LOCK_FLAG_MUST_RENEW;
    neorados::cls::lock::lock(op, name, type, cookie, tag,
                              description, duration, flags);
    rados->execute(object, io, std::move(op),
                   std::forward<Handler>(handler));
  }
  template <typename Handler>
  void async_unlock(Handler&& handler) {
    neorados::WriteOp op;
    neorados::cls::lock::unlock(op, name, cookie);
    rados->execute(object, io, std::move(op),
                   std::forward<Handler>(handler));
  }
};

} // namespace detail

template <typename Clock, typename Executor>
using ClsLease = Lease<Clock, detail::ClsLocker, Executor>;

template <typename Clock, typename Executor>
auto make_cls_lease(Executor ex,
                    neorados::RADOS* rados,
                    neorados::IOContext io,
                    neorados::Object object,
                    std::string_view name,
                    std::string_view cookie)
  -> ClsLease<Clock, Executor>
{
  return {ex, rados, std::move(io), std::move(object), name, cookie};
}

} // namespace ceph::async
