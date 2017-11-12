// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RGW_ASIO_YIELD_CONTEXT_H
#define RGW_ASIO_YIELD_CONTEXT_H

#ifndef WITH_RADOSGW_BEAST_FRONTEND

// hide the dependencies on boost::context and boost::coroutines when
// the beast frontend is disabled
namespace boost {
namespace asio {
struct yield_context;
}
}

#else // WITH_RADOSGW_BEAST_FRONTEND

#include <boost/asio/spawn.hpp>

#endif // WITH_RADOSGW_BEAST_FRONTEND


/// optional-like wrapper for a boost::asio::yield_context pointer. the only
/// real utility of this is to force the use of 'null_yield' instead of nullptr
/// to document calls that could eventually be made asynchronous
class optional_yield_context {
  boost::asio::yield_context *y = nullptr;
 public:
  /// construct with a valid yield_context pointer
  optional_yield_context(boost::asio::yield_context *y) noexcept
    : y(y) {}

  /// type tag to construct an empty object
  struct empty_t {};
  optional_yield_context(empty_t) noexcept : y(nullptr) {}

  /// disable construction from nullptr. and because NULL implicitly converts to
  /// both nullptr_t and yield_context*, constructing with NULL will fail to
  /// compile because the call is ambiguous
  optional_yield_context(std::nullptr_t) = delete;

  /// offer implicit conversion to yield_context* whether it's empty or not
  operator boost::asio::yield_context*() const noexcept { return y; }
};

// type tag object to construct an empty optional_yield_context
static constexpr optional_yield_context::empty_t null_yield{};

#endif // RGW_ASIO_YIELD_CONTEXT_H
