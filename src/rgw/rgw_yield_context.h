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

#ifndef RGW_YIELD_CONTEXT_H
#define RGW_YIELD_CONTEXT_H

#include <boost/asio.hpp>

#include "acconfig.h"

#ifndef HAVE_BOOST_CONTEXT

// hide the dependencies on boost::context and boost::coroutines when
// the beast frontend is disabled
namespace boost::asio {
struct yield_context;
}

#else // HAVE_BOOST_CONTEXT

#include <boost/asio/spawn.hpp>

#endif // HAVE_BOOST_CONTEXT


/// optional-like wrapper for a boost::asio::yield_context pointer and its
/// associated boost::asio::io_context. the only real utility of this is to
/// force the use of 'null_yield' instead of nullptr to document calls that
/// could eventually be made asynchronous
class optional_yield_context {
  boost::asio::io_context *c = nullptr;
  boost::asio::yield_context *y = nullptr;
 public:
  /// construct with a valid yield_context pointer
  explicit optional_yield_context(boost::asio::io_context* c,
                                  boost::asio::yield_context *y) noexcept
    : c(c), y(y) {}

  /// type tag to construct an empty object
  struct empty_t {};
  optional_yield_context(empty_t) noexcept {}

  /// disable construction with nullptr as either argument
  optional_yield_context(std::nullptr_t, boost::asio::yield_context*) = delete;
  optional_yield_context(boost::asio::io_service*, std::nullptr_t) = delete;

  /// implicit conversion to bool, returns true if non-empty
  operator bool() const noexcept { return c && y; }

  /// return a reference to the associated io_context. only valid if non-empty
  boost::asio::io_context& get_io_context() const noexcept { return *c; }

  /// return a reference to the yield_context. only valid if non-empty
  boost::asio::yield_context& get_yield_context() const noexcept { return *y; }
};

// type tag object to construct an empty optional_yield_context
static constexpr optional_yield_context::empty_t null_yield{};

#endif // RGW_YIELD_CONTEXT_H
