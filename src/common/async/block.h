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

#include <optional>
#include <boost/asio.hpp>
#include "include/ceph_assert.h"

namespace ceph::async {

/// BlockOn is a CompletionToken type that turns async interfaces into
/// synchronous ones.
struct BlockOn {
  boost::asio::io_context& ioctx;
  boost::system::error_code* ec;

  BlockOn(boost::asio::io_context& ioctx)
    : ioctx(ioctx), ec(nullptr) {}
  BlockOn(boost::asio::io_context& ioctx, boost::system::error_code& ec)
    : ioctx(ioctx), ec(&ec) {}

  // operator[] to capture error_code
  BlockOn operator[](boost::system::error_code& ec) {
    return BlockOn(ioctx, ec);
  }
};

inline auto block_on(boost::asio::io_context& context)
{
  return BlockOn{context};
}


namespace detail::blocking {

// a Handler that captures any number of arguments in a tuple
template <typename ...Args>
struct Handler {
  const BlockOn& context;
  Handler(const BlockOn& context) : context(context) {}

  // the value and error code point into memory owned by the Result so it can
  // access them after the Handler is destroyed. the pointers are initialized
  // in Result's constructor
  using value_type = std::tuple<Args...>;
  std::optional<value_type>* value = nullptr;
  boost::system::error_code* ec = nullptr;

  template <typename ...UArgs>
  void operator()(UArgs&&... args) {
    value->emplace(std::forward<UArgs>(args)...);
  }
  template <typename ...UArgs>
  void operator()(boost::system::error_code e, UArgs&&... args) {
    *ec = e;
    value->emplace(std::forward<UArgs>(args)...);
  }

  // the handler must execute on the same io context for thread safety
  using executor_type = boost::asio::io_context::executor_type;
  executor_type get_executor() const noexcept {
    return context.ioctx.get_executor();
  }
};
// specialization that captures a single value
template <typename T>
struct Handler<T> {
  const BlockOn& context;
  Handler(const BlockOn& context) : context(context) {}
  using value_type = T;
  std::optional<value_type>* value = nullptr;
  boost::system::error_code* ec = nullptr;
  void operator()(value_type v) {
    *value = std::move(v);
  }
  void operator()(boost::system::error_code e, value_type v) {
    *ec = e;
    *value = std::move(v);
  }
  using executor_type = boost::asio::io_context::executor_type;
  executor_type get_executor() const noexcept {
    return context.ioctx.get_executor();
  }
};
// specialization that captures an error_code only
template <>
struct Handler<> {
  const BlockOn& context;
  Handler(const BlockOn& context) : context(context) {}
  using value_type = void;
  std::optional<boost::system::error_code>* ec = nullptr;
  void operator()(boost::system::error_code e = boost::system::error_code()) {
    *ec = e;
  }
  using executor_type = boost::asio::io_context::executor_type;
  executor_type get_executor() const noexcept {
    return context.ioctx.get_executor();
  }
};

// an async_result type that causes async initiator functions to return any
// values captured by the associated Handler
template <typename ...Args>
class Result {
 public:
  using completion_handler_type = Handler<Args...>;
  using return_type = typename completion_handler_type::value_type;

  explicit Result(completion_handler_type& handler) noexcept
    : handler(handler), capture_ec(handler.context.ec) {
    handler.value = &value;
    handler.ec = &ec;
  }
  return_type get() {
    // run the io context until completion
    handler.context.ioctx.reset();
    do {
      [[maybe_unused]] auto count = handler.context.ioctx.run_one();
      ceph_assert(count > 0);
    } while (!value);
    if (capture_ec) {
      *capture_ec = ec;
    } else if (ec) {
      throw boost::system::system_error(ec);
    }
    return std::move(*value);
  }
 private:
  std::optional<return_type> value;
  boost::system::error_code ec;
  completion_handler_type& handler;
  boost::system::error_code* capture_ec;
};
template <>
class Result<> {
 public:
  using completion_handler_type = Handler<>;
  using return_type = void;

  explicit Result(completion_handler_type& handler) noexcept
    : handler(handler), capture_ec(handler.context.ec) {
    handler.ec = &ec;
  }
  return_type get() {
    // run the io context until completion
    handler.context.ioctx.reset();
    do {
      [[maybe_unused]] auto count = handler.context.ioctx.run_one();
      ceph_assert(count > 0);
    } while (!ec);
    if (capture_ec) {
      *capture_ec = *ec;
    } else if (*ec) {
      throw boost::system::system_error(*ec);
    }
  }
 private:
  std::optional<boost::system::error_code> ec;
  completion_handler_type& handler;
  boost::system::error_code* capture_ec;
};

} // namespace detail::blocking

} // namespace ceph::async

namespace boost::asio {

// async_result<> specializations
template <typename ReturnType, typename ...Args>
class async_result<ceph::async::BlockOn,
                   ReturnType(boost::system::error_code, Args...)>
    : public ceph::async::detail::blocking::Result<Args...> {
 public:
  using ceph::async::detail::blocking::Result<Args...>::Result;
};
template <typename ReturnType, typename ...Args>
class async_result<ceph::async::BlockOn, ReturnType(Args...)>
    : public ceph::async::detail::blocking::Result<Args...> {
 public:
  using ceph::async::detail::blocking::Result<Args...>::Result;
};

} // namespace boost::asio
