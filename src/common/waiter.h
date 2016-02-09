// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_WAITER_H
#define CEPH_COMMON_WAITER_H

#include <condition_variable>
#include <tuple>

#include "common/ceph_mutex.h"

#include "include/ceph_assert.h"

namespace ceph {
namespace _waiter {
// For safety reasons (avoiding undefined behavior around sequence
// points) std::reference_wrapper disallows move construction. This
// harms us in cases where we want to pass a reference in to something
// that unavoidably moves.
//
// It should not be used generally.
template<typename T>
class rvalue_reference_wrapper {
public:
  // types
  using type = T;

  rvalue_reference_wrapper(T& r) noexcept
    : p(std::addressof(r)) {}

  // We write our semantics to match those of reference collapsing. If
  // we're treated as an lvalue, collapse to one.

  rvalue_reference_wrapper(const rvalue_reference_wrapper&) noexcept = default;
  rvalue_reference_wrapper(rvalue_reference_wrapper&&) noexcept = default;

  // assignment
  rvalue_reference_wrapper& operator=(
    const rvalue_reference_wrapper& x) noexcept = default;
  rvalue_reference_wrapper& operator=(
    rvalue_reference_wrapper&& x) noexcept = default;

  operator T& () const noexcept {
    return *p;
  }
  T& get() const noexcept {
    return *p;
  }

  operator T&& () noexcept {
    return std::move(*p);
  }
  T&& get() noexcept {
    return std::move(*p);
  }

  template<typename... Args>
  std::result_of_t<T&(Args&&...)> operator ()(Args&&... args ) const {
    return (*p)(std::forward<Args>(args)...);
  }

  template<typename... Args>
  std::result_of_t<T&&(Args&&...)> operator ()(Args&&... args ) {
    return std::move(*p)(std::forward<Args>(args)...);
  }

private:
  T* p;
};

class base {
protected:
  ceph::mutex lock = ceph::make_mutex("_waiter::base::lock");
  ceph::condition_variable cond;
  bool done = false;

  ~base() = default;

  auto wait_base() {
    std::unique_lock l(lock);
    cond.wait(l, [this](){ return done; });
    done = false; // If someone waits, we can be called again
    return l;
  }

  auto exec_base() {
    std::unique_lock l(lock);
    // There's no really good way to handle being called twice
    // without being reset.
    ceph_assert(!done);
    done = true;
    cond.notify_one();
    return l;
  }
};
}
// waiter is a replacement for C_SafeCond and friends. It is the
// moral equivalent of a future but plays well with a world of
// callbacks.
template<typename ...S>
class waiter;

template<>
class waiter<void> : public _waiter::base {
public:
  void wait() {
    wait_base();
  }

  void operator()() {
    exec_base();
  }

  auto ref() {
    return _waiter::rvalue_reference_wrapper(*this);
  }
};

template<typename Ret>
class waiter<Ret> : public _waiter::base {
  Ret ret;

public:
  Ret&& wait() {
    auto l = wait_base();
    return std::move(ret);
  }

  void operator()(Ret&& _ret) {
    auto l = exec_base();
    ret = std::move(_ret);
  }

  void operator()(const Ret& _ret) {
    auto l = exec_base();
    ret = _ret;
  }

  auto ref() {
    return _waiter::rvalue_reference_wrapper(*this);
  }
};

template<typename ...Ret>
class waiter : public _waiter::base {
  std::tuple<Ret...> ret;

public:
  std::tuple<Ret...>&& wait() {
    auto l = wait_base();
    return std::move(ret);
  }

  void operator()(Ret&&... _ret) {
    auto l = exec_base();
    ret = std::forward_as_tuple(_ret...);
  }

  void operator()(const Ret&... _ret) {
    auto l = exec_base();
    ret = std::forward_as_tuple(_ret...);
  }

  auto ref() {
    return _waiter::rvalue_reference_wrapper(*this);
  }
};
}

#endif // CEPH_COMMON_WAITER_H
