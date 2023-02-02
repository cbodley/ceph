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

#pragma once

#include <memory>
#include <optional>
#include <utility>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "common/async/co_waiter.h"
#include "common/async/service.h"
#include "include/ceph_assert.h"

namespace ceph::async::detail {

template <typename I, typename T, typename Executor>
concept input_awaitable_iterator = std::input_iterator<I>
    && std::same_as<std::iter_value_t<I>, boost::asio::awaitable<T, Executor>>;

template <typename T>
struct wait_for_all_output {
  using type = std::pair<std::exception_ptr, T>;
  static type create(std::exception_ptr eptr, T value) {
    return std::make_pair(eptr, std::move(value));
  }
};
template <>
struct wait_for_all_output<void> {
  using type = std::exception_ptr;
  static type create(std::exception_ptr eptr) { return eptr; }
};
template <typename T>
using wait_for_all_output_t = typename wait_for_all_output<T>::type;

template <typename I, typename T>
concept wait_for_all_output_iterator =
    std::output_iterator<I, wait_for_all_output_t<T>>;


template <typename T, boost::asio::execution::executor Executor,
          wait_for_all_output_iterator<T> OutIter>
class wait_for_all_state :
    public boost::intrusive_ref_counter<
        wait_for_all_state<T, Executor, OutIter>,
        boost::thread_unsafe_counter>,
    public service_list_base_hook
{
 public:
  wait_for_all_state(Executor ex, size_t count, OutIter out)
    : svc(boost::asio::use_service<service<wait_for_all_state>>(
            boost::asio::query(ex, boost::asio::execution::context))),
      count(count), out(out), children(std::make_unique<child[]>(count))
  {
    // register for service_shutdown() notifications
    svc.add(*this);
  }
  ~wait_for_all_state()
  {
    svc.remove(*this);
  }

  // return the co_spawn completion handler for a given coroutine index
  auto completion_handler(size_t i)
  {
    return boost::asio::bind_cancellation_slot(children[i].signal.slot(),
        [self = boost::intrusive_ptr{this}, i] (auto&& ...args) {
          using output = wait_for_all_output<T>;
          self->child_complete(i, output::create(std::move(args)...));
        });
  }

  // wait for all children to complete
  boost::asio::awaitable<void, Executor> wait()
  {
    return waiter.get();
  }

  // cancel unfinished children
  void cancel()
  {
    for (size_t i = index; i < count; i++) {
      if (!children[i].result) {
        children[i].signal.emit(boost::asio::cancellation_type::terminal);
      }
    }
  }

  void service_shutdown()
  {
    waiter.shutdown();
  }

 private:
  service<wait_for_all_state>& svc;
  const size_t count; // total number of coroutines
  OutIter out;
  size_t index = 0; // index corresponding to output iterator position
  co_waiter<void, Executor> waiter;

  using output_type = wait_for_all_output_t<T>;
  struct child {
    boost::asio::cancellation_signal signal;
    std::optional<output_type> result;
  };
  std::unique_ptr<child[]> children;

  void complete(std::exception_ptr eptr)
  {
    waiter.complete(eptr);
  }

  void child_complete(size_t i, output_type result)
  {
    if (!waiter.waiting()) {
      return; // canceled, so we can't safely write to 'out'
    }

    if (i != index) {
      // the output iterator isn't here yet, save the result in the child
      children[i].result = std::move(result);
      return;
    }

    *out++ = std::move(result); // copy directly to output
    index++;

    // continue copying results until a child isn't complete
    while (index < count && children[index].result) {
      *out++ = std::move(*children[index].result);
      index++;
    }

    if (index == count) { // all children completed
      complete(nullptr);
    }
  }
};

template <typename T, boost::asio::execution::executor Executor,
          wait_for_all_output_iterator<T> OutIter>
auto create_wait_for_all_state(Executor ex, size_t count, OutIter out)
{
  using state_type = wait_for_all_state<T, Executor, OutIter>;
  return boost::intrusive_ptr{new state_type(ex, count, out)};
}

} // namespace ceph::async::detail
