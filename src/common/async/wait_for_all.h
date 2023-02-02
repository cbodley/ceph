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

#include <concepts>
#include <iterator>
#include <exception>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/asio/this_coro.hpp>
#include "include/scope_guard.h"
#include "detail/wait_for_all.h"

namespace ceph::async {

/// An input iterator whose value type is awaitable<T, Executor>.
template <typename I, typename T, typename Executor>
concept input_awaitable_iterator =
    detail::input_awaitable_iterator<I, T, Executor>;

/// The output iterator value_type expected by wait_for_all() for a given
/// coroutine return type T.
template <typename T>
using wait_for_all_output_t = detail::wait_for_all_output_t<T>;

/// An output iterator to store the results from wait_for_all().
template <typename I, typename T>
concept wait_for_all_output_iterator =
    detail::wait_for_all_output_iterator<I, T>;

/// \brief Spawn the given coroutines and wait for all of them to complete.
///
/// The result of each coroutine is written to the output iterator in
/// corresponding order. If the given coroutines return void, the output
/// iterator's value type is std::exception_ptr. Otherwise the value type is
/// std::pair<std::exception_ptr, T>.
///
/// If wait_for_all() is canceled, all spawned coroutines are also canceled
/// without writing their results to output iterator.
///
/// This class is not thread-safe, so a strand executor should be used in
/// multi-threaded contexts.
///
/// Example:
/// \code
/// awaitable<void> child(task& t);
///
/// awaitable<void> parent(std::span<task> tasks)
/// {
///   std::vector<awaitable<void>> children;
///   for (auto& t : tasks) {
///     children.push_back(child(t));
///   }
///
///   std::vector<std::exception_ptr> results;
///   co_await wait_for_all(children.begin(), children.end(),
///                         std::back_inserter(results));
/// }
/// \endcode
///
/// \param begin Starting iterator for the range of coroutines to spawn
/// \param end Past-the-end iterator for the range of coroutines to spawn
/// \param out Output iterator for coroutine results
///
/// \tparam InIter Input iterator whose value type is awaitable<T, Executor>
/// \tparam Outter Output iterator whose value type depends on T
template <typename InIter, typename OutIter,
          typename T = typename std::iter_value_t<InIter>::value_type,
          typename Executor = typename std::iter_value_t<InIter>::executor_type>
    requires (input_awaitable_iterator<InIter, T, Executor> &&
              boost::asio::execution::executor<Executor> &&
              wait_for_all_output_iterator<OutIter, T>)
auto wait_for_all(InIter begin, InIter end, OutIter out)
    -> boost::asio::awaitable<void, Executor>
{
  const size_t count = std::distance(begin, end);
  if (!count) {
    co_return;
  }
  auto ex = co_await boost::asio::this_coro::executor;

  // allocate the ref-counted completion state with scoped cancellation so that
  // wait_for_all()'s cancellation triggers cancellation of all child coroutines
  auto state = detail::create_wait_for_all_state<T>(ex, count, out);
  const auto state_guard = make_scope_guard([&state] { state->cancel(); });

  // spawn each coroutine with a cancellable completion handler
  size_t i = 0;
  for (auto cr = begin; cr != end; ++cr, ++i) {
    boost::asio::co_spawn(ex, std::move(*cr), state->completion_handler(i));
  }

  // wait for all spawned coroutines to complete
  co_await state->wait();
}

} // namespace ceph::async
