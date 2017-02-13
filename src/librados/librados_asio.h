// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef LIBRADOS_ASIO_H
#define LIBRADOS_ASIO_H

#include <memory>
#include <boost/asio.hpp>
#include "include/rados/librados.hpp"

/// Defines librados operations for use in boost::asio asynchronous contexts.

namespace librados {

namespace detail {

/// unique_ptr with custom deleter for AioCompletion
struct release_completion {
  void operator()(AioCompletion *c) { c->release(); }
};
using unique_completion_ptr =
    std::unique_ptr<AioCompletion, release_completion>;

/// invoker knows how/where to invoke a handler within an asio context.
/// When the type of Result is not void, storage is provided for it and
/// that result is passed as an additional argument to the handler.
template <typename Result>
struct invoker {
  Result result;
  template <typename Handler>
  void invoke(Handler& handler, boost::system::error_code ec) {
    boost_asio_handler_invoke_helpers::invoke(
        std::bind(handler, ec, std::move(result)), handler);
  }
};
// specialization for Result=void
template <>
struct invoker<void> {
  template <typename Handler>
  void invoke(Handler& handler, boost::system::error_code ec) {
    boost_asio_handler_invoke_helpers::invoke(
        std::bind(handler, ec), handler);
  }
};

/// Notifier state needed to invoke the handler on completion.
template <typename Handler, typename Result>
struct notifier_state : public invoker<Result> {
  Handler handler;
  unique_completion_ptr completion;

  notifier_state(Handler& handler, unique_completion_ptr&& completion)
    : handler(handler),
      completion(std::move(completion))
  {}
};

/// A minimal unique pointer type for notifier_state that uses the
/// boost_asio_handler_alloc_helpers to manage its memory. These allocators
/// require that the memory is freed before the handler is invoked (so that it
/// can reuse the memory in the continuation instead of allocating again), so
/// a notify() function is provided to coordinate that.
template <typename Handler, typename Result>
struct unique_notifier_ptr {
  using state_type = notifier_state<Handler, Result>;
  state_type *notifier = nullptr;

  void delete_notifier(Handler& handler) {
    notifier->~notifier_state();
    boost_asio_handler_alloc_helpers::deallocate(
        notifier, sizeof(state_type), handler);
  }
 public:
  unique_notifier_ptr(state_type *notifier) noexcept
    : notifier(notifier) {}

  ~unique_notifier_ptr() {
    if (notifier) {
      Handler handler{std::move(notifier->handler)};
      delete_notifier(handler);
    }
  }

  state_type* operator->() const noexcept { return notifier; }

  /// free the handler-allocated state and invoke the handler
  void notify(int ret) {
    assert(notifier);
    // move notifier state out of the memory being destroyed
    state_type state{std::move(*notifier)};
    delete_notifier(state.handler);
    notifier = nullptr;
    // invoke handler after memory is released
    boost::system::error_code ec;
    if (ret < 0) {
      ec.assign(-ret, boost::system::system_category());
    }
    state.invoke(state.handler, ec);
  }

  /// release ownership of the state
  state_type* release() noexcept {
    auto state = notifier;
    notifier = nullptr;
    return state;
  }
};

/// AioCompletion callback function
template <typename Handler, typename Result>
inline void aio_notify(completion_t cb, void *arg)
{
  // reclaim ownership of the notifier state
  using ptr = unique_notifier_ptr<Handler, Result>;
  using state_type = notifier_state<Handler, Result>;
  ptr notifier{static_cast<state_type*>(arg)};
  // destroy the state and invoke the completion handler
  const int ret = notifier->completion->get_return_value();
  notifier.notify(ret);
}

/// Create an AioCompletion and return it in a unique_ptr
template <typename Handler, typename Result>
inline unique_completion_ptr make_completion(void *state)
{
  using ptr = unique_completion_ptr;
  return ptr{Rados::aio_create_completion(state, nullptr,
                                          aio_notify<Handler, Result>)};
}

/// Allocate a completion notifier using the asio handler allocator
template <typename Result, typename Handler>
unique_notifier_ptr<Handler, Result> make_notifier(Handler& handler)
{
  // use the handler alloc hook to allocate a notifier
  using state_type = notifier_state<Handler, Result>;
  auto p = boost_asio_handler_alloc_helpers::allocate(
      sizeof(state_type), handler);
  try {
    auto completion = make_completion<Handler, Result>(p);
    using ptr = unique_notifier_ptr<Handler, Result>;
    return ptr{new (p) state_type(handler, std::move(completion))};
  } catch (...) {
    boost_asio_handler_alloc_helpers::deallocate(
        p, sizeof(state_type), handler);
    throw;
  }
}

// Type aliases to help apply the customization points provided by the
// boost::asio::handler_type<> and boost::asio::async_result<> templates.
// These are what allow you to pass in a handler of 'use_future', for
// example, and get a std::future<> back as a return value.
template <typename Handler, typename Signature>
using handler_t = typename boost::asio::handler_type<Handler, Signature>::type;
template <typename Handler>
using result_t = typename boost::asio::async_result<Handler>::type;

} // namespace detail


/// Calls IoCtx::aio_read() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code, bufferlist).
template <typename CompletionToken,
          typename Signature = void(boost::system::error_code, bufferlist),
          typename Handler = detail::handler_t<CompletionToken, Signature>,
          typename Result = detail::result_t<Handler>>
Result async_read(IoCtx& io, const std::string& oid, size_t len, uint64_t off,
                  CompletionToken&& token)
{
  Handler handler{std::forward<CompletionToken>(token)};
  boost::asio::async_result<Handler> result{handler};
  auto notifier = detail::make_notifier<bufferlist>(handler);

  int ret = io.aio_read(oid, notifier->completion.get(),
                        &notifier->result, len, off);
  if (ret < 0) {
    notifier.notify(ret);
  } else {
    notifier.release(); // release ownership until completion
  }
  return result.get();
}

/// Calls IoCtx::aio_write() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code).
template <typename CompletionToken,
          typename Signature = void(boost::system::error_code),
          typename Handler = detail::handler_t<CompletionToken, Signature>,
          typename Result = detail::result_t<Handler>>
Result async_write(IoCtx& io, const std::string& oid, bufferlist &bl,
                   size_t len, uint64_t off, CompletionToken&& token)
{
  Handler handler{std::forward<CompletionToken>(token)};
  boost::asio::async_result<Handler> result{handler};
  auto notifier = detail::make_notifier<void>(handler);

  int ret = io.aio_write(oid, notifier->completion.get(), bl, len, off);
  if (ret < 0) {
    notifier.notify(ret);
  } else {
    notifier.release(); // release ownership until completion
  }
  return result.get();
}

/// Calls IoCtx::aio_operate() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code, bufferlist).
template <typename CompletionToken,
          typename Signature = void(boost::system::error_code, bufferlist),
          typename Handler = detail::handler_t<CompletionToken, Signature>,
          typename Result = detail::result_t<Handler>>
Result async_operate(IoCtx& io, const std::string& oid,
                     ObjectReadOperation *op, int flags,
                     CompletionToken&& token)
{
  Handler handler{std::forward<CompletionToken>(token)};
  boost::asio::async_result<Handler> result{handler};
  auto notifier = detail::make_notifier<bufferlist>(handler);

  int ret = io.aio_operate(oid, notifier->completion.get(), op,
                           flags, &notifier->result);
  if (ret < 0) {
    notifier.notify(ret);
  } else {
    notifier.release(); // release ownership until completion
  }
  return result.get();
}

/// Calls IoCtx::aio_operate() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code).
template <typename CompletionToken,
          typename Signature = void(boost::system::error_code),
          typename Handler = detail::handler_t<CompletionToken, Signature>,
          typename Result = detail::result_t<Handler>>
Result async_operate(IoCtx& io, const std::string& oid,
                     ObjectWriteOperation *op, int flags,
                     CompletionToken &&token)
{
  Handler handler{std::forward<CompletionToken>(token)};
  boost::asio::async_result<Handler> result{handler};
  auto notifier = detail::make_notifier<void>(handler);

  int ret = io.aio_operate(oid, notifier->completion.get(), op, flags);
  if (ret < 0) {
    notifier.notify(ret);
  } else {
    notifier.release(); // release ownership until completion
  }
  return result.get();
}

} // namespace librados

#endif // LIBRADOS_ASIO_H
