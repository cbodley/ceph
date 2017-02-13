// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
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

/// When the type of Result is not void, storage is provided for it and
/// that result is passed as an additional argument to the handler.
template <typename Result>
struct invoker {
  Result result;
  template <typename Handler>
  void invoke(Handler& handler, boost::system::error_code ec) {
    handler(ec, std::move(result));
  }
};
// specialization for Result=void
template <>
struct invoker<void> {
  template <typename Handler>
  void invoke(Handler& handler, boost::system::error_code ec) {
    handler(ec);
  }
};

/// This is the object that eventually gets post()ed to the io_service on
/// completion in order to call the original handler with the contained results.
/// Inherits from invoker for empty base optimization when Result=void.
template <typename Handler, typename Result>
struct completion_handler : public invoker<Result> {
  Handler handler;
  boost::system::error_code ec;
  boost::asio::io_service::work work;

  completion_handler(Handler& handler, boost::asio::io_service& service)
    : handler(handler), work(service) {
    // static check for CompletionHandler requirements
    using namespace boost::asio;
    BOOST_ASIO_COMPLETION_HANDLER_CHECK(completion_handler, *this) type_check;
  }

  void operator()() {
    this->invoke(handler, ec);
  }

  // forward asio handler hooks to the original handler
  friend void* asio_handler_allocate(size_t size, completion_handler* h) {
    return boost_asio_handler_alloc_helpers::allocate(size, h->handler);
  }
  friend void asio_handler_deallocate(void* pointer, std::size_t size,
                                      completion_handler* h) {
    boost_asio_handler_alloc_helpers::deallocate(pointer, size, h->handler);
  }
  friend bool asio_handler_is_continuation(completion_handler* h) {
    return boost_asio_handler_cont_helpers::is_continuation(h->handler);
  }
  template<class F>
  friend void asio_handler_invoke(F& f, completion_handler* h) {
    boost_asio_handler_invoke_helpers::invoke(f, h->handler);
  }
};

/// Notifier state needed to invoke the handler on completion.
template <typename Handler, typename Result>
struct notifier_state {
  completion_handler<Handler, Result> handler;
  unique_completion_ptr completion; // the AioCompletion

  notifier_state(Handler& h, boost::asio::io_service& service,
                 unique_completion_ptr&& completion)
    : handler(h, service),
      completion(std::move(completion))
  {}
};

/// A minimal unique pointer type for notifier_state that uses the
/// boost_asio_handler_alloc_helpers to manage its memory. These allocators
/// require that the memory is freed before the handler is invoked (so that it
/// can reuse the memory in the continuation instead of allocating again). A
/// notify() is added to coordinate that.
template <typename Handler, typename Result>
struct unique_notifier_ptr {
  using state_type = notifier_state<Handler, Result>;
  state_type *notifier = nullptr;

  /// destroy the notifier and return its memory to the given handler, which
  /// must live outside of the notifier itself
  void destroy(Handler& handler) noexcept {
    notifier->~notifier_state();
    boost_asio_handler_alloc_helpers::deallocate(
        notifier, sizeof(state_type), handler);
  }
 public:
  unique_notifier_ptr(state_type *notifier) noexcept
    : notifier(notifier) {}

  ~unique_notifier_ptr() {
    if (notifier) {
      auto handler = std::move(notifier->handler);
      destroy(handler.handler);
    }
  }

  state_type* operator->() const noexcept { return notifier; }

  /// release handler-allocated memory and post the completion handler to its
  /// associated io_service
  void notify(int ret) {
    // move completion handler out of the memory being freed
    auto handler = std::move(notifier->handler);
    if (ret < 0) {
      handler.ec.assign(-ret, boost::system::system_category());
    }
    // free the handler-allocated memory before invoking it
    destroy(handler.handler);
    notifier = nullptr;
    auto& service = handler.work.get_io_service();
    service.post(std::move(handler));
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
auto make_notifier(boost::asio::io_service& service, Handler& handler)
  -> unique_notifier_ptr<Handler, Result>
{
  // use the handler alloc hook to allocate a notifier
  using state_type = notifier_state<Handler, Result>;
  auto p = boost_asio_handler_alloc_helpers::allocate(
      sizeof(state_type), handler);
  try {
    auto completion = make_completion<Handler, Result>(p);
    using ptr = unique_notifier_ptr<Handler, Result>;
    return ptr{new (p) state_type(handler, service, std::move(completion))};
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
Result async_read(boost::asio::io_service& service, IoCtx& io,
                  const std::string& oid, size_t len, uint64_t off,
                  CompletionToken&& token)
{
  Handler handler{std::forward<CompletionToken>(token)};
  boost::asio::async_result<Handler> result{handler};
  auto notifier = detail::make_notifier<bufferlist>(service, handler);

  int ret = io.aio_read(oid, notifier->completion.get(),
                        &notifier->handler.result, len, off);
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
Result async_write(boost::asio::io_service& service, IoCtx& io,
                   const std::string& oid, bufferlist &bl, size_t len,
                   uint64_t off, CompletionToken&& token)
{
  Handler handler{std::forward<CompletionToken>(token)};
  boost::asio::async_result<Handler> result{handler};
  auto notifier = detail::make_notifier<void>(service, handler);

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
Result async_operate(boost::asio::io_service& service, IoCtx& io,
                     const std::string& oid, ObjectReadOperation *op, int flags,
                     CompletionToken&& token)
{
  Handler handler{std::forward<CompletionToken>(token)};
  boost::asio::async_result<Handler> result{handler};
  auto notifier = detail::make_notifier<bufferlist>(service, handler);

  int ret = io.aio_operate(oid, notifier->completion.get(), op,
                           flags, &notifier->handler.result);
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
Result async_operate(boost::asio::io_service& service, IoCtx& io,
                     const std::string& oid, ObjectWriteOperation *op,
                     int flags, CompletionToken &&token)
{
  Handler handler{std::forward<CompletionToken>(token)};
  boost::asio::async_result<Handler> result{handler};
  auto notifier = detail::make_notifier<void>(service, handler);

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
