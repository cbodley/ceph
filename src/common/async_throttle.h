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

#ifndef CEPH_ASYNC_THROTTLE_H
#define CEPH_ASYNC_THROTTLE_H

#include <boost/asio.hpp>
#include <boost/beast/core/bind_handler.hpp>
#include <boost/intrusive/list.hpp>


namespace ceph::async {

/**
 * An asynchronous throttle implementation for use with boost::asio.
 *
 * Example use:
 *
 *   // process a list of jobs, running up to 10 jobs in parallel
 *   boost::asio::io_context context;
 *   ceph::async::Throttle throttle(context.get_executor(), 10);
 *
 *   for (auto& job : jobs) {
 *     // request a throttle unit for this job
 *     throttle.async_get(1, [&] (boost::system::error_code ec) {
 *         if (!ec) { // throttle was granted
 *           job.process();
 *           throttle.put(1);
 *         }
 *       });
 *   }
 *   context.run();
 */
template <typename ExecutorT>
class Throttle {
  /// wrap the executor in a strand so we don't need a mutex for shared state
  using Executor1 = boost::asio::strand<ExecutorT>;
 public:
  Throttle(const boost::asio::strand<ExecutorT>& ex, size_t maximum)
      : ex1(ex), maximum(maximum) {}

  Throttle(const ExecutorT& ex, size_t maximum)
    : ex1(ex), maximum(maximum) {}

  ~Throttle() {
    on_cancel();
  }

  Throttle(const Throttle&) = delete;
  Throttle& operator=(const Throttle&) = delete;

  Throttle(Throttle&&) = default;
  Throttle& operator=(Throttle&&) = default;

  /// returns the strand executor responsible for intermediate work
  Executor1 get_executor() const noexcept { return ex1; }

  /// requests a number of throttle units, invoking the given completion handler
  /// with a successful error code once those units are available
  template <typename CompletionToken,
            typename Signature = void(boost::system::error_code)>
  BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
    async_get(size_t count, CompletionToken&& token);

  /// returns a number of previously-granted throttle units, then attempts to
  /// grant the next pending requests
  void put(size_t count);

  /// adjust the maximum throttle, invoking the given completion handler once
  /// the number of granted throttle units are at or below the new maximum
  /// value. if a previous call to async_set_maximum() is still pending, its
  /// completion handler is invoked with operation_aborted. if pending calls to
  /// async_get() can be satisfied by the new maximum throttle, their completion
  /// handlers are invoked
  template <typename CompletionToken,
            typename Signature = void(boost::system::error_code)>
  BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
    async_set_maximum(size_t count, CompletionToken&& token);

  /// cancel any pending calls to async_get() and async_set_maximum(), invoking
  /// their completion handlers with operation_aborted
  void cancel();

 private:
  Executor1 ex1; //< strand executor for all access to shared state
  size_t value = 0; //< amount of outstanding throttle
  size_t maximum; //< maximum amount of throttle

  /// virtual base provides type-erasure on the Handler
  class Request : public boost::intrusive::list_base_hook<> {
   public:
    const size_t count; //< number of throttle units requested
    explicit Request(size_t count) : count(count) {}

    /// complete the request and dispose of its memory
    virtual void complete(boost::system::error_code ec) = 0;
   protected:
    /// destructor is protected because you should destroy with complete()
    virtual ~Request() {}
  };
  /// maintain a list of throttle requests that we're not yet able to satisfy
  boost::intrusive::list<Request> pending_requests;

  /// allow a single request to adjust the maximum throttle
  Request *pending_max_request = nullptr;

  /// the concrete request type that knows how to invoke its completion handler
  template <typename Handler>
  class RequestImpl;

  // private member functions must be run within the strand's execution context

  template <typename Handler, typename Executor2>
  void on_set_maximum(size_t count, Handler&& handler,
                      boost::asio::executor_work_guard<Executor2>&& work2);

  template <typename Handler, typename Executor2>
  void on_get(size_t count, Handler&& handler,
              boost::asio::executor_work_guard<Executor2>&& work2);

  void on_put(size_t count);

  void on_cancel();

  void grant_pending();
};

// deduction guides
template <typename ExecutorT>
Throttle(const boost::asio::strand<ExecutorT>&, size_t) -> Throttle<ExecutorT>;

template <typename ExecutorT>
Throttle(const ExecutorT&, size_t) -> Throttle<ExecutorT>;


template <typename ExecutorT>
template <typename CompletionToken, typename Signature>
BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
Throttle<ExecutorT>::async_get(size_t count, CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);

  auto& handler = init.completion_handler;
  auto ex2 = boost::asio::get_associated_executor(handler, ex1);
  auto alloc2 = boost::asio::get_associated_allocator(handler);

  // maintain work on ex2 while we're queued on ex1
  auto work2 = boost::asio::make_work_guard(ex2);

  // dispatch on_get() on the strand executor
  auto f = [this, count, handler=std::move(handler),
            work2=std::move(work2)] () mutable {
    on_get(count, std::move(handler), std::move(work2));
  };
  ex1.dispatch(f, alloc2);

  return init.result.get();
}

template <typename ExecutorT>
void Throttle<ExecutorT>::put(size_t count)
{
  // post on_put() to the strand executor
  std::allocator<void> alloc1;
  ex1.post([this, count] { on_put(count); }, alloc1);
}

template <typename ExecutorT>
template <typename CompletionToken, typename Signature>
BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
Throttle<ExecutorT>::async_set_maximum(size_t count, CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);

  auto& handler = init.completion_handler;
  auto ex2 = boost::asio::get_associated_executor(handler, ex1);
  auto alloc2 = boost::asio::get_associated_allocator(handler);
  auto work2 = boost::asio::make_work_guard(ex2);

  // dispatch on_set_maximum() on the strand executor
  auto f = [this, count, handler=std::move(handler),
            work2=std::move(work2)] () mutable {
    on_set_maximum(count, std::move(handler), std::move(work2));
  };
  ex1.dispatch(f, alloc2);

  return init.result.get();
}

template <typename ExecutorT>
void Throttle<ExecutorT>::cancel()
{
  // dispatch on_cancel() on the strand executor
  std::allocator<void> alloc1;
  ex1.dispatch([this] { on_cancel(); }, alloc1);
}


template <typename ExecutorT>
template <typename Handler>
class Throttle<ExecutorT>::RequestImpl : public Request {
 private:
  using Executor2 = boost::asio::associated_executor_t<Handler, Executor1>;
  boost::asio::executor_work_guard<Executor1> work1;
  boost::asio::executor_work_guard<Executor2> work2;
  Handler handler;

  /// constructor is private, use the factory function create()
  RequestImpl(size_t count, const Handler& handler, const Executor1& ex1)
    : Request(count),
      work1(ex1),
      work2(boost::asio::get_associated_executor(handler, ex1)),
      handler(handler)
  {}

  /// Defines a scoped 'RequestImpl::ptr' type that uses Handler's associated
  /// allocator to manage its memory
  BOOST_ASIO_DEFINE_HANDLER_PTR(RequestImpl);

 public:
  static RequestImpl* create(size_t count, Handler& handler,
                             const Executor1& ex1) {
    ptr p = {std::addressof(handler), ptr::allocate(handler), 0};
    p.p = new (p.v) RequestImpl(count, handler, ex1);
    auto result = p.p;
    p.v = p.p = nullptr; // release ownership to the caller
    return result;
  }

  void complete(boost::system::error_code ec) override {
    // move the work and completion handler out of the memory being freed
    auto w1 = std::move(work1);
    auto w2 = std::move(work2);
    auto f = std::move(handler);
    // destroy 'this' and release its memory back to the handler allocator
    ptr{std::addressof(f), this, this}.reset();
    // dispatch the completion handler on its executor
    auto ex2 = w2.get_executor();
    auto alloc2 = boost::asio::get_associated_allocator(f);
    ex2.dispatch(boost::beast::bind_handler(std::move(f), ec), alloc2);
  }
};

template <typename ExecutorT>
template <typename Handler, typename Executor2>
void Throttle<ExecutorT>::on_set_maximum(size_t count, Handler&& handler,
                                         boost::asio::executor_work_guard<Executor2>&& work2)
{
  maximum = count;

  if (pending_max_request) {
    auto tmp = pending_max_request;
    pending_max_request = nullptr;
    tmp->complete(boost::asio::error::operation_aborted);
  }

  if (value <= maximum) {
    // complete successfully
    auto ex2 = boost::asio::get_associated_executor(handler, ex1);
    auto alloc2 = boost::asio::get_associated_allocator(handler);
    boost::system::error_code ec;
    ex2.post(boost::beast::bind_handler(std::move(handler), ec), alloc2);
  } else {
    // allocate a Request for later invocation
    pending_max_request = RequestImpl<Handler>::create(count, handler, ex1);
  }

  grant_pending();
}

template <typename ExecutorT>
template <typename Handler, typename Executor2>
void Throttle<ExecutorT>::on_get(size_t count, Handler&& handler,
                                 boost::asio::executor_work_guard<Executor2>&& work2)
{
  // if the throttle is available, grant it
  if (pending_requests.empty() && value + count <= maximum) {
    value += count;

    auto ex2 = boost::asio::get_associated_executor(handler, ex1);
    auto alloc2 = boost::asio::get_associated_allocator(handler);
    boost::system::error_code ec;
    ex2.post(boost::beast::bind_handler(std::move(handler), ec), alloc2);
    return;
  }

  // allocate a Request and add it to the pending list
  auto request = RequestImpl<Handler>::create(count, handler, ex1);
  // transfer ownership to the list (push_back doesn't throw)
  pending_requests.push_back(*request);
}

template <typename ExecutorT>
void Throttle<ExecutorT>::on_put(size_t count)
{
  assert(value >= count);
  value -= count;

  if (pending_max_request && value <= maximum) {
    auto tmp = pending_max_request;
    pending_max_request = nullptr;
    boost::system::error_code ec;
    tmp->complete(ec);
  }

  grant_pending();
}

template <typename ExecutorT>
void Throttle<ExecutorT>::on_cancel()
{
  while (!pending_requests.empty()) {
    auto& request = pending_requests.front();
    pending_requests.pop_front();
    request.complete(boost::asio::error::operation_aborted);
  }
  if (pending_max_request) {
    auto tmp = pending_max_request;
    pending_max_request = nullptr;
    tmp->complete(boost::asio::error::operation_aborted);
  }
}

template <typename ExecutorT>
void Throttle<ExecutorT>::grant_pending()
{
  boost::system::error_code ec;
  for (auto i = pending_requests.begin(); i != pending_requests.end(); ) {
    auto& request = *i;
    if (value + request.count > maximum) {
      return;
    }
    value += request.count;
    i = pending_requests.erase(i);
    request.complete(ec);
  }
}

} // namespace ceph::async

#endif // CEPH_ASYNC_THROTTLE_H
