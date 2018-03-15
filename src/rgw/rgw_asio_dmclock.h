// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef RGW_ASIO_DMCLOCK_H
#define RGW_ASIO_DMCLOCK_H

#include <boost/asio.hpp>
#include <boost/beast/core/bind_handler.hpp>
#include "common/ceph_time.h"
#include "dmclock/src/dmclock_server.h"

namespace rgw::dmclock {

using namespace crimson::dmclock;

/// A dmclock request scheduling service for use with boost::asio.
template <typename C, typename R, bool U1=false, uint B=2>
class Service {
  /// abstract request wrapper, needed for Handler type erasure
  class Request;

  using Queue = PullPriorityQueue<C, Request, U1, B>;
  Queue queue; //< dmclock priority queue

  using Clock = ceph::coarse_real_clock;
  using Timer = boost::asio::basic_waitable_timer<Clock>;
  Timer timer; //< timer for the next scheduled request

  using Executor1 = Timer::executor_type;

  /// set a timer to process the next request
  void schedule(const Time& time);

  /// process ready requests, then schedule the next pending request
  void process(const Time& now);

  /// concrete request type that can invoke the completion handler
  template <typename Handler>
  class RequestImpl;

  /// Request factory function
  template <typename Handler>
  auto make_request(R&& request, const Executor1& ex1, Handler&& handler);

 public:
  Service(boost::asio::io_context& context,
          const typename Queue::ClientInfoFunc& client_info_f,
          ceph::timespan idle_age = std::chrono::minutes(10),
          ceph::timespan erase_age = std::chrono::minutes(15),
          ceph::timespan check_time = std::chrono::minutes(6),
          bool allow_limit_break = false,
          double anticipation_timeout = 0.0)
    : queue(client_info_f, idle_age, erase_age, check_time,
            allow_limit_break, anticipation_timeout),
      timer(context)
  {}

  ~Service() {
    cancel();
  }

  using executor_type = Executor1;

  /// return the default executor for async_request() callbacks
  executor_type get_executor() noexcept {
    return timer.get_executor();
  }

  /// result type passed by reference to async_request() callbacks
  using result_type = typename Queue::PullReq::Retn;

  /// submit an async request for dmclock scheduling. the given completion
  /// handler will be invoked when the request is ready or canceled
  template <typename CompletionToken,
            typename Signature = void(boost::system::error_code, result_type&)>
  BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
  async_request(R&& request, const C& client_id, const ReqParams& req_params,
                const Time& time, double addl_cost, CompletionToken&& token);

  /// cancel all queued requests, invoking their completion handlers with an
  /// operation_aborted error and default-constructed result
  void cancel();

  /// cancel all queued requests for a given client, invoking their completion
  /// handler with an operation_aborted error and default-constructed result
  void cancel(const C& client_id);
};

template <typename C, typename R, bool U1, uint B>
class Service<C, R, U1, B>::Request : public R {
 public:
  Request(R&& request) : R(std::move(request)) {}
  virtual ~Request() {}

  virtual void complete(boost::system::error_code ec, result_type&& result) = 0;
};

template <typename C, typename R, bool U1, uint B>
template <typename Handler>
class Service<C, R, U1, B>::RequestImpl : public Request {
  using Executor2 = boost::asio::associated_executor_t<Handler, Executor1>;
  boost::asio::executor_work_guard<Executor1> work1;
  boost::asio::executor_work_guard<Executor2> work2;
  Handler handler;
 public:
  RequestImpl(R&& request, const Executor1& ex1, Handler&& handler)
    : Request(std::move(request)),
      work1(ex1),
      work2(boost::asio::get_associated_executor(handler, ex1)),
      handler(std::move(handler))
  {}

  void complete(boost::system::error_code ec, result_type&& result) override
  {
    // dispatch the completion handler on its executor
    auto ex2 = work2.get_executor();
    auto alloc2 = boost::asio::get_associated_allocator(handler);
    auto f = boost::beast::bind_handler(std::move(handler), ec,
                                        std::move(result));
    ex2.dispatch(std::move(f), alloc2);
    work1.reset();
    work2.reset();
  }
};

template <typename C, typename R, bool U1, uint B>
template <typename Handler>
auto Service<C, R, U1, B>::make_request(R&& request, const Executor1& ex1,
                                        Handler&& handler)
{
  // it would be nice to use Handler's associated allocator for this, but the
  // priority queue interface wants RequestRef = std::unique_ptr<R>, which
  // a) can't provide a custom deleter, and b) needs to outlive the handler
  // itself
  return std::make_unique<RequestImpl<Handler>>(std::move(request), ex1,
                                                std::move(handler));
}

template <typename C, typename R, bool U1, uint B>
template <typename CompletionToken, typename Signature>
BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
Service<C, R, U1, B>::async_request(R&& request, const C& client_id,
                                    const ReqParams& req_params,
                                    const Time& time, double addl_cost,
                                    CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);

  auto ex1 = get_executor();

  // allocate the request and add it to the queue
  auto req = make_request(std::move(request), ex1,
                          std::move(init.completion_handler));
  queue.add_request(std::move(req), client_id, req_params, time, addl_cost);

  // schedule an immediate call to process() on the executor
  schedule(TimeZero);

  return init.result.get();
}

template<typename C, typename R, bool U1, uint B>
void Service<C, R, U1, B>::cancel()
{
  queue.remove_by_req_filter([] (Request&& request) {
      request.complete(boost::asio::error::operation_aborted, result_type{});
      return true;
    });
  timer.cancel();
}

template<typename C, typename R, bool U1, uint B>
void Service<C, R, U1, B>::cancel(const C& client_id)
{
  queue.remove_by_client(client_id, false, [] (Request&& request) {
      request.complete(boost::asio::error::operation_aborted, result_type{});
    });
  schedule(TimeZero);
}

template<typename C, typename R, bool U1, uint B>
void Service<C, R, U1, B>::schedule(const Time& time)
{
  timer.expires_at(Clock::from_double(time));
  timer.async_wait([this] (boost::system::error_code ec) {
      // process requests unless the wait was canceled. note that a canceled
      // wait may execute after this Service destructs
      if (ec != boost::asio::error::operation_aborted) {
        process(get_time());
      }
    });
}

template<typename C, typename R, bool U1, uint B>
void Service<C, R, U1, B>::process(const Time& now)
{
  // must run in the executor. we should only invoke completion handlers if the
  // executor is running
  assert(get_executor().running_in_this_thread());

  for (;;) {
    auto pull = queue.pull_request(now);

    if (pull.is_none()) {
      // no pending requests, cancel the timer
      timer.cancel();
      return;
    }
    if (pull.is_future()) {
      // update the timer based on the future time
      schedule(pull.getTime());
      return;
    }

    // complete the request
    auto& r = pull.get_retn();
    r.request->complete(boost::system::error_code{}, std::move(r));
  }
}

} // namespace rgw::dmclock

#endif // RGW_ASIO_DMCLOCK_H
