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

#include "include/rados/librados.hpp"
#include "librados/librados_asio.h"

#include "rgw_putobj_throttle.h"
#include "rgw_rados.h"

namespace rgw::putobj {

bool Throttle::waiter_ready() const
{
  switch (waiter) {
  case Wait::Available: return is_available();
  case Wait::Completion: return has_completion();
  case Wait::Drained: return is_drained();
  default: return false;
  }
}

void BlockingAioThrottle::aio_cb(void *cb, void *arg)
{
  Pending& p = *static_cast<Pending*>(arg);
  p.result = p.completion->get_return_value();
  p.parent->put(p);
}

ResultList BlockingAioThrottle::submit(rgw_rados_ref& ref,
                                       const rgw_raw_obj& obj,
                                       librados::ObjectWriteOperation *op,
                                       uint64_t cost)
{
  auto p = std::make_unique<Pending>();
  p->obj = obj;
  p->cost = cost;

  if (cost > window) {
    p->result = -EDEADLK; // would never succeed
    completed.push_back(*p);
  } else {
    get(*p);
    p->result = ref.ioctx.aio_operate(ref.oid, p->completion, op);
    if (p->result < 0) {
      put(*p);
    }
  }
  p.release();
  return std::move(completed);
}

ResultList BlockingAioThrottle::submit(rgw_rados_ref& ref,
                                       const rgw_raw_obj& obj,
                                       librados::ObjectReadOperation *op,
                                       bufferlist *data, uint64_t cost)
{
  auto p = std::make_unique<Pending>();
  p->obj = obj;
  p->cost = cost;

  if (cost > window) {
    p->result = -EDEADLK; // would never succeed
    completed.push_back(*p);
  } else {
    get(*p);
    p->result = ref.ioctx.aio_operate(ref.oid, p->completion, op, data);
    if (p->result < 0) {
      put(*p);
    }
  }
  p.release();
  return std::move(completed);
}

void BlockingAioThrottle::get(Pending& p)
{
  std::unique_lock lock{mutex};

  // wait for the size to become available
  pending_size += p.cost;
  if (!is_available()) {
    ceph_assert(waiter == Wait::None);
    waiter = Wait::Available;
    cond.wait(lock, [this] { return is_available(); });
    waiter = Wait::None;
  }

  // register the pending write and attach a completion
  p.parent = this;
  p.completion = librados::Rados::aio_create_completion(&p, nullptr, aio_cb);
  pending.push_back(p);
}

void BlockingAioThrottle::put(Pending& p)
{
  p.completion->release();
  p.completion = nullptr;

  std::scoped_lock lock{mutex};

  // move from pending to completed
  pending.erase(pending.iterator_to(p));
  completed.push_back(p);

  pending_size -= p.cost;

  if (waiter_ready()) {
    cond.notify_one();
  }
}

ResultList BlockingAioThrottle::poll()
{
  std::unique_lock lock{mutex};
  return std::move(completed);
}

ResultList BlockingAioThrottle::wait()
{
  std::unique_lock lock{mutex};
  if (completed.empty() && !pending.empty()) {
    ceph_assert(waiter == Wait::None);
    waiter = Wait::Completion;
    cond.wait(lock, [this] { return has_completion(); });
    waiter = Wait::None;
  }
  return std::move(completed);
}

ResultList BlockingAioThrottle::drain()
{
  std::unique_lock lock{mutex};
  if (!pending.empty()) {
    ceph_assert(waiter == Wait::None);
    waiter = Wait::Drained;
    cond.wait(lock, [this] { return is_drained(); });
    waiter = Wait::None;
  }
  return std::move(completed);
}

#ifdef HAVE_BOOST_CONTEXT

struct YieldingAioThrottle::Handler {
  YieldingAioThrottle *throttle = nullptr;
  Pending *p = nullptr;

  // write callback
  void operator()(boost::system::error_code ec) const {
    p->result = -ec.value();
    throttle->put(*p);
  }
  // read callback
  void operator()(boost::system::error_code ec, bufferlist bl) const {
    p->result = -ec.value();
    throttle->put(*p);
  }
};

template <typename CompletionToken>
auto token_executor(CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, void()> init(token);
  return boost::asio::get_associated_executor(init.completion_handler);
}

ResultList YieldingAioThrottle::submit(rgw_rados_ref& ref,
                                       const rgw_raw_obj& obj,
                                       librados::ObjectWriteOperation *op,
                                       uint64_t cost)
{
  auto p = std::make_unique<Pending>();
  p->cost = cost;
  if (cost > window) {
    p->result = -EDEADLK; // would never succeed
    completed.push_back(*p);
  } else {
    get(*p);
    librados::async_operate(context, ref.ioctx, ref.oid, op, 0,
                            boost::asio::bind_executor(token_executor(yield),
                                                       Handler{this, p.get()}));
  }
  p.release();
  return std::move(completed);
}

ResultList YieldingAioThrottle::submit(rgw_rados_ref& ref,
                                       const rgw_raw_obj& obj,
                                       librados::ObjectReadOperation *op,
                                       bufferlist *data, uint64_t cost)
{
  auto p = std::make_unique<Pending>();
  p->cost = cost;
  if (cost > window) {
    p->result = -EDEADLK; // would never succeed
    completed.push_back(*p);
  } else {
    get(*p);
    librados::async_operate(context, ref.ioctx, ref.oid, op, 0,
                            boost::asio::bind_executor(token_executor(yield),
                                                       Handler{this, p.get()}));
  }
  p.release();
  return std::move(completed);
}

template <typename CompletionToken>
auto YieldingAioThrottle::async_wait(CompletionToken&& token)
{
  using boost::asio::async_completion;
  using Signature = void(boost::system::error_code);
  async_completion<CompletionToken, Signature> init(token);
  completion = Completion::create(context.get_executor(),
                                  std::move(init.completion_handler));
  return init.result.get();
}

void YieldingAioThrottle::get(Pending& p)
{
  // wait for the size to become available
  pending_size += p.cost;
  if (!is_available()) {
    ceph_assert(waiter == Wait::None);
    ceph_assert(!completion);

    boost::system::error_code ec;
    waiter = Wait::Available;
    async_wait(yield[ec]);
  }
  pending.push_back(p);
}

void YieldingAioThrottle::put(Pending& p)
{
  // move from pending to completed
  pending.erase(pending.iterator_to(p));
  completed.push_back(p);

  pending_size -= p.cost;

  if (waiter_ready()) {
    ceph_assert(completion);
    ceph::async::post(std::move(completion), boost::system::error_code{});
    waiter = Wait::None;
  }
}

ResultList YieldingAioThrottle::poll()
{
  return std::move(completed);
}

ResultList YieldingAioThrottle::wait()
{
  if (!has_completion() && !pending.empty()) {
    ceph_assert(waiter == Wait::None);
    ceph_assert(!completion);

    boost::system::error_code ec;
    waiter = Wait::Completion;
    async_wait(yield[ec]);
  }
  return std::move(completed);
}

ResultList YieldingAioThrottle::drain()
{
  if (!is_drained()) {
    ceph_assert(waiter == Wait::None);
    ceph_assert(!completion);

    boost::system::error_code ec;
    waiter = Wait::Drained;
    async_wait(yield[ec]);
  }
  return std::move(completed);
}
#endif // HAVE_BOOST_CONTEXT

} // namespace rgw::putobj
