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
#include "rgw_putobj.h"

namespace rgw::putobj {

void AioThrottle::Handle::cancel()
{
  if (pending) {
    parent->cancel(pending);
  }
  if (completion) {
    completion->release();
  }
}

void AioThrottle::Handle::release()
{
  pending = nullptr;
  completion = nullptr;
}

void AioThrottle::aio_cb(void *cb, void *arg)
{
  auto p = static_cast<Pending*>(arg);
  p->result = p->completion->get_return_value();
  p->completion->release();
  p->parent->put(p);
}

void AioThrottle::put(Pending *p)
{
  std::scoped_lock lock{mutex};

  // move from pending to completed
  pending.erase(pending.iterator_to(*p));
  completed.push_back(*p);

  pending_size -= p->size;
  if (is_available()) {
    cond_available.notify_one();
  }
  if (is_empty()) {
    cond_empty.notify_one();
  }
}

void AioThrottle::cancel(Pending *p)
{
  std::scoped_lock lock{mutex};
  pending_size -= p->size;
  pending.erase_and_dispose(pending.iterator_to(*p),
                            std::default_delete<Pending>{});
  if (is_available()) {
    cond_available.notify_one();
  }
  if (is_empty()) {
    cond_empty.notify_one();
  }
}

AioThrottle::Handle AioThrottle::get(Write&& write)
{
  ceph_assert(write.size <= window); // would never succeed

  std::unique_lock lock{mutex};

  // wait for the write size to become available
  pending_size += write.size;
  cond_available.wait(lock, [this] { return is_available(); });

  // register the pending write
  auto p = std::make_unique<Pending>(std::move(write), this);
  auto c = librados::Rados::aio_create_completion(p.get(), nullptr, aio_cb);
  p->completion = c;
  pending.push_back(*p);
  return {this, p.release(), c, std::move(completed)};
}

WriteList AioThrottle::drain()
{
  std::unique_lock lock{mutex};
  cond_available.wait(lock, [this] { return is_empty(); });
  return std::move(completed);
}

} // namespace rgw::putobj
