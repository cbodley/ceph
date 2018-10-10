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

#pragma once

#include <memory>
#include "common/ceph_mutex.h"
#include "rgw_putobj_aio.h"

namespace librados {
class AioCompletion;
}

namespace rgw::putobj {

class Throttle {
 protected:
  const uint64_t window;
  uint64_t pending_size = 0;

  ResultList pending;
  ResultList completed;

  bool is_available() const { return pending_size <= window; }
  bool has_completion() const { return !completed.empty(); }
  bool is_drained() const { return pending.empty(); }

  enum class Wait { None, Available, Completion, Drained };
  Wait waiter = Wait::None;

  bool waiter_ready() const;

 public:
  Throttle(uint64_t window) : window(window) {}

  ~Throttle() {
    // must drain before destructing
    ceph_assert(pending.empty());
    ceph_assert(completed.empty());
  }
};

// a throttle for aio operations. all public functions must be called from
// the same thread
class BlockingAioThrottle final : public Aio, private Throttle {
  ceph::mutex mutex = ceph::make_mutex("AioThrottle");
  ceph::condition_variable cond;

  struct Pending : ResultEntry {
    BlockingAioThrottle *parent = nullptr;
    uint64_t cost = 0;
    librados::AioCompletion *completion = nullptr;
  };

  void get(Pending& p);
  void put(Pending& p);

  // aio completion callback that calls put()
  static void aio_cb(void *cb, void *arg);

 public:
  BlockingAioThrottle(uint64_t window) : Throttle(window) {}


  ResultList submit(rgw_rados_ref& ref, const rgw_raw_obj& obj,
                    librados::ObjectReadOperation *op, bufferlist *data,
                    uint64_t cost) override final;

  ResultList submit(rgw_rados_ref& ref, const rgw_raw_obj& obj,
                    librados::ObjectWriteOperation *op,
                    uint64_t cost) override final;

  ResultList poll() override final;

  ResultList wait() override final;

  ResultList drain() override final;
};

} // namespace rgw::putobj
