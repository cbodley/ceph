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

#include <boost/intrusive/list.hpp>
#include "rgw_common.h"

namespace librados {
class AioCompletion;
}

namespace rgw::putobj {

struct Write {
  rgw_raw_obj obj;
  uint64_t size;
};

struct WriteEntry : Write, boost::intrusive::list_base_hook<> {
  int result = 0;
  WriteEntry(Write&& write) : Write(std::move(write)) {}
  virtual ~WriteEntry() {}
};

// a list of polymorphic entries that frees them on destruction
template <typename T, typename ...Args>
struct List : boost::intrusive::list<T, Args...> {
  List() = default;
  ~List() { this->clear_and_dispose(std::default_delete<T>{}); }
  List(const List&) = default;
  List& operator=(const List&) = default;
  List(List&&) = default;
  List& operator=(List&&) = default;
};

using WriteList = List<WriteEntry>;

// returns the first error code or 0 if all succeeded
inline int check_for_errors(const WriteList& results) {
  for (auto& e : results) {
    if (e.result < 0) {
      return e.result;
    }
  }
  return 0;
}

// a throttle for aio operations that enforces a maximum window on outstanding
// bytes. only supports a single waiter, so all calls to get() and drain() must
// be made from the same thread/strand
class AioThrottle {
  const uint64_t window;
  uint64_t pending_size = 0;

  bool is_available() const { return pending_size <= window; }
  bool is_empty() const { return pending_size == 0; }

  struct Pending : WriteEntry {
    AioThrottle *const parent;
    librados::AioCompletion *completion = nullptr;

    Pending(Write&& write, AioThrottle *parent)
      : WriteEntry(std::move(write)), parent(parent) {}
  };
  List<Pending> pending;
  WriteList completed;

  std::mutex mutex; // TODO: ceph::mutex
  std::condition_variable cond_empty; // notified when is_empty()
  std::condition_variable cond_available; // notified when is_available()

  void put(Pending *p);
  void cancel(Pending *p);

  static void aio_cb(void *cb, void *arg);

 public:
  AioThrottle(uint64_t window) : window(window) {}

  ~AioThrottle() {
    // must drain before destructing
    ceph_assert(pending.empty());
    ceph_assert(completed.empty());
  }

  // completion handle  that contains an AioCompletion with a callback that
  // returns the throttle. after a successful call to aio_operate(), call
  // release() to release ownership of the completion until the callback is
  // made. if release() is not called, the destructor will cancel the
  // completion. the results of previous completed operations are also included
  class Handle {
    AioThrottle *const parent;
    Pending *pending;

    Handle(AioThrottle *parent, Pending *pending,
           librados::AioCompletion *completion, WriteList&& completed)
      : parent(parent), pending(pending), completion(completion),
        completed(std::move(completed))
    {}
    friend class AioThrottle;
   public:
    // cancel if ownership hasn't been released
    ~Handle() { cancel(); }

    // updates the throttle when the write completes
    librados::AioCompletion *completion;
    // list of previous write completions/results
    WriteList completed;

    // release ownership after successfully submitting to aio_operate()
    void release();
    // cancel and free memory that we otherwise would on completion
    void cancel();
  };

  // registers a pending write, waits for the throttle to become available, then
  // returns a handle that includes both the AioCompletion for aio_operate() and
  // a list of results from previous operations
  Handle get(Write&& write);

  // waits for all pending operations to complete and returns their results
  WriteList drain();
};


} // namespace rgw::putobj
