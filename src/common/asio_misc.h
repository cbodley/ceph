// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_ASIO_MISC_H
#define CEPH_COMMON_ASIO_MISC_H

#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <thread>
#include <vector>

#include <boost/asio.hpp>

#include "common/detail/construct_suspended.h"

#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/config.h"
#include "common/dout.h"

#include "include/ceph_assert.h"

namespace ceph {
class io_context_pool {
  const std::size_t n;
  std::vector<std::thread> t;
  boost::asio::io_context c;
  std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> w;
  ceph::mutex m = make_mutex("ceph::io_context_pool::m");

  void cleanup() noexcept {
    w = std::nullopt;
    for (auto& th : t) {
      th.join();
    }
    t.clear();
  }
public:
  io_context_pool(CephContext* cct, construct_suspended_t) noexcept
    : n(cct->_conf.get_val<std::uint64_t>("osdc_thread_count")) {}
  io_context_pool(CephContext* cct) noexcept
    : n(cct->_conf.get_val<std::uint64_t>("osdc_thread_count")) {
    start();
  }
  io_context_pool(CephContext* cct, std::size_t n, construct_suspended_t) noexcept
    : n(n) {}
  io_context_pool(CephContext* cct, std::size_t n) noexcept
    : n(n) {
    start();
  }
  ~io_context_pool() {
    stop();
  }
  void start() noexcept {
    auto l = std::scoped_lock(m);
    if (t.empty()) {
      w.emplace(boost::asio::make_work_guard(c));
      c.restart();
      for (std::size_t i = 0; i < n; ++i) {
	t.emplace_back([this] {
			 try {
			   c.run();
			 } catch (const std::exception& e) {
			   ceph_abort_msg(e.what());
			 }
		       });
      }
    }
  }
  void finish() noexcept {
    auto l = std::scoped_lock(m);
    if (!t.empty()) {
      cleanup();
    }
  }
  void stop() noexcept {
    auto l = std::scoped_lock(m);
    if (!t.empty()) {
      c.stop();
      cleanup();
    }
  }

  boost::asio::io_context& get_io_context() {
    return c;
  }
  operator boost::asio::io_context&() {
    return c;
  }
  decltype(auto) get_executor() {
    return c.get_executor();
  }
};
}

#endif // CEPH_COMMON_ASIO_MISC_H
