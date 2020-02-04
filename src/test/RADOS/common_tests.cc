// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <cstring>
#include <string>
#include <string_view>

#include <unistd.h>

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>

#include <boost/asio/spawn.hpp>

#include <fmt/format.h>

#include "common_tests.h"
#include "include/RADOS/RADOS.hpp"

namespace ba = boost::asio;
namespace R = RADOS;

std::string get_temp_pool_name(std::string_view prefix)
{
  char hostname[80];
  static int num = 1;
  std::memset(hostname, 0, sizeof(hostname));
  gethostname(hostname, sizeof(hostname) - 1);
  return fmt::format("{}{}-{}-{}", prefix, hostname, getpid(), num++);
}

std::int64_t create_pool(R::RADOS& r, std::string_view pname,
			 ba::yield_context y)
{
  r.create_pool(pname, std::nullopt, y);
  return r.lookup_pool(pname, y);
}
