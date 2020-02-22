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

#include <fmt/format.h>

#include "common_tests.h"

std::string get_temp_pool_name(std::string_view prefix)
{
  char hostname[80];
  static int num = 1;
  std::memset(hostname, 0, sizeof(hostname));
  gethostname(hostname, sizeof(hostname) - 1);
  return fmt::format("{}{}-{}-{}", prefix, hostname, getpid(), num++);
}
