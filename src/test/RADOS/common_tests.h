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

#include <string>
#include <string_view>

#include <boost/asio/spawn.hpp>

#include "include/RADOS/RADOS.hpp"

std::string get_temp_pool_name(std::string_view prefix = {});

std::int64_t create_pool(RADOS::RADOS& r, std::string_view pname,
			 boost::asio::yield_context y);
