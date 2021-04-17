// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "include/neorados/RADOS.hpp"
#include "cls/lock/cls_lock_types.h"

namespace neorados::cls::lock {

void lock(WriteOp& op,
          std::string_view name, ClsLockType type,
          std::string_view cookie, std::string_view tag,
          std::string_view description, const utime_t& duration,
          uint8_t flags);

void unlock(WriteOp& op,
            std::string_view name, std::string_view cookie);

void assert_locked(Op& op,
                   std::string_view name, ClsLockType type,
                   std::string_view cookie,
                   std::string_view tag);

} // namespace neorados::cls::lock
