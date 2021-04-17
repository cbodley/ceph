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

#include "lock.h"
#include "cls/lock/cls_lock_ops.h"

namespace neorados::cls::lock {

void lock(WriteOp& op,
          std::string_view name, ClsLockType type,
          std::string_view cookie, std::string_view tag,
          std::string_view description,
          const utime_t& duration, uint8_t flags)
{
  cls_lock_lock_op call;
  call.name = name;
  call.type = type;
  call.cookie = cookie;
  call.tag = tag;
  call.description = description;
  call.duration = duration;
  call.flags = flags;
  bufferlist in;
  encode(call, in);
  op.exec("lock", "lock", in);
}

void unlock(WriteOp& op,
            std::string_view name, std::string_view cookie)
{
  cls_lock_unlock_op call;
  call.name = name;
  call.cookie = cookie;
  bufferlist in;
  encode(call, in);
  op.exec("lock", "unlock", in);
}

void assert_locked(Op& op,
                   std::string_view name, ClsLockType type,
                   std::string_view cookie, std::string_view tag)
{
  cls_lock_assert_op call;
  call.name = name;
  call.type = type;
  call.cookie = cookie;
  call.tag = tag;
  bufferlist in;
  encode(call, in);
  op.exec("lock", "assert_locked", in);
}

} // namespace neorados::cls::lock
