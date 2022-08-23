// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <span>
#include <string>

namespace rgw { namespace sal {

  class Store;
  class User;
  class Bucket;
  class BucketList;
  class Object;
  class MultipartUpload;
  class Lifecycle;
  class Notification;
  class Writer;
  class PlacementTier;
  class ZoneGroup;
  class Zone;
  class LuaManager;
  struct RGWRoleInfo;
  class ConfigStore;
  class ReloadPauser;

  /// Results of a listing operation
  template <typename T>
  struct ListResult {
    /// The subspan of the input entries that were intialized with results
    std::span<T> entries;
    /// The next marker to resume listing, or empty
    std::string next;
  };

} } // namespace rgw::sal
