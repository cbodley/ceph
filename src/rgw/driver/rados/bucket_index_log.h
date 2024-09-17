// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "include/rados/librados_fwd.hpp"

class DoutPrefixProvider;
class optional_yield;
struct rgw_bi_log_entry;
class RGWBucketInfo;

namespace rgw {
struct bucket_log_layout_generation;
class SiteConfig;
}

namespace rgwrados::bucket_index_log {

/// Read the max marker for a given bucket index shard object.
int max_marker(const DoutPrefixProvider* dpp,
               optional_yield y,
               librados::Rados& rados,
               const rgw::SiteConfig& site,
               const RGWBucketInfo& info,
               const rgw::bucket_log_layout_generation& log,
               int shard,
               std::string& marker);

/// Read the max marker from each bucket index shard object.
int max_markers(const DoutPrefixProvider* dpp,
                optional_yield y,
                librados::Rados& rados,
                const rgw::SiteConfig& site,
                const RGWBucketInfo& info,
                const rgw::bucket_log_layout_generation& log,
                std::vector<std::string>& markers);

} // namespace rgwrados::bucket_index_log
