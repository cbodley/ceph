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
#include <span>
#include <string>
#include <vector>

#include "include/rados/librados_fwd.hpp"

class DoutPrefixProvider;
class optional_yield;
struct rgw_bucket_dir_header;
struct rgw_cls_list_ret;
struct rgw_obj_index_key;
class RGWBucketInfo;

namespace rgw {
struct bucket_index_layout_generation;
struct bucket_index_normal_layout;
class SiteConfig;
}

namespace rgwrados::bucket_index {

/// Format the rados object name for a shard of the index layout generation.
auto shard_oid(std::string_view bucket_id, uint64_t gen,
               const rgw::bucket_index_normal_layout& index, uint32_t shard)
    -> std::string;

/// Initialize an IoCtx for the given bucket's index pool. The pool is created
/// if it doesn't exist.
int open_index_pool(const DoutPrefixProvider* dpp,
                    librados::Rados& rados,
                    const rgw::SiteConfig& site,
                    const RGWBucketInfo& info,
                    librados::IoCtx& ioctx);

/// Initialize all of the index shard objects using exclusive create.
///
/// If judge_support_logrecord is true, issue the cls_rgw_bucket_init_index2()
/// op to detect whether the OSD supports the reshard log.
int init(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw::SiteConfig& site,
         const RGWBucketInfo& info,
         const rgw::bucket_index_layout_generation& index,
         bool judge_support_logrecord = false);

/// Remove all of the index shard objects for the given layout.
int clean(const DoutPrefixProvider *dpp,
          optional_yield y,
          librados::Rados& rados,
          const rgw::SiteConfig& site,
          const RGWBucketInfo& info,
          const rgw::bucket_index_layout_generation& index);

/// Fetch the next entries from each index shard object.
int list_objects(const DoutPrefixProvider* dpp,
                 optional_yield y,
                 librados::IoCtx& ioctx,
                 std::span<const std::string> shard_oids,
                 const rgw_obj_index_key& start_obj,
                 const std::string& filter_prefix,
                 const std::string& delimiter,
                 uint32_t num_entries,
                 bool list_versions,
                 std::span<rgw_cls_list_ret> results);

/// Set the tag timeout on all of the index shards for the given layout.
int set_tag_timeout(const DoutPrefixProvider *dpp,
                    optional_yield y,
                    librados::Rados& rados,
                    const rgw::SiteConfig& site,
                    const RGWBucketInfo& info,
                    const rgw::bucket_index_layout_generation& index,
                    uint64_t timeout);

/// Read the dir header for a given index shard object.
int read_header(const DoutPrefixProvider *dpp,
                optional_yield y,
                librados::Rados& rados,
                const rgw::SiteConfig& site,
                const RGWBucketInfo& info,
                const rgw::bucket_index_layout_generation& index,
                int shard,
                rgw_bucket_dir_header& header);

/// Read the dir headers from all of the index shards for the given layout.
int read_headers(const DoutPrefixProvider *dpp,
                 optional_yield y,
                 librados::Rados& rados,
                 const rgw::SiteConfig& site,
                 const RGWBucketInfo& info,
                 const rgw::bucket_index_layout_generation& index,
                 std::vector<rgw_bucket_dir_header>& headers);

} // namespace rgwrados::bucket_index
