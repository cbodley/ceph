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

#include "bucket_index_log.h"

#include <algorithm>

#include "include/rados/librados.hpp"
#include "common/async/yield_context.h"
#include "cls/rgw/cls_rgw_client.h"

#include "rgw_aio_throttle.h"
#include "rgw_bucket_layout.h"

#include "bucket_index.h"
#include "rgw_tools.h"
#include "rgw_zone.h"

namespace rgwrados::bucket_index_log {

/// Read the max marker for a given bucket index shard object.
int max_marker(const DoutPrefixProvider* dpp,
               optional_yield y,
               librados::Rados& rados,
               const rgw::SiteConfig& site,
               const RGWBucketInfo& info,
               const rgw::bucket_log_layout_generation& log,
               int shard,
               std::string& marker)
{
  if (log.layout.type != rgw::BucketLogType::InIndex) {
    return -ENOTSUP;
  }

  rgw_bucket_dir_header header;
  int ret = bucket_index::read_header(dpp, y, rados, site, info,
                                      log.layout.in_index, shard, header);
  if (ret < 0) {
    return ret;
  }

  marker = std::move(header.max_marker);
  return 0;
}

/// Read the max marker from each bucket index shard object.
int max_markers(const DoutPrefixProvider* dpp,
                optional_yield y,
                librados::Rados& rados,
                const rgw::SiteConfig& site,
                const RGWBucketInfo& info,
                const rgw::bucket_log_layout_generation& log,
                std::vector<std::string>& markers)
{
  if (log.layout.type != rgw::BucketLogType::InIndex) {
    return -ENOTSUP;
  }

  std::vector<rgw_bucket_dir_header> headers;
  int ret = bucket_index::read_headers(dpp, y, rados, site, info,
                                       log.layout.in_index, headers);
  if (ret < 0) {
    return ret;
  }

  std::transform(headers.begin(), headers.end(), std::back_inserter(markers),
      [] (rgw_bucket_dir_header& h) { return std::move(h.max_marker); });
  return 0;
}

int trim(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw::SiteConfig& site,
         const RGWBucketInfo& info,
         const rgw::bucket_log_layout_generation& log,
         int shard,
         const std::string& start_marker,
         const std::string& end_marker)
{
  if (log.layout.type != rgw::BucketLogType::InIndex) {
    return -ENOTSUP;
  }
  const rgw::bucket_index_layout_generation& index = log.layout.in_index;
  if (std::cmp_greater_equal(shard, num_shards(index.layout.normal))) {
    return -EDOM; // shard index out of range
  }

  librados::IoCtx ioctx;
  int ret = bucket_index::open_index_pool(dpp, rados, site, info, ioctx);
  if (ret < 0) {
    return ret;
  }

  const auto oid = bucket_index::shard_oid(info.bucket.bucket_id, index.gen,
                                           index.layout.normal, shard);

  librados::ObjectWriteOperation op;
  cls_rgw_bilog_trim(op, start_marker, end_marker);

  return rgw_rados_operate(dpp, ioctx, oid, &op, y);
}

int list(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw::SiteConfig& site,
         const RGWBucketInfo& info,
         const rgw::bucket_log_layout_generation& log,
         int shard,
         const std::string& marker,
         uint32_t max,
         std::list<rgw_bi_log_entry>& entries,
         std::string& next_marker)
{
  if (log.layout.type != rgw::BucketLogType::InIndex) {
    return -ENOTSUP;
  }
  const rgw::bucket_index_layout_generation& index = log.layout.in_index;
  if (std::cmp_greater_equal(shard, num_shards(index.layout.normal))) {
    return -EDOM; // shard index out of range
  }

  librados::IoCtx ioctx;
  int ret = bucket_index::open_index_pool(dpp, rados, site, info, ioctx);
  if (ret < 0) {
    return ret;
  }

  const auto oid = bucket_index::shard_oid(info.bucket.bucket_id, index.gen,
                                           index.layout.normal, shard);

  cls_rgw_bi_log_list_ret reply;
  librados::ObjectReadOperation op;
  cls_rgw_bilog_list(op, marker, max, &reply);

  ret = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  entries = std::move(reply.entries);
  if (reply.truncated && !entries.empty()) {
    next_marker = entries.back().id;
  }
  return 0;
}

int start(const DoutPrefixProvider* dpp,
          optional_yield y,
          librados::Rados& rados,
          const rgw::SiteConfig& site,
          const RGWBucketInfo& info,
          const rgw::bucket_log_layout_generation& log)
{
  if (log.layout.type != rgw::BucketLogType::InIndex) {
    return -ENOTSUP;
  }
  const rgw::bucket_index_layout_generation& index = log.layout.in_index;
  if (index.layout.type != rgw::BucketIndexType::Normal) {
    return -ENOTSUP;
  }

  librados::IoCtx ioctx;
  int ret = bucket_index::open_index_pool(dpp, rados, site, info, ioctx);
  if (ret < 0) {
    return ret;
  }

  // issue up to max_aio requests in parallel
  const auto max_aio = dpp->get_cct()->_conf->rgw_bucket_index_max_aio;
  auto aio = rgw::make_throttle(max_aio, y);
  constexpr uint64_t cost = 1; // 1 throttle unit per request
  constexpr uint64_t id = 0; // ids unused

  constexpr auto is_error = [] (int r) { return r < 0; };

  for (uint32_t shard = 0; shard < num_shards(index.layout.normal); shard++) {
    librados::ObjectWriteOperation op;
    cls_rgw_bi_log_resync(op);

    rgw_raw_obj obj; // obj.pool is empty and unused
    obj.oid = bucket_index::shard_oid(info.bucket.bucket_id, index.gen,
                                      index.layout.normal, shard);

    auto completed = aio->get(obj, rgw::Aio::librados_op(
            ioctx, std::move(op), y), cost, id);
    int r = rgw::check_for_errors(completed, is_error, dpp,
        "failed to write RESYNC entry to index object");
    if (ret == 0) {
      ret = r;
    }
  }

  auto completed = aio->drain();
  int r = rgw::check_for_errors(completed, is_error, dpp,
      "failed to write RESYNC entry to index object");
  if (r < 0) {
    return r;
  }
  return ret;
}

int stop(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw::SiteConfig& site,
         const RGWBucketInfo& info,
         const rgw::bucket_log_layout_generation& log)
{
  if (log.layout.type != rgw::BucketLogType::InIndex) {
    return -ENOTSUP;
  }
  const rgw::bucket_index_layout_generation& index = log.layout.in_index;
  if (index.layout.type != rgw::BucketIndexType::Normal) {
    return -ENOTSUP;
  }

  librados::IoCtx ioctx;
  int ret = bucket_index::open_index_pool(dpp, rados, site, info, ioctx);
  if (ret < 0) {
    return ret;
  }

  // issue up to max_aio requests in parallel
  const auto max_aio = dpp->get_cct()->_conf->rgw_bucket_index_max_aio;
  auto aio = rgw::make_throttle(max_aio, y);
  constexpr uint64_t cost = 1; // 1 throttle unit per request
  constexpr uint64_t id = 0; // ids unused

  constexpr auto is_error = [] (int r) { return r < 0; };

  for (uint32_t shard = 0; shard < num_shards(index.layout.normal); shard++) {
    librados::ObjectWriteOperation op;
    cls_rgw_bi_log_stop(op);

    rgw_raw_obj obj; // obj.pool is empty and unused
    obj.oid = bucket_index::shard_oid(info.bucket.bucket_id, index.gen,
                                      index.layout.normal, shard);

    auto completed = aio->get(obj, rgw::Aio::librados_op(
            ioctx, std::move(op), y), cost, id);
    int r = rgw::check_for_errors(completed, is_error, dpp,
        "failed to write SYNCSTOP entry to index object");
    if (ret == 0) {
      ret = r;
    }
  }

  auto completed = aio->drain();
  int r = rgw::check_for_errors(completed, is_error, dpp,
      "failed to write SYNCSTOP entry to index object");
  if (r < 0) {
    return r;
  }
  return ret;
}

} // namespace rgwrados::bucket_index_log
