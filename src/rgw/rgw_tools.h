// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_TOOLS_H
#define CEPH_RGW_TOOLS_H

#include <string>

#include "include/types.h"
#include "common/ceph_time.h"
#include "rgw_common.h"

class RGWRados;
class RGWSysObjectCtx;
struct RGWObjVersionTracker;
class optional_yield;

struct obj_version;

int rgw_put_system_obj(RGWRados *rgwstore, const rgw_pool& pool,
                       const string& oid, bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, real_time set_mtime,
                       optional_yield y, map<string, bufferlist> *pattrs = NULL);
int rgw_get_system_obj(RGWSysObjectCtx& obj_ctx, const rgw_pool& pool,
                       const string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker,
                       optional_yield y, real_time *pmtime = nullptr,
                       map<string, bufferlist> *pattrs = nullptr,
                       rgw_cache_entry_info *cache_info = nullptr,
		       boost::optional<obj_version> refresh_version = boost::none);
int rgw_delete_system_obj(RGWRados *rgwstore, const rgw_pool& pool, const string& oid,
                          RGWObjVersionTracker *objv_tracker);

const char *rgw_find_mime_by_ext(string& ext);

void rgw_filter_attrset(map<string, bufferlist>& unfiltered_attrset, const string& check_prefix,
                        map<string, bufferlist> *attrset);

/// indicates whether the current thread is in boost::asio::io_context::run(),
/// used to log warnings if synchronous librados calls are made
extern thread_local bool is_asio_thread;

/// perform the rados operation, using the yield context when given
int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectReadOperation *op, bufferlist* pbl,
                      optional_yield y);
int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectWriteOperation *op, optional_yield y);
int rgw_rados_notify(librados::IoCtx& ioctx, const std::string& oid,
                     bufferlist& bl, uint64_t timeout_ms, bufferlist* pbl,
                     optional_yield y);

int rgw_tools_init(CephContext *cct);
void rgw_tools_cleanup();

#endif
