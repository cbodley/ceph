#ifndef CEPH_CLS_VERSION_CLIENT_H
#define CEPH_CLS_VERSION_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include "include/RADOS/RADOS.hpp"
#include "cls_version_ops.h"

#include "include/expected.hpp"
#include "common/async/yield_context.h"

/*
 * version objclass
 */

void cls_version_set(librados::ObjectWriteOperation& op, obj_version& ver);

/* increase anyway */
void cls_version_inc(librados::ObjectWriteOperation& op);

/* conditional increase, return -EAGAIN if condition fails */
void cls_version_inc(librados::ObjectWriteOperation& op, obj_version& ver, VersionCond cond);

void cls_version_read(librados::ObjectReadOperation& op, obj_version *objv);

int cls_version_read(librados::IoCtx& io_ctx, string& oid, obj_version *ver);

void cls_version_check(librados::ObjectOperation& op, obj_version& ver, VersionCond cond);

// New World Order

void cls_version_set(RADOS::WriteOp& op, obj_version& ver);

/* increase anyway */
void cls_version_inc(RADOS::WriteOp& op);

/* conditional increase, return -EAGAIN if condition fails */
void cls_version_inc(RADOS::WriteOp& op, obj_version& ver, VersionCond cond);

void cls_version_read(RADOS::ReadOp& op, obj_version *objv);

boost::system::error_code
cls_version_read(RADOS::RADOS& rados, const RADOS::Object& obj,
		 const RADOS::IOContext& ioc, obj_version *ver);

void cls_version_check(RADOS::ReadOp& op, obj_version& ver, VersionCond cond);
void cls_version_check(RADOS::WriteOp& op, obj_version& ver, VersionCond cond);

#endif
