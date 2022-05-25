// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_USER_CLIENT_H
#define CEPH_CLS_USER_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include "cls_user_ops.h"
#include "common/RefCountedObj.h"

class RGWGetUserHeader_CB : public RefCountedObject {
public:
  ~RGWGetUserHeader_CB() override {}
  virtual void handle_response(int r, cls_user_header& header) = 0;
};

/*
 * user objclass
 */

void cls_user_set_buckets(librados::ObjectWriteOperation& op, std::list<cls_user_bucket_entry>& entries, bool add);
void cls_user_complete_stats_sync(librados::ObjectWriteOperation& op);
void cls_user_remove_bucket(librados::ObjectWriteOperation& op,  const cls_user_bucket& bucket);
void cls_user_bucket_list(librados::ObjectReadOperation& op,
			  const std::string& in_marker,
			  const std::string& end_marker,
			  int max_entries,
			  std::list<cls_user_bucket_entry>& entries,
			  std::string *out_marker,
			  bool *truncated,
			  int *pret);
void cls_user_get_header(librados::ObjectReadOperation& op, cls_user_header *header, int *pret);
int cls_user_get_header_async(librados::IoCtx& io_ctx, std::string& oid, RGWGetUserHeader_CB *ctx);
void cls_user_reset_stats(librados::ObjectWriteOperation& op);

// accounts

// add an entry to the account's list of users. returns -EUSERS (Too many users)
// if the user count would exceed the given max_users
void cls_account_users_add(librados::ObjectWriteOperation& op,
                           std::string user, uint32_t max_users);
// remove an entry from the account's list of users
void cls_account_users_rm(librados::ObjectWriteOperation& op,
                          std::string user);
// list the users linked to an account
void cls_account_users_list(librados::ObjectReadOperation& op,
                            std::string marker, uint32_t max_entries,
                            std::vector<std::string>& entries,
                            bool *truncated, int *pret);

#endif
