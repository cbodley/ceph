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

#include "rgw_account.h"

#include <algorithm>
#include <fmt/format.h>

#include "common/random_string.h"
#include "common/utf8.h"

#include "rgw_oidc_provider.h"
#include "rgw_role.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::account {

// account ids start with 'RGW' followed by 17 numeric digits
static constexpr std::string_view id_prefix = "RGW";
static constexpr std::size_t id_len = 20;

std::string generate_id(CephContext* cct)
{
  // fill with random numeric digits
  std::string id = gen_rand_numeric(cct, id_len);
  // overwrite the prefix bytes
  std::copy(id_prefix.begin(), id_prefix.end(), id.begin());
  return id;
}

bool validate_id(std::string_view id, std::string* err_msg)
{
  if (id.size() != id_len) {
    if (err_msg) {
      *err_msg = fmt::format("account id must be {} bytes long", id_len);
    }
    return false;
  }
  if (id.compare(0, id_prefix.size(), id_prefix) != 0) {
    if (err_msg) {
      *err_msg = fmt::format("account id must start with {}", id_prefix);
    }
    return false;
  }
  auto suffix = id.substr(id_prefix.size());
  // all remaining bytes must be digits
  constexpr auto digit = [] (int c) { return std::isdigit(c); };
  if (!std::all_of(suffix.begin(), suffix.end(), digit)) {
    if (err_msg) {
      *err_msg = "account id must end with numeric digits";
    }
    return false;
  }
  return true;
}

bool validate_name(std::string_view name, std::string* err_msg)
{
  if (name.empty()) {
    if (err_msg) {
      *err_msg = "account name must not be empty";
    }
    return false;
  }
  // must not contain the tenant delimiter $
  if (name.find('$') != name.npos) {
    if (err_msg) {
      *err_msg = "account name must not contain $";
    }
    return false;
  }
  // must not contain the metadata section delimeter :
  if (name.find(':') != name.npos) {
    if (err_msg) {
      *err_msg = "account name must not contain :";
    }
    return false;
  }
  // must be valid utf8
  if (check_utf8(name.data(), name.size()) != 0) {
    if (err_msg) {
      *err_msg = "account name must be valid utf8";
    }
    return false;
  }
  return true;
}


int create(const DoutPrefixProvider* dpp,
           rgw::sal::Driver* driver,
           AdminOpState& op_state,
           std::string& err_msg,
           RGWFormatterFlusher& flusher,
           optional_yield y)
{
  // validate account name if specified
  if (!op_state.account_name.empty() &&
      !validate_name(op_state.account_name, &err_msg)) {
    return -EINVAL;
  }

  auto info = RGWAccountInfo{
    .tenant = op_state.tenant,
    .name = op_state.account_name,
    .email = op_state.email,
  };

  if (op_state.max_users) {
    info.max_users = *op_state.max_users;
  }
  if (op_state.max_roles) {
    info.max_roles = *op_state.max_roles;
  }
  if (op_state.max_groups) {
    info.max_groups = *op_state.max_groups;
  }
  if (op_state.max_access_keys) {
    info.max_access_keys = *op_state.max_access_keys;
  }
  if (op_state.max_buckets) {
    info.max_buckets = *op_state.max_buckets;
  }

  // account id is optional, but must be valid
  if (op_state.account_id.empty()) {
    info.id = generate_id(dpp->get_cct());
  } else if (!validate_id(op_state.account_id, &err_msg)) {
    return -EINVAL;
  } else {
    info.id = op_state.account_id;
  }

  constexpr RGWAccountInfo* old_info = nullptr;
  constexpr bool exclusive = true;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;
  objv.generate_new_write_ver(dpp->get_cct());

  int ret = driver->store_account(dpp, y, exclusive, info,
                                  old_info, attrs, objv);
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  encode_json("AccountInfo", info, flusher.get_formatter());
  flusher.flush();

  return 0;
}

int modify(const DoutPrefixProvider* dpp,
           rgw::sal::Driver* driver,
           AdminOpState& op_state,
           std::string& err_msg,
           RGWFormatterFlusher& flusher,
           optional_yield y)
{
  int ret = 0;
  RGWAccountInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;
  if (!op_state.account_id.empty()) {
    ret = driver->load_account_by_id(dpp, y, op_state.account_id,
                                     info, attrs, objv);
  } else if (!op_state.account_name.empty()) {
    ret = driver->load_account_by_name(dpp, y, op_state.tenant,
                                       op_state.account_name,
                                       info, attrs, objv);
  } else if (!op_state.email.empty()) {
    ret = driver->load_account_by_email(dpp, y, op_state.email,
                                        info, attrs, objv);
  } else {
    err_msg = "requires --account-id or --account-name or --email";
    return -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }
  const RGWAccountInfo old_info = info;

  if (!op_state.tenant.empty() && op_state.tenant != info.tenant) {
    err_msg = "cannot modify account tenant";
    return -EINVAL;
  }

  if (!op_state.account_name.empty()) {
    // name must be valid
    if (!validate_name(op_state.account_name, &err_msg)) {
      return -EINVAL;
    }
    info.name = op_state.account_name;
  }

  if (!op_state.email.empty()) {
    info.email = op_state.email;
  }

  if (op_state.max_users) {
    info.max_users = *op_state.max_users;
  }
  if (op_state.max_roles) {
    info.max_roles = *op_state.max_roles;
  }
  if (op_state.max_groups) {
    info.max_groups = *op_state.max_groups;
  }
  if (op_state.max_access_keys) {
    info.max_access_keys = *op_state.max_access_keys;
  }
  if (op_state.max_buckets) {
    info.max_buckets = *op_state.max_buckets;
  }

  if (op_state.quota_max_size) {
    info.quota.max_size = *op_state.quota_max_size;
  }
  if (op_state.quota_max_objects) {
    info.quota.max_objects = *op_state.quota_max_objects;
  }
  if (op_state.quota_enabled) {
    info.quota.enabled = *op_state.quota_enabled;
  }

  constexpr bool exclusive = false;

  ret = driver->store_account(dpp, y, exclusive, info, &old_info, attrs, objv);
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  encode_json("AccountInfo", info, flusher.get_formatter());
  flusher.flush();

  return 0;
}

int remove(const DoutPrefixProvider* dpp,
           rgw::sal::Driver* driver,
           AdminOpState& op_state,
           std::string& err_msg,
           RGWFormatterFlusher& flusher,
           optional_yield y)
{
  int ret = 0;
  RGWAccountInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;

  if (!op_state.account_id.empty()) {
    ret = driver->load_account_by_id(dpp, y, op_state.account_id,
                                     info, attrs, objv);
  } else if (!op_state.account_name.empty()) {
    ret = driver->load_account_by_name(dpp, y, op_state.tenant,
                                       op_state.account_name,
                                       info, attrs, objv);
  } else if (!op_state.email.empty()) {
    ret = driver->load_account_by_email(dpp, y, op_state.email,
                                        info, attrs, objv);
  } else {
    err_msg = "requires --account-id or --account-name or --email";
    return -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }

  // make sure the account is empty
  constexpr std::string_view path_prefix; // empty
  const std::string marker; // empty
  constexpr uint32_t max_items = 1;

  rgw::sal::UserList users;
  ret = driver->list_account_users(dpp, y, info.id, info.tenant, path_prefix,
                                   marker, max_items, users);
  if (ret < 0) {
    return ret;
  }
  if (!users.users.empty()) {
    err_msg = "The account cannot be deleted until all users are removed.";
    return -ENOTEMPTY;
  }

  constexpr bool need_stats = false;
  rgw::sal::BucketList buckets;
  ret = driver->list_buckets(dpp, info.id, info.tenant, marker, marker,
                             max_items, need_stats, buckets, y);
  if (ret < 0) {
    return ret;
  }
  if (!buckets.buckets.empty()) {
    err_msg = "The account cannot be deleted until all buckets are removed.";
    return -ENOTEMPTY;
  }

  rgw::sal::RoleList roles;
  ret = driver->list_account_roles(dpp, y, info.id, path_prefix,
                                   marker, max_items, roles);
  if (ret < 0) {
    return ret;
  }
  if (!users.users.empty()) {
    err_msg = "The account cannot be deleted until all roles are removed.";
    return -ENOTEMPTY;
  }

  rgw::sal::GroupList groups;
  ret = driver->list_account_groups(dpp, y, info.id, path_prefix,
                                    marker, max_items, groups);
  if (ret < 0) {
    return ret;
  }
  if (!groups.groups.empty()) {
    err_msg = "The account cannot be deleted until all groups are removed.";
    return -ENOTEMPTY;
  }

  std::vector<RGWOIDCProviderInfo> providers;
  ret = driver->get_oidc_providers(dpp, y, info.id, providers);
  if (ret < 0) {
    return ret;
  }
  if (!providers.empty()) {
    err_msg = "The account cannot be deleted until all OpenIDConnectProviders are removed.";
    return -ENOTEMPTY;
  }

  return driver->delete_account(dpp, y, info, objv);
}

int info(const DoutPrefixProvider* dpp,
         rgw::sal::Driver* driver,
         AdminOpState& op_state,
         std::string& err_msg,
         RGWFormatterFlusher& flusher,
         optional_yield y)
{
  int ret = 0;
  RGWAccountInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;

  if (!op_state.account_id.empty()) {
    ret = driver->load_account_by_id(dpp, y, op_state.account_id,
                                     info, attrs, objv);
  } else if (!op_state.account_name.empty()) {
    ret = driver->load_account_by_name(dpp, y, op_state.tenant,
                                       op_state.account_name,
                                       info, attrs, objv);
  } else if (!op_state.email.empty()) {
    ret = driver->load_account_by_email(dpp, y, op_state.email,
                                        info, attrs, objv);
  } else {
    err_msg = "requires --account-id or --account-name or --email";
    return -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  encode_json("AccountInfo", info, flusher.get_formatter());
  flusher.flush();

  return 0;
}

int stats(const DoutPrefixProvider* dpp,
          rgw::sal::Driver* driver,
          AdminOpState& op_state,
          bool sync_stats,
          bool reset_stats,
          std::string& err_msg,
          RGWFormatterFlusher& flusher,
          optional_yield y)
{
  int ret = 0;
  RGWAccountInfo info;
  rgw::sal::Attrs attrs; // ignored
  RGWObjVersionTracker objv; // ignored

  if (!op_state.account_id.empty()) {
    // look up account by id
    ret = driver->load_account_by_id(dpp, y, op_state.account_id,
                                     info, attrs, objv);
  } else if (!op_state.account_name.empty()) {
    // look up account by tenant/name
    ret = driver->load_account_by_name(dpp, y, op_state.tenant,
                                       op_state.account_name,
                                       info, attrs, objv);
  } else {
    err_msg = "requires account id or name";
    return -EINVAL;
  }
  if (ret < 0) {
    err_msg = "failed to load account";
    return ret;
  }

  const rgw_owner owner = rgw_account_id{info.id};

  if (sync_stats) {
    ret = rgw_sync_all_stats(dpp, y, driver, owner, info.tenant);
    if (ret < 0) {
      err_msg = "failed to sync account stats";
      return ret;
    }
  } else if (reset_stats) {
    ret = driver->reset_stats(dpp, y, owner);
    if (ret < 0) {
      err_msg = "failed to reset account stats";
      return ret;
    }
  }

  RGWStorageStats stats;
  ceph::real_time last_synced;
  ceph::real_time last_updated;
  ret = driver->load_stats(dpp, y, owner, stats,
                           last_synced, last_updated);
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  auto f = flusher.get_formatter();
  f->open_object_section("AccountStats");
  encode_json("stats", stats, f);
  encode_json("last_synced", last_synced, f);
  encode_json("last_updated", last_updated, f);
  f->close_section(); // AccountStats
  flusher.flush();

  return 0;
}

} // namespace rgw::account
