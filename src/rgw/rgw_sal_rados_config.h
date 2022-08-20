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

#include <list>
#include <memory>
#include <string>
#include "rgw_sal_config.h"

class DoutPrefixProvider;
class optional_yield;
struct RGWRealm;
struct RGWObjVersionTracker;

namespace rgw::sal {

class RadosConfigStore : public ConfigStore {
  struct Impl;
  std::unique_ptr<Impl> impl;
 public:
  explicit RadosConfigStore(std::unique_ptr<Impl> impl);
  virtual ~RadosConfigStore() override;

  // Realm
  virtual int create_realm(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           const RGWRealm& info,
                           RGWObjVersionTracker& objv) override;
  virtual int set_default_realm_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view realm_id,
                                   RGWObjVersionTracker& objv) override;
  virtual int read_default_realm_id(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string& realm_id,
                                    RGWObjVersionTracker& objv) override;
  virtual int delete_default_realm_id(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      RGWObjVersionTracker& objv) override;
  virtual int read_realm(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view realm_id,
                         std::string_view realm_name,
                         RGWRealm& info,
                         RGWObjVersionTracker& objv) override;
  virtual int update_realm(const DoutPrefixProvider* dpp,
                           optional_yield y, const RGWRealm& info,
                           RGWObjVersionTracker& objv) override;
  virtual int rename_realm(const DoutPrefixProvider* dpp,
                           optional_yield y, RGWRealm& info,
                           std::string_view new_name,
                           RGWObjVersionTracker& objv) override;
  virtual int delete_realm(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           const RGWRealm& old_info,
                           RGWObjVersionTracker& objv) override;
  virtual int list_realm_names(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               std::list<std::string>& names) override;

  // factory function
  static auto create(const DoutPrefixProvider* dpp)
      -> std::unique_ptr<RadosConfigStore>;

}; // RadosConfigStore

} // namespace rgw::sal
