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

#include "rgw_sal_config.h"

namespace rgw::sal {

class DBConfigStore : public ConfigStore {
 public:
  virtual int write_default_realm_id(const DoutPrefixProvider* dpp,
                                     optional_yield y, bool exclusive,
                                     std::string_view realm_id) override;
  virtual int read_default_realm_id(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string& realm_id) override;
  virtual int delete_default_realm_id(const DoutPrefixProvider* dpp,
                                      optional_yield y) override;

  virtual int create_realm(const DoutPrefixProvider* dpp,
                           optional_yield y, bool exclusive,
                           const RGWRealm& info,
                           std::unique_ptr<RealmWriter>* writer) override;
  virtual int read_realm_by_id(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               std::string_view realm_id,
                               RGWRealm& info,
                               std::unique_ptr<RealmWriter>* writer) override;
  virtual int read_realm_by_name(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::string_view realm_name,
                                 RGWRealm& info,
                                 std::unique_ptr<RealmWriter>* writer) override;
  virtual int read_default_realm(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 RGWRealm& info,
                                 std::unique_ptr<RealmWriter>* writer) override;
  virtual int read_realm_id(const DoutPrefixProvider* dpp,
                            optional_yield y, std::string_view realm_name,
                            std::string& realm_id) override;
  virtual int realm_notify_new_period(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      const RGWPeriod& period) override;
  virtual int list_realm_names(const DoutPrefixProvider* dpp,
                               optional_yield y, const std::string& marker,
                               std::span<std::string> entries,
                               ListResult<std::string>& result) override;

  virtual int create_period(const DoutPrefixProvider* dpp,
                            optional_yield y, bool exclusive,
                            const RGWPeriod& info) override;
  virtual int read_period(const DoutPrefixProvider* dpp,
                          optional_yield y, std::string_view period_id,
                          std::optional<epoch_t> epoch, RGWPeriod& info) override;
  virtual int delete_period(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            std::string_view period_id) override;
  virtual int list_period_ids(const DoutPrefixProvider* dpp,
                              optional_yield y, const std::string& marker,
                              std::span<std::string> entries,
                              ListResult<std::string>& result) override;

  virtual int write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                         optional_yield y, bool exclusive,
                                         std::string_view zonegroup_id) override;
  virtual int read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string& zonegroup_id) override;
  virtual int delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                          optional_yield y) override;

  virtual int create_zonegroup(const DoutPrefixProvider* dpp,
                               optional_yield y, bool exclusive,
                               const RGWZoneGroup& info,
                               std::unique_ptr<ZoneGroupWriter>* writer) override;
  virtual int read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view zonegroup_id,
                                   RGWZoneGroup& info,
                                   std::unique_ptr<ZoneGroupWriter>* writer) override;
  virtual int read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view zonegroup_name,
                                     RGWZoneGroup& info,
                                     std::unique_ptr<ZoneGroupWriter>* writer) override;
  virtual int read_default_zonegroup(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     RGWZoneGroup& info,
                                     std::unique_ptr<ZoneGroupWriter>* writer) override;
  virtual int list_zonegroup_names(const DoutPrefixProvider* dpp,
                                   optional_yield y, const std::string& marker,
                                   std::span<std::string> entries,
                                   ListResult<std::string>& result) override;

  virtual int write_default_zone_id(const DoutPrefixProvider* dpp,
                                    optional_yield y, bool exclusive,
                                    std::string_view realm_id,
                                    std::string_view zone_id) override;
  virtual int read_default_zone_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view realm_id,
                                   std::string& zone_id) override;
  virtual int delete_default_zone_id(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view realm_id) override;

  virtual int create_zone(const DoutPrefixProvider* dpp,
                          optional_yield y, bool exclusive,
                          const RGWZoneParams& info,
                          std::unique_ptr<ZoneWriter>* writer) override;
  virtual int read_zone_by_id(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              std::string_view zone_id,
                              RGWZoneParams& info,
                              std::unique_ptr<ZoneWriter>* writer) override;
  virtual int read_zone_by_name(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view zone_name,
                                RGWZoneParams& info,
                                std::unique_ptr<ZoneWriter>* writer) override;
  virtual int read_default_zone(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view realm_id,
                                RGWZoneParams& info,
                                std::unique_ptr<ZoneWriter>* writer) override;
  virtual int list_zone_names(const DoutPrefixProvider* dpp,
                              optional_yield y, const std::string& marker,
                              std::span<std::string> entries,
                              ListResult<std::string>& result) override;

  virtual int read_period_config(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 RGWPeriodConfig& info) override;
  virtual int write_period_config(const DoutPrefixProvider* dpp,
                                  optional_yield y,
                                  const RGWPeriodConfig& info) override;
}; // DBConfigStore


class DBRealmWriter : public RealmWriter {
 public:
  virtual int write(const DoutPrefixProvider* dpp,
                    optional_yield y,
                    const RGWRealm& info) override;
  virtual int rename(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     RGWRealm& info,
                     std::string_view new_name) override;
  virtual int remove(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     const RGWRealm& info) override;
};

class DBZoneGroupWriter : public ZoneGroupWriter {
 public:
  virtual ~ZoneGroupWriter() {}

  virtual int write(const DoutPrefixProvider* dpp,
                    optional_yield y,
                    const RGWZoneGroup& info) override;
  virtual int rename(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     RGWZoneGroup& info,
                     std::string_view new_name) override;
  virtual int remove(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     const RGWZoneGroup& info) override;
};

class DBZoneWriter : public ZoneWriter {
 public:
  virtual ~ZoneWriter() {}

  virtual int write(const DoutPrefixProvider* dpp,
                    optional_yield y,
                    const RGWZoneParams& info) override;
  virtual int rename(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     RGWZoneParams& info,
                     std::string_view new_name) override;
  virtual int remove(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     const RGWZoneParams& info) override;
};

} // namespace rgw::sal
