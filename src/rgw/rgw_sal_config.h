// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
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
#include "rgw_sal_fwd.h"

class DoutPrefixProvider;
class optional_yield;
struct RGWRealm;
struct RGWPeriod;
class RGWObjVersionTracker;

namespace rgw::sal {

/// When the realm's period configuration changes, all Stores are reloaded with
/// that new configuration. Everything outside of sal that refers to these
/// Stores must register for these callbacks to stop using the old store and
/// start using the new one.
class ReloadPauser {
 public:
  virtual ~ReloadPauser() = default;

  /// suspend any futher activity that requires access to the existing Store
  virtual void pause() = 0;
  /// resume activity with the new Store
  virtual void resume(rgw::sal::Store* store) = 0;
};

/// Abstraction for storing realm/zonegroup/zone configuration
class ConfigStore {
 public:
  virtual ~ConfigStore() {}

  /// Results of a listing operation
  struct ListResult {
    /// The initialized subspan of the input entries
    std::span<std::string> entries;
    /// The next marker to resume listing, or empty
    std::string next;
  };

  /// @group Realm
  ///@{

  /// Create a realm. May return EEXIST
  virtual int create_realm(const DoutPrefixProvider* dpp,
                           optional_yield y, bool exclusive,
                           const RGWRealm& info,
                           RGWObjVersionTracker* objv) = 0;
  /// Set the cluster-wide default realm id
  virtual int write_default_realm_id(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view realm_id,
                                     RGWObjVersionTracker* objv) = 0;
  /// Read the cluster's default realm id
  virtual int read_default_realm_id(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string& realm_id,
                                    RGWObjVersionTracker* objv) = 0;
  /// Delete the cluster's default realm id
  virtual int delete_default_realm_id(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      RGWObjVersionTracker* objv) = 0;
  /// Load a realm by id or name. If both are empty, try to load the cluster's
  /// default realm
  virtual int read_realm(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view realm_id,
                         std::string_view realm_name,
                         RGWRealm& info,
                         RGWObjVersionTracker* objv) = 0;
  /// Look up a realm id by its name
  virtual int read_realm_id(const DoutPrefixProvider* dpp,
                            optional_yield y, std::string_view realm_name,
                            std::string& realm_id) = 0;
  /// Overwrite an existing realm. Must not change id or name
  virtual int update_realm(const DoutPrefixProvider* dpp,
                           optional_yield y, const RGWRealm& info,
                           RGWObjVersionTracker* objv) = 0;
  /// Rename an existing realm
  virtual int rename_realm(const DoutPrefixProvider* dpp,
                           optional_yield y, RGWRealm& info,
                           std::string_view new_name,
                           RGWObjVersionTracker* objv) = 0;
  /// Delete an existing realm
  virtual int delete_realm(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           const RGWRealm& old_info,
                           RGWObjVersionTracker* objv) = 0;
  /// List up to 'entries.size()' realm names starting from the given marker
  virtual int list_realm_names(const DoutPrefixProvider* dpp,
                               optional_yield y, const std::string& marker,
                               std::span<std::string> entries,
                               ListResult& result) = 0;
  ///@}

  /// @group Period
  ///@{
  /// write a period. may return EEXIST
  virtual int create_period(const DoutPrefixProvider* dpp,
                            optional_yield y, bool exclusive,
                            const RGWPeriod& info,
                            RGWObjVersionTracker* objv) = 0;
  /// set the latest epoch for a given period id
  virtual int write_period_latest_epoch(const DoutPrefixProvider* dpp,
                                        optional_yield y, bool exclusive,
                                        std::string_view period_id,
                                        epoch_t epoch,
                                        RGWObjVersionTracker* objv) = 0;
  /// read the latest epoch for a given period id
  virtual int read_period_latest_epoch(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string_view period_id,
                                        epoch_t& epoch,
                                        RGWObjVersionTracker* objv) = 0;
  /// load a period by id and epoch. if no epoch is given, read the latest
  virtual int read_period(const DoutPrefixProvider* dpp,
                          optional_yield y, std::string_view period_id,
                          std::optional<epoch_t> epoch, RGWPeriod& info,
                          RGWObjVersionTracker* objv) = 0;
  /// overwrite an existing period. must not change id
  virtual int update_period(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            const RGWPeriod& info,
                            RGWObjVersionTracker* objv) = 0;
  /// delete an existing period
  virtual int delete_period(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            const RGWPeriod& old_info,
                            RGWObjVersionTracker* objv) = 0;
  /// list all period ids
  virtual int list_period_ids(const DoutPrefixProvider* dpp,
                              optional_yield y, const std::string& marker,
                              std::span<std::string> entries,
                              ListResult& result) = 0;
  ///@}
#if 0
  /// write a zonegroup. may return EEXIST
  virtual int create_zonegroup(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               const RGWZoneGroup& info,
                               RGWObjVersionTracker& objv) = 0;
  /// load a zonegroup by id
  virtual int read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view zonegroup_id,
                                   RGWZoneGroup& info,
                                   RGWObjVersionTracker& objv) = 0;
  /// load a zonegroup by name
  virtual int read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view zonegroup_name,
                                     RGWZoneGroup& info,
                                     RGWObjVersionTracker& objv) = 0;
  /// overwrite an existing zonegroup. must not change id
  virtual int update_zonegroup(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               const RGWZoneGroup& info,
                               const RGWZoneGroup& old_info,
                               RGWObjVersionTracker& objv) = 0;
  /// delete an existing zonegroup
  virtual int delete_zonegroup(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               const RGWZoneGroup& old_info,
                               RGWObjVersionTracker& objv) = 0;
  /// list all zonegroup names
  virtual int list_zonegroup_names(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::list<std::string>& names) = 0;

  /// write a zone. may return EEXIST
  virtual int create_zone(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          const RGWZone& info,
                          RGWObjVersionTracker& objv) = 0;
  /// load a zone by id
  virtual int read_zone_by_id(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              std::string_view zone_id,
                              RGWZone& info,
                              RGWObjVersionTracker& objv) = 0;
  /// load a zone by name
  virtual int read_zone_by_name(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view zone_name,
                                RGWZone& info,
                                RGWObjVersionTracker& objv) = 0;
  /// overwrite an existing zone. must not change id
  virtual int update_zone(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          const RGWZone& info,
                          const RGWZone& old_info,
                          RGWObjVersionTracker& objv) = 0;
  /// delete an existing zone
  virtual int delete_zone(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          const RGWZone& old_info,
                          RGWObjVersionTracker& objv) = 0;
  /// list all zone names
  virtual int list_zone_names(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              std::list<std::string>& names) = 0;

  /// write zone params. may return EEXIST
  virtual int create_zone_params(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 const RGWZoneParams& info,
                                 RGWObjVersionTracker& objv) = 0;
  /// load zone params by id
  virtual int read_zone_params_by_id(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view zone_id,
                                     RGWZoneParams& info,
                                     RGWObjVersionTracker& objv) = 0;
  /// load zone params by name
  virtual int read_zone_params_by_name(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::string_view zone_name,
                                       RGWZoneParams& info,
                                       RGWObjVersionTracker& objv) = 0;
  /// overwrite existing zone params. must not change id
  virtual int update_zone_params(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 const RGWZoneParams& info,
                                 const RGWZoneParams& old_info,
                                 RGWObjVersionTracker& objv) = 0;
  /// delete existing zone params
  virtual int delete_zone_params(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 const RGWZoneParams& old_info,
                                 RGWObjVersionTracker& objv) = 0;

  /// read period config for zones that don't belong to a realm
  virtual int read_realmless_config(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    const RGWPeriodConfig& info,
                                    RGWObjVersionTracker& objv) = 0;
  /// write period config for zones that don't belong to a realm
  virtual int write_realmless_config(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     const RGWPeriodConfig& info,
                                     RGWObjVersionTracker& objv) = 0;
#endif
}; // ConfigStore

} // namespace rgw::sal
