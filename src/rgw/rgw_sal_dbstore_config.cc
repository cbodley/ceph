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

constexpr std::string_view create_table_realms =
R"(CREATE TABLE IF NOT EXISTS Realms (
ID TEXT PRIMARY KEY NOT NULL,
Name TEXT UNIQUE NOT NULL,
CurrentPeriod TEXT REFERENCES Periods (ID),
Epoch INTEGER DEFAULT 0
))";

constexpr std::string_view create_table_periods =
R"(CREATE TABLE IF NOT EXISTS Periods (
ID TEXT PRIMARY KEY NOT NULL,
Epoch INTEGER PRIMARY KEY DEFAULT 0,
RealmID TEXT NOT NULL REFERENCES Realms (ID),
Data BLOB
))";

constexpr std::string_view create_table_period_config =
R"(CREATE TABLE IF NOT EXISTS PeriodConfig (
RealmID TEXT PRIMARY KEY NOT NULL REFERENCES Realms (ID),
Data BLOB
))";

constexpr std::string_view create_table_zonegroups =
R"(CREATE TABLE IF NOT EXISTS ZoneGroups (
ID TEXT PRIMARY KEY NOT NULL,
Name TEXT UNIQUE NOT NULL,
RealmID TEXT NOT NULL REFERENCES Realms (ID),
Data BLOB
))";

constexpr std::string_view create_table_zones =
R"(CREATE TABLE IF NOT EXISTS Zones (
ID TEXT PRIMARY KEY NOT NULL,
Name TEXT UNIQUE NOT NULL,
RealmID TEXT NOT NULL REFERENCES Realms (ID),
Data BLOB
))";

constexpr std::string_view create_table_defaults =
R"(CREATE TABLE IF NOT EXISTS Defaults (
Type TEXT NOT NULL,
ID TEXT,
RealmID TEXT REFERENCES Realms (ID)
PRIMARY KEY (Type, RealmID)
))";


int DBConfigStore::write_default_realm_id(const DoutPrefixProvider* dpp,
                                          optional_yield y, bool exclusive,
                                          std::string_view realm_id)
{
  constexpr std::string_view insert_default_realm =
R"(INSERT INTO Defaults (Type, ID) VALUES ("realm", `realm_id`)
ON CONFLICT(Type) DO UPDATE SET ID = `realm_id`)";
  return -ENOTSUP;
}

int DBConfigStore::read_default_realm_id(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         std::string& realm_id)
{
  constexpr std::string_view select_default_realm =
R"(SELECT ID FROM Defaults WHERE Type = "realm")";
  return -ENOTSUP;
}

int DBConfigStore::delete_default_realm_id(const DoutPrefixProvider* dpp,
                                           optional_yield y)

{
  constexpr std::string_view delete_default_realm =
R"(DELETE FROM Defaults WHERE Type = "realm")";
  return -ENOTSUP;
}

int DBConfigStore::create_realm(const DoutPrefixProvider* dpp,
                                optional_yield y, bool exclusive,
                                const RGWRealm& info,
                                std::unique_ptr<RealmWriter>* writer)
{
  constexpr std::string_view insert_realm =
R"(INSERT INTO Realms (ID, Name) VALUES (`info.id`, `info.name`))";
  return -ENOTSUP;
}

int DBConfigStore::read_realm_by_id(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string_view realm_id,
                                    RGWRealm& info,
                                    std::unique_ptr<RealmWriter>* writer)
{
  constexpr std::string_view select_realm =
R"(SELECT * FROM Realms WHERE ID = `realm_id`)";
  return -ENOTSUP;
}

int DBConfigStore::read_realm_by_name(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      std::string_view realm_name,
                                      RGWRealm& info,
                                      std::unique_ptr<RealmWriter>* writer)
{
  constexpr std::string_view select_realm =
R"(SELECT * FROM Realms WHERE Name = `realm_name`)";
  return -ENOTSUP;
}

int DBConfigStore::read_default_realm(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      RGWRealm& info,
                                      std::unique_ptr<RealmWriter>* writer)
{
  constexpr std::string_view select_realm =
R"(SELECT * FROM Realms r INNER JOIN Defaults d ON d.Type = "realm" and d.ID = r.ID)";
  return -ENOTSUP;
}

int DBConfigStore::read_realm_id(const DoutPrefixProvider* dpp,
                                 optional_yield y, std::string_view realm_name,
                                 std::string& realm_id)
{
  constexpr std::string_view select_realm =
R"(SELECT ID FROM Realms WHERE Name = `realm_name`)";
  return -ENOTSUP;
}

int DBConfigStore::realm_notify_new_period(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           const RGWPeriod& period)
{
  return -ENOTSUP;
}

int DBConfigStore::list_realm_names(const DoutPrefixProvider* dpp,
                                    optional_yield y, const std::string& marker,
                                    std::span<std::string> entries,
                                    ListResult<std::string>& result)
{
  constexpr std::string_view select_realm =
R"(SELECT Name FROM Realms)";
  return -ENOTSUP;
}


int DBConfigStore::create_period(const DoutPrefixProvider* dpp,
                                 optional_yield y, bool exclusive,
                                 const RGWPeriod& info)
{
  return -ENOTSUP;
}

int DBConfigStore::read_period(const DoutPrefixProvider* dpp,
                               optional_yield y, std::string_view period_id,
                               std::optional<epoch_t> epoch, RGWPeriod& info)
{
  return -ENOTSUP;
}

int DBConfigStore::delete_period(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::string_view period_id)
{
  return -ENOTSUP;
}

int DBConfigStore::list_period_ids(const DoutPrefixProvider* dpp,
                                   optional_yield y, const std::string& marker,
                                   std::span<std::string> entries,
                                   ListResult<std::string>& result)
{
  return -ENOTSUP;
}


int DBConfigStore::write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                              optional_yield y, bool exclusive,
                                              std::string_view zonegroup_id)
{
  return -ENOTSUP;
}

int DBConfigStore::read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string& zonegroup_id)
{
  return -ENOTSUP;
}

int DBConfigStore::delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                               optional_yield y)
{
  return -ENOTSUP;
}


int DBConfigStore::create_zonegroup(const DoutPrefixProvider* dpp,
                                    optional_yield y, bool exclusive,
                                    const RGWZoneGroup& info,
                                    std::unique_ptr<ZoneGroupWriter>* writer)
{
  return -ENOTSUP;
}

int DBConfigStore::read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string_view zonegroup_id,
                                        RGWZoneGroup& info,
                                        std::unique_ptr<ZoneGroupWriter>* writer)
{
  return -ENOTSUP;
}

int DBConfigStore::read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          std::string_view zonegroup_name,
                                          RGWZoneGroup& info,
                                          std::unique_ptr<ZoneGroupWriter>* writer)
{
  return -ENOTSUP;
}

int DBConfigStore::read_default_zonegroup(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          RGWZoneGroup& info,
                                          std::unique_ptr<ZoneGroupWriter>* writer)
{
  return -ENOTSUP;
}

int DBConfigStore::list_zonegroup_names(const DoutPrefixProvider* dpp,
                                        optional_yield y, const std::string& marker,
                                        std::span<std::string> entries,
                                        ListResult<std::string>& result)
{
  return -ENOTSUP;
}


int DBConfigStore::write_default_zone_id(const DoutPrefixProvider* dpp,
                                         optional_yield y, bool exclusive,
                                         std::string_view realm_id,
                                         std::string_view zone_id)
{
  return -ENOTSUP;
}

int DBConfigStore::read_default_zone_id(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string_view realm_id,
                                        std::string& zone_id)
{
  return -ENOTSUP;
}

int DBConfigStore::delete_default_zone_id(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          std::string_view realm_id)
{
  return -ENOTSUP;
}


int DBConfigStore::create_zone(const DoutPrefixProvider* dpp,
                               optional_yield y, bool exclusive,
                               const RGWZoneParams& info,
                               std::unique_ptr<ZoneWriter>* writer)
{
  return -ENOTSUP;
}

int DBConfigStore::read_zone_by_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view zone_id,
                                   RGWZoneParams& info,
                                   std::unique_ptr<ZoneWriter>* writer)
{
  return -ENOTSUP;
}

int DBConfigStore::read_zone_by_name(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view zone_name,
                                     RGWZoneParams& info,
                                     std::unique_ptr<ZoneWriter>* writer)
{
  return -ENOTSUP;
}

int DBConfigStore::read_default_zone(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view realm_id,
                                     RGWZoneParams& info,
                                     std::unique_ptr<ZoneWriter>* writer)
{
  return -ENOTSUP;
}

int DBConfigStore::list_zone_names(const DoutPrefixProvider* dpp,
                                   optional_yield y, const std::string& marker,
                                   std::span<std::string> entries,
                                   ListResult<std::string>& result)
{
  return -ENOTSUP;
}


int DBConfigStore::read_period_config(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      RGWPeriodConfig& info)
{
  return -ENOTSUP;
}

int DBConfigStore::write_period_config(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const RGWPeriodConfig& info)
{
  return -ENOTSUP;
}


int DBRealmWriter::write(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         const RGWRealm& info)
{
  return -ENOTSUP;
}

int DBRealmWriter::rename(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          RGWRealm& info,
                          std::string_view new_name)
{
  return -ENOTSUP;
}

int DBRealmWriter::remove(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          const RGWRealm& info)
{
  return -ENOTSUP;
}


int DBZoneGroupWriter::write(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             const RGWZoneGroup& info)
{
  return -ENOTSUP;
}

int DBZoneGroupWriter::rename(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              RGWZoneGroup& info,
                              std::string_view new_name)
{
  return -ENOTSUP;
}

int DBZoneGroupWriter::remove(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              const RGWZoneGroup& info)
{
  return -ENOTSUP;
}


int DBZoneWriter::write(const DoutPrefixProvider* dpp,
                        optional_yield y,
                        const RGWZoneParams& info)
{
  return -ENOTSUP;
}

int DBZoneWriter::rename(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         RGWZoneParams& info,
                         std::string_view new_name)
{
  return -ENOTSUP;
}

int DBZoneWriter::remove(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         const RGWZoneParams& info)
{
  return -ENOTSUP;
}

} // namespace rgw::sal
