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

#include <optional>
#include <set>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include <pqxx/pqxx>

#include "common/random_string.h"
#include "rgw_zone.h"

#include "connection_pool.h"
#include "postgres.h"
#include "postgres_schema.h"

namespace rgw::dbstore::config {

namespace {

// parameter names for prepared statement bindings
static constexpr const char* P1 = "$1";
static constexpr const char* P2 = "$2";
static constexpr const char* P3 = "$3";
static constexpr const char* P4 = "$4";
static constexpr const char* P5 = "$5";
static constexpr const char* P6 = "$6";

std::string generate_version_tag(CephContext* cct)
{
  static constexpr auto TAG_LEN = 24;
  return gen_rand_alphanumeric(cct, TAG_LEN);
}

struct RealmRow {
  RGWRealm info;
  int ver;
  std::string tag;
};

void read_realm_row(const pqxx::row& row, RealmRow& realm)
{
  realm.info.id = row[0].as<std::string>();
  realm.info.name = row[1].as<std::string>();
  realm.info.current_period = row[2].as<std::string>();
  realm.info.epoch = row[3].as<int>();
  realm.ver = row[4].as<int>();
  realm.tag = row[5].as<std::string>();
}

void read_period_row(const pqxx::row& row, RGWPeriod& period)
{
  std::string data = row["Data"].as<std::string>();
  bufferlist bl = bufferlist::static_from_string(data);
  auto p = bl.cbegin();
  decode(period, p);
}

struct ZoneGroupRow {
  RGWZoneGroup info;
  int ver;
  std::string tag;
};

void read_zonegroup_row(const pqxx::row& row, ZoneGroupRow& zonegroup)
{
  std::string data = row[3].as<std::string>();
  zonegroup.ver = row[4].as<int>();
  zonegroup.tag = row[5].as<std::string>();

  bufferlist bl = bufferlist::static_from_string(data);
  auto p = bl.cbegin();
  decode(zonegroup.info, p);
}

struct ZoneRow {
  RGWZoneParams info;
  int ver;
  std::string tag;
};

void read_zone_row(const pqxx::row& row, ZoneRow& zone)
{
  std::string data = row[3].as<std::string>();
  zone.ver = row[4].as<int>();
  zone.tag = row[5].as<std::string>();

  bufferlist bl = bufferlist::static_from_string(data);
  auto p = bl.cbegin();
  decode(zone.info, p);
}

void read_period_config_row(const pqxx::row& row, RGWPeriodConfig& period)
{
  std::string data = row["Data"].as<std::string>();
  bufferlist bl = bufferlist::static_from_string(data);
  auto p = bl.cbegin();
  decode(period, p);
}

struct PostgreSQLConnection {
  pqxx::connection conn;
  std::set<std::string_view> statements;

  explicit PostgreSQLConnection(pqxx::connection conn)
      : conn(std::move(conn)) {}
};

class PostgreSQLConnectionFactory {
  std::string uri;
 public:
  PostgreSQLConnectionFactory(std::string uri) : uri(std::move(uri)) {}

  auto operator()(const DoutPrefixProvider* dpp)
    -> std::unique_ptr<PostgreSQLConnection>
  {
    auto conn = pqxx::connection{uri};
    return std::make_unique<PostgreSQLConnection>(std::move(conn));
  }
};

using PostgreSQLConnectionHandle = ConnectionHandle<PostgreSQLConnection>;

using PostgreSQLConnectionPool = ConnectionPool<
    PostgreSQLConnection, PostgreSQLConnectionFactory>;

} // anonymous namespace

class PostgreSQLImpl : public PostgreSQLConnectionPool {
 public:
  using PostgreSQLConnectionPool::PostgreSQLConnectionPool;
};


PostgreSQLConfigStore::PostgreSQLConfigStore(std::unique_ptr<PostgreSQLImpl> impl)
  : impl(std::move(impl))
{
}

PostgreSQLConfigStore::~PostgreSQLConfigStore() = default;


// Realm

class PostgreSQLRealmWriter : public sal::RealmWriter {
  PostgreSQLConnectionHandle conn;
  int ver;
  std::string tag;
  std::string realm_id;
  std::string realm_name;
 public:
  PostgreSQLRealmWriter(PostgreSQLConnectionHandle conn,
                        int ver, std::string tag,
                        std::string_view realm_id,
                        std::string_view realm_name)
    : conn(std::move(conn)), ver(ver), tag(std::move(tag)),
      realm_id(realm_id), realm_name(realm_name)
  {
  }

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWRealm& info) override
  {
    if (realm_id != info.id || realm_name != info.name) {
      return -EINVAL; // can't modify realm id or name directly
    }

    try {
      static constexpr pqxx::zview stmt = "realm_upd";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::realm_update5,
                                            P1, P2, P3, P4, P5);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, realm_id, info.current_period,
                                      info.epoch, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const pqxx::foreign_key_violation& e) {
      ldpp_dout(dpp, 20) << "realm update failed: " << e.what() << dendl;
      return -EINVAL; // refers to nonexistent CurrentPeriod
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "realm update failed: " << e.what() << dendl;
      return -EIO;
    }
    ++ver;
    return 0;
  }

  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWRealm& info, std::string_view new_name) override
  {
    if (realm_id != info.id || realm_name != info.name) {
      return -EINVAL; // can't modify realm id or name directly
    }
    if (new_name.empty()) {
      ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
      return -EINVAL;
    }
    auto name = std::string{new_name};

    try {
      static constexpr pqxx::zview stmt = "realm_rename";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::realm_rename4,
                                            P1, P2, P3, P4);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, realm_id, new_name, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const pqxx::unique_violation& e) {
      ldpp_dout(dpp, 20) << "realm rename failed: " << e.what() << dendl;
      return -EEXIST; // Name already taken
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "realm rename failed: " << e.what() << dendl;
      return -EIO;
    }
    info.name = std::string{new_name};
    ++ver;
    return 0;
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    try {
      static constexpr pqxx::zview stmt = "realm_del";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::realm_delete3, P1, P2, P3);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, realm_id, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "realm delete failed: " << e.what() << dendl;
      return -EIO;
    }
    return 0;
  }
}; // PostgreSQLRealmWriter


int PostgreSQLConfigStore::write_default_realm_id(const DoutPrefixProvider* dpp,
                                                  optional_yield y, bool exclusive,
                                                  std::string_view realm_id)
{
  if (realm_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a realm id" << dendl;
    return -EINVAL;
  }

  auto conn = impl->get(dpp);
  try {
    auto tx = pqxx::work{conn->conn};
    if (exclusive) {
      static constexpr pqxx::zview stmt = "def_realm_ins";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::default_realm_insert1, P1);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, realm_id);
    } else {
      static constexpr pqxx::zview stmt = "def_realm_ups";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::default_realm_upsert1, P1);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, realm_id);
    }
    tx.commit();
  } catch (const pqxx::integrity_constraint_violation& e) {
    ldpp_dout(dpp, 20) << "default realm insert failed: " << e.what() << dendl;
    return -EEXIST;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default realm insert failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::read_default_realm_id(const DoutPrefixProvider* dpp,
                                                 optional_yield y,
                                                 std::string& realm_id)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "def_realm_sel";
    if (conn->statements.insert(stmt).second) {
      static constexpr pqxx::zview sql = schema::default_realm_select0;
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt);
    realm_id = row[0].as<std::string>();
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "default realm select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default realm select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::delete_default_realm_id(const DoutPrefixProvider* dpp,
                                                   optional_yield y)

{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "def_realm_del";
    if (conn->statements.insert(stmt).second) {
      static constexpr pqxx::zview sql = schema::default_realm_delete0;
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::work{conn->conn};
    auto result = tx.exec_prepared0(stmt);
    if (result.affected_rows() == 0) {
      return -ENOENT;
    }
    tx.commit();
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default realm delete failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}


int PostgreSQLConfigStore::create_realm(const DoutPrefixProvider* dpp,
                                        optional_yield y, bool exclusive,
                                        const RGWRealm& info,
                                        std::unique_ptr<sal::RealmWriter>* writer)
{
  if (info.id.empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.name.empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
    return -EINVAL;
  }

  int ver = 1;
  auto tag = generate_version_tag(dpp->get_cct());

  auto conn = impl->get(dpp);
  try {
    auto tx = pqxx::work{conn->conn};
    if (exclusive) {
      static constexpr pqxx::zview stmt = "realm_ins";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::realm_insert4,
                                            P1, P2, P3, P4);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, info.id, info.name, ver, tag);
    } else {
      static constexpr pqxx::zview stmt = "realm_ups";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::realm_upsert4,
                                            P1, P2, P3, P4);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, info.id, info.name, ver, tag);
    }
    tx.commit();
  } catch (const pqxx::unique_violation& e) {
    ldpp_dout(dpp, 20) << "realm insert failed: " << e.what() << dendl;
    return -EEXIST; // ID or Name already taken
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "realm insert failed: " << e.what() << dendl;
    return -EIO;
  }

  if (writer) {
    *writer = std::make_unique<PostgreSQLRealmWriter>(
        std::move(conn), ver, std::move(tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_realm_by_id(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            std::string_view realm_id,
                                            RGWRealm& info,
                                            std::unique_ptr<sal::RealmWriter>* writer)
{
  if (realm_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a realm id" << dendl;
    return -EINVAL;
  }

  RealmRow realm;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "realm_sel_id";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::realm_select_id1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, realm_id);
    read_realm_row(row, realm);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "realm decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(realm.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLRealmWriter>(
        std::move(conn), realm.ver, std::move(realm.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_realm_by_name(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              std::string_view realm_name,
                                              RGWRealm& info,
                                              std::unique_ptr<sal::RealmWriter>* writer)
{
  if (realm_name.empty()) {
    ldpp_dout(dpp, 0) << "requires a realm name" << dendl;
    return -EINVAL;
  }

  RealmRow realm;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "realm_sel_name";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::realm_select_name1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, realm_name);
    read_realm_row(row, realm);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "realm decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(realm.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLRealmWriter>(
        std::move(conn), realm.ver, std::move(realm.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_default_realm(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              RGWRealm& info,
                                              std::unique_ptr<sal::RealmWriter>* writer)
{
  RealmRow realm;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "realm_sel_def";
    if (conn->statements.insert(stmt).second) {
      static constexpr pqxx::zview sql = schema::realm_select_default0;
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt);
    read_realm_row(row, realm);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "realm decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(realm.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLRealmWriter>(
        std::move(conn), realm.ver, std::move(realm.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_realm_id(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         std::string_view realm_name,
                                         std::string& realm_id)
{
  if (realm_name.empty()) {
    ldpp_dout(dpp, 0) << "requires a realm name" << dendl;
    return -EINVAL;
  }

  RealmRow realm;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "realm_sel_name";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::realm_select_name1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, realm_name);
    read_realm_row(row, realm);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "realm decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -EIO;
  }

  realm_id = std::move(realm.info.id);
  return 0;
}

int PostgreSQLConfigStore::realm_notify_new_period(const DoutPrefixProvider* dpp,
                                                   optional_yield y,
                                                   const RGWPeriod& period)
{
  return -ENOTSUP;
}

int PostgreSQLConfigStore::list_realm_names(const DoutPrefixProvider* dpp,
                                            optional_yield y, const std::string& marker,
                                            std::span<std::string> entries,
                                            sal::ListResult<std::string>& listing)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "realm_sel_names";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::realm_select_names2, P1, P2);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto result = tx.exec_prepared(stmt, marker, entries.size());
    auto row = result.begin();
    auto entry = entries.begin();
    std::size_t count = 0;
    while (row != result.end() && entry != entries.end()) {
      *entry = row->front().as<std::string>();
      ++row;
      ++entry;
      ++count;
    }
    listing.entries = entries.first(count);
    if (count < entries.size()) { // end of listing
      listing.next.clear();
    } else {
      listing.next = listing.entries.back();
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}


// Period

int PostgreSQLConfigStore::create_period(const DoutPrefixProvider* dpp,
                                         optional_yield y, bool exclusive,
                                         const RGWPeriod& info)
{
  if (info.id.empty()) {
    ldpp_dout(dpp, 0) << "period cannot have an empty id" << dendl;
    return -EINVAL;
  }

  bufferlist bl;
  encode(info, bl);
  const auto data = pqxx::binary_cast(bl.c_str(), bl.length());

  auto conn = impl->get(dpp);
  try {
    auto tx = pqxx::work{conn->conn};
    if (exclusive) {
      static constexpr pqxx::zview stmt = "period_ins";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::period_insert4,
                                            P1, P2, P3, P4);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, info.id, info.epoch, info.realm_id, data);
    } else {
      static constexpr pqxx::zview stmt = "period_ups";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::period_upsert4,
                                            P1, P2, P3, P4);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, info.id, info.epoch, info.realm_id, data);
    }
    tx.commit();
  } catch (const pqxx::unique_violation& e) {
    ldpp_dout(dpp, 20) << "period insert failed: " << e.what() << dendl;
    return -EEXIST; // ID/Epoch already taken
  } catch (const pqxx::foreign_key_violation& e) {
    ldpp_dout(dpp, 20) << "period insert failed: " << e.what() << dendl;
    return -EINVAL; // refers to nonexistent RealmID
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "period insert failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::read_period(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::string_view period_id,
                                       std::optional<uint32_t> epoch,
                                       RGWPeriod& info)
{
  if (period_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a period id" << dendl;
    return -EINVAL;
  }

  auto conn = impl->get(dpp);
  try {
    auto tx = pqxx::read_transaction{conn->conn};
    if (epoch) {
      static constexpr pqxx::zview stmt = "period_sel_epoch";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::period_select_epoch2,
                                            P1, P2);
        conn->conn.prepare(stmt, sql);
      }
      auto row = tx.exec_prepared1(stmt, period_id, *epoch);
      read_period_row(row, info);
    } else {
      static constexpr pqxx::zview stmt = "period_sel_latest";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::period_select_latest1, P1);
        conn->conn.prepare(stmt, sql);
      }
      auto row = tx.exec_prepared1(stmt, period_id);
      read_period_row(row, info);
    }
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "period decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "period select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "period select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::delete_period(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         std::string_view period_id)
{
  if (period_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a period id" << dendl;
    return -EINVAL;
  }

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "period_del";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::period_delete1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::work{conn->conn};
    auto result = tx.exec_prepared(stmt, period_id);
    if (!result.affected_rows()) {
      return -ENOENT;
    }
    tx.commit();
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "period delete failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::list_period_ids(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           const std::string& marker,
                                           std::span<std::string> entries,
                                           sal::ListResult<std::string>& listing)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "period_sel_ids";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::period_select_ids2, P1, P2);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto result = tx.exec_prepared(stmt, marker, entries.size());
    auto row = result.begin();
    auto entry = entries.begin();
    std::size_t count = 0;
    while (row != result.end() && entry != entries.end()) {
      *entry = row->front().as<std::string>();
      ++row;
      ++entry;
      ++count;
    }
    listing.entries = entries.first(count);
    if (count < entries.size()) { // end of listing
      listing.next.clear();
    } else {
      listing.next = listing.entries.back();
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "period select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}


// ZoneGroup

class PostgreSQLZoneGroupWriter : public sal::ZoneGroupWriter {
  PostgreSQLConnectionHandle conn;
  int ver;
  std::string tag;
  std::string zonegroup_id;
  std::string zonegroup_name;
 public:
  PostgreSQLZoneGroupWriter(PostgreSQLConnectionHandle conn,
                            int ver, std::string tag,
                            std::string_view zonegroup_id,
                            std::string_view zonegroup_name)
    : conn(std::move(conn)), ver(ver), tag(std::move(tag)),
      zonegroup_id(zonegroup_id), zonegroup_name(zonegroup_name)
  {}

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWZoneGroup& info) override
  {
    if (zonegroup_id != info.id || zonegroup_name != info.name) {
      return -EINVAL; // can't modify zonegroup id or name directly
    }

    bufferlist bl;
    encode(info, bl);
    const auto data = pqxx::binary_cast(bl.c_str(), bl.length());

    try {
      static constexpr pqxx::zview stmt = "zonegroup_upd";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zonegroup_update5,
                                            P1, P2, P3, P4, P5);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, info.id, info.realm_id,
                                      data, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const pqxx::foreign_key_violation& e) {
      ldpp_dout(dpp, 20) << "zonegroup update failed: " << e.what() << dendl;
      return -EINVAL; // refers to nonexistent RealmID
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "zonegroup update failed: " << e.what() << dendl;
      return -EIO;
    }
    ++ver;
    return 0;
  }

  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWZoneGroup& info, std::string_view new_name) override
  {
    if (zonegroup_id != info.get_id() || zonegroup_name != info.get_name()) {
      return -EINVAL; // can't modify zonegroup id or name directly
    }
    if (new_name.empty()) {
      ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
      return -EINVAL;
    }

    try {
      static constexpr pqxx::zview stmt = "zonegroup_rename";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zonegroup_rename4,
                                            P1, P2, P3, P4);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, zonegroup_id, new_name, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const pqxx::unique_violation& e) {
      ldpp_dout(dpp, 20) << "zonegroup rename failed: " << e.what() << dendl;
      return -EEXIST; // Name already taken
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "zonegroup rename failed: " << e.what() << dendl;
      return -EIO;
    }
    info.name = std::string{new_name};
    ++ver;
    return 0;
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    try {
      static constexpr pqxx::zview stmt = "zonegroup_del";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zonegroup_delete3,
                                            P1, P2, P3);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, zonegroup_id, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "zonegroup delete failed: " << e.what() << dendl;
      return -EIO;
    }
    return 0;
  }
}; // PostgreSQLZoneGroupWriter


int PostgreSQLConfigStore::write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                      optional_yield y, bool exclusive,
                                                      std::string_view realm_id,
                                                      std::string_view zonegroup_id)
{
  auto conn = impl->get(dpp);
  try {
    auto tx = pqxx::work{conn->conn};
    if (exclusive) {
      static constexpr pqxx::zview stmt = "def_zonegroup_ins";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::default_zonegroup_insert2,
                                            P1, P2);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, realm_id, zonegroup_id);
    } else {
      static constexpr pqxx::zview stmt = "def_zonegroup_ups";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::default_zonegroup_upsert2,
                                            P1, P2);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, realm_id, zonegroup_id);
    }
    tx.commit();
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default zonegroup insert failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                     optional_yield y,
                                                     std::string_view realm_id,
                                                     std::string& zonegroup_id)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "def_zonegroup_sel";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::default_zonegroup_select1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, realm_id);
    zonegroup_id = row[0].as<std::string>();
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "default zonegroup select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default zonegroup select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                       optional_yield y,
                                                       std::string_view realm_id)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "def_zonegroup_del";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::default_zonegroup_delete1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::work{conn->conn};
    auto result = tx.exec_prepared0(stmt, realm_id);
    if (!result.affected_rows()) {
      return -ENOENT;
    }
    tx.commit();
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default zonegroup delete failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}


int PostgreSQLConfigStore::create_zonegroup(const DoutPrefixProvider* dpp,
                                            optional_yield y, bool exclusive,
                                            const RGWZoneGroup& info,
                                            std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  if (info.id.empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.name.empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
    return -EINVAL;
  }

  int ver = 1;
  auto tag = generate_version_tag(dpp->get_cct());

  bufferlist bl;
  encode(info, bl);
  const auto data = pqxx::binary_cast(bl.c_str(), bl.length());

  auto conn = impl->get(dpp);

  try {
    auto tx = pqxx::work{conn->conn};
    if (exclusive) {
      static constexpr pqxx::zview stmt = "zonegroup_ins";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zonegroup_insert6,
                                            P1, P2, P3, P4, P5, P6);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, info.id, info.name,
                        info.realm_id, data, ver, tag);
    } else {
      static constexpr pqxx::zview stmt = "zonegroup_ups";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zonegroup_upsert6,
                                            P1, P2, P3, P4, P5, P6);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, info.id, info.name,
                        info.realm_id, data, ver, tag);
    }
    tx.commit();
  } catch (const pqxx::unique_violation& e) {
    ldpp_dout(dpp, 20) << "zonegroup insert failed: " << e.what() << dendl;
    return -EEXIST; // ID or Name already taken
  } catch (const pqxx::foreign_key_violation& e) {
    ldpp_dout(dpp, 20) << "zonegroup insert failed: " << e.what() << dendl;
    return -EINVAL; // refers to nonexistent RealmID
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zonegroup insert failed: " << e.what() << dendl;
    return -EIO;
  }

  if (writer) {
    *writer = std::make_unique<PostgreSQLZoneGroupWriter>(
        std::move(conn), ver, std::move(tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                                optional_yield y,
                                                std::string_view zonegroup_id,
                                                RGWZoneGroup& info,
                                                std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  if (zonegroup_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a zonegroup id" << dendl;
    return -EINVAL;
  }

  ZoneGroupRow zonegroup;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "zonegroup_sel_id";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::zonegroup_select_id1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, zonegroup_id);
    read_zonegroup_row(row, zonegroup);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(zonegroup.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLZoneGroupWriter>(
        std::move(conn), zonegroup.ver,
        std::move(zonegroup.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                                  optional_yield y,
                                                  std::string_view zonegroup_name,
                                                  RGWZoneGroup& info,
                                                  std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  if (zonegroup_name.empty()) {
    ldpp_dout(dpp, 0) << "requires a zonegroup name" << dendl;
    return -EINVAL;
  }

  ZoneGroupRow zonegroup;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "zonegroup_sel_name";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::zonegroup_select_name1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, zonegroup_name);
    read_zonegroup_row(row, zonegroup);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(zonegroup.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLZoneGroupWriter>(
        std::move(conn), zonegroup.ver,
        std::move(zonegroup.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_default_zonegroup(const DoutPrefixProvider* dpp,
                                                  optional_yield y,
                                                  std::string_view realm_id,
                                                  RGWZoneGroup& info,
                                                  std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  ZoneGroupRow zonegroup;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "zonegroup_sel_def";
    if (conn->statements.insert(stmt).second) {
      static constexpr pqxx::zview sql = schema::zonegroup_select_default0;
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt);
    read_zonegroup_row(row, zonegroup);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(zonegroup.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLZoneGroupWriter>(
        std::move(conn), zonegroup.ver,
        std::move(zonegroup.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::list_zonegroup_names(const DoutPrefixProvider* dpp,
                                                optional_yield y,
                                                const std::string& marker,
                                                std::span<std::string> entries,
                                                sal::ListResult<std::string>& listing)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "zonegroup_sel_names";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::zonegroup_select_names2,
                                          P1, P2);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto result = tx.exec_prepared(stmt, marker, entries.size());
    auto row = result.begin();
    auto entry = entries.begin();
    std::size_t count = 0;
    while (row != result.end() && entry != entries.end()) {
      *entry = row->front().as<std::string>();
      ++row;
      ++entry;
      ++count;
    }
    listing.entries = entries.first(count);
    if (count < entries.size()) { // end of listing
      listing.next.clear();
    } else {
      listing.next = listing.entries.back();
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}


// Zone

class PostgreSQLZoneWriter : public sal::ZoneWriter {
  PostgreSQLConnectionHandle conn;
  int ver;
  std::string tag;
  std::string zone_id;
  std::string zone_name;
 public:
  PostgreSQLZoneWriter(PostgreSQLConnectionHandle conn,
                       int ver, std::string tag,
                       std::string_view zone_id,
                       std::string_view zone_name)
    : conn(std::move(conn)), ver(ver), tag(std::move(tag)),
      zone_id(zone_id), zone_name(zone_name)
  {}

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWZoneParams& info) override
  {
    if (zone_id != info.id || zone_name != info.name) {
      return -EINVAL; // can't modify zone id or name directly
    }

    bufferlist bl;
    encode(info, bl);
    const auto data = pqxx::binary_cast(bl.c_str(), bl.length());

    try {
      static constexpr pqxx::zview stmt = "zone_upd";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zone_update5,
                                            P1, P2, P3, P4, P5);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, info.id, info.realm_id,
                                      data, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const pqxx::foreign_key_violation& e) {
      ldpp_dout(dpp, 20) << "zone update failed: " << e.what() << dendl;
      return -EINVAL; // refers to nonexistent RealmID
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "zone update failed: " << e.what() << dendl;
      return -EIO;
    }
    ++ver;
    return 0;
  }

  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWZoneParams& info, std::string_view new_name) override
  {
    if (zone_id != info.get_id() || zone_name != info.get_name()) {
      return -EINVAL; // can't modify zone id or name directly
    }
    if (new_name.empty()) {
      ldpp_dout(dpp, 0) << "zone cannot have an empty name" << dendl;
      return -EINVAL;
    }

    try {
      static constexpr pqxx::zview stmt = "zone_rename";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zone_rename4,
                                            P1, P2, P3, P4);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, zone_id, new_name, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const pqxx::unique_violation& e) {
      ldpp_dout(dpp, 20) << "zone rename failed: " << e.what() << dendl;
      return -EEXIST; // Name already taken
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "zone rename failed: " << e.what() << dendl;
      return -EIO;
    }
    info.name = std::string{new_name};
    ++ver;
    return 0;
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    try {
      static constexpr pqxx::zview stmt = "zone_del";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zone_delete3, P1, P2, P3);
        conn->conn.prepare(stmt, sql);
      }
      auto tx = pqxx::work{conn->conn};
      auto result = tx.exec_prepared0(stmt, zone_id, ver, tag);
      if (!result.affected_rows()) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
      tx.commit();
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 20) << "zone delete failed: " << e.what() << dendl;
      return -EIO;
    }
    return 0;
  }
}; // PostgreSQLZoneWriter


int PostgreSQLConfigStore::write_default_zone_id(const DoutPrefixProvider* dpp,
                                                 optional_yield y, bool exclusive,
                                                 std::string_view realm_id,
                                                 std::string_view zone_id)
{
  auto conn = impl->get(dpp);
  try {
    auto tx = pqxx::work{conn->conn};
    if (exclusive) {
      static constexpr pqxx::zview stmt = "def_zone_ins";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::default_zone_insert2,
                                            P1, P2);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, realm_id, zone_id);
    } else {
      static constexpr pqxx::zview stmt = "def_zone_ups";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::default_zone_upsert2,
                                            P1, P2);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, realm_id, zone_id);
    }
    tx.commit();
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default zone insert failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::read_default_zone_id(const DoutPrefixProvider* dpp,
                                                optional_yield y,
                                                std::string_view realm_id,
                                                std::string& zone_id)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "def_zone_sel";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::default_zone_select1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, realm_id);
    zone_id = row[0].as<std::string>();
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "default zone select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default zone select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::delete_default_zone_id(const DoutPrefixProvider* dpp,
                                                  optional_yield y,
                                                  std::string_view realm_id)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "def_zone_del";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::default_zone_delete1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::work{conn->conn};
    auto result = tx.exec_prepared0(stmt, realm_id);
    if (!result.affected_rows()) {
      return -ENOENT;
    }
    tx.commit();
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "default zone delete failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}


int PostgreSQLConfigStore::create_zone(const DoutPrefixProvider* dpp,
                                       optional_yield y, bool exclusive,
                                       const RGWZoneParams& info,
                                       std::unique_ptr<sal::ZoneWriter>* writer)
{
  if (info.id.empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.name.empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty name" << dendl;
    return -EINVAL;
  }

  int ver = 1;
  auto tag = generate_version_tag(dpp->get_cct());

  bufferlist bl;
  encode(info, bl);
  const auto data = pqxx::binary_cast(bl.c_str(), bl.length());

  auto conn = impl->get(dpp);

  try {
    auto tx = pqxx::work{conn->conn};
    if (exclusive) {
      static constexpr pqxx::zview stmt = "zone_ins";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zone_insert6,
                                            P1, P2, P3, P4, P5, P6);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, info.id, info.name,
                        info.realm_id, data, ver, tag);
    } else {
      static constexpr pqxx::zview stmt = "zone_ups";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::zone_upsert6,
                                            P1, P2, P3, P4, P5, P6);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, info.id, info.name,
                        info.realm_id, data, ver, tag);
    }
    tx.commit();
  } catch (const pqxx::unique_violation& e) {
    ldpp_dout(dpp, 20) << "zone insert failed: " << e.what() << dendl;
    return -EEXIST; // ID or Name already taken
  } catch (const pqxx::foreign_key_violation& e) {
    ldpp_dout(dpp, 20) << "zone insert failed: " << e.what() << dendl;
    return -EINVAL; // refers to nonexistent RealmID
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zone insert failed: " << e.what() << dendl;
    return -EIO;
  }

  if (writer) {
    *writer = std::make_unique<PostgreSQLZoneWriter>(
        std::move(conn), ver, std::move(tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_zone_by_id(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           std::string_view zone_id,
                                           RGWZoneParams& info,
                                           std::unique_ptr<sal::ZoneWriter>* writer)
{
  if (zone_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a zone id" << dendl;
    return -EINVAL;
  }

  ZoneRow zone;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "zone_sel_id";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::zone_select_id1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, zone_id);
    read_zone_row(row, zone);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zone decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(zone.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLZoneWriter>(
        std::move(conn), zone.ver, std::move(zone.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_zone_by_name(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string_view zone_name,
                                             RGWZoneParams& info,
                                             std::unique_ptr<sal::ZoneWriter>* writer)
{
  if (zone_name.empty()) {
    ldpp_dout(dpp, 0) << "requires a zone name" << dendl;
    return -EINVAL;
  }

  ZoneRow zone;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "zone_sel_name";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::zone_select_name1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt, zone_name);
    read_zone_row(row, zone);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zone decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(zone.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLZoneWriter>(
        std::move(conn), zone.ver, std::move(zone.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::read_default_zone(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string_view realm_id,
                                             RGWZoneParams& info,
                                             std::unique_ptr<sal::ZoneWriter>* writer)
{
  ZoneRow zone;

  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "zone_sel_def";
    if (conn->statements.insert(stmt).second) {
      static constexpr pqxx::zview sql = schema::zone_select_default0;
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto row = tx.exec_prepared1(stmt);
    read_zone_row(row, zone);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zone decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    return -EIO;
  }

  info = std::move(zone.info);

  if (writer) {
    *writer = std::make_unique<PostgreSQLZoneWriter>(
        std::move(conn), zone.ver, std::move(zone.tag), info.id, info.name);
  }
  return 0;
}

int PostgreSQLConfigStore::list_zone_names(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           const std::string& marker,
                                           std::span<std::string> entries,
                                           sal::ListResult<std::string>& listing)
{
  auto conn = impl->get(dpp);
  try {
    static constexpr pqxx::zview stmt = "zone_sel_names";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::zone_select_names2,
                                          P1, P2);
      conn->conn.prepare(stmt, sql);
    }
    auto tx = pqxx::read_transaction{conn->conn};
    auto result = tx.exec_prepared(stmt, marker, entries.size());
    auto row = result.begin();
    auto entry = entries.begin();
    std::size_t count = 0;
    while (row != result.end() && entry != entries.end()) {
      *entry = row->front().as<std::string>();
      ++row;
      ++entry;
      ++count;
    }
    listing.entries = entries.first(count);
    if (count < entries.size()) { // end of listing
      listing.next.clear();
    } else {
      listing.next = listing.entries.back();
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}


// PeriodConfig

int PostgreSQLConfigStore::read_period_config(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              std::string_view realm_id,
                                              RGWPeriodConfig& info)
{
  auto conn = impl->get(dpp);
  try {
    auto tx = pqxx::read_transaction{conn->conn};
    static constexpr pqxx::zview stmt = "period_conf_sel";
    if (conn->statements.insert(stmt).second) {
      const std::string sql = fmt::format(schema::period_config_select1, P1);
      conn->conn.prepare(stmt, sql);
    }
    auto row = tx.exec_prepared1(stmt, realm_id);
    read_period_config_row(row, info);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "period config decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const pqxx::unexpected_rows& e) {
    ldpp_dout(dpp, 20) << "period config select failed: " << e.what() << dendl;
    return -ENOENT;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "period config select failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int PostgreSQLConfigStore::write_period_config(const DoutPrefixProvider* dpp,
                                               optional_yield y, bool exclusive,
                                               std::string_view realm_id,
                                               const RGWPeriodConfig& info)
{
  bufferlist bl;
  encode(info, bl);
  const auto data = pqxx::binary_cast(bl.c_str(), bl.length());

  auto conn = impl->get(dpp);
  try {
    auto tx = pqxx::work{conn->conn};
    if (exclusive) {
      static constexpr pqxx::zview stmt = "period_conf_ins";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::period_config_insert2,
                                            P1, P2);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, realm_id, data);
    } else {
      static constexpr pqxx::zview stmt = "period_conf_ups";
      if (conn->statements.insert(stmt).second) {
        const std::string sql = fmt::format(schema::period_config_upsert2,
                                            P1, P2);
        conn->conn.prepare(stmt, sql);
      }
      tx.exec_prepared0(stmt, realm_id, data);
    }
    tx.commit();
  } catch (const pqxx::unique_violation& e) {
    ldpp_dout(dpp, 20) << "period config insert failed: " << e.what() << dendl;
    return -EEXIST; // ID/Epoch already taken
  } catch (const pqxx::foreign_key_violation& e) {
    ldpp_dout(dpp, 20) << "period config insert failed: " << e.what() << dendl;
    return -EINVAL; // refers to nonexistent RealmID
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 20) << "period config insert failed: " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

namespace {



void apply_schema_migrations(const DoutPrefixProvider* dpp,
                             pqxx::connection& conn)
{
  auto tx = std::optional<pqxx::work>{conn};

  int version = 0;
  try {
    auto row = tx->exec1("SELECT Version FROM SchemaVersion");
    version = row[0].as<int>();
  } catch (const pqxx::undefined_table& e) {
    // SchemaVersion table doesn't exist, start at version 0

    // this error causes the transaction to rollback, so start another one
    tx.emplace(conn);
  }

  const int initial_version = version;
  ldpp_dout(dpp, 4) << "current schema version " << version << dendl;

  // use the version as an index into schema::migrations
  auto m = std::next(schema::migrations.begin(), version);

  for (; m != schema::migrations.end(); ++m, ++version) {
    try {
      tx->exec0(m->up);
    } catch (const pqxx::sql_error& e) {
      ldpp_dout(dpp, -1) << "ERROR: schema migration failed with " << e.what()
          << " when applying v" << version << ": '" << m->description
          << "'\nmigration query: " << e.query() << dendl;
      throw;
    }
  }

  const std::string query = fmt::format("UPDATE SchemaVersion SET Version = {}", version);
  tx->exec0(query);
  tx->commit();

  if (version > initial_version) {
    ldpp_dout(dpp, 4) << "upgraded database schema to version " << version << dendl;
  }
}

} // anonymous namespace

auto create_postgres_store(const DoutPrefixProvider* dpp,
                           const std::string& uri)
  -> std::unique_ptr<config::PostgreSQLConfigStore>
{
  // build the connection pool
  auto factory = PostgreSQLConnectionFactory{uri};
  std::size_t max_connections = 8;
  auto impl = std::make_unique<PostgreSQLImpl>(std::move(factory), max_connections);

  // open a connection to apply schema migrations
  auto conn = impl->get(dpp);
  apply_schema_migrations(dpp, conn->conn);

  return std::make_unique<PostgreSQLConfigStore>(std::move(impl));
}

} // namespace rgw::dbstore::config
