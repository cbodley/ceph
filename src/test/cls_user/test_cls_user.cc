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
 * Foundation.  See file COPYING.
 */

#include "cls/user/cls_user_client.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

#include <optional>
#include <system_error>

// create/destroy a pool that's shared by all tests in the process
struct RadosEnv : public ::testing::Environment {
  static std::optional<std::string> pool_name;
 public:
  static librados::Rados rados;
  static librados::IoCtx ioctx;

  void SetUp() override {
    // create pool
    std::string name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(name, rados));
    pool_name = name;
    ASSERT_EQ(rados.ioctx_create(name.c_str(), ioctx), 0);
  }
  void TearDown() override {
    ioctx.close();
    if (pool_name) {
      ASSERT_EQ(destroy_one_pool_pp(*pool_name, rados), 0);
    }
  }
};
std::optional<std::string> RadosEnv::pool_name;
librados::Rados RadosEnv::rados;
librados::IoCtx RadosEnv::ioctx;

auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

// test fixture with helper functions
class ClsAccount : public ::testing::Test {
 protected:
  librados::IoCtx& ioctx = RadosEnv::ioctx;

  int add(const std::string& oid, const cls_user_account_resource& entry,
          uint32_t limit)
  {
    librados::ObjectWriteOperation op;
    cls_user_account_resource_add(op, entry, limit);
    return ioctx.operate(oid, &op);
  }

  int rm(const std::string& oid, const std::string& name)
  {
    librados::ObjectWriteOperation op;
    cls_user_account_resource_rm(op, name);
    return ioctx.operate(oid, &op);
  }

  int list(const std::string& oid, const std::string& marker,
           const std::string& path_prefix, uint32_t max_entries,
           std::vector<cls_user_account_resource>& entries, bool& truncated,
           std::string& next_marker, int& ret)
  {
    librados::ObjectReadOperation op;
    cls_user_account_resource_list(op, marker, path_prefix, max_entries,
                                   entries, &truncated, &next_marker, &ret);
    return ioctx.operate(oid, &op, nullptr);
  }

  auto list_all(const std::string& oid,
                const std::string& path_prefix = "",
                uint32_t max_chunk = 1000)
    -> std::vector<cls_user_account_resource>
  {
    std::vector<cls_user_account_resource> all_entries;
    std::string marker;
    bool truncated = true;

    while (truncated) {
      std::vector<cls_user_account_resource> entries;
      std::string next_marker;
      int r2 = 0;
      int r1 = list(oid, marker, path_prefix, max_chunk,
                    entries, truncated, next_marker, r2);
      if (r1 < 0) throw std::system_error(r1, std::system_category());
      if (r2 < 0) throw std::system_error(r2, std::system_category());
      marker = std::move(next_marker);
      std::move(entries.begin(), entries.end(),
                std::back_inserter(all_entries));
    }
    return all_entries;
  }
};

template <typename ...Args>
std::vector<cls_user_account_resource> make_list(Args&& ...args)
{
  return {std::forward<Args>(args)...};
}

bool operator==(const cls_user_account_resource& lhs,
                const cls_user_account_resource& rhs)
{
  if (lhs.name != rhs.name) {
    return false;
  }
  return lhs.path == rhs.path;
  // ignore metadata
}
std::ostream& operator<<(std::ostream& out, const cls_user_account_resource& r)
{
  return out << r.path << r.name;
}

TEST_F(ClsAccount, add)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const auto u1 = cls_user_account_resource{.name = "user1"};
  const auto u2 = cls_user_account_resource{.name = "user2"};
  EXPECT_EQ(-EUSERS, add(oid, u1, 0));
  EXPECT_EQ(0, add(oid, u1, 1));
  EXPECT_EQ(-EUSERS, add(oid, u2, 1));
  EXPECT_EQ(0, add(oid, u1, 1)); // allow overwrite at limit
}

TEST_F(ClsAccount, rm)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const auto u1 = cls_user_account_resource{.name = "user1"};
  EXPECT_EQ(-ENOENT, rm(oid, u1.name));
  ASSERT_EQ(0, add(oid, u1, 1));
  ASSERT_EQ(0, rm(oid, u1.name));
  EXPECT_EQ(-ENOENT, rm(oid, u1.name));
  ASSERT_EQ(0, add(oid, u1, 1));
}

TEST_F(ClsAccount, list)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const auto u1 = cls_user_account_resource{.name = "user1", .path = ""};
  const auto u2 = cls_user_account_resource{.name = "user2", .path = "A"};
  const auto u3 = cls_user_account_resource{.name = "user3", .path = "AA"};
  const auto u4 = cls_user_account_resource{.name = "user4", .path = ""};
  constexpr uint32_t max_users = 1024;

  ASSERT_EQ(0, ioctx.create(oid, true));
  ASSERT_EQ(make_list(), list_all(oid));
  ASSERT_EQ(0, add(oid, u1, max_users));
  EXPECT_EQ(make_list(u1), list_all(oid));
  ASSERT_EQ(0, add(oid, u2, max_users));
  ASSERT_EQ(0, add(oid, u3, max_users));
  ASSERT_EQ(0, add(oid, u4, max_users));
  EXPECT_EQ(make_list(u3, u2, u1, u4), list_all(oid, ""));
  EXPECT_EQ(make_list(u3, u2, u1, u4), list_all(oid, "", 1)); // paginated
  EXPECT_EQ(make_list(u3, u2), list_all(oid, "A"));
  EXPECT_EQ(make_list(u3, u2), list_all(oid, "A", 1)); // paginated
  EXPECT_EQ(make_list(u3), list_all(oid, "AA"));
  EXPECT_EQ(make_list(u3), list_all(oid, "AA", 1)); // paginated
  EXPECT_EQ(make_list(), list_all(oid, "AAu")); // don't match AAuser3
  ASSERT_EQ(0, rm(oid, u2.name));
  EXPECT_EQ(make_list(u3, u1, u4), list_all(oid, ""));
  EXPECT_EQ(make_list(u3, u1, u4), list_all(oid, "", 1)); // paginated
}
