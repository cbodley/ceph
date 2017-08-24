// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include "common/bounded_key_counter.h"
#include <gtest/gtest.h>

namespace {

template <typename Key, typename Count>
struct MockCounter : public BoundedKeyCounter<Key, Count> {
  using BoundedKeyCounter<Key, Count>::BoundedKeyCounter;
  // expose as public for testing sort invalidations
  using BoundedKeyCounter<Key, Count>::get_num_sorted;
};

// call get_highest() and return the number of callbacks
template <typename Key, typename Count>
size_t count_highest(MockCounter<Key, Count>& counter, size_t count)
{
  size_t callbacks = 0;
  counter.get_highest(count, [&callbacks] (const Key& key, Count count) {
                        ++callbacks;
                      });
  return callbacks;
}

// call get_highest() and return the key/value pairs as a vector
template <typename Key, typename Count,
          typename Vector = std::vector<std::pair<Key, Count>>>
Vector get_highest(MockCounter<Key, Count>& counter, size_t count)
{
  Vector results;
  counter.get_highest(count, [&results] (const Key& key, Count count) {
                        results.emplace_back(key, count);
                      });
  return results;
}

} // anonymous namespace

TEST(BoundedKeyCounter, Insert)
{
  MockCounter<int, int> counter(2);
  EXPECT_EQ(1, counter.insert(0)); // insert new key
  EXPECT_EQ(2, counter.insert(0)); // increment counter
  EXPECT_EQ(7, counter.insert(0, 5)); // add 5 to counter
  EXPECT_EQ(1, counter.insert(1)); // insert new key
  EXPECT_EQ(0, counter.insert(2)); // reject new key
}

TEST(BoundedKeyCounter, Erase)
{
  MockCounter<int, int> counter(10);

  counter.erase(0); // ok to erase nonexistent key
  EXPECT_EQ(1, counter.insert(1, 1));
  EXPECT_EQ(2, counter.insert(2, 2));
  EXPECT_EQ(3, counter.insert(3, 3));
  counter.erase(2);
  counter.erase(1);
  counter.erase(3);
  counter.erase(3);
  EXPECT_EQ(0u, count_highest(counter, 10));
}

TEST(BoundedKeyCounter, GetHighest)
{
  MockCounter<int, int> counter(10);
  using Vector = std::vector<std::pair<int, int>>;

  EXPECT_EQ(0u, count_highest(counter, 0)); // ok to request 0
  EXPECT_EQ(0u, count_highest(counter, 10)); // empty
  EXPECT_EQ(0u, count_highest(counter, 999)); // ok to request count >> 10

  EXPECT_EQ(1, counter.insert(1, 1));
  EXPECT_EQ(Vector({{1,1}}), get_highest(counter, 10));
  EXPECT_EQ(2, counter.insert(2, 2));
  EXPECT_EQ(Vector({{2,2},{1,1}}), get_highest(counter, 10));
  EXPECT_EQ(3, counter.insert(3, 3));
  EXPECT_EQ(Vector({{3,3},{2,2},{1,1}}), get_highest(counter, 10));
  EXPECT_EQ(3, counter.insert(4, 3)); // insert duplicated count=3
  // still returns 4 entries (but order of {3,3} and {4,3} is unspecified)
  EXPECT_EQ(4u, count_highest(counter, 10));
  counter.erase(3);
  EXPECT_EQ(Vector({{4,3},{2,2},{1,1}}), get_highest(counter, 10));
  EXPECT_EQ(0u, count_highest(counter, 0)); // requesting 0 still returns 0
}

TEST(BoundedKeyCounter, Clear)
{
  MockCounter<int, int> counter(2);
  EXPECT_EQ(1, counter.insert(0)); // insert new key
  EXPECT_EQ(1, counter.insert(1)); // insert new key
  EXPECT_EQ(0u, counter.get_num_sorted());
  EXPECT_EQ(2u, count_highest(counter, 2)); // return 2 entries
  EXPECT_EQ(2u, counter.get_num_sorted());

  counter.clear();

  EXPECT_EQ(0u, counter.get_num_sorted());
  EXPECT_EQ(0u, count_highest(counter, 2)); // return 0 entries
  EXPECT_EQ(1, counter.insert(1)); // insert new key
  EXPECT_EQ(1, counter.insert(2)); // insert new unique key
  EXPECT_EQ(2u, count_highest(counter, 2)); // return 2 entries
  EXPECT_EQ(2u, counter.get_num_sorted());
}

// tests for partial sort and invalidation
TEST(BoundedKeyCounter, GetNumSorted)
{
  MockCounter<int, int> counter(10);

  EXPECT_EQ(0u, counter.get_num_sorted());
  EXPECT_EQ(0u, count_highest(counter, 10));
  EXPECT_EQ(0u, counter.get_num_sorted());

  EXPECT_EQ(2, counter.insert(2, 2));
  EXPECT_EQ(3, counter.insert(3, 3));
  EXPECT_EQ(4, counter.insert(4, 4));
  EXPECT_EQ(0u, counter.get_num_sorted());

  EXPECT_EQ(0u, count_highest(counter, 0));
  EXPECT_EQ(0u, counter.get_num_sorted());
  EXPECT_EQ(1u, count_highest(counter, 1));
  EXPECT_EQ(1u, counter.get_num_sorted());
  EXPECT_EQ(2u, count_highest(counter, 2));
  EXPECT_EQ(2u, counter.get_num_sorted());
  EXPECT_EQ(3u, count_highest(counter, 10));
  EXPECT_EQ(3u, counter.get_num_sorted());

  EXPECT_EQ(1, counter.insert(1, 1)); // insert at bottom invalidates sort
  EXPECT_EQ(0u, counter.get_num_sorted());

  EXPECT_EQ(0u, count_highest(counter, 0));
  EXPECT_EQ(0u, counter.get_num_sorted());
  EXPECT_EQ(1u, count_highest(counter, 1));
  EXPECT_EQ(1u, counter.get_num_sorted());
  EXPECT_EQ(2u, count_highest(counter, 2));
  EXPECT_EQ(2u, counter.get_num_sorted());
  EXPECT_EQ(3u, count_highest(counter, 3));
  EXPECT_EQ(3u, counter.get_num_sorted());
  EXPECT_EQ(4u, count_highest(counter, 10));
  EXPECT_EQ(4u, counter.get_num_sorted());

  EXPECT_EQ(5, counter.insert(5, 5)); // insert at top invalidates sort
  EXPECT_EQ(0u, counter.get_num_sorted());

  EXPECT_EQ(0u, count_highest(counter, 0));
  EXPECT_EQ(0u, counter.get_num_sorted());
  EXPECT_EQ(1u, count_highest(counter, 1));
  EXPECT_EQ(1u, counter.get_num_sorted());
  EXPECT_EQ(2u, count_highest(counter, 2));
  EXPECT_EQ(2u, counter.get_num_sorted());
  EXPECT_EQ(3u, count_highest(counter, 3));
  EXPECT_EQ(3u, counter.get_num_sorted());
  EXPECT_EQ(4u, count_highest(counter, 4));
  EXPECT_EQ(4u, counter.get_num_sorted());
  EXPECT_EQ(5u, count_highest(counter, 10));
  EXPECT_EQ(5u, counter.get_num_sorted());

  // updating an existing counter only invalidates entries <= that counter
  EXPECT_EQ(2, counter.insert(1)); // invalidates {1,2} and {2,2}
  EXPECT_EQ(3u, counter.get_num_sorted());

  EXPECT_EQ(5u, count_highest(counter, 10));
  EXPECT_EQ(5u, counter.get_num_sorted());
}

