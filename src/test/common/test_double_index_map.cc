#include "common/double_index_map.h"
#include <gtest/gtest.h>

namespace ceph {

using Pair = std::pair<std::string, int>;

TEST(DoubleIndexMap, key_comp_less)
{
  DoubleIndexMap<std::string, int> m;
  m.insert("b", 1);
  m.insert("a", 1);
  ASSERT_EQ(2u, m.size());
  EXPECT_EQ(Pair("a", 1), *m.begin());
  EXPECT_EQ(Pair("b", 1), *m.rbegin());

  EXPECT_TRUE(m.key_comp()("a", "b"));
  EXPECT_TRUE(m.value_comp()(*m.begin(), *m.rbegin()));
}

TEST(DoubleIndexMap, key_comp_greater)
{
  DoubleIndexMap<std::string, int, std::greater<std::string>> m;
  m.insert("a", 1);
  m.insert("b", 1);
  ASSERT_EQ(2u, m.size());
  EXPECT_EQ(Pair("b", 1), *m.begin());
  EXPECT_EQ(Pair("a", 1), *m.rbegin());

  EXPECT_TRUE(m.key_comp()("b", "a"));
  EXPECT_TRUE(m.value_comp()(*m.begin(), *m.rbegin()));
}

TEST(DoubleIndexMap, value_comp_less)
{
  DoubleIndexMap<std::string, int> m;
  const auto& values = m.reverse_mapping();
  m.insert("b", 2);
  m.insert("a", 1);
  ASSERT_EQ(2u, values.size());
  EXPECT_EQ(Pair("a", 1), *values.begin());
  EXPECT_EQ(Pair("b", 2), *values.rbegin());

  EXPECT_TRUE(values.key_comp()(1, 2));
  EXPECT_TRUE(values.value_comp()(*values.begin(), *values.rbegin()));
}

TEST(DoubleIndexMap, value_comp_greater)
{
  DoubleIndexMap<std::string, int, std::less<std::string>, std::greater<int>> m;
  const auto& values = m.reverse_mapping();
  m.insert("a", 1);
  m.insert("b", 2);
  ASSERT_EQ(2u, values.size());
  EXPECT_EQ(Pair("b", 2), *values.begin());
  EXPECT_EQ(Pair("a", 1), *values.rbegin());

  EXPECT_TRUE(values.key_comp()(2, 1));
  EXPECT_TRUE(values.value_comp()(*values.begin(), *values.rbegin()));
}

TEST(DoubleIndexMap, insert)
{
  DoubleIndexMap<std::string, int> m;
  auto result = m.insert("a", 1);
  ASSERT_TRUE(result.second);
  EXPECT_EQ(Pair("a", 1), *result.first);
  EXPECT_EQ(m.begin(), result.first);
  EXPECT_EQ(1u, m.size());
}

TEST(DoubleIndexMap, insert_existing_key)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_EQ(1u, m.size());
  auto result = m.insert("a", 2);
  EXPECT_FALSE(result.second);
  EXPECT_EQ(Pair("a", 1), *result.first);
  EXPECT_EQ(m.begin(), result.first);
  EXPECT_EQ(1u, m.size());
}

TEST(DoubleIndexMap, insert_existing_value)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_EQ(1u, m.size());
  EXPECT_TRUE(m.insert("b", 1).second);
  EXPECT_EQ(2u, m.size());
  EXPECT_EQ(2, m.reverse_mapping().count(1));
}

TEST(DoubleIndexMap, insert_move_only)
{
  DoubleIndexMap<std::string, std::unique_ptr<int>> m;
  ASSERT_TRUE(m.insert("a", std::make_unique<int>(1)).second);
  ASSERT_TRUE(m.insert("b", std::make_unique<int>(2)).second);
}

TEST(DoubleIndexMap, replace_value_by_key)
{
  DoubleIndexMap<std::string, int> m;
  const auto& values = m.reverse_mapping();
  ASSERT_TRUE(m.insert("a", 1).second);
  auto [k, inserted] = m.insert("b", 2);
  ASSERT_TRUE(inserted);
  ASSERT_EQ(1, values.begin()->second);
  
  m.replace(k, 0);
  EXPECT_EQ(Pair("b", 0), *k);
  EXPECT_EQ(0, values.begin()->second); // value index is updated
}

TEST(DoubleIndexMap, replace_by_key)
{
  DoubleIndexMap<std::string, int> m;
  const auto& values = m.reverse_mapping();
  ASSERT_TRUE(m.insert("a", 1).second);
  auto [k, inserted] = m.insert("b", 2);
  ASSERT_TRUE(inserted);
  ASSERT_TRUE(m.insert("c", 3).second);

  m.replace(k, "d", 0);
  EXPECT_EQ(Pair("d", 0), *k);
  EXPECT_EQ("d", m.rbegin()->first); // key index is updated
  EXPECT_EQ(0, values.begin()->second); // value index is updated
}

TEST(DoubleIndexMap, replace_key_by_value)
{
  DoubleIndexMap<std::string, int> m;
  auto values = m.reverse_mapping();
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 2).second);

  auto v = values.begin();
  values.replace(v, "c");
  EXPECT_EQ(Pair("c", 1), *v);
  EXPECT_EQ("c", m.rbegin()->first); // key index is updated
}

TEST(DoubleIndexMap, replace_by_value)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 2).second);

  auto values = m.reverse_mapping();
  auto v = values.begin();
  values.replace(v, "c", 3);
  EXPECT_EQ(Pair("c", 3), *v);
  EXPECT_EQ("c", m.rbegin()->first); // key index is updated
  EXPECT_EQ(3, values.rbegin()->second); // value index is updated
}

TEST(DoubleIndexMap, erase_by_key_iterator)
{
  DoubleIndexMap<std::string, int> m;
  m.insert("a", 1);
  m.insert("b", 2);
  ASSERT_EQ(2u, m.size());
  m.erase(m.begin());
  EXPECT_EQ(1u, m.size());
  EXPECT_EQ(Pair("b", 2), *m.begin());
}

TEST(DoubleIndexMap, erase_by_key_reference)
{
  DoubleIndexMap<std::string, int> m;
  m.insert("a", 1);
  m.insert("b", 2);
  ASSERT_EQ(2u, m.size());
  EXPECT_EQ(1u, m.erase("a"));
  EXPECT_EQ(1u, m.size());
  EXPECT_EQ(Pair("b", 2), *m.begin());
}

TEST(DoubleIndexMap, erase_by_key_range)
{
  DoubleIndexMap<std::string, int> m;
  m.insert("a", 1);
  m.insert("b", 2);
  m.insert("c", 3);
  m.insert("d", 4);
  ASSERT_EQ(4u, m.size());
  {
    auto from = std::next(m.begin()); // begin + 1
    auto to = std::next(m.begin(), 3); // begin + 3
    m.erase(from, to); // erase [b, d)
  }
  EXPECT_EQ(2u, m.size());
  EXPECT_EQ(Pair("a", 1), *m.begin());
  EXPECT_EQ(Pair("d", 4), *m.rbegin());
}

TEST(DoubleIndexMap, erase_by_value_iterator)
{
  DoubleIndexMap<std::string, int> m;
  m.insert("a", 1);
  m.insert("b", 2);
  auto values = m.reverse_mapping();
  ASSERT_EQ(2u, values.size());
  values.erase(values.begin());
  EXPECT_EQ(1u, values.size());
  EXPECT_EQ(Pair("b", 2), *values.begin());
}

TEST(DoubleIndexMap, erase_by_value_reference)
{
  DoubleIndexMap<std::string, int> m;
  m.insert("a", 1);
  m.insert("b", 1);
  m.insert("c", 2);
  auto values = m.reverse_mapping();
  ASSERT_EQ(3u, values.size());
  EXPECT_EQ(2u, values.erase(1));
  EXPECT_EQ(1u, values.size());
  EXPECT_EQ(Pair("c", 2), *values.begin());
}

TEST(DoubleIndexMap, erase_by_value_range)
{
  DoubleIndexMap<std::string, int> m;
  m.insert("d", 1);
  m.insert("c", 2);
  m.insert("b", 3);
  m.insert("a", 4);
  auto values = m.reverse_mapping();
  ASSERT_EQ(4u, values.size());
  {
    auto from = std::next(values.begin()); // begin + 1
    auto to = std::next(values.begin(), 3); // begin + 3
    values.erase(from, to); // erase [2, 4)
  }
  EXPECT_EQ(2u, values.size());
  EXPECT_EQ(Pair("d", 1), *values.begin());
  EXPECT_EQ(Pair("a", 4), *values.rbegin());
}

TEST(DoubleIndexMap, count_by_key)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  EXPECT_EQ(1u, m.count("a"));
  EXPECT_EQ(0u, m.count("b"));
}

TEST(DoubleIndexMap, count_by_value)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 1).second);
  const auto& values = m.reverse_mapping();
  EXPECT_EQ(2u, values.count(1));
  EXPECT_EQ(0u, values.count(2));
}

TEST(DoubleIndexMap, find_by_key)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  EXPECT_EQ(m.begin(), m.find("a"));
  EXPECT_EQ(m.end(), m.find("b"));
}

TEST(DoubleIndexMap, find_by_value)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  const auto& values = m.reverse_mapping();
  EXPECT_EQ(values.begin(), values.find(1));
  EXPECT_EQ(values.end(), values.find(2));
}

TEST(DoubleIndexMap, equal_range_by_key)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 2).second);
  ASSERT_EQ(2u, m.size());
  {
    auto range = m.equal_range("a");
    EXPECT_EQ(m.begin(), range.first);
    EXPECT_EQ(1u, std::distance(range.first, range.second));
  }
  {
    auto range = m.equal_range("x");
    EXPECT_EQ(range.first, range.second);
  }
}

TEST(DoubleIndexMap, equal_range_by_value)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 1).second);
  ASSERT_TRUE(m.insert("c", 2).second);
  const auto& values = m.reverse_mapping();
  ASSERT_EQ(3u, values.size());
  {
    auto range = values.equal_range(1);
    EXPECT_EQ(values.begin(), range.first);
    EXPECT_EQ(2u, std::distance(range.first, range.second));
  }
  {
    auto range = values.equal_range(-1);
    EXPECT_EQ(range.first, range.second);
  }
}

TEST(DoubleIndexMap, lower_bound_by_key)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  EXPECT_EQ(m.begin(), m.lower_bound("a"));
  EXPECT_EQ(m.end(), m.lower_bound("b"));
}

TEST(DoubleIndexMap, lower_bound_by_value)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  const auto& values = m.reverse_mapping();
  EXPECT_EQ(values.begin(), values.lower_bound(1));
  EXPECT_EQ(values.end(), values.lower_bound(2));
}

TEST(DoubleIndexMap, upper_bound_by_key)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 2).second);
  EXPECT_EQ(std::next(m.begin()), m.upper_bound("a"));
  EXPECT_EQ(m.end(), m.upper_bound("x"));
}

TEST(DoubleIndexMap, upper_bound_by_value)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 2).second);
  const auto& values = m.reverse_mapping();
  EXPECT_EQ(std::next(values.begin()), values.upper_bound(1));
  EXPECT_EQ(values.end(), values.upper_bound(9));
}

TEST(DoubleIndexMap, clear_keys)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 2).second);
  ASSERT_EQ(2u, m.size());
  ASSERT_FALSE(m.empty());
  m.clear();
  EXPECT_EQ(0u, m.size());
  EXPECT_TRUE(m.empty());
  m.clear();
  EXPECT_EQ(0u, m.size());
  EXPECT_TRUE(m.empty());
}

TEST(DoubleIndexMap, clear_values)
{
  DoubleIndexMap<std::string, int> m;
  ASSERT_TRUE(m.insert("a", 1).second);
  ASSERT_TRUE(m.insert("b", 2).second);
  auto values = m.reverse_mapping();
  ASSERT_EQ(2u, values.size());
  ASSERT_FALSE(values.empty());
  values.clear();
  EXPECT_EQ(0u, values.size());
  EXPECT_TRUE(values.empty());
  values.clear();
  EXPECT_EQ(0u, values.size());
  EXPECT_TRUE(values.empty());
}

// allocation events recorded by mock_allocator
enum class event : bool {
  allocation = true,
  deallocation = false,
};
using event_log = std::vector<event>;

template <typename T>
struct mock_allocator {
  event_log* log = nullptr;

  using value_type = T;

  mock_allocator() = default;
  explicit mock_allocator(event_log* log) noexcept
    : log(log) {}
  mock_allocator(const mock_allocator& other) noexcept
    : log(other.log) {}
  template <typename U>
  mock_allocator(const mock_allocator<U>& other) noexcept
    : log(other.log) {}

  mock_allocator(mock_allocator&& other) noexcept
    : log(std::exchange(other.log, nullptr)) {}
  template <typename U>
  mock_allocator(mock_allocator<U>&& other) noexcept
    : log(std::exchange(other.log, nullptr)) {}

  mock_allocator& operator=(const mock_allocator& other) {
    log = other.log;
    return *this;
  }
  template <typename U>
  mock_allocator& operator=(const mock_allocator<U>& other) {
    log = other.log;
    return *this;
  }

  mock_allocator& operator=(mock_allocator&& other) {
    log = std::exchange(other.log, nullptr);
    return *this;
  }
  template <typename U>
  mock_allocator& operator=(mock_allocator<U>&& other) {
    log = std::exchange(other.log, nullptr);
    return *this;
  }

  T* allocate(size_t n, const void* hint=0) {
    assert(log);
    auto p = std::allocator<T>{}.allocate(n, hint);
    log->push_back(event::allocation);
    return p;
  }
  void deallocate(T* p, size_t n) {
    assert(log);
    std::allocator<T>{}.deallocate(p, n);
    if (p) {
      log->push_back(event::deallocation);
    }
  }
};

template <typename T>
bool operator==(const mock_allocator<T>& lhs, const mock_allocator<T>& rhs) {
  return lhs.log == rhs.log;
}
template <typename T>
bool operator!=(const mock_allocator<T>& lhs, const mock_allocator<T>& rhs) {
  return lhs.log != rhs.log;
}

TEST(DoubleIndexMap, move_construct)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log;
  allocator_type alloc{&log};
  {
    map_type m1{alloc};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_EQ(1u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));
    {
      map_type m2{std::move(m1)};
      EXPECT_EQ(1u, std::count(log.begin(), log.end(), event::allocation));
      EXPECT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));
    }
    EXPECT_EQ(1u, std::count(log.begin(), log.end(), event::allocation));
    EXPECT_EQ(1u, std::count(log.begin(), log.end(), event::deallocation));
  }
  EXPECT_EQ(1u, std::count(log.begin(), log.end(), event::allocation));
  EXPECT_EQ(1u, std::count(log.begin(), log.end(), event::deallocation));
}

TEST(DoubleIndexMap, move_construct_move_only_value)
{
  DoubleIndexMap<std::string, std::unique_ptr<int>> m1;
  ASSERT_TRUE(m1.insert("a", std::make_unique<int>(1)).second);
  DoubleIndexMap<std::string, std::unique_ptr<int>> m2{std::move(m1)};
  ASSERT_EQ(1, m2.size());
  EXPECT_EQ(1, *m2.begin()->second);
}

TEST(DoubleIndexMap, move_assign_no_propagate_smaller)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc1{&log1}, alloc2{&log2};
  {
    map_type m1{alloc1};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{alloc2};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_TRUE(m2.insert("B", 20).second);
    ASSERT_TRUE(m2.insert("C", 30).second);
    ASSERT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));

    m2 = std::move(m1); // recycles A and frees B and C
    EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(2u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
}

TEST(DoubleIndexMap, move_assign_no_propagate_larger)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc1{&log1}, alloc2{&log2};
  {
    map_type m1{alloc1};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_TRUE(m1.insert("b", 2).second);
    ASSERT_TRUE(m1.insert("c", 3).second);
    ASSERT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{alloc2};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_EQ(1u, std::count(log2.begin(), log2.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));

    m2 = std::move(m1); // recycles A and allocates b and c
    EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
}

TEST(DoubleIndexMap, move_assign_no_propagate_same_larger)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log;
  allocator_type alloc{&log};
  {
    map_type m1{alloc};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_TRUE(m1.insert("b", 2).second);
    ASSERT_TRUE(m1.insert("c", 3).second);
    ASSERT_EQ(3u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    map_type m2{alloc};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    m2 = std::move(m1); // recycles A and takes ownership of b c
    EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    EXPECT_EQ(1u, std::count(log.begin(), log.end(), event::deallocation));
  }
  EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
  EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::deallocation));
}

TEST(DoubleIndexMap, move_assign_no_propagate_same_smaller)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log;
  allocator_type alloc{&log};
  {
    map_type m1{alloc};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_EQ(1u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    map_type m2{alloc};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_TRUE(m2.insert("B", 20).second);
    ASSERT_TRUE(m2.insert("C", 30).second);
    ASSERT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    m2 = std::move(m1); // recycles A and frees a B C
    EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    EXPECT_EQ(3u, std::count(log.begin(), log.end(), event::deallocation));
  }
  EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
  EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::deallocation));
}

TEST(DoubleIndexMap, copy_assign_no_propagate_smaller)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc1{&log1}, alloc2{&log2};
  {
    map_type m1{alloc1};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{alloc2};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_TRUE(m2.insert("B", 20).second);
    ASSERT_TRUE(m2.insert("C", 30).second);
    ASSERT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));

    m2 = m1; // recycles A and frees B and C
    EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(2u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
}

TEST(DoubleIndexMap, copy_assign_no_propagate_larger)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc1{&log1}, alloc2{&log2};
  {
    map_type m1{alloc1};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_TRUE(m1.insert("b", 2).second);
    ASSERT_TRUE(m1.insert("c", 3).second);
    ASSERT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{alloc2};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_EQ(1u, std::count(log2.begin(), log2.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));

    m2 = m1; // recycles A and allocates b and c
    EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
}

TEST(DoubleIndexMap, copy_assign_no_propagate_same_smaller)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log;
  allocator_type alloc{&log};
  {
    map_type m1{alloc};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_EQ(1u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    map_type m2{alloc};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_TRUE(m2.insert("B", 20).second);
    ASSERT_TRUE(m2.insert("C", 30).second);
    ASSERT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    m2 = m1; // recycles A and frees b and c
    EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    EXPECT_EQ(2u, std::count(log.begin(), log.end(), event::deallocation));
  }
  EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
  EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::deallocation));
}

TEST(DoubleIndexMap, copy_assign_no_propagate_same_larger)
{
  using allocator_type = mock_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log;
  allocator_type alloc{&log};
  {
    map_type m1{alloc};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_TRUE(m1.insert("b", 2).second);
    ASSERT_TRUE(m1.insert("c", 3).second);
    ASSERT_EQ(3u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    map_type m2{alloc};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    m2 = m1; // recycles A and allocates b and c
    EXPECT_EQ(6u, std::count(log.begin(), log.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));
  }
  EXPECT_EQ(6u, std::count(log.begin(), log.end(), event::allocation));
  EXPECT_EQ(6u, std::count(log.begin(), log.end(), event::deallocation));
}

template <typename T>
struct mock_propagate_allocator : mock_allocator<T> {
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using mock_allocator<T>::mock_allocator;
};

TEST(DoubleIndexMap, move_assign_propagate_smaller)
{
  using allocator_type = mock_propagate_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc1{&log1}, alloc2{&log2};
  {
    map_type m1{alloc1};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{alloc2};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_TRUE(m2.insert("B", 20).second);
    ASSERT_TRUE(m2.insert("C", 30).second);
    ASSERT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));

    m2 = std::move(m1); // frees A B C using alloc2 and takes ownership of a
    EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(1u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
}

TEST(DoubleIndexMap, move_assign_propagate_larger)
{
  using allocator_type = mock_propagate_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc1{&log1}, alloc2{&log2};
  {
    map_type m1{alloc1};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_TRUE(m1.insert("b", 2).second);
    ASSERT_TRUE(m1.insert("c", 3).second);
    ASSERT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{alloc2};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_EQ(1u, std::count(log2.begin(), log2.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));

    m2 = std::move(m1); // frees A using alloc2 and takes ownership of a b c
    EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(1u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(1u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(1u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(1u, std::count(log2.begin(), log2.end(), event::deallocation));
}

TEST(DoubleIndexMap, copy_assign_propagate_smaller)
{
  using allocator_type = mock_propagate_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc1{&log1}, alloc2{&log2};
  {
    map_type m1{alloc1};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_EQ(1u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{alloc2};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_TRUE(m2.insert("B", 20).second);
    ASSERT_TRUE(m2.insert("C", 30).second);
    ASSERT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));

    m2 = m1; // frees A B C using alloc2 and allocates a using alloc1
    EXPECT_EQ(2u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(2u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(2u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
}

TEST(DoubleIndexMap, copy_assign_propagate_larger)
{
  using allocator_type = mock_propagate_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc1{&log1}, alloc2{&log2};
  {
    map_type m1{alloc1};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_TRUE(m1.insert("b", 2).second);
    ASSERT_TRUE(m1.insert("c", 3).second);
    ASSERT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{alloc2};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_EQ(1u, std::count(log2.begin(), log2.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));

    m2 = m1; // frees A using alloc2 and allocates a b c using alloc1
    EXPECT_EQ(6u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(1u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(1u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(6u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(6u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(1u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(1u, std::count(log2.begin(), log2.end(), event::deallocation));
}

TEST(DoubleIndexMap, copy_assign_propagate_same_smaller)
{
  using allocator_type = mock_propagate_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log;
  allocator_type alloc{&log};
  {
    map_type m1{alloc};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_EQ(1u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    map_type m2{alloc};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_TRUE(m2.insert("B", 20).second);
    ASSERT_TRUE(m2.insert("C", 30).second);
    ASSERT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    m2 = m1; // recycles A and frees B and C
    EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    EXPECT_EQ(2u, std::count(log.begin(), log.end(), event::deallocation));
  }
  EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
  EXPECT_EQ(4u, std::count(log.begin(), log.end(), event::deallocation));
}

TEST(DoubleIndexMap, copy_assign_propagate_same_larger)
{
  using allocator_type = mock_propagate_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log;
  allocator_type alloc{&log};
  {
    map_type m1{alloc};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_TRUE(m1.insert("b", 2).second);
    ASSERT_TRUE(m1.insert("c", 3).second);
    ASSERT_EQ(3u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    map_type m2{alloc};
    ASSERT_TRUE(m2.insert("A", 10).second);
    ASSERT_EQ(4u, std::count(log.begin(), log.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));

    m2 = m1; // recycles A and allocates b and c
    EXPECT_EQ(6u, std::count(log.begin(), log.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log.begin(), log.end(), event::deallocation));
  }
  EXPECT_EQ(6u, std::count(log.begin(), log.end(), event::allocation));
  EXPECT_EQ(6u, std::count(log.begin(), log.end(), event::deallocation));
}

template <typename T>
struct mock_select_allocator : mock_allocator<T> {
  event_log* select_log = nullptr;

  explicit mock_select_allocator(event_log* log, event_log* select_log) noexcept
    : mock_allocator<T>(log), select_log(select_log) {}

  mock_select_allocator(const mock_select_allocator& other) noexcept
    : mock_allocator<T>(other.log), select_log(other.select_log) {}
  template <typename U>
  mock_select_allocator(const mock_select_allocator<U>& other) noexcept
    : mock_allocator<T>(other.log), select_log(other.select_log) {}

  // on copy construction, return an allocator that writes to the other log
  mock_select_allocator select_on_container_copy_construction() const {
    return mock_select_allocator{select_log, select_log};
  }
};

TEST(DoubleIndexMap, copy_construct_select)
{
  using allocator_type = mock_select_allocator<char>;
  using map_type = DoubleIndexMap<std::string, int,
        std::less<std::string>, std::less<int>,
        allocator_type>;

  event_log log1, log2;
  allocator_type alloc{&log1, &log2};
  {
    map_type m1{alloc};
    ASSERT_TRUE(m1.insert("a", 1).second);
    ASSERT_TRUE(m1.insert("b", 2).second);
    ASSERT_TRUE(m1.insert("c", 3).second);
    ASSERT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    ASSERT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));

    map_type m2{m1};
    EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log1.begin(), log1.end(), event::deallocation));
    EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
    EXPECT_EQ(0u, std::count(log2.begin(), log2.end(), event::deallocation));
  }
  EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log1.begin(), log1.end(), event::deallocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::allocation));
  EXPECT_EQ(3u, std::count(log2.begin(), log2.end(), event::deallocation));
}

} // namespace ceph
