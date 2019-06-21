// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc
 *
 * Author: Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <limits>
#include <memory>
#include <boost/intrusive/set.hpp>

namespace ceph {

/** @brief a bidirectional associative container
 *
 * an associative container that keeps an index on both the keys and values.
 * keys are required to be unique, but values are not. the class presents a
 * std::map-like interface where iterators are sorted by key. member function
 * reverse_mapping() returns a std::multimap-like interface that provides the
 * mapping from values back to their keys. all iterators are exposed as
 * const_iterators in order to keep both indices consistent. use member function
 * replace() to modify an element's key and/or value
 *
 * @tparam Key key type that meets the requirements of MoveConstructible and
 *         MoveAssignable
 * @tparam T mapped value type that meets the requirements of MoveConstructible
 *         and MoveAssignable
 * @tparam KeyCompare key comparison type that meets the requirements of Compare
 * @tparam TCompare value comparison type that meets the requirements of Compare
 * @tparam Allocator allocator type that meets the requirements of Allocator
 */
template <typename Key,
          typename T,
          typename KeyCompare = std::less<Key>,
          typename TCompare = std::less<T>,
          typename Allocator = std::allocator<std::pair<Key, T>>>
class DoubleIndexMap {
  using pair_type = std::pair<Key, T>;
  /// internal element type that maintains a position in both indices
  struct entry_type : pair_type {
    using pair_type::pair_type;
    boost::intrusive::set_member_hook<> key_hook;
    boost::intrusive::set_member_hook<> value_hook;
  };
  struct value_compare_key; //< compare value_types by key
  using key_hook = boost::intrusive::member_hook<entry_type,
        boost::intrusive::set_member_hook<>, &entry_type::key_hook>;
  using key_set = boost::intrusive::set<entry_type, key_hook,
        boost::intrusive::compare<value_compare_key>>;

  struct value_compare_mapped; //< compare value_types by mapped value
  using value_hook = boost::intrusive::member_hook<entry_type,
        boost::intrusive::set_member_hook<>, &entry_type::value_hook>;
  using value_set = boost::intrusive::multiset<entry_type, value_hook,
        boost::intrusive::compare<value_compare_mapped>>;

  using alloc_traits = std::allocator_traits<Allocator>;

 public:
  using key_type = Key;
  using mapped_type = T;
  using value_type = pair_type;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using key_compare = KeyCompare;
  using mapped_compare = TCompare;
  using value_compare = value_compare_key;
  using allocator_type = Allocator;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = typename std::allocator_traits<allocator_type>::pointer;
  using const_pointer = typename std::allocator_traits<allocator_type>::const_pointer;

  /// default constructs an empty map
  DoubleIndexMap()
      noexcept(std::is_nothrow_default_constructible<allocator_type>::value) {}
  /// default constructs an empty map with the given allocator
  explicit DoubleIndexMap(const Allocator& alloc) noexcept : alloc(alloc) {}
  /// disposes of all entries
  ~DoubleIndexMap() { clear(); }

  /// move constructs a new map
  DoubleIndexMap(DoubleIndexMap&& other) noexcept;
  /// move constructs a new map using the given allocator
  DoubleIndexMap(DoubleIndexMap&& other, const allocator_type& alloc)
      noexcept(alloc_traits::is_always_equal::value);
  /// move assigns from the given map
  DoubleIndexMap& operator=(DoubleIndexMap&& other)
      noexcept(alloc_traits::propagate_on_container_move_assignment::value ||
               alloc_traits::is_always_equal::value);

  /// copy constructs a new map
  DoubleIndexMap(const DoubleIndexMap& other);
  /// copy constructs a new map using the given allocator
  DoubleIndexMap(const DoubleIndexMap& other, const allocator_type& alloc);
  /// copy assigns from the given map
  DoubleIndexMap& operator=(const DoubleIndexMap& other);

  /// returns the associated allocator
  allocator_type get_allocator() const noexcept { return alloc; }

  /// @brief a generic reverse-mapping interface similar to std::multimap, where
  /// values are mapped back to their keys
  ///
  /// @tparam Mutable true for a mutable view that provides Modifer interfaces
  template <bool Mutable> class basic_reversed_multimap;

  /// a mutable view of the reverse-mapping from values back to their keys
  class reversed_multimap final : public basic_reversed_multimap<true> {};
  /// returns a mutable handle to the reverse mapping from values to keys
  reversed_multimap reverse_mapping();

  /// an immutable view of the reverse-mapping from values back to their keys
  class const_reversed_multimap final : public basic_reversed_multimap<false> {};
  /// returns an immutable handle to the reverse mapping from values to keys
  const_reversed_multimap reverse_mapping() const;

  /** @name Observers */
  ///@{

  /// returns a function object that compares keys
  key_compare key_comp() const { return {}; }
  /// returns a function object that compares elements by key
  value_compare value_comp() const { return {}; }

  ///@}

  /** @name Capacity */
  ///@{

  /// returns the number of elements
  size_type size() const noexcept { return keys.size(); }
  /// returns the implementation-defined maximum number of elements
  constexpr size_type max_size() const noexcept {
    return std::numeric_limits<size_type>::max();
  }
  /// returns whether or not the container is empty
  bool empty() const noexcept { return keys.empty(); }

  ///@}

  /** @name Iterators */
  ///@{
  using const_iterator = typename key_set::const_iterator;
  using const_reverse_iterator = typename key_set::const_reverse_iterator;

  /// returns a const iterator to the first key
  const_iterator begin() const noexcept { return keys.begin(); }
  /// returns a const iterator to the first key
  const_iterator cbegin() const noexcept { return keys.cbegin(); }

  /// returns a const iterator to one-past the last key
  const_iterator end() const noexcept { return keys.end(); }
  /// returns a const iterator to one-past the last key
  const_iterator cend() const noexcept { return keys.cend(); }

  /// returns a const reverse iterator to the first key
  const_reverse_iterator rbegin() const noexcept { return keys.rbegin(); }
  /// returns a const reverse iterator to the first key
  const_reverse_iterator crbegin() const noexcept { return keys.crbegin(); }

  /// returns a const reverse iterator to one-past the last key
  const_reverse_iterator rend() const noexcept { return keys.rend(); }
  /// returns a const reverse iterator to one-past the last key
  const_reverse_iterator crend() const noexcept { return keys.crend(); }

  ///@}

  /** @name Lookup */
  ///@{

  /// returns the number of elements matching the given key
  size_type count(const key_type& key) const {
    return keys.count(key, value_comp());
  }
  /// finds an element matching the given key
  const_iterator find(const key_type& key) const {
    return keys.find(key, value_comp());
  }
  /// returns the range of elements matching the given key
  std::pair<const_iterator, const_iterator>
  equal_range(const key_type& key) const {
    return keys.equal_range(key, value_comp());
  }
  /// returns the first element greater than or equal to the given key
  const_iterator lower_bound(const key_type& key) const {
    return keys.lower_bound(key, value_comp());
  }
  /// returns the first element greater than the given key
  const_iterator upper_bound(const key_type& key) const {
    return keys.upper_bound(key, value_comp());
  }

  ///@}

  /** @name Modifiers */
  ///@{

  /// erase all elements, invalidating outstanding references and iterators
  void clear() noexcept;

  /// try to insert a key/value pair. if an element with that key exists,
  /// returns the existing iterator and false. otherwise returns the inserted
  /// iterator and true
  std::pair<const_iterator, bool> insert(key_type key, mapped_type value);

  /// removes the element at the given position and returns the next iterator.
  /// references and iterators to the erased element are invalidated
  const_iterator erase(const_iterator pos);

  /// erase elements in the range [first, last). references and iterators to
  /// erased elements are invalidated
  void erase(const_iterator first, const_iterator last);

  /// erase the element that matches the given key, if any
  size_type erase(const key_type& key);

  /// replace the value of an existing element and update the value index. does
  /// not invalidate any references or iterators
  void replace(const_iterator pos, mapped_type value);

  /// replace the key and value of an existing element and update both indices.
  /// does not invalidate any references or iterators
  void replace(const_iterator pos, key_type key, mapped_type value);

  /// swaps the contents of two containers. does not invalidate any references
  /// or iterators. all associations between the containers and their handles
  /// returned by reverse_mapping() are preserved
  void swap(DoubleIndexMap& other) noexcept;

  ///@}

 private:
  /// primary index sorted by key, with ownership of its entries
  key_set keys;
  /// secondary index sorted by value, with no ownership
  value_set values;
  /// custom entry allocator
  allocator_type alloc;

  /// allocator-aware deleter
  struct custom_delete;

  using rebind_alloc = typename alloc_traits::template rebind_alloc<entry_type>;
  using rebind_traits = std::allocator_traits<rebind_alloc>;

  /// move assign from the given container, recycling any existing elements
  void move_assign(DoubleIndexMap& other);
  /// copy assign from the given container, recycling any existing elements
  void copy_assign(const DoubleIndexMap& other);

  using const_mapped_iterator = typename value_set::const_iterator;
  using const_reverse_mapped_iterator = typename value_set::const_reverse_iterator;

  /// erase a mapped range of elements
  void erase(const_mapped_iterator first, const_mapped_iterator last);
  /// replace the key of a value iterator
  void replace(const_mapped_iterator first, key_type key);
};

/// compare value_types by key
template <typename K, typename T, typename KC, typename TC, typename A>
struct DoubleIndexMap<K, T, KC, TC, A>::value_compare_key {
  bool operator()(const_reference lhs, const_reference rhs) const {
    return KC{}(lhs.first, rhs.first);
  }
  bool operator()(const key_type& lhs, const_reference rhs) const {
    return KC{}(lhs, rhs.first);
  }
  bool operator()(const_reference lhs, const key_type& rhs) const {
    return KC{}(lhs.first, rhs);
  }
};

/// compare value_types by mapped value
template <typename K, typename T, typename KC, typename TC, typename A>
struct DoubleIndexMap<K, T, KC, TC, A>::value_compare_mapped {
  bool operator()(const_reference lhs, const_reference rhs) const {
    return TC{}(lhs.second, rhs.second);
  }
  bool operator()(const mapped_type& lhs, const_reference rhs) const {
    return TC{}(lhs, rhs.second);
  }
  bool operator()(const_reference lhs, const mapped_type& rhs) const {
    return TC{}(lhs.second, rhs);
  }
};

/// allocator-aware deleter
template <typename K, typename T, typename KC, typename TC, typename A>
struct DoubleIndexMap<K, T, KC, TC, A>::custom_delete {
  rebind_alloc a;
  custom_delete(rebind_alloc a) : a(a) {}
  void operator()(entry_type* p) {
    rebind_traits::destroy(a, p);
    rebind_traits::deallocate(a, p, 1);
  }
};

template <typename K, typename T, typename KC, typename TC, typename A>
DoubleIndexMap<K, T, KC, TC, A>::DoubleIndexMap(DoubleIndexMap&& other) noexcept
  : keys(std::move(other.keys)),
    values(std::move(other.values)),
    alloc(std::move(other.alloc))
{
}

template <typename K, typename T, typename KC, typename TC, typename A>
DoubleIndexMap<K, T, KC, TC, A>::DoubleIndexMap(DoubleIndexMap&& other,
                                                const allocator_type& alloc)
  noexcept(alloc_traits::is_always_equal::value)
  : alloc(alloc)
{
  if constexpr (alloc_traits::is_always_equal::value) {
    keys = std::move(other.keys);
    values = std::move(other.values);
  } else if (other.alloc == alloc) {
    keys = std::move(other.keys);
    values = std::move(other.values);
  } else {
    try {
      move_assign(other);
    } catch (const std::exception&) {
      values.clear();
      keys.clear_and_dispose(custom_delete{alloc});
      throw;
    }
  }
}

template <typename K, typename T, typename KC, typename TC, typename A>
DoubleIndexMap<K, T, KC, TC, A>&
DoubleIndexMap<K, T, KC, TC, A>::operator=(DoubleIndexMap&& other)
  noexcept(alloc_traits::propagate_on_container_move_assignment::value ||
           alloc_traits::is_always_equal::value)
{
  if constexpr (alloc_traits::propagate_on_container_move_assignment::value) {
    clear();
    alloc = std::move(other.alloc);
    keys = std::move(other.keys);
    values = std::move(other.values);
  } else if constexpr (alloc_traits::is_always_equal::value) {
    clear();
    keys = std::move(other.keys);
    values = std::move(other.values);
  } else if (alloc == other.alloc) {
    clear();
    keys = std::move(other.keys);
    values = std::move(other.values);
  } else {
    move_assign(other);
  }
  return *this;
}

template <typename K, typename T, typename KC, typename TC, typename A>
DoubleIndexMap<K, T, KC, TC, A>::DoubleIndexMap(const DoubleIndexMap& other)
  : alloc(alloc_traits::select_on_container_copy_construction(other.alloc))
{
  try {
    copy_assign(other);
  } catch (const std::exception&) {
    values.clear();
    keys.clear_and_dispose(custom_delete{alloc});
    throw;
  }
}

template <typename K, typename T, typename KC, typename TC, typename A>
DoubleIndexMap<K, T, KC, TC, A>::DoubleIndexMap(const DoubleIndexMap& other,
                                                const allocator_type& alloc)
  : alloc(alloc)
{
  try {
    copy_assign(other);
  } catch (const std::exception&) {
    values.clear();
    keys.clear_and_dispose(custom_delete{alloc});
    throw;
  }
}

template <typename K, typename T, typename KC, typename TC, typename A>
DoubleIndexMap<K, T, KC, TC, A>&
DoubleIndexMap<K, T, KC, TC, A>::operator=(const DoubleIndexMap& other)
{
  if (this == &other) {
    return *this;
  }
  if constexpr (alloc_traits::propagate_on_container_copy_assignment::value &&
                !alloc_traits::is_always_equal::value) {
    // cannot recycle entries if the allocator is propagated and not equal
    if (alloc != other.alloc) {
      clear();
      alloc = other.alloc;
    }
  }
  copy_assign(other);
  return *this;
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::clear() noexcept
{
  values.clear();
  keys.clear_and_dispose(custom_delete{alloc});
}

template <typename K, typename T, typename KC, typename TC, typename A>
std::pair<typename DoubleIndexMap<K, T, KC, TC, A>::const_iterator, bool>
DoubleIndexMap<K, T, KC, TC, A>::insert(key_type key, mapped_type value)
{
  // check whether we can insert the key
  typename key_set::insert_commit_data commit;
  auto result = keys.insert_check(key, value_compare{}, commit);
  if (result.second) {
    // key not found, allocate a new entry and insert in place
    rebind_alloc a{alloc};
    auto p = rebind_traits::allocate(a, 1);
    try {
      rebind_traits::construct(a, p, std::move(key), std::move(value));
    } catch (const std::exception&) {
      rebind_traits::deallocate(a, p, 1);
      throw;
    }
    result.first = keys.insert_commit(*p, commit);
    values.insert(*p);
  }
  return result;
}

template <typename K, typename T, typename KC, typename TC, typename A>
typename DoubleIndexMap<K, T, KC, TC, A>::const_iterator
DoubleIndexMap<K, T, KC, TC, A>::erase(const_iterator pos)
{
  values.erase(values.iterator_to(*pos));
  return keys.erase_and_dispose(pos, custom_delete{alloc});
}

template <typename K, typename T, typename KC, typename TC, typename A>
typename DoubleIndexMap<K, T, KC, TC, A>::size_type
DoubleIndexMap<K, T, KC, TC, A>::erase(const key_type& key)
{
  if (auto k = find(key); k != end()) {
    erase(k);
    return 1;
  }
  return 0;
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::erase(const_iterator first,
                                            const_iterator last)
{
  for (auto k = first; k != last; ) {
    values.erase(values.iterator_to(*k));
    k = keys.erase_and_dispose(k, custom_delete{alloc});
  }
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::erase(const_mapped_iterator first,
                                            const_mapped_iterator last)
{
  for (auto i = first; i != last; ) {
    auto k = keys.iterator_to(*i);
    i = values.erase(i);
    keys.erase_and_dispose(k, custom_delete{alloc});
  }
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::replace(const_iterator pos,
                                              mapped_type value)
{
  auto& entry = const_cast<entry_type&>(*pos);
  // reinsert with the new value
  values.erase(values.iterator_to(entry));
  try {
    entry.second = std::move(value);
  } catch (const std::exception&) {
    keys.erase_and_dispose(pos, custom_delete{alloc});
    throw;
  }
  values.insert(entry);
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::replace(const_mapped_iterator pos,
                                              key_type key)
{
  auto& entry = const_cast<entry_type&>(*pos);
  // reinsert with the new key
  keys.erase(keys.iterator_to(entry));
  try {
    entry.first = std::move(key);
  } catch (const std::exception&) {
    custom_delete{alloc}(&entry);
    throw;
  }
  keys.insert(entry);
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::replace(const_iterator pos,
                                              key_type key, mapped_type value)
{
  auto& entry = const_cast<entry_type&>(*pos);
  // reinsert with the new key/value
  keys.erase(pos);
  values.erase(values.iterator_to(entry));
  try {
    entry.first = std::move(key);
    entry.second = std::move(value);
  } catch (const std::exception&) {
    custom_delete{alloc}(&entry);
    throw;
  }
  keys.insert(entry);
  values.insert(entry);
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::swap(DoubleIndexMap& other) noexcept
{
  using std::swap;
  if constexpr (alloc_traits::propagate_on_container_swap::value) {
    swap(alloc, other.alloc);
  }
  swap(keys, other.keys);
  swap(values, other.values);
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::move_assign(DoubleIndexMap& other)
{
  values.clear();
  key_set free_keys = std::move(keys);
  auto free_key = free_keys.begin();
  auto key = other.keys.begin();
  // recycle and move assign old entries
  while (key != other.keys.end() && free_key != free_keys.end()) {
    auto& entry = *free_key;
    free_key = free_keys.erase(free_key);
    try {
      entry = std::move(*key);
    } catch (const std::exception&) {
      custom_delete deleter{alloc};
      deleter(&entry);
      free_keys.clear_and_dispose(deleter);
      throw;
    }
    keys.insert(entry);
    values.insert(entry);
    // erase other key
    other.values.erase(other.values.iterator_to(*key));
    key = other.keys.erase_and_dispose(key, custom_delete{other.alloc});
  }
  // allocate and move construct additional entries
  rebind_alloc a{alloc};
  while (key != other.keys.end()) {
    // allocate and move construct
    auto p = rebind_traits::allocate(a, 1);
    try {
      rebind_traits::construct(a, p, std::move(*key));
    } catch (const std::exception&) {
      rebind_traits::deallocate(a, p, 1);
      free_keys.clear_and_dispose(custom_delete{alloc});
      throw;
    }
    keys.insert(*p);
    values.insert(*p);
    // erase other key
    other.values.erase(other.values.iterator_to(*key));
    key = other.keys.erase_and_dispose(key, custom_delete{other.alloc});
  }
  // clean up any old entries remaining
  if (free_key != free_keys.end()) {
    free_keys.clear_and_dispose(custom_delete{alloc});
  }
}

template <typename K, typename T, typename KC, typename TC, typename A>
void DoubleIndexMap<K, T, KC, TC, A>::copy_assign(const DoubleIndexMap& other)
{
  values.clear();
  key_set free_keys = std::move(keys);
  auto free_key = free_keys.begin();
  auto key = other.begin();
  // recycle and copy assign old entries
  for (; key != other.end() && free_key != free_keys.end(); ++key) {
    auto& entry = *free_key;
    free_key = free_keys.erase(free_key);
    try {
      entry = *key;
    } catch (const std::exception&) {
      custom_delete deleter{alloc};
      deleter(&entry);
      free_keys.clear_and_dispose(deleter);
      throw;
    }
    keys.insert(entry);
    values.insert(entry);
  }
  // allocate and copy construct additional entries
  rebind_alloc a{alloc};
  for (; key != other.end(); ++key) {
    auto p = rebind_traits::allocate(a, 1);
    try {
      rebind_traits::construct(a, p, *key);
    } catch (const std::exception&) {
      rebind_traits::deallocate(a, p, 1);
      free_keys.clear_and_dispose(custom_delete{alloc});
      throw;
    }
    keys.insert(*p);
    values.insert(*p);
  }
  // clean up any old entries remaining
  if (free_key != free_keys.end()) {
    free_keys.clear_and_dispose(custom_delete{alloc});
  }
}


/// swaps the contents of two containers. does not invalidate any references
/// or iterators. all associations between the containers and their handles
/// returned by reverse_mapping() are preserved
template <typename K, typename T, typename KC, typename TC, typename A>
void swap(DoubleIndexMap<K, T, KC, TC, A>& lhs,
          DoubleIndexMap<K, T, KC, TC, A>& rhs) noexcept
{
  lhs.swap(rhs);
}

/// checks the contents of two DoubleIndexMap containers for equality
template <typename K, typename T, typename KC, typename TC, typename A>
void operator==(const DoubleIndexMap<K, T, KC, TC, A>& lhs,
                const DoubleIndexMap<K, T, KC, TC, A>& rhs)
{
  return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

/// checks the contents of two DoubleIndexMap containers for inequality
template <typename K, typename T, typename KC, typename TC, typename A>
void operator!=(const DoubleIndexMap<K, T, KC, TC, A>& lhs,
                const DoubleIndexMap<K, T, KC, TC, A>& rhs)
{
  return !(lhs == rhs);
}


template <typename K, typename T, typename KC, typename TC, typename A>
template <bool Mutable>
class DoubleIndexMap<K, T, KC, TC, A>::basic_reversed_multimap {
  friend class DoubleIndexMap<K, T, KC, TC, A>;
  using map_type = DoubleIndexMap<K, T, KC, TC, A>;
  using map_reference = std::conditional_t<Mutable, map_type&, const map_type&>;
  map_reference m;
  basic_reversed_multimap(map_reference m) noexcept : m(m) {}
 public:
  // copy/move disabled
  basic_reversed_multimap(basic_reversed_multimap&&) = delete;
  basic_reversed_multimap& operator=(basic_reversed_multimap&&) = delete;
  basic_reversed_multimap(const basic_reversed_multimap&) = delete;
  basic_reversed_multimap& operator=(const basic_reversed_multimap&) = delete;

  using key_type = typename map_type::mapped_type;
  using mapped_type = typename map_type::key_type;
  using value_type = typename map_type::value_type;
  using size_type = typename map_type::size_type;
  using difference_type = typename map_type::difference_type;
  using key_compare = typename map_type::mapped_compare;
  using mapped_compare = typename map_type::key_compare;
  using value_compare = value_compare_mapped;
  using allocator_type = typename map_type::allocator_type;
  using reference = typename map_type::reference;
  using const_reference = typename map_type::const_reference;
  using pointer = typename map_type::pointer;
  using const_pointer = typename map_type::const_pointer;

  /** @name Observers */
  ///@{

  /// return a function object that compares two values
  key_compare key_comp() const { return {}; }
  /// return a function object that compares two elements by value
  value_compare value_comp() const { return {}; }

  ///@}

  /** @name Capacity */
  ///@{

  /// returns the number of elements
  size_type size() const noexcept { return m.values.size(); }
  /// returns the implementation-defined maximum number of elements
  constexpr size_type max_size() const noexcept { return m.max_size(); }
  /// returns whether or not the container is empty
  bool empty() const noexcept { return m.values.empty(); }

  ///@}

  /** @name Iterators */
  ///@{
  using const_iterator = const_mapped_iterator;
  using const_reverse_iterator = const_reverse_mapped_iterator;

  /// returns a const iterator to the first key
  const_iterator begin() const noexcept { return m.values.begin(); }
  /// returns a const iterator to the first key
  const_iterator cbegin() const noexcept { return m.values.cbegin(); }

  /// returns a const iterator to the one-past the last key
  const_iterator end() const noexcept { return m.values.end(); }
  /// returns a const iterator to the one-past the last key
  const_iterator cend() const noexcept { return m.values.cend(); }

  /// returns a const reverse iterator to the first key
  const_reverse_iterator rbegin() const noexcept { return m.values.rbegin(); }
  /// returns a const reverse iterator to the first key
  const_reverse_iterator crbegin() const noexcept { return m.values.crbegin(); }

  /// returns a const reverse iterator to the one-past the last key
  const_reverse_iterator rend() const noexcept { return m.values.rend(); }
  /// returns a const reverse iterator to the one-past the last key
  const_reverse_iterator crend() const noexcept { return m.values.crend(); }

  ///@}

  /** @name Lookup */
  ///@{

  /// returns the number of elements matching the given key
  size_type count(const key_type& value) const {
    return m.values.count(value, value_comp());
  }
  /// finds an element matching the given key
  const_iterator find(const key_type& value) const {
    return m.values.find(value, value_comp());
  }
  /// returns the range of elements matching the given key
  std::pair<const_iterator, const_iterator>
  equal_range(const key_type& value) const {
    return m.values.equal_range(value, value_comp());
  }
  /// returns the first element greater than or equal to the given key
  const_iterator lower_bound(const key_type& value) const {
    return m.values.lower_bound(value, value_comp());
  }
  /// returns the first element greater than the given key
  const_iterator upper_bound(const key_type& value) const {
    return m.values.upper_bound(value, value_comp());
  }

  ///@}

  /** @name Modifiers
   * Modifiers are only provided for the Mutable reversed_multimap
   */
  ///@{

  /// erase all elements, invalidating outstanding references and iterators
  std::enable_if_t<Mutable, void> clear() noexcept {
    m.clear();
  }
  /// removes the element at the given position and returns the next iterator.
  /// references and iterators to the erased element are invalidated
  std::enable_if_t<Mutable, const_iterator> erase(const_iterator pos) {
    auto next = std::next(pos);
    m.erase(m.keys.iterator_to(*pos));
    return next;
  }
  /// erase elements in the range [first, last). references and iterators to
  /// erased elements are invalidated
  std::enable_if_t<Mutable, void>
  erase(const_iterator first, const_iterator last) {
    m.erase(first, last);
  }
  /// erase all elements that match the given key
  std::enable_if_t<Mutable, size_type> erase(const key_type& value) {
    size_type n = 0;
    for (auto i = lower_bound(value); i != end() && i->second == value; n++) {
      i = erase(i);
    }
    return n;
  }
  /// replace the key of an existing element and update the key index. does
  /// not invalidate any references or iterators
  std::enable_if_t<Mutable, void> replace(const_iterator pos, mapped_type key) {
    m.replace(pos, std::move(key));
  }
  /// replace the key and value of an existing element and update both indices.
  /// does not invalidate any references or iterators
  std::enable_if_t<Mutable, void>
  replace(const_iterator pos, mapped_type key, key_type value) {
    m.replace(m.keys.iterator_to(*pos), std::move(key), std::move(value));
  }

  ///@}
};

template <typename K, typename T, typename KC, typename TC, typename A>
typename DoubleIndexMap<K, T, KC, TC, A>::reversed_multimap
DoubleIndexMap<K, T, KC, TC, A>::reverse_mapping()
{
  return {*this};
}

template <typename K, typename T, typename KC, typename TC, typename A>
typename DoubleIndexMap<K, T, KC, TC, A>::const_reversed_multimap
DoubleIndexMap<K, T, KC, TC, A>::reverse_mapping() const
{
  return {*this};
}

} // namespace ceph
