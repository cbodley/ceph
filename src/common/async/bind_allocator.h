// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

// Based on bind_executor.h from Boost.Asio which is Copyright (c)
// 2003-2019 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/execution_context.hpp>
#include <boost/asio/is_executor.hpp>
#include <boost/asio/uses_executor.hpp>

#include "include/uses_allocator.h"

namespace ceph::async {
namespace detail {
template<typename T>
struct allocator_binder_check
{
  typedef void type;
};

// Helper to automatically define nested typedef result_type.

template<typename T, typename = void>
struct allocator_binder_result_type
{
protected:
  using result_type_or_voide = void;
};

template<typename T>
struct allocator_binder_result_type<
  T, typename allocator_binder_check<typename T::result_type>::type>
{
  using result_type = typename T::result_type;
protected:
  using result_type_or_void = result_type;
};

template<typename R>
struct allocator_binder_result_type<R(*)()>
{
  using result_type = R;
protected:
  using result_type_or_void = result_type;
};

template<typename R>
struct allocator_binder_result_type<R(&)()>
{
  using result_type = R;
protected:
  using result_type_or_void = result_type;
};

template<typename R, typename A1>
struct allocator_binder_result_type<R(*)(A1)>
{
  using result_type = R;
protected:
  using result_type_or_void = result_type;
};

template<typename R, typename A1>
struct allocator_binder_result_type<R(&)(A1)>
{
  using result_type = R;
protected:
  using result_type_or_void = result_type;
};

template <typename R, typename A1, typename A2>
struct allocator_binder_result_type<R(*)(A1, A2)>
{
  using result_type = R;
protected:
  using result_type_or_void = result_type;
};

template <typename R, typename A1, typename A2>
struct allocator_binder_result_type<R(&)(A1, A2)>
{
  using result_type = R;
protected:
  using result_type_or_void = result_type;
};

// Helper to automatically define nested typedef argument_type.

template<typename T, typename = void>
struct allocator_binder_argument_type {};

template<typename T>
struct allocator_binder_argument_type<T,
  typename allocator_binder_check<typename T::argument_type>::type>
{
  using argument_type = typename T::argument_type;
};

template<typename R, typename A1>
struct allocator_binder_argument_type<R(*)(A1)>
{
  using argument_type = A1;
};

template<typename R, typename A1>
struct allocator_binder_argument_type<R(&)(A1)>
{
  using argument_type = A1;
};

// Helper to automatically define nested typedefs first_argument_type and
// second_argument_type.

template <typename T, typename = void>
struct allocator_binder_argument_types {};

template<typename T>
struct allocator_binder_argument_types<T,
  typename allocator_binder_check<typename T::first_argument_type>::type>
{
  using first_argument_type = typename T::first_argument_type;
  using second_argumen_type = typename T::second_argument_type;
};

template<typename R, typename A1, typename A2>
struct allocator_binder_argument_type<R(*)(A1, A2)>
{
  using first_argument_type = A1;
  using second_argument_type = A2;
};

template<typename R, typename A1, typename A2>
struct allocator_binder_argument_type<R(&)(A1, A2)>
{
  using first_argument_type = A1;
  using second_argument_type = A2;
};

// Helper to:
// - Apply the empty base optimisation to the executor.
// - Perform uses_executor construction of the target type, if required.

template<typename T, typename Allocator, bool UsesAllocator>
class allocator_binder_base;

template<typename T, typename Allocator>
class allocator_binder_base<T, Allocator, true>
  : protected Allocator
{
protected:
  template<typename A, typename U>
  allocator_binder_base(A&& a, U&& u)
    : allocator(std::forward<A>(a),
		target(ceph::make_obj_using_allocator(allocator,
						      std::forward<U>(u)))) {}

  Allocator allocator;
  T target;
};

template <typename T, typename Allocator>
class allocator_binder_base<T, Allocator, false>
{
protected:
  template<typename A, typename U>
  allocator_binder_base(A&& a, U&& u)
    : allocator(std::forward<A>(a)),
      target(std::forward<U>(u))
  {
  }

  Allocator allocator;
  T target;
};

// Helper to enable SFINAE on zero-argument operator() below.

template<typename T, typename = void>
struct allocator_binder_result_of0
{
  using type = void;
};

template<typename T>
struct allocator_binder_result_of0<
  T, typename allocator_binder_check<std::result_of_t<T()>>::type>
{
  using type = typename std::result_of_t<T()>;
};
} // namespace detail

/// A call wrapper type to bind an allocator of type @c Allocator to
/// an object of type @c T.
template<typename T, typename Allocator>
class allocator_binder
  : public detail::allocator_binder_result_type<T>,
    public detail::allocator_binder_argument_type<T>,
    public detail::allocator_binder_argument_types<T>,
    private detail::allocator_binder_base<T, Allocator,
					  std::uses_allocator<T, Allocator>
					  ::value>
{
public:
  /// The type of the target object.
  using target_type = T;

  /// The type of the associated executor.
  using allocator_type = Allocator;

  /// Construct an allocator wrapper for the specified object.
  /**
   * This constructor is only valid if the type @c T is constructible from type
   * @c U.
   */
  template<typename U>
  allocator_binder(std::allocator_arg_t, const allocator_type& a,
		   U&& u)
    : base_type(a, std::forward<U>(u)) {}

  /// Copy constructor.
  allocator_binder(const allocator_binder& other)
    : base_type(other.get_allocator(), other.get()) {}

  /// Construct a copy, but specify a different executor.
  allocator_binder(std::allocator_arg_t, const allocator_type& e,
		   const allocator_binder& other)
    : base_type(e, other.get()) {}

  /// Construct a copy of a different allocator wrapper type.
  /**
   * This constructor is only valid if the @c Allocator type is
   * constructible from type @c OtherAllocator, and the type @c T is
   * constructible from type @c U.
   */
  template<typename U, typename OtherAllocator>
  allocator_binder(const allocator_binder<U, OtherAllocator>& other)
    : base_type(other.get_allocator(), other.get()) {}

  /// Construct a copy of a different allocator wrapper type, but specify a
  /// different executor.
  /**
   * This constructor is only valid if the type @c T is constructible from type
   * @c U.
   */
  template<typename U, typename OtherAllocator>
  allocator_binder(std::allocator_arg_t, const allocator_type& a,
		   const allocator_binder<U, OtherAllocator>& other)
    : base_type(a, other.get()) {}

  /// Move constructor.
  allocator_binder(allocator_binder&& other)
    : base_type(std::move(other.get_allocator()),
		std::move(other.get())) {}

  /// Move construct the target object, but specify a different executor.
  allocator_binder(std::allocator_arg_t, const allocator_type& e,
		   allocator_binder&& other)
    : base_type(e, std::move(other.get())) {}

  /// Move construct from a different executor wrapper type.
  template<typename U, typename OtherExecutor>
  allocator_binder(allocator_binder<U, OtherExecutor>&& other)
    : base_type(std::move(other.get_executor()), std::move(other.get())) {}

  /// Move construct from a different executor wrapper type, but specify a
  /// different executor.
  template<typename U, typename OtherAllocator>
  allocator_binder(std::allocator_arg_t, const allocator_type& a,
		   allocator_binder<U, OtherAllocator>&& other)
    : base_type(a, std::move(other.get())) {}

  /// Destructor.
  ~allocator_binder() = default;

  /// Obtain a reference to the target object.
  target_type& get() noexcept
  {
    return this->target;
  }

  /// Obtain a reference to the target object.
  const target_type& get() const noexcept
  {
    return this->target;
  }

  /// Obtain the associated executor.
  allocator_type get_allocator() const noexcept {
    return this->allocator;
  }

  /// Forwarding function call operator.
  template<typename... Args>
  decltype(auto) operator()(Args&&... args) {
    return this->target(std::forward<Args>(args)...);
  }

  /// Forwarding function call operator.
  template<typename... Args>
  decltype(auto) operator()(Args&&... args) const {
    return this->target(std::forward<Args>(args)...);
  }


private:
  using base_type =
    detail::allocator_binder_base<T, Allocator,
				  std::uses_allocator_v<T, Allocator>>;
};

/// Associate an object of type @c T with an allocator of type @c Allocator.
template<typename Allocator, typename T>
inline allocator_binder<typename std::decay_t<T>, Allocator>
bind_allocator(const Allocator& a, T&& t)
{
  return allocator_binder<std::decay_t<T>, Allocator>(std::allocator_arg_t(),
						      a, std::forward<T>(t));
}
} // namespace ceph::async

// Since we have an allocator_type member we shouldn't need a
// uses_allocator specialization.

namespace boost::asio {
template<typename T, typename Allocator, typename Signature>
class async_result<ceph::async::allocator_binder<T, Allocator>, Signature>
{
public:
  using completion_handler_type =
    ceph::async::allocator_binder<
  typename async_result<T, Signature>::completion_handler_type, Allocator>;

  using return_type = typename async_result<T, Signature>::return_type;

  explicit async_result(ceph::async::allocator_binder<T, Allocator>& b)
    : target(b.get()) {}

  return_type get() {
    return target.get();
  }

private:
  async_result(const async_result&) = delete;
  async_result& operator=(const async_result&) = delete;

  async_result<T, Signature> target;
};

template<typename T, typename Allocator, typename Allocator1>
struct associated_allocator<ceph::async::allocator_binder<T, Allocator>,
			    Allocator1>
{
  using type = Allocator1;

  static type get(const ceph::async::allocator_binder<T,Allocator>& b,
		  const Allocator1& = {}) noexcept {
    return b.get_allocator();
  }
};

template<typename T, typename Allocator>
struct associated_executor<ceph::async::allocator_binder<T, Allocator>>
{
  using type = typename associated_executor<T>::type;

  static type get(const ceph::async::allocator_binder<T, Allocator>& b) noexcept {
    return associated_executor<T>::get(b.get());
  }
};
} // namespace boost::asio
