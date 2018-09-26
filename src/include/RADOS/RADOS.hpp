// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <cstddef>
#include <memory>
#include <tuple>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>


#include <boost/asio.hpp>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>


// HATE. LET ME TELL YOU HOW MUCH I'VE COME TO HATE BOOST.SYSTEM SINCE
// I BEGAN TO LIVE.

#include <boost/system/error_code.hpp>

// Needed for type erasure and template support. We can't really avoid
// it.

#include "common/async/completion.h"

// These are needed for RGW, but in general as a 'shiny new interface'
// we should try to use forward declarations and provide standard alternatives.

#include "include/rados/rados_types.hpp"
#include "include/buffer.h"
#include "common/ceph_time.h"

#ifndef RADOS_UNLEASHED_HPP
#define RADOS_UNLEASHED_HPP

class CephContext;

namespace RADOS {
class Object;
}
namespace std {
template<>
struct hash<RADOS::Object>;
}

namespace RADOS {

class RADOS;

// Exists mostly so that repeated operations on the same object don't
// have to pay for the string copy to construct an object_t.

class Object {
  friend RADOS;
  friend std::hash<Object>;
  Object(std::string_view s);
  Object(std::string&& s);
  Object(const std::string& s);
  ~Object();

  Object(const Object& o);
  Object& operator =(const Object& o);

  Object(Object&& o) = delete;
  Object& operator =(Object&& o) = delete;

  operator std::string_view() const;

  friend bool operator <(const Object& lhs, const Object& rhs);
  friend bool operator <=(const Object& lhs, const Object& rhs);
  friend bool operator >=(const Object& lhs, const Object& rhs);
  friend bool operator >(const Object& lhs, const Object& rhs);

  friend bool operator ==(const Object& lhs, const Object& rhs);
  friend bool operator !=(const Object& lhs, const Object& rhs);

private:

  static constexpr std::size_t impl_size = 4 * 8;
  std::aligned_storage_t<impl_size> impl;
};

// Not the same as the librados::IoCtx, but it does gather together
// some of the same metadata. Since we're likely to do multiple
// operations in the same pool or namespace, it doesn't make sense to
// redo a bunch of lookups and string copies.

struct IOContext {
  friend RADOS;

  IOContext();
  explicit IOContext(std::int64_t pool);
  IOContext(std::int64_t _pool, std::string_view _ns);
  IOContext(std::int64_t _pool, std::string&& _ns);
  ~IOContext();

  IOContext(const IOContext& rhs);
  IOContext& operator =(const IOContext& rhs);

  IOContext(IOContext&& rhs);
  IOContext& operator =(IOContext&& rhs);

  std::int64_t pool() const;
  void pool(std::int64_t _pool);

  std::string_view ns() const;
  void ns(std::string_view _ns);
  void ns(std::string&& _ns);

  // Because /some fool/ decided to disallow optional references,
  // you'd have to construct a string in an optional which I would then
  // take an optional reference to. Thus a separate 'clear' method.
  std::optional<std::string_view> key() const;
  void key(std::string_view _key);
  void key(std::string&& _key);
  void clear_key();

  std::optional<std::int64_t> hash() const;
  void hash(std::int64_t _hash);
  void clear_hash();

  std::optional<std::uint64_t> read_snap() const;
  void read_snap(std::optional<std::uint64_t> _snapid);

  // I can't actually move-construct here since snapid_t is its own
  // separate class type, not an alias.
  std::optional<
    std::pair<std::uint64_t,
	      std::vector<std::uint64_t>>> write_snap_context() const;
  void write_snap_context(std::optional<
			  std::pair<std::uint64_t,
			              std::vector<std::uint64_t>>> snapc);
private:

  static constexpr std::size_t impl_size = 16 * 8;
  std::aligned_storage_t<impl_size> impl;
};

class Op {
  friend RADOS;

public:

  Op(const Op&) = delete;
  Op& operator =(const Op&) = delete;
  Op(Op&&);
  Op& operator =(Op&&);
  ~Op();

  void set_excl();
  void set_failok();
  void set_fadvise_random();
  void set_fadvise_sequential();
  void set_fadvise_willneed();
  void set_fadvise_dontneed();
  void set_fadvise_nocache();

  void cmpext(uint64_t off, buffer::list&& cmp_bl); // → size_t
  void cmpxattr(std::string_view name, uint8_t op, const bufferlist& val);
  void cmpxattr(std::string_view name, uint8_t op, std::uint64_t val);
  void assert_version(uint64_t ver);
  void assert_exists();
  void cmp_omap(const boost::container::flat_map<
		std::string, std::pair<ceph::buffer::list, int>>& assertions);


  using Result = std::vector<
    std::pair<
      boost::system::error_code,
      std::variant<std::monostate,
		   ceph::buffer::list,
		   std::pair<
		     std::vector<std::pair<std::uint64_t, std::uint64_t>>,
		     ceph::buffer::list>,
		   std::pair<std::uint64_t, ceph::real_time>,
		   std::pair<boost::container::flat_set<std::string>, bool>,
		   boost::container::flat_map<std::string, ceph::buffer::list>,
		   std::pair<boost::container::flat_map<std::string,
							 ceph::buffer::list>,
			      bool>,
		   std::vector<obj_watch_t>,
		   librados::snap_set_t,
		   size_t>>>;

  using Signature = void(boost::system::error_code, Result);
  using Completion = ceph::async::Completion<Signature>;
protected:
  Op();
  static constexpr std::size_t impl_size = 20 * 8;
  std::aligned_storage_t<impl_size> impl;
};

// This class is /not/ thread-safe. If you want you can wrap it in
// something that locks it.

class ReadOp : public Op {
  friend RADOS;

public:

  using Op::Op;
  using Op::operator =;

  void read(size_t off, uint64_t len); // → ceph::buffer::list
  void getxattr(std::string_view name); // → ceph::buffer::list
  void get_omap_header(); // → ceph::buffer::list

  void sparse_read(uint64_t off, uint64_t len);
  // → std::pair<ceph::buffer::list,
  //             std::vector<std::pair<std::uint64_t, std::uint64_t>>>,

  void stat(); // → std::pair<std::uint64_t, ceph::real_time>

  void get_omap_keys(std::optional<std::string_view> start_after,
		     uint64_t max_return);
  // → std::pair<boost::container::flat_set<std::string>, bool>


  void get_xattrs();
  // → boost::container::flat_map<std::string, ceph::buffer::list>;

  struct GetOmapValsRes {
  };
  void get_omap_vals(std::optional<std::string_view> start_after,
		     std::optional<std::string_view> filter_prefix,
		     uint64_t max_return);
  // → std::pair<boost::container::flat_map<std::string, ceph::buffer::list>,
  //                                        bool>

  void get_omap_vals_by_keys(const boost::container::flat_set<std::string>& keys);
  // → boost::container::flat_map<std::string, ceph::buffer::list>;

  void list_watchers(); // → std::vector<obj_watch_t>
  void list_snaps(); // → librados::snap_set_t

  void exec(std::string_view cls, std::string_view method,
	    const bufferlist& inbl); // → ceph::buffer::list

private:
  ReadOp();
};

class WriteOp : public Op {
  friend RADOS;
public:

  using Op::Op;
  using Op::operator =;

  WriteOp(const WriteOp&) = delete;
  WriteOp& operator =(const WriteOp&) = delete;
  WriteOp(WriteOp&&);
  WriteOp& operator =(WriteOp&&);

  ~WriteOp();

  void set_mtime(ceph::real_time t);
  void create(bool exclusive);
  void write(uint64_t off, bufferlist&& bl);
  void write_full(bufferlist&& bl);
  void writesame(uint64_t off, uint64_t write_len,
		 bufferlist&& bl);
  void append(bufferlist&& bl);
  void remove();
  void truncate(uint64_t off);
  void zero(uint64_t off, uint64_t len);
  void rmxattr(std::string_view name);
  void setxattr(std::string_view name,
		bufferlist&& bl);
  void rollback(uint64_t snapid);
  void set_omap(const boost::container::flat_map<std::string, bufferlist>& map);
  void set_omap_header(bufferlist&& bl);
  void clear_omap();
  void rm_omap_keys(const boost::container::flat_set<std::string>& to_rm);
  void set_alloc_hint(uint64_t expected_object_size,
		      uint64_t expected_write_size,
		      uint32_t flags);
  void exec(std::string_view cls, std::string_view method,
	    const bufferlist& inbl);
private:
  WriteOp();
};

class RADOS
{
public:
  static constexpr std::tuple<uint32_t, uint32_t, uint32_t> version() {
    return {0, 0, 1};
  }

  RADOS(boost::asio::io_context& ioctx);
  RADOS(boost::asio::io_context& ioctx, std::string_view id);
  RADOS(boost::asio::io_context& ioctx, std::string_view name,
	std::string_view cluster);
  RADOS(boost::asio::io_context& ioctx, CephContext* cct);

  RADOS(const RADOS&) = delete;
  RADOS& operator =(const RADOS&) = delete;
  RADOS(RADOS&&) = delete;
  RADOS& operator =(RADOS&&) = delete;

  CephContext* cct();

  ReadOp make_ReadOp();
  WriteOp make_WriteOp();

  using executor_type = boost::asio::io_context::executor_type;
  executor_type get_executor();

  template<typename CompletionToken>
  auto execute(const Object& o, const IOContext& ioc, ReadOp&& op,
	       CompletionToken&& token) {
    boost::asio::async_completion<CompletionToken, Op::Signature> init(token);
    execute(o, ioc, std::move(op),
	    ReadOp::Completion::create(get_executor(),
				       std::move(init.completion_handler)));
    return init.result.get();
  }

  template<typename Executor, typename CompletionToken>
  auto execute(const Object& o, const IOContext& ioc, WriteOp&& op,
	       CompletionToken&& token) {
    boost::asio::async_completion<CompletionToken, Op::Signature> init(token);
    execute(o, ioc, std::move(op),
	    Op::Completion::create(get_executor(),
				   std::move(init.completion_handler)));
    return init.result.get();
  }

private:

  void execute(const Object& o, const IOContext& ioc, ReadOp&& op,
	       std::unique_ptr<Op::Completion> c);

  void execute(const Object& o, const IOContext& ioc, WriteOp&& op,
	       std::unique_ptr<Op::Completion> c);

  static constexpr std::size_t impl_size = 512 * 8;
  std::aligned_storage_t<impl_size> impl;
};
}

namespace std {
template<>
struct hash<RADOS::Object> {
  size_t operator ()(const RADOS::Object& r) const;
};
} // namespace std

#endif
