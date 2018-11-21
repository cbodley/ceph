// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#define BOOST_BIND_NO_PLACEHOLDERS

#include <optional>
#include <string_view>

#include <boost/intrusive_ptr.hpp>

#include "common/ceph_context.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/hobject.h"

#include "global/global_init.h"

#include "osd/osd_types.h"

#include "RADOS/RADOSImpl.h"
#include "include/RADOS/RADOS.hpp"

namespace RADOS {
// Object

Object::Object(std::string_view s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(s);
}

Object::Object(std::string&& s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(std::move(s));
}

Object::Object(const std::string& s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(s);
}

Object::~Object() {
  reinterpret_cast<object_t*>(&impl)->~object_t();
}

Object::Object(const Object& o) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(*reinterpret_cast<const object_t*>(&o.impl));
}
Object& Object::operator =(const Object& o) {
  *reinterpret_cast<object_t*>(&impl) =
    *reinterpret_cast<const object_t*>(&o.impl);
  return *this;
}

Object::operator std::string_view() const {
  return std::string_view(reinterpret_cast<const object_t*>(&impl)->name);
}

bool operator <(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) <
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}
bool operator <=(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) <=
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}
bool operator >=(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) >=
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}
bool operator >(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) >
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}

bool operator ==(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) ==
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}
bool operator !=(const Object& lhs, const Object& rhs) {
  return (*reinterpret_cast<const object_t*>(&lhs.impl) !=
	  *reinterpret_cast<const object_t*>(&rhs.impl));
}

// IOContext

struct IOContextImpl {
  object_locator_t oloc;
  snapid_t snap_seq;
  SnapContext snapc;
};

IOContext::IOContext() {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl();
}

IOContext::IOContext(std::int64_t _pool) : IOContext() {
  pool(_pool);
}

IOContext::IOContext(std::int64_t _pool, std::string_view _ns)
  : IOContext() {
  pool(_pool);
  ns(_ns);
}

IOContext::IOContext(std::int64_t _pool, std::string&& _ns)
  : IOContext() {
  pool(_pool);
  ns(std::move(_ns));
}

IOContext::~IOContext() {
  reinterpret_cast<IOContextImpl*>(&impl)->~IOContextImpl();
}

IOContext::IOContext(const IOContext& rhs) {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl(*reinterpret_cast<const IOContextImpl*>(&rhs.impl));
}

IOContext& IOContext::operator =(const IOContext& rhs) {
  *reinterpret_cast<IOContextImpl*>(&impl) =
    *reinterpret_cast<const IOContextImpl*>(&rhs.impl);
  return *this;
}

IOContext::IOContext(IOContext&& rhs) {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl(
    std::move(*reinterpret_cast<IOContextImpl*>(&rhs.impl)));
}

IOContext& IOContext::operator =(IOContext&& rhs) {
  *reinterpret_cast<IOContextImpl*>(&impl) =
    std::move(*reinterpret_cast<IOContextImpl*>(&rhs.impl));
  return *this;
}

std::int64_t IOContext::pool() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.pool;
}

void IOContext::pool(std::int64_t _pool) {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.pool = _pool;
}

std::string_view IOContext::ns() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.nspace;
}

void IOContext::ns(std::string_view _ns) {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.nspace = _ns;
}

void IOContext::ns(std::string&& _ns) {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.nspace = std::move(_ns);
}

std::optional<std::string_view> IOContext::key() const {
  auto& oloc = reinterpret_cast<const IOContextImpl*>(&impl)->oloc;
  if (oloc.key.empty())
    return std::nullopt;
  else
    return std::string_view(oloc.key);
}

void IOContext::key(std::string_view _key) {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  if (_key.empty()) {
    throw boost::system::system_error(EINVAL,
				      boost::system::system_category(),
				      "An empty key is no key at all.");
  } else {
    oloc.hash = -1;
    oloc.key = _key;
  }
}

void IOContext::key(std::string&&_key) {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  if (_key.empty()) {
    throw boost::system::system_error(EINVAL,
				      boost::system::system_category(),
				      "An empty key is no key at all.");
  } else {
    oloc.hash = -1;
    oloc.key = std::move(_key);
  }
}

void IOContext::clear_key() {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = -1;
  oloc.key.clear();
}

std::optional<std::int64_t> IOContext::hash() const {
  auto& oloc = reinterpret_cast<const IOContextImpl*>(&impl)->oloc;
  if (oloc.hash < 0)
    return std::nullopt;
  else
    return oloc.hash;
}

void IOContext::hash(std::int64_t _hash) {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  if (_hash < 0) {
    throw boost::system::system_error(EINVAL,
				      boost::system::system_category(),
				      "A negative hash is no hash at all.");
  } else {
    oloc.hash = _hash;
    oloc.key.clear();
  }
}

void IOContext::clear_hash() {
  auto& oloc = reinterpret_cast<IOContextImpl*>(&impl)->oloc;
  oloc.hash = -1;
  oloc.key.clear();
}


std::optional<std::uint64_t> IOContext::read_snap() const {
  auto& snap_seq = reinterpret_cast<const IOContextImpl*>(&impl)->snap_seq;
  if (snap_seq == CEPH_NOSNAP)
    return std::nullopt;
  else
    return snap_seq;
}
void IOContext::read_snap(std::optional<std::uint64_t> _snapid) {
  auto& snap_seq = reinterpret_cast<IOContextImpl*>(&impl)->snap_seq;
  snap_seq = _snapid.value_or(CEPH_NOSNAP);
}

std::optional<
  std::pair<std::uint64_t,
	    std::vector<std::uint64_t>>> IOContext::write_snap_context() const {
  auto& snapc = reinterpret_cast<const IOContextImpl*>(&impl)->snapc;
  if (snapc.empty()) {
    return std::nullopt;
  } else {
    std::vector<uint64_t> v(snapc.snaps.begin(), snapc.snaps.end());
    return std::make_optional(std::make_pair(uint64_t(snapc.seq), v));
  }
}

void IOContext::write_snap_context(
  std::optional<std::pair<std::uint64_t, std::vector<std::uint64_t>>> _snapc) {
  auto& snapc = reinterpret_cast<IOContextImpl*>(&impl)->snapc;
  if (!_snapc) {
    snapc.clear();
  } else {
    SnapContext n(_snapc->first, { _snapc->second.begin(), _snapc->second.end()});
    if (!n.is_valid()) {
      throw boost::system::system_error(EINVAL,
					boost::system::system_category(),
					"Invalid snap context.");

    } else {
      snapc = n;
    }
  }
}


// RADOS

namespace {
auto create_cct(std::optional<std::string_view> clustername,
                const CephInitParameters& iparams)
{
  boost::intrusive_ptr<CephContext> cct(common_preinit(iparams,
						       CODE_ENVIRONMENT_LIBRARY,
						       0),
					false);
  if (clustername)
    cct->_conf->cluster = *clustername;
  cct->_conf.parse_env(cct->get_module_type()); // environment variables override
  cct->_conf.apply_changes(nullptr);
  return cct;
}
}

// Op

struct OpImpl {
  ObjectOperation op;
  std::unique_ptr<Op::Result> res;
  std::optional<ceph::real_time> mtime;

  OpImpl() : res(make_unique<Op::Result>()) {}
  OpImpl(OpImpl&& rhs)
    : op(std::move(rhs.op)),
      res(std::move(rhs.res)),
      mtime(std::move(rhs.mtime)) {
    rhs.clear();
  }
  OpImpl& operator =(OpImpl&& rhs) {
    op = std::move(rhs.op);
    res = std::move(rhs.res);
    mtime = std::move(rhs.mtime);
    rhs.clear();
    return *this;
  }

  void clear() {
    op.clear();
    res = make_unique<Op::Result>();
  }
};

Op::Op() {
  static_assert(Op::impl_size >= sizeof(OpImpl));
  new (&impl) OpImpl;
}

Op::Op(Op&& rhs) {
  new (&impl) OpImpl(std::move(*reinterpret_cast<OpImpl*>(&rhs.impl)));
}
Op& Op::operator =(Op&& rhs) {
  reinterpret_cast<OpImpl*>(&impl)->~OpImpl();
  new (&impl) OpImpl(std::move(*reinterpret_cast<OpImpl*>(&rhs.impl)));
  return *this;
}
Op::~Op() {
  reinterpret_cast<OpImpl*>(&impl)->~OpImpl();
}

void Op::set_excl() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(CEPH_OSD_OP_FLAG_EXCL);
}
void Op::set_failok() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FAILOK);
}
void Op::set_fadvise_random() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_RANDOM);
}
void Op::set_fadvise_sequential() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
}
void Op::set_fadvise_willneed() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
}
void Op::set_fadvise_dontneed() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
}
void Op::set_fadvise_nocache() {
  reinterpret_cast<OpImpl*>(&impl)->op.set_last_op_flags(
    CEPH_OSD_OP_FLAG_FADVISE_NOCACHE);
}

void Op::cmpext(uint64_t off, bufferlist&& cmp_bl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, std::size_t(0));
  ceph_assert(o->res->size() == o->op.size());
  o->op.cmpext(off, std::move(cmp_bl), &o->res->back().first,
	       &std::get<std::size_t>(o->res->back().second));
}
void Op::cmpxattr(std::string_view name, uint8_t op, const bufferlist& val) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, std::monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.cmpxattr(name, op, CEPH_OSD_CMPXATTR_MODE_STRING, val);
}
void Op::cmpxattr(std::string_view name, uint8_t op, std::uint64_t val) {
  bufferlist bl;
  encode(val, bl);
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, std::monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.cmpxattr(name, op, CEPH_OSD_CMPXATTR_MODE_U64, bl);
}

void Op::assert_version(uint64_t ver) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, std::monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.assert_version(ver);
  o->op.out_ec.back() = &o->res->back().first;
}
void Op::assert_exists() {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, std::monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.stat(nullptr, nullptr, &o->res->back().first);
}
void Op::cmp_omap(const boost::container::flat_map<
		  std::string, std::pair<ceph::buffer::list,
		                         int>>& assertions) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, std::monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_cmp(assertions, &o->res->back().first);
}

// ---

ReadOp RADOS::make_ReadOp() {
  return ReadOp{};
}

WriteOp RADOS::make_WriteOp() {
  return WriteOp{};
}

// ReadOp / WriteOp

ReadOp::ReadOp() = default;

void ReadOp::read(size_t off, uint64_t len) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, ceph::buffer::list{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.read(off, len, &o->res->back().first,
	     &std::get<ceph::buffer::list>(o->res->back().second));
}

void ReadOp::getxattr(std::string_view name) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, ceph::buffer::list{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.getxattr(name, &o->res->back().first,
		 &std::get<ceph::buffer::list>(o->res->back().second));
}

void ReadOp::get_omap_header() {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, ceph::buffer::list{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_get_header(&o->res->back().first,
			&std::get<ceph::buffer::list>(o->res->back().second));
}

void ReadOp::sparse_read(uint64_t off, uint64_t len) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       std::pair<std::vector<std::pair<uint64_t, uint64_t>>,
		                 ceph::buffer::list>{});
  auto& kv = std::get<std::pair<std::vector<std::pair<uint64_t, uint64_t>>,
				ceph::buffer::list>>(o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.sparse_read(off, len, &o->res->back().first, &kv.first, &kv.second);
}

void ReadOp::stat() {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       std::pair<std::uint64_t, ceph::real_time>{});
  auto& kv = std::get<std::pair<std::uint64_t, ceph::real_time>>(o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.stat(&kv.first, &kv.second, &o->res->back().first);
}

void ReadOp::get_omap_keys(std::optional<std::string_view> start_after,
                           uint64_t max_return) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       std::pair<boost::container::flat_set<std::string>,
		                 bool>{});
  auto& kv = std::get<std::pair<boost::container::flat_set<std::string>,
				bool>>(o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_get_keys(start_after, max_return, &o->res->back().first, &kv.first,
		      &kv.second);
}

void ReadOp::get_xattrs() {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       boost::container::flat_map<std::string, buffer::list>{});
  auto& k = std::get<boost::container::flat_map<std::string, buffer::list>>(
    o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.getxattrs(&o->res->back().first, &k);
}

void ReadOp::get_omap_vals(std::optional<std::string_view> start_after,
                           std::optional<std::string_view> filter_prefix,
                           uint64_t max_return) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       std::pair<boost::container::flat_map<
		                   std::string, buffer::list>, bool>{});
  auto& k = std::get<std::pair<boost::container::flat_map<
                       std::string, buffer::list>, bool>>(o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_get_vals(start_after, filter_prefix, max_return,
			&o->res->back().first, &k.first, &k.second);
}

void ReadOp::get_omap_vals_by_keys(
  const boost::container::flat_set<std::string>& keys) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       boost::container::flat_map<std::string, buffer::list>{});
  auto& k = std::get<boost::container::flat_map<std::string, buffer::list>>(o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_get_vals_by_keys(keys, &o->res->back().first, &k);
}

void ReadOp::list_watchers() {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       std::vector<obj_watch_t>{});
  auto& k = std::get<std::vector<obj_watch_t>>(o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.list_watchers(&k, &o->res->back().first);
}

void ReadOp::list_snaps() {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       librados::snap_set_t{});
  auto& k = std::get<librados::snap_set_t>(o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.list_snaps(&k, nullptr, &o->res->back().first);
}

void ReadOp::exec(std::string_view cls, std::string_view method,
		  const bufferlist& inbl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       buffer::list{});
  auto& k = std::get<buffer::list>(o->res->back().second);
  ceph_assert(o->res->size() == o->op.size());
  o->op.call(cls, method, inbl, &o->res->back().first, &k);
}

// WriteOp

WriteOp::WriteOp() = default;

void WriteOp::set_mtime(ceph::real_time t) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->mtime = t;
}

void WriteOp::create(bool exclusive) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.create(exclusive);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::write(uint64_t off, bufferlist&& bl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.write(off, bl);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::write_full(bufferlist&& bl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.write_full(bl);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::writesame(uint64_t off, uint64_t write_len,
                        bufferlist&& bl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.writesame(off, write_len, bl);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::append(bufferlist&& bl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.append(bl);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::remove() {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.remove();
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::truncate(uint64_t off) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.truncate(off);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::zero(uint64_t off, uint64_t len) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.zero(off, len);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::rmxattr(std::string_view name) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.rmxattr(name);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::setxattr(std::string_view name,
                       bufferlist&& bl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.setxattr(name, bl);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::rollback(uint64_t snapid) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.rollback(snapid);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::set_omap(
  const boost::container::flat_map<std::string, ceph::buffer::list>& map) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_set(map);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::set_omap_header(bufferlist&& bl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_set_header(bl);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::clear_omap() {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_clear();
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::rm_omap_keys(
  const boost::container::flat_set<std::string>& to_rm) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.omap_rm_keys(to_rm);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::set_alloc_hint(uint64_t expected_object_size,
			     uint64_t expected_write_size,
			     uint32_t flags) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{}, monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.set_alloc_hint(expected_object_size, expected_write_size, flags);
  o->op.out_ec.back() = &o->res->back().first;
}

void WriteOp::exec(std::string_view cls, std::string_view method,
		   const bufferlist& inbl) {
  auto o = reinterpret_cast<OpImpl*>(&impl);
  o->res->emplace_back(boost::system::error_code{},
		       monostate{});
  ceph_assert(o->res->size() == o->op.size());
  o->op.call(cls, method, inbl, &o->res->back().first);
}


RADOS::RADOS(boost::asio::io_context& ioctx) {
  static_assert(impl_size >= sizeof(_::RADOS));
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  auto cct = create_cct(nullopt, iparams);
  new (&impl) _::RADOS(ioctx, cct);
}

RADOS::RADOS(boost::asio::io_context& ioctx, std::string_view id) {
  static_assert(impl_size >= sizeof(_::RADOS));
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  auto cct = create_cct(nullopt, iparams);
  new (&impl) _::RADOS(ioctx, cct);
}

RADOS::RADOS(boost::asio::io_context& ioctx, std::string_view name,
	     std::string_view cluster) {
  static_assert(impl_size >= sizeof(_::RADOS));
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (!iparams.name.from_str(name)) {
    throw boost::system::system_error(EINVAL, boost::system::system_category());
  }
  auto cct = create_cct(cluster, iparams);
  new (&impl) _::RADOS(ioctx, cct);
}

RADOS::RADOS(boost::asio::io_context& ioctx, CephContext* cct) {
  static_assert(impl_size >= sizeof(_::RADOS));
  new (&impl) _::RADOS(ioctx, cct);
}

auto resulter(std::unique_ptr<Op::Completion> c,
	      std::unique_ptr<Op::Result> r) {

  return [c = std::move(c), r = std::move(r)]
    (boost::system::error_code ec) mutable {
	   ceph::async::dispatch(std::move(c), ec, std::move(*r));
	 };
}

RADOS::executor_type RADOS::get_executor() {
  return reinterpret_cast<_::RADOS*>(&impl)->ioctx.get_executor();
}

void RADOS::execute(const Object& o, const IOContext& _ioc, ReadOp&& _op,
		    std::unique_ptr<ReadOp::Completion> c) {
  auto rados = reinterpret_cast<_::RADOS*>(&impl);
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto op = reinterpret_cast<OpImpl*>(&_op.impl);
  auto flags = 0; // Should be in Op.

  rados->objecter->read(
    *oid, ioc->oloc, std::move(op->op), ioc->snap_seq, nullptr, flags,
    resulter(std::move(c), std::move(op->res)));
  op->clear();
}

void RADOS::execute(const Object& o, const IOContext& _ioc, WriteOp&& _op,
		    std::unique_ptr<WriteOp::Completion> c) {
  auto rados = reinterpret_cast<_::RADOS*>(&impl);
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto op = reinterpret_cast<OpImpl*>(&_op.impl);
  auto flags = 0; // Should be in Op.
  ceph::real_time mtime;
  if (op->mtime)
    mtime = *op->mtime;
  else
    mtime = ceph::real_clock::now();

  rados->objecter->mutate(
    *oid, ioc->oloc, std::move(op->op), ioc->snapc,
    mtime, flags,
    resulter(std::move(c), std::move(op->res)));
  op->clear();
}

boost::uuids::uuid RADOS::get_fsid() const noexcept {
  auto rados = reinterpret_cast<const _::RADOS*>(&impl);
  return rados->monclient.get_fsid().uuid;
}


void RADOS::lookup_pool(std::string name, std::unique_ptr<LookupPoolComp> c)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  int64_t ret = objecter->with_osdmap(std::mem_fn(&OSDMap::lookup_pg_pool_name),
				      name);
  if (-ENOENT == ret) {
    objecter->wait_for_latest_osdmap(
      [name = std::move(name), c = std::move(c), objecter]
      (boost::system::error_code ec) mutable {
	int64_t ret =
	  objecter->with_osdmap(std::mem_fn(&OSDMap::lookup_pg_pool_name),
				name);
	if (ret < 0)
	  ceph::async::dispatch(std::move(c), ceph::to_error_code(ret),
				std::int64_t(0));
	else
	  ceph::async::dispatch(std::move(c), boost::system::error_code{}, ret);
      });
    ret = objecter->with_osdmap(std::mem_fn(&OSDMap::lookup_pg_pool_name),
                                 name);
  } else if (ret < 0) {
    ceph::async::dispatch(std::move(c), ceph::to_error_code(ret),
			  std::int64_t(0));
  } else {
    ceph::async::dispatch(std::move(c), boost::system::error_code{}, ret);
  }
}


std::optional<uint64_t> RADOS::get_pool_alignment(int64_t pool_id)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  return objecter->with_osdmap(
    [pool_id](const OSDMap &o) -> std::optional<uint64_t> {
      if (!o.have_pg_pool(pool_id)) {
	throw boost::system::system_error(
	  ENOENT, boost::system::system_category(),
	  "Cannot find pool in OSDMap.");
      } else if (o.get_pg_pool(pool_id)->requires_aligned_append()) {
	return o.get_pg_pool(pool_id)->required_alignment();
      } else {
	return std::nullopt;
      }
    });
}

std::vector<std::pair<std::int64_t, std::string>> RADOS::list_pools() {
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  return objecter->with_osdmap(
    [&](const OSDMap& o) {
      std::vector<std::pair<std::int64_t, std::string>> v;
      for (auto p : o.get_pools())
	v.push_back(std::make_pair(p.first, o.get_pool_name(p.first)));
      return v;
    });
}

void RADOS::create_pool_snap(std::int64_t pool,
			     std::string_view snapName,
			     std::unique_ptr<SimpleOpComp> c)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  objecter->create_pool_snap(
    pool, snapName,
    [c = std::move(c)](boost::system::error_code e, const bufferlist&) mutable {
      ceph::async::dispatch(std::move(c), e);
    });
}

void RADOS::allocate_selfmanaged_snap(int64_t pool,
				      std::unique_ptr<SMSnapComp> c) {
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  objecter->allocate_selfmanaged_snap(
    pool,
    [c = std::move(c)](boost::system::error_code e, snapid_t snap) mutable {
      ceph::async::dispatch(std::move(c), e, snap);
    });
}

void RADOS::delete_pool_snap(std::int64_t pool,
			     std::string_view snapName,
			     std::unique_ptr<SimpleOpComp> c)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  objecter->delete_pool_snap(
    pool, snapName,
    [c = std::move(c)](boost::system::error_code e, const bufferlist&) mutable {
      ceph::async::dispatch(std::move(c), e);
    });
}

void RADOS::delete_selfmanaged_snap(std::int64_t pool,
				    snapid_t snap,
				    std::unique_ptr<SimpleOpComp> c)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  objecter->delete_selfmanaged_snap(
    pool, snap,
    [c = std::move(c)](boost::system::error_code e, const bufferlist&) mutable {
      ceph::async::dispatch(std::move(c), e);
    });
}

void RADOS::create_pool(std::string_view name,
			std::optional<int> crush_rule,
			std::unique_ptr<SimpleOpComp> c)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  objecter->create_pool(
    name,
    [c = std::move(c)](boost::system::error_code e, const bufferlist&) mutable {
      ceph::async::dispatch(std::move(c), e);
    },
    crush_rule.value_or(-1));
}

void RADOS::delete_pool(std::string_view name,
			std::unique_ptr<SimpleOpComp> c)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  objecter->delete_pool(
    name,
    [c = std::move(c)](boost::system::error_code e, const bufferlist&) mutable {
      ceph::async::dispatch(std::move(c), e);
    });
}

void RADOS::delete_pool(std::int64_t pool,
			std::unique_ptr<SimpleOpComp> c)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  objecter->delete_pool(
    pool,
    [c = std::move(c)](boost::system::error_code e, const bufferlist&) mutable {
      ceph::async::dispatch(std::move(c), e);
    });
}

void RADOS::watch(const Object& o, const IOContext& _ioc,
		  std::optional<std::chrono::seconds> timeout, WatchCB&& cb,
		  std::unique_ptr<WatchComp> c) {
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  ObjectOperation op;

  Objecter::LingerOp *linger_op = objecter->linger_register(*oid, ioc->oloc, 0);
  uint64_t cookie = linger_op->get_cookie();
  linger_op->handle = std::move(cb);
  op.watch(cookie, CEPH_OSD_WATCH_OP_WATCH, timeout.value_or(0s).count());
  bufferlist bl;
  objecter->linger_watch(linger_op, op,
			 ioc->snapc, ceph::real_clock::now(), bl,
			 [c = std::move(c), cookie](boost::system::error_code e,
						    const bufferlist&) mutable {
			   ceph::async::dispatch(std::move(c), e, cookie);
			 }, nullptr);

}

void RADOS::notify_ack(const Object& o,
		       const IOContext& _ioc,
		       uint64_t notify_id,
		       uint64_t cookie,
		       bufferlist&& bl,
		       std::unique_ptr<SimpleOpComp> c)
{
  auto rados = reinterpret_cast<_::RADOS*>(&impl);
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  ObjectOperation op;
  op.notify_ack(notify_id, cookie, bl);

  rados->objecter->read(
    *oid, ioc->oloc, std::move(op), ioc->snap_seq, nullptr, 0,
    [c = std::move(c)](boost::system::error_code e) mutable {
      ceph::async::dispatch(std::move(c), e);
    });
}

boost::system::error_code RADOS::watch_check(uint64_t cookie)
{
  auto rados = reinterpret_cast<_::RADOS*>(&impl);
  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);
  return ceph::to_error_code(rados->objecter->linger_check(linger_op));
}

void RADOS::unwatch(uint64_t cookie, const IOContext& _ioc,
		    std::unique_ptr<SimpleOpComp> c)
{
  auto objecter = reinterpret_cast<_::RADOS*>(&impl)->objecter.get();
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);

  ObjectOperation op;
  op.watch(cookie, CEPH_OSD_WATCH_OP_UNWATCH);
  objecter->mutate(linger_op->target.base_oid, ioc->oloc, std::move(op),
		   ioc->snapc, ceph::real_clock::now(), 0,
		   [objecter, linger_op, c = std::move(c)]
		   (boost::system::error_code ec) mutable {
		     objecter->linger_cancel(linger_op);
		     ceph::async::dispatch(std::move(c), ec);
		   });
}

struct NotifyHandler {
  boost::asio::io_context& ioc;
  boost::asio::io_context::strand strand;
  Objecter* objecter;
  Objecter::LingerOp* op;
  std::unique_ptr<RADOS::NotifyComp> c;

  bool acked = false;
  bool finished = false;
  boost::system::error_code res;
  bufferlist rbl;

  NotifyHandler(boost::asio::io_context& ioc,
		Objecter* objecter,
		Objecter::LingerOp* op,
		std::unique_ptr<RADOS::NotifyComp> c)
    : ioc(ioc), strand(ioc), objecter(objecter), op(op), c(std::move(c)) {}

  // Use bind or a lambda to pass this in.
  void handle_ack(boost::system::error_code ec,
		  bufferlist&&) {
    boost::asio::post(
      strand,
      [this, ec]() mutable {
	acked = true;
	maybe_cleanup(ec);
      });
  }

  // Notify finish callback. It can actually own the object's storage.

  void operator()(boost::system::error_code ec,
		  bufferlist&& bl) {
    boost::asio::post(
      strand,
      [this, ec]() mutable {
	finished = true;
	maybe_cleanup(ec);
      });
  }

  // Should be called from strand.
  void maybe_cleanup(boost::system::error_code ec) {
    if (!res && ec)
      res = ec;
    if ((acked && finished) || res)
      boost::asio::defer(
	ioc.get_executor(),
	[this]() mutable {
	  auto bl = std::move(rbl);
	  objecter->linger_cancel(op);
	  ceph::async::dispatch(std::move(c), res, std::move(bl));
	});
  }
};

void RADOS::notify(const Object& o, const IOContext& _ioc, bufferlist&& bl,
		   std::optional<std::chrono::milliseconds> timeout,
		   std::unique_ptr<NotifyComp> c)
{
  auto rados = reinterpret_cast<_::RADOS*>(&impl);
  auto oid = reinterpret_cast<const object_t*>(&o.impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);
  auto linger_op = rados->objecter->linger_register(*oid, ioc->oloc, 0);

  auto x = new NotifyHandler(rados->ioctx, rados->objecter.get(), linger_op, std::move(c));
  linger_op->on_notify_finish =
    [cb = std::unique_ptr<NotifyHandler>(x)](boost::system::error_code ec,
					     ceph::bufferlist&& bl) mutable {
      (*cb)(ec, std::move(bl));
    };
  ObjectOperation rd;
  bufferlist inbl;
  rd.notify(
    linger_op->get_cookie(), 1,
    timeout ? timeout->count() : rados->cct->_conf->client_notify_timeout,
    bl, &inbl);

  rados->objecter->linger_notify(
    linger_op, rd, ioc->snap_seq, inbl,
    [x](boost::system::error_code ec, bufferlist&& bl) mutable {
      x->handle_ack(ec, std::move(bl));
    }, nullptr);
}

// Enumeration

EnumerationCursor::EnumerationCursor() {
};

EnumerationCursor EnumerationCursor::begin() {
  EnumerationCursor e;
  static_assert(impl_size >= sizeof(hobject_t));
  new (&e.impl) hobject_t();
  return e;
}

EnumerationCursor EnumerationCursor::end() {
  EnumerationCursor e;
  static_assert(impl_size >= sizeof(hobject_t));
  new (&e.impl) hobject_t(hobject_t::get_max());
  return e;
}

EnumerationCursor::EnumerationCursor(const EnumerationCursor& rhs) {
  static_assert(impl_size >= sizeof(hobject_t));
  new (&impl) hobject_t(*reinterpret_cast<const hobject_t*>(&rhs.impl));
}

EnumerationCursor& EnumerationCursor::operator =(const EnumerationCursor& rhs) {
  static_assert(impl_size >= sizeof(hobject_t));
  reinterpret_cast<hobject_t*>(&impl)->~hobject_t();
  new (&impl) hobject_t(*reinterpret_cast<const hobject_t*>(&rhs.impl));
  return *this;
}

EnumerationCursor::EnumerationCursor(EnumerationCursor&& rhs) {
  static_assert(impl_size >= sizeof(hobject_t));
  new (&impl) hobject_t(std::move(*reinterpret_cast<hobject_t*>(&rhs.impl)));
}

EnumerationCursor& EnumerationCursor::operator =(EnumerationCursor&& rhs) {
  static_assert(impl_size >= sizeof(hobject_t));
  reinterpret_cast<hobject_t*>(&impl)->~hobject_t();
  new (&impl) hobject_t(std::move(*reinterpret_cast<hobject_t*>(&rhs.impl)));
  return *this;
}
EnumerationCursor::~EnumerationCursor() {
  reinterpret_cast<hobject_t*>(&impl)->~hobject_t();
}

bool operator ==(const EnumerationCursor& lhs, const EnumerationCursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) ==
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator !=(const EnumerationCursor& lhs, const EnumerationCursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) !=
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator <(const EnumerationCursor& lhs, const EnumerationCursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) <
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator <=(const EnumerationCursor& lhs, const EnumerationCursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) <=
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator >=(const EnumerationCursor& lhs, const EnumerationCursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) >=
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

bool operator >(const EnumerationCursor& lhs, const EnumerationCursor& rhs) {
  return (*reinterpret_cast<const hobject_t*>(&lhs.impl) >
	  *reinterpret_cast<const hobject_t*>(&rhs.impl));
}

void RADOS::enumerate_objects(const IOContext& _ioc,
			      const EnumerationCursor& begin,
			      const EnumerationCursor& end,
			      const std::uint32_t max,
			      const bufferlist& filter,
			      std::unique_ptr<EnumerateComp> c) {
  auto rados = reinterpret_cast<_::RADOS*>(&impl);
  auto ioc = reinterpret_cast<const IOContextImpl*>(&_ioc.impl);

  rados->objecter->enumerate_objects(
    ioc->oloc.pool,
    ioc->oloc.nspace,
    *reinterpret_cast<const hobject_t*>(&begin.impl),
    *reinterpret_cast<const hobject_t*>(&end.impl),
    max,
    filter,
    [c = std::move(c)]
    (boost::system::error_code ec, std::vector<EnumeratedObject>&& v,
     hobject_t&& n) mutable {
      EnumerationCursor next;
      new (&next.impl) hobject_t(std::move(n));
      ceph::async::dispatch(std::move(c), ec, std::move(v), std::move(next));
    });
}
}

namespace std {
size_t hash<RADOS::Object>::operator ()(
  const RADOS::Object& r) const {
  static constexpr const hash<object_t> H;
  return H(*reinterpret_cast<const object_t*>(&r.impl));
}
}
