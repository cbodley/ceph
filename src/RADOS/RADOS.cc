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

#include <optional>
#include <string_view>

#include <boost/intrusive_ptr.hpp>

#include "common/ceph_context.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"

#include "global/global_init.h"

#include "RADOS/RADOSImpl.h"
#include "include/RADOS/RADOS.hpp"


namespace RADOS {
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
  cct->_conf.parse_env(); // environment variables override
  cct->_conf.apply_changes(nullptr);
  return cct;
}
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
}
