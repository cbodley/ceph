// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <boost/system/system_error.hpp>

#include "common/common_init.h"

#include "global/global_init.h"

#include "RADOSImpl.h"

namespace RADOS {
namespace _ {

RADOS::RADOS(boost::asio::io_context& ioctx,
	     boost::intrusive_ptr<CephContext> _cct)
  : ioctx(ioctx), cct(std::move(_cct)),
    monclient(cct.get(), ioctx),
    moncsd(monclient),
    mgrclient(cct.get(), nullptr),
    mgrcsd(mgrclient) {
  {
    MonClient mc_bootstrap(cct.get(), ioctx);
    auto err = mc_bootstrap.get_monmap_and_config();
    if (err < 0)
      throw std::system_error(ceph::to_error_code(err));
  }
  common_init_finish(cct.get());
  auto err = monclient.build_initial_monmap();
  if (err < 0)
    throw std::system_error(ceph::to_error_code(err));

  messenger.reset(Messenger::create_client_messenger(cct.get(), "radosclient"));
  if (!messenger)
    throw std::bad_alloc();

  // require OSDREPLYMUX feature.  this means we will fail to talk to
  // old servers.  this is necessary because otherwise we won't know
  // how to decompose the reply data into its constituent pieces.
  messenger->set_default_policy(
    Messenger::Policy::lossy_client(CEPH_FEATURE_OSDREPLYMUX));

  objecter.reset(new Objecter(cct.get(), messenger.get(), &monclient,
			      ioctx,
			      cct->_conf->rados_mon_op_timeout,
			      cct->_conf->rados_osd_op_timeout));

  objecter->set_balanced_budget();
  monclient.set_messenger(messenger.get());
  mgrclient.set_messenger(messenger.get());
  objecter->init();
  messenger->add_dispatcher_head(&mgrclient);
  messenger->add_dispatcher_tail(objecter.get());
  messenger->start();
  monclient.set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD | CEPH_ENTITY_TYPE_MGR);
  err = monclient.init();
  if (err) {
    throw boost::system::system_error(ceph::to_error_code(err));
  }
  err = monclient.authenticate(cct->_conf->client_mount_timeout);
  if (err) {
    throw boost::system::system_error(ceph::to_error_code(err));
  }
  messenger->set_myname(entity_name_t::CLIENT(monclient.get_global_id()));
  // Detect older cluster, put mgrclient into compatible mode
  mgrclient.set_mgr_optional(
      !get_required_monitor_features().contains_all(
        ceph::features::mon::FEATURE_LUMINOUS));

  // MgrClient needs this (it doesn't have MonClient reference itself)
  monclient.sub_want("mgrmap", 0, 0);
  monclient.renew_subs();

  // if (service_daemon) {
  // mgrclient.service_daemon_register(service_name, daemon_name,
  //				      daemon_metadata);
  // }
  mgrclient.init();
  objecter->set_client_incarnation(0);
  objecter->start();

  std::unique_lock l(lock);
  instance_id = monclient.get_global_id();
}

RADOS::~RADOS() = default;

mon_feature_t RADOS::get_required_monitor_features() const
{
  return monclient.with_monmap(std::mem_fn(&MonMap::get_required_features));
}

}
}
