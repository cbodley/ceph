// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_service.h"

#include "services/svc_finisher.h"
#include "services/svc_notify.h"
#include "services/svc_rados.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_sys_obj_core.h"

#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw


RGWServices_Def::RGWServices_Def() = default;
RGWServices_Def::~RGWServices_Def()
{
  shutdown();
}

boost::system::error_code RGWServices_Def::init(CephContext *cct,
						boost::asio::io_context& ioc,
						bool have_cache,
						bool raw)
{
  finisher = std::make_unique<RGWSI_Finisher>(cct, ioc);
  notify = std::make_unique<RGWSI_Notify>(cct, ioc);
  rados = std::make_unique<RGWSI_RADOS>(cct, ioc);
  zone = std::make_unique<RGWSI_Zone>(cct, ioc);
  zone_utils = std::make_unique<RGWSI_ZoneUtils>(cct, ioc);
  quota = std::make_unique<RGWSI_Quota>(cct, ioc);
  sync_modules = std::make_unique<RGWSI_SyncModules>(cct, ioc);
  sysobj = std::make_unique<RGWSI_SysObj>(cct, ioc);
  sysobj_core = std::make_unique<RGWSI_SysObj_Core>(cct, ioc);

  if (have_cache) {
    sysobj_cache = std::make_unique<RGWSI_SysObj_Cache>(cct, ioc);
  }
  finisher->init();
  notify->init(zone.get(), rados.get(), finisher.get());
  rados->init();
  zone->init(sysobj.get(), rados.get(), sync_modules.get());
  zone_utils->init(rados.get(), zone.get());
  quota->init(zone.get());
  sync_modules->init();
  sysobj_core->core_init(rados.get(), zone.get());
  if (have_cache) {
    sysobj_cache->init(rados.get(), zone.get(), notify.get());
    sysobj->init(rados.get(), sysobj_cache.get());
  } else {
    sysobj->init(rados.get(), sysobj_core.get());
  }

  can_shutdown = true;

  auto r = finisher->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start finisher service (" << r << dendl;
    return r;
  }

  if (!raw) {
    r = notify->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start notify service (" << r << dendl;
      return r;
    }
  }

  r = rados->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start rados service (" << r << dendl;
    return r;
  }

  if (!raw) {
    r = zone->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start zone service (" << r << dendl;
      return r;
    }
  }

  r = zone_utils->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start zone_utils service (" << r << dendl;
    return r;
  }

  r = quota->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start quota service (" << r << dendl;
    return r;
  }

  r = sysobj_core->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start sysobj_core service (" << r << dendl;
    return r;
  }

  if (have_cache) {
    r = sysobj_cache->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start sysobj_cache service (" << r << dendl;
      return r;
    }
  }

  r = sysobj->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start sysobj service (" << r << dendl;
    return r;
  }

  /* cache or core services will be started by sysobj */

  return {};
}

void RGWServices_Def::shutdown()
{
  if (!can_shutdown) {
    return;
  }

  if (has_shutdown) {
    return;
  }

  sysobj->shutdown();
  sysobj_core->shutdown();
  notify->shutdown();
  if (sysobj_cache) {
    sysobj_cache->shutdown();
  }
  quota->shutdown();
  zone_utils->shutdown();
  zone->shutdown();
  rados->shutdown();

  has_shutdown = true;

}


boost::system::error_code RGWServices::do_init(CephContext* cct,
					       boost::asio::io_context& ioc,
					       bool have_cache, bool raw)
{
  auto r = svc.init(cct, ioc, have_cache, raw);
  if (r) {
    return r;
  }

  finisher = svc.finisher.get();
  notify = svc.notify.get();
  rados = svc.rados.get();
  zone = svc.zone.get();
  zone_utils = svc.zone_utils.get();
  quota = svc.quota.get();
  sync_modules = svc.sync_modules.get();
  sysobj = svc.sysobj.get();
  cache = svc.sysobj_cache.get();
  core = svc.sysobj_core.get();

  return {};
}

boost::system::error_code RGWServiceInstance::start()
{
  if (start_state != StateInit) {
    return {};
  }

  start_state = StateStarting; /* setting started prior to do_start() on purpose so that circular
                                  references can call start() on each other */

  auto r = do_start();
  if (r) {
    return r;
  }

  start_state = StateStarted;

  return {};
}
