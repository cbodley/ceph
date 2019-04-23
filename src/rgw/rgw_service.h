// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_SERVICE_H
#define CEPH_RGW_SERVICE_H


#include <string>
#include <vector>
#include <memory>

#include "rgw/rgw_common.h"

struct RGWServices_Def;

class RGWServiceInstance
{
  friend struct RGWServices_Def;

protected:
  CephContext* cct;
  boost::asio::io_context& ioc;

  enum StartState {
    StateInit = 0,
    StateStarting = 1,
    StateStarted = 2,
  } start_state{StateInit};

  virtual void shutdown() {}
  virtual boost::system::error_code do_start() {
    return {};
  }
public:
  RGWServiceInstance(CephContext* cct, boost::asio::io_context& ioc)
    : cct(cct), ioc(ioc) {}
  virtual ~RGWServiceInstance() {}

  boost::system::error_code start();
  bool is_started() {
    return (start_state == StateStarted);
  }

  CephContext* ctx() {
    return cct;
  }

  boost::asio::io_context& ioctx() {
    return ioc;
  }
};

class RGWSI_Finisher;
class RGWSI_Notify;
class RGWSI_RADOS;
class RGWSI_Zone;
class RGWSI_ZoneUtils;
class RGWSI_Quota;
class RGWSI_SyncModules;
class RGWSI_SysObj;
class RGWSI_SysObj_Core;
class RGWSI_SysObj_Cache;

struct RGWServices_Def
{
  bool can_shutdown{false};
  bool has_shutdown{false};

  std::unique_ptr<RGWSI_Finisher> finisher;
  std::unique_ptr<RGWSI_Notify> notify;
  std::unique_ptr<RGWSI_RADOS> rados;
  std::unique_ptr<RGWSI_Zone> zone;
  std::unique_ptr<RGWSI_ZoneUtils> zone_utils;
  std::unique_ptr<RGWSI_Quota> quota;
  std::unique_ptr<RGWSI_SyncModules> sync_modules;
  std::unique_ptr<RGWSI_SysObj> sysobj;
  std::unique_ptr<RGWSI_SysObj_Core> sysobj_core;
  std::unique_ptr<RGWSI_SysObj_Cache> sysobj_cache;

  RGWServices_Def();
  ~RGWServices_Def();

  boost::system::error_code init(CephContext *cct, boost::asio::io_context& ioc,
				 bool have_cache, bool raw_storage);
  void shutdown();
};


struct RGWServices
{
  RGWServices_Def svc;

  RGWSI_Finisher *finisher{nullptr};
  RGWSI_Notify *notify{nullptr};
  RGWSI_RADOS *rados{nullptr};
  RGWSI_Zone *zone{nullptr};
  RGWSI_ZoneUtils *zone_utils{nullptr};
  RGWSI_Quota *quota{nullptr};
  RGWSI_SyncModules *sync_modules{nullptr};
  RGWSI_SysObj *sysobj{nullptr};
  RGWSI_SysObj_Cache *cache{nullptr};
  RGWSI_SysObj_Core *core{nullptr};

  boost::system::error_code do_init(CephContext* cct,
				    boost::asio::io_context& ioc,
				    bool have_cache, bool raw_storage);

  boost::system::error_code init(CephContext *cct,
				 boost::asio::io_context& ioc, bool have_cache) {
    return do_init(cct, ioc, have_cache, false);
  }

  boost::system::error_code init_raw(CephContext *cct, boost::asio::io_context& ioc,
				     bool have_cache) {
    return do_init(cct, ioc, have_cache, true);
  }
  void shutdown() {
    svc.shutdown();
  }
};


#endif
