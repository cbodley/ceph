#ifndef CEPH_RGW_SERVICES_FINISHER_H
#define CEPH_RGW_SERVICES_FINISHER_H


#include "rgw/rgw_service.h"
#include "common/Finisher.h"

class Context;

class RGWSI_Finisher : public RGWServiceInstance
{
  friend struct RGWServices_Def;
public:
  class ShutdownCB;

private:
  std::unique_ptr<Finisher> finisher;
  bool finalized{false};

  void shutdown() override;

  std::map<int, ShutdownCB *> shutdown_cbs;
  std::atomic<int> handles_counter{0};

protected:
  void init() {}
  boost::system::error_code do_start() override;

public:
  RGWSI_Finisher(CephContext* cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc) {}
  ~RGWSI_Finisher();

  class ShutdownCB {
  public:
      virtual ~ShutdownCB() {}
      virtual void call() = 0;
  };

  void register_caller(ShutdownCB *cb, int *phandle);
  void unregister_caller(int handle);

  void schedule_context(Context *c);
};

#endif
