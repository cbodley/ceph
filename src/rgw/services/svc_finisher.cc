#include "common/Finisher.h"

#include "svc_finisher.h"

void RGWSI_Finisher::shutdown()
{
  if (finalized) {
    return;
  }

  decltype(shutdown_cbs) cbs;
  cbs.swap(shutdown_cbs); /* move cbs out, in case caller unregisetrs */
  for (auto& iter : cbs) {
    std::move(iter.second)();
  }
  finalized = true;
}

RGWSI_Finisher::~RGWSI_Finisher()
{
  shutdown();
}

int RGWSI_Finisher::register_caller(ShutdownCB&& cb)
{
  int handle = ++handles_counter;
  shutdown_cbs[handle] = std::move(cb);
  return handle;
}

void RGWSI_Finisher::unregister_caller(int handle)
{
  shutdown_cbs.erase(handle);
}
