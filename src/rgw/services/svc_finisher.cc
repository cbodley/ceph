#include "common/Finisher.h"

#include "svc_finisher.h"

boost::system::error_code RGWSI_Finisher::do_start()
{
  finisher = std::make_unique<Finisher>(cct);
  finisher->start();

  return {};
}

void RGWSI_Finisher::shutdown()
{
  if (finalized) {
    return;
  }

  if (finisher) {
    finisher->stop();

    map<int, ShutdownCB *> cbs;
    cbs.swap(shutdown_cbs); /* move cbs out, in case caller unregisetrs */
    for (auto& iter : cbs) {
      iter.second->call();
    }
    finisher.reset();
  }

  finalized = true;
}

RGWSI_Finisher::~RGWSI_Finisher()
{
  shutdown();
}

void RGWSI_Finisher::register_caller(ShutdownCB *cb, int *phandle)
{
  *phandle = ++handles_counter;
  shutdown_cbs[*phandle] = cb;
}

void RGWSI_Finisher::unregister_caller(int handle)
{
  shutdown_cbs.erase(handle);
}

void RGWSI_Finisher::schedule_context(Context *c)
{
  finisher->queue(c);
}
