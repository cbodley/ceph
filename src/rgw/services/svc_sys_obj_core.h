#ifndef CEPH_RGW_SERVICES_SYS_OBJ_CORE_H
#define CEPH_RGW_SERVICES_SYS_OBJ_CORE_H


#include "rgw/rgw_service.h"

#include "svc_rados.h"


class RGWSI_Zone;

struct rgw_cache_entry_info;

struct RGWSysObjState {
  rgw_raw_obj obj;
  bool has_attrs{false};
  bool exists{false};
  uint64_t size{0};
  ceph::real_time mtime;
  uint64_t epoch{0};
  bufferlist obj_tag;
  bool has_data{false};
  bufferlist data;
  bool prefetch_data{false};
  uint64_t pg_ver{0};

  /* important! don't forget to update copy constructor */

  RGWObjVersionTracker objv_tracker;

  boost::container::flat_map<std::string, ceph::buffer::list> attrset;
  RGWSysObjState() {}
  RGWSysObjState(const RGWSysObjState& rhs) : obj (rhs.obj) {
    has_attrs = rhs.has_attrs;
    exists = rhs.exists;
    size = rhs.size;
    mtime = rhs.mtime;
    epoch = rhs.epoch;
    if (rhs.obj_tag.length()) {
      obj_tag = rhs.obj_tag;
    }
    has_data = rhs.has_data;
    if (rhs.data.length()) {
      data = rhs.data;
    }
    prefetch_data = rhs.prefetch_data;
    pg_ver = rhs.pg_ver;
    objv_tracker = rhs.objv_tracker;
  }
};

class RGWSysObjectCtxBase {
  std::map<rgw_raw_obj, RGWSysObjState> objs_state;
  RWLock lock;

public:
  explicit RGWSysObjectCtxBase() : lock("RGWSysObjectCtxBase") {}

  RGWSysObjectCtxBase(const RGWSysObjectCtxBase& rhs) : objs_state(rhs.objs_state),
                                                  lock("RGWSysObjectCtxBase") {}
  RGWSysObjectCtxBase(const RGWSysObjectCtxBase&& rhs) : objs_state(std::move(rhs.objs_state)),
                                                   lock("RGWSysObjectCtxBase") {}

  RGWSysObjState *get_state(const rgw_raw_obj& obj) {
    RGWSysObjState *result;
    std::map<rgw_raw_obj, RGWSysObjState>::iterator iter;
    lock.get_read();
    assert (!obj.empty());
    iter = objs_state.find(obj);
    if (iter != objs_state.end()) {
      result = &iter->second;
      lock.unlock();
    } else {
      lock.unlock();
      lock.get_write();
      result = &objs_state[obj];
      lock.unlock();
    }
    return result;
  }

  void set_prefetch_data(rgw_raw_obj& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].prefetch_data = true;
  }
  void invalidate(rgw_raw_obj& obj) {
    RWLock::WLocker wl(lock);
    auto iter = objs_state.find(obj);
    if (iter == objs_state.end()) {
      return;
    }
    objs_state.erase(iter);
  }
};

class RGWSI_SysObj_Core : public RGWServiceInstance
{
  friend class RGWServices_Def;
  friend class RGWSI_SysObj;

protected:
  RGWSI_RADOS *rados_svc{nullptr};
  RGWSI_Zone *zone_svc{nullptr};

  struct GetObjState {
    boost::optional<RGWSI_RADOS::Obj> rados_obj;
    uint64_t last_ver{0};

    GetObjState() {}

    boost::system::error_code get_rados_obj(RGWSI_RADOS *rados_svc,
                                            RGWSI_Zone *zone_svc,
                                            const rgw_raw_obj& obj,
                                            RGWSI_RADOS::Obj **pobj,
                                            optional_yield y);
  };


  void core_init(RGWSI_RADOS *_rados_svc,
                 RGWSI_Zone *_zone_svc) {
    rados_svc = _rados_svc;
    zone_svc = _zone_svc;
  }

  tl::expected<RGWSI_RADOS::Obj, boost::system::error_code>
  get_rados_obj(RGWSI_Zone *zone_svc, const rgw_raw_obj& obj, optional_yield y);

  virtual boost::system::error_code
  raw_stat(const rgw_raw_obj& obj, uint64_t *psize,
           real_time *pmtime, uint64_t *epoch,
           boost::container::flat_map<std::string, ceph::buffer::list>* attrs,
           ceph::buffer::list *first_chunk,
           RGWObjVersionTracker *objv_tracker,
           optional_yield y);

  virtual boost::system::error_code read(RGWSysObjectCtxBase& obj_ctx,
                                         GetObjState& read_state,
                                         RGWObjVersionTracker *objv_tracker,
                                         const rgw_raw_obj& obj,
                                         bufferlist *bl, off_t ofs, off_t end,
                                         boost::container::flat_map<string, bufferlist> *attrs,
                                         bool raw_attrs,
                                         rgw_cache_entry_info *cache_info,
                                         boost::optional<obj_version>,
                                         optional_yield y);

  virtual boost::system::error_code remove(RGWSysObjectCtxBase& obj_ctx,
                                           RGWObjVersionTracker *objv_tracker,
                                           const rgw_raw_obj& obj,
                                           optional_yield y);

  virtual boost::system::error_code write(const rgw_raw_obj& obj,
                                          real_time *pmtime,
                                          boost::container::flat_map<std::string, bufferlist>& attrs,
                                          bool exclusive,
                                          const bufferlist& data,
                                          RGWObjVersionTracker *objv_tracker,
                                          real_time set_mtime,
                                          optional_yield y);

  virtual boost::system::error_code write_data(const rgw_raw_obj& obj,
                                               const bufferlist& bl,
                                               bool exclusive,
                                               RGWObjVersionTracker *objv_tracker,
                                               optional_yield y);

  virtual boost::system::error_code get_attr(const rgw_raw_obj& obj, const char *name, bufferlist *dest,
                                             optional_yield y);

  virtual boost::system::error_code set_attrs(const rgw_raw_obj& obj,
                                              boost::container::flat_map<string, bufferlist>& attrs,
                                              boost::container::flat_map<string, bufferlist> *rmattrs,
                                              RGWObjVersionTracker *objv_tracker,
                                              optional_yield y);

  virtual boost::system::error_code omap_get_all(const rgw_raw_obj& obj,
                                                 boost::container::flat_map<string, bufferlist> *m,
                                                 optional_yield y);
  virtual boost::system::error_code omap_get_vals(const rgw_raw_obj& obj,
                                                  const string& marker,
                                                  uint64_t count,
                                                  boost::container::flat_map<string, bufferlist> *m,
                                                  bool *pmore,
                                                  optional_yield y);
  virtual boost::system::error_code omap_set(const rgw_raw_obj& obj, const std::string& key,
                                             bufferlist& bl, bool must_exist,
                                             optional_yield y);
  virtual boost::system::error_code omap_set(const rgw_raw_obj& obj,
                                             const boost::container::flat_map<std::string, bufferlist>& m, bool must_exist,
                                             optional_yield y);
  virtual boost::system::error_code omap_del(const rgw_raw_obj& obj, const std::string& key,
                                             optional_yield y);

  virtual boost::system::error_code notify(const rgw_raw_obj& obj, bufferlist& bl,
                                           std::optional<std::chrono::milliseconds> timeout,
                                           bufferlist *pbl, optional_yield y);

  /* wrappers */
  boost::system::error_code get_system_obj_state_impl(RGWSysObjectCtxBase *rctx,
                                                      const rgw_raw_obj& obj, RGWSysObjState **state,
                                                      RGWObjVersionTracker *objv_tracker,
                                                      optional_yield y);
  boost::system::error_code get_system_obj_state(RGWSysObjectCtxBase *rctx, const rgw_raw_obj& obj,
                                                 RGWSysObjState **state,
                                                 RGWObjVersionTracker *objv_tracker,
                                                 optional_yield y);

  boost::system::error_code
  stat(RGWSysObjectCtxBase& obj_ctx,
       GetObjState& state,
       const rgw_raw_obj& obj,
       boost::container::flat_map<std::string, ceph::bufferlist>* attrs,
       bool raw_attrs,
       real_time *lastmod,
       uint64_t *obj_size,
       RGWObjVersionTracker *objv_tracker,
       optional_yield y);

public:
  RGWSI_SysObj_Core(CephContext *cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc) {}

  RGWSI_Zone *get_zone_svc() {
    return zone_svc;
  }
};

#endif
