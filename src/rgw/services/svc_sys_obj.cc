#include "svc_sys_obj.h"
#include "svc_sys_obj_core.h"
#include "svc_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

RGWSysObjectCtx RGWSI_SysObj::init_obj_ctx()
{
  return RGWSysObjectCtx(this);
}

RGWSI_SysObj::Obj RGWSI_SysObj::get_obj(RGWSysObjectCtx& obj_ctx, const rgw_raw_obj& obj)
{
  return Obj(core_svc, obj_ctx, obj);
}

void RGWSI_SysObj::Obj::invalidate()
{
  ctx.invalidate(obj);
}

boost::system::error_code RGWSI_SysObj::Obj::ROp::stat(optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->stat(source.get_ctx(), state, obj,
		   attrs, raw_attrs,
                   lastmod, obj_size,
                   objv_tracker, y);
}

boost::system::error_code RGWSI_SysObj::Obj::ROp::read(int64_t ofs, int64_t end, bufferlist *bl,
                                                       optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->read(source.get_ctx(), state,
                   objv_tracker,
                   obj, bl, ofs, end,
                   attrs,
		   raw_attrs,
                   cache_info,
                   refresh_version, y);
}

boost::system::error_code RGWSI_SysObj::Obj::ROp::get_attr(const char *name, bufferlist *dest,
                                                           optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->get_attr(obj, name, dest, y);
}

boost::system::error_code RGWSI_SysObj::Obj::WOp::remove(optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->remove(source.get_ctx(),
                     objv_tracker,
                     obj, y);
}

boost::system::error_code RGWSI_SysObj::Obj::WOp::write(bufferlist& bl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write(obj, pmtime, attrs, exclusive,
                    bl, objv_tracker, mtime, y);
}

boost::system::error_code RGWSI_SysObj::Obj::WOp::write_data(bufferlist& bl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write_data(obj, bl, exclusive, objv_tracker, y);
}

boost::system::error_code RGWSI_SysObj::Obj::WOp::write_attrs(optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->set_attrs(obj, attrs, nullptr, objv_tracker, y);
}

boost::system::error_code RGWSI_SysObj::Obj::WOp::write_attr(const char *name, bufferlist& bl,
                                                             optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  boost::container::flat_map<string, bufferlist> m;
  m[name] = bl;

  return svc->set_attrs(obj, m, nullptr, objv_tracker, y);
}

boost::system::error_code
RGWSI_SysObj::Pool::Op::list_prefixed_objs(const string& prefix,
                                           std::vector<std::string> *result,
                                           optional_yield y)
{
  bool is_truncated;

  auto rados_pool = source.rados_svc->pool(source.pool, y);
  if (!rados_pool)
    return rados_pool.error();

  auto op = rados_pool->list(RGWAccessListFilterPrefix(prefix));

  do {
    std::vector<std::string> oids;
    static constexpr auto MAX_OBJS_DEFAULT = 1000;
    auto r = op.get_next(MAX_OBJS_DEFAULT, &oids, &is_truncated, y);
    if (r) {
      return r;
    }
    for (auto& val : oids) {
      if (val.size() > prefix.size()) {
        result->push_back(val.substr(prefix.size()));
      }
    }
  } while (is_truncated);

  return {};
}

boost::system::error_code RGWSI_SysObj::Obj::OmapOp::get_all(boost::container::flat_map<std::string, bufferlist> *m,
                                                             optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_get_all(obj, m, y);
}

boost::system::error_code RGWSI_SysObj::Obj::OmapOp::get_vals(const string& marker, uint64_t count,
                                                              boost::container::flat_map<std::string, bufferlist> *m,
                                                              bool *pmore, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_get_vals(obj, marker, count, m, pmore, y);
}

boost::system::error_code RGWSI_SysObj::Obj::OmapOp::set(const std::string& key, bufferlist& bl,
                                                         optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_set(obj, key, bl, must_exist, y);
}

boost::system::error_code RGWSI_SysObj::Obj::OmapOp::set(const boost::container::flat_map<std::string, bufferlist>& m,
                                                         optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_set(obj, m, must_exist, y);
}

boost::system::error_code RGWSI_SysObj::Obj::OmapOp::del(const std::string& key, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_del(obj, key, y);
}

boost::system::error_code RGWSI_SysObj::Obj::WNOp::notify(
  bufferlist& bl,
  std::optional<std::chrono::milliseconds> timeout,
  bufferlist *pbl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->notify(obj, bl, timeout, pbl, y);
}

RGWSI_Zone *RGWSI_SysObj::get_zone_svc()
{
  return core_svc->get_zone_svc();
}
