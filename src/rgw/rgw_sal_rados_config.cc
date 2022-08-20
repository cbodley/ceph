#include "include/rados/librados.hpp"
#include "common/async/yield_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "rgw_basic_types.h"
#include "rgw_sal_rados_config.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"

constexpr std::string_view realm_names_oid_prefix = "realms_names.";
constexpr std::string_view realm_info_oid_prefix = "realms.";
constexpr std::string_view default_realm_info_oid = "default.realm";
constexpr std::string_view zonegroup_names_oid_prefix = "zonegroups_names.";

constexpr std::string_view default_zone_root_pool = "rgw.root";
constexpr std::string_view default_zonegroup_root_pool = "rgw.root";
constexpr std::string_view default_realm_root_pool = "rgw.root";
constexpr std::string_view default_period_root_pool = "rgw.root";

namespace rgw::sal {

class RadosConfigPrefix : public DoutPrefixPipe {
 public:
  RadosConfigPrefix(const DoutPrefixProvider& dpp) : DoutPrefixPipe(dpp) {}
  void add_prefix(std::ostream& out) const override {
    out << "RadosConfigStore: ";
  }
};

static std::string name_or_default(std::string_view name,
                                   std::string_view default_name)
{
  if (!name.empty()) {
    return std::string{name};
  }
  return std::string{default_name};
}

struct RadosConfigStore::Impl {
  librados::Rados rados;

  rgw_pool realm_pool;
  rgw_pool period_pool;
  rgw_pool zonegroup_pool;
  rgw_pool zone_pool;

  Impl(CephContext* cct)
    : realm_pool(name_or_default(cct->_conf->rgw_realm_root_pool,
                                 default_realm_root_pool)),
      period_pool(name_or_default(cct->_conf->rgw_period_root_pool,
                                  default_period_root_pool)),
      zonegroup_pool(name_or_default(cct->_conf->rgw_zonegroup_root_pool,
                                     default_zonegroup_root_pool)),
      zone_pool(name_or_default(cct->_conf->rgw_zone_root_pool,
                                default_zone_root_pool))
  {}

  int read(const DoutPrefixProvider* dpp, optional_yield y,
           const rgw_pool& pool, const std::string& oid,
           bufferlist& bl, RGWObjVersionTracker& objv)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }
    librados::ObjectReadOperation op;
    objv.prepare_op_for_read(&op);
    op.read(0, 0, &bl, nullptr);
    r = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, y);
    if (r >= 0) {
      objv.apply_write();
    }
    return r;
  }

  template <typename T>
  int read(const DoutPrefixProvider* dpp, optional_yield y,
           const rgw_pool& pool, const std::string& oid,
           T& data, RGWObjVersionTracker& objv)
  {
    bufferlist bl;
    int r = read(dpp, y, pool, oid, bl, objv);
    if (r < 0) {
      return r;
    }
    try {
      auto p = bl.cbegin();
      decode(data, p);
    } catch (const buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from "
          << pool << ":" << oid << dendl;
      return -EIO;
    }
    return 0;
  }

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const rgw_pool& pool, const std::string& oid, bool exclusive,
            const bufferlist& bl, RGWObjVersionTracker& objv)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }

    librados::ObjectWriteOperation op;
    op.create(exclusive);
    objv.prepare_op_for_write(&op);
    op.write_full(bl);

    r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
    if (r >= 0) {
      objv.apply_write();
    }
    return r;
  }

  template <typename T>
  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const rgw_pool& pool, const std::string& oid, bool exclusive,
            const T& data, RGWObjVersionTracker& objv)
  {
    bufferlist bl;
    encode(data, bl);

    return write(dpp, y, pool, oid, exclusive, bl, objv);
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y,
             const rgw_pool& pool, const std::string& oid,
             RGWObjVersionTracker& objv)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }

    librados::ObjectWriteOperation op;
    objv.prepare_op_for_write(&op);
    op.remove();

    r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
    if (r >= 0) {
      objv.apply_write();
    }
    return r;
  }
};


RadosConfigStore::RadosConfigStore(std::unique_ptr<Impl> impl)
  : impl(std::move(impl))
{
}

RadosConfigStore::~RadosConfigStore() = default;

int RadosConfigStore::create_realm(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   const RGWRealm& info,
                                   RGWObjVersionTracker& objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->realm_pool;
  const auto info_oid = string_cat_reserve(realm_info_oid_prefix, info.get_id());
  const auto name_oid = string_cat_reserve(realm_names_oid_prefix, info.get_name());
  constexpr bool exclusive = true;

  int r = impl->write(dpp, y, pool, info_oid, exclusive, info, objv);
  if (r < 0) {
    return r;
  }

  const auto name = RGWNameToId{info.get_id()};
  RGWObjVersionTracker name_objv;
  name_objv.generate_new_write_ver(dpp->get_cct());

  r = impl->write(dpp, y, pool, name_oid, exclusive, name, name_objv);
  if (r < 0) {
    (void) impl->remove(dpp, y, pool, info_oid, objv);
  }
  return r;
}

int RadosConfigStore::set_default_realm_id(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           std::string_view realm_id,
                                           RGWObjVersionTracker& objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  const auto oid = name_or_default(
      dpp->get_cct()->_conf->rgw_default_realm_info_oid,
      default_realm_info_oid);

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = realm_id;

  return impl->write(dpp, y, pool, oid, false, default_info, objv);
}

int RadosConfigStore::read_default_realm_id(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            std::string& realm_id,
                                            RGWObjVersionTracker& objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  const auto oid = name_or_default(
      dpp->get_cct()->_conf->rgw_default_realm_info_oid,
      default_realm_info_oid);

  RGWDefaultSystemMetaObjInfo default_info;
  int r = impl->read(dpp, y, pool, oid, default_info, objv);
  if (r >= 0) {
    realm_id = default_info.default_id;
  }
  return r;
}

int RadosConfigStore::delete_default_realm_id(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              RGWObjVersionTracker& objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  const auto oid = name_or_default(
      dpp->get_cct()->_conf->rgw_default_realm_info_oid,
      default_realm_info_oid);

  return impl->remove(dpp, y, pool, oid, objv);
}

int RadosConfigStore::read_realm(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::string_view realm_id,
                                 std::string_view realm_name,
                                 RGWRealm& info,
                                 RGWObjVersionTracker& objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  RGWDefaultSystemMetaObjInfo default_info;
  RGWNameToId name;

  int r = 0;
  if (realm_id.empty()) {
    if (realm_name.empty()) {
      // read default realm id
      const auto default_oid = name_or_default(
          dpp->get_cct()->_conf->rgw_default_realm_info_oid,
          default_realm_info_oid);
      RGWObjVersionTracker objv_ignored;
      r = impl->read(dpp, y, pool, default_oid,
                     default_info, objv_ignored);
      if (r < 0) {
        return r;
      }
      realm_id = default_info.default_id;
    } else {
      // look up realm id by name
      const auto name_oid = string_cat_reserve(realm_names_oid_prefix,
                                               realm_name);
      RGWObjVersionTracker objv_ignored;
      r = impl->read(dpp, y, pool, name_oid, name, objv_ignored);
      if (r < 0) {
        return r;
      }
      realm_id = name.obj_id;
    }
  }

  const auto info_oid = string_cat_reserve(realm_info_oid_prefix, realm_id);
  return impl->read(dpp, y, pool, info_oid, info, objv);
}

int RadosConfigStore::update_realm(const DoutPrefixProvider* dpp,
                                   optional_yield y, const RGWRealm& info,
                                   RGWObjVersionTracker& objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->realm_pool;
  const auto info_oid = string_cat_reserve(realm_info_oid_prefix, info.get_id());
  return impl->write(dpp, y, pool, info_oid, false, info, objv);
}

int RadosConfigStore::rename_realm(const DoutPrefixProvider* dpp,
                                   optional_yield y, RGWRealm& info,
                                   std::string_view new_name,
                                   RGWObjVersionTracker& objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
    return -EINVAL;
  }
  if (new_name.empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->realm_pool;
  const auto name = RGWNameToId{info.get_id()};
  const auto info_oid = string_cat_reserve(realm_info_oid_prefix, info.get_id());
  const auto old_oid = string_cat_reserve(realm_names_oid_prefix, info.get_name());
  const auto new_oid = string_cat_reserve(realm_names_oid_prefix, new_name);

  // link the new name
  RGWObjVersionTracker new_objv;
  int r = impl->write(dpp, y, pool, new_oid, true, name, new_objv);
  if (r < 0) {
    return r;
  }

  // write the info with updated name
  info.set_name(std::string{new_name});
  r = impl->write(dpp, y, pool, info_oid, false, info, objv);
  if (r < 0) {
    // on failure, unlink the new name
    (void) impl->remove(dpp, y, pool, new_oid, new_objv);
    return r;
  }

  // unlink the old name
  RGWObjVersionTracker old_objv;
  (void) impl->remove(dpp, y, pool, old_oid, old_objv);
  return 0;
}

int RadosConfigStore::delete_realm(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   const RGWRealm& info,
                                   RGWObjVersionTracker& objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  const auto info_oid = string_cat_reserve(realm_info_oid_prefix, info.get_id());
  int r = impl->remove(dpp, y, pool, info_oid, objv);
  if (r < 0) {
    return r;
  }
  const auto name_oid = string_cat_reserve(realm_names_oid_prefix, info.get_name());
  RGWObjVersionTracker name_objv;
  return impl->remove(dpp, y, pool, name_oid, name_objv);
}

int RadosConfigStore::list_realm_names(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::list<std::string>& names)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;
  return -ENOTSUP;
}

auto RadosConfigStore::create(const DoutPrefixProvider* dpp)
  -> std::unique_ptr<RadosConfigStore>
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  auto impl = std::make_unique<RadosConfigStore::Impl>(dpp->get_cct());

  // initialize a Rados client
  int r = impl->rados.init_with_context(dpp->get_cct());
  if (r < 0) {
    ldpp_dout(dpp, -1) << "Rados client initialization failed with "
        << cpp_strerror(-r) << dendl;
    return nullptr;
  }
  r = impl->rados.connect();
  if (r < 0) {
    ldpp_dout(dpp, -1) << "Rados client connection failed with "
        << cpp_strerror(-r) << dendl;
    return nullptr;
  }

  return std::make_unique<RadosConfigStore>(std::move(impl));
}

} // namespace rgw::sal
