#include <system_error>
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

constexpr std::string_view period_info_oid_prefix = "periods.";
constexpr std::string_view period_latest_epoch_info_oid = ".latest_epoch";
constexpr std::string_view period_staging_suffix = ":staging";

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

  const rgw_pool realm_pool;
  const rgw_pool period_pool;
  const rgw_pool zonegroup_pool;
  const rgw_pool zone_pool;

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
           bufferlist& bl, RGWObjVersionTracker* objv)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }
    librados::ObjectReadOperation op;
    if (objv) {
      objv->prepare_op_for_read(&op);
    }
    op.read(0, 0, &bl, nullptr);
    r = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, y);
    if (r >= 0 && objv) {
      objv->apply_write();
    }
    return r;
  }

  template <typename T>
  int read(const DoutPrefixProvider* dpp, optional_yield y,
           const rgw_pool& pool, const std::string& oid,
           T& data, RGWObjVersionTracker* objv)
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

  int create(const DoutPrefixProvider* dpp, optional_yield y,
             const rgw_pool& pool, const std::string& oid,
             bool exclusive, RGWObjVersionTracker* objv)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }

    librados::ObjectWriteOperation op;
    op.create(exclusive);
    if (objv) {
      objv->prepare_op_for_write(&op);
    }

    r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
    if (r >= 0 && objv) {
      objv->apply_write();
    }
    return r;
  }

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const rgw_pool& pool, const std::string& oid, bool exclusive,
            const bufferlist& bl, RGWObjVersionTracker* objv)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }

    librados::ObjectWriteOperation op;
    op.create(exclusive);
    if (objv) {
      objv->prepare_op_for_write(&op);
    }
    op.write_full(bl);

    r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
    if (r >= 0 && objv) {
      objv->apply_write();
    }
    return r;
  }

  template <typename T>
  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const rgw_pool& pool, const std::string& oid, bool exclusive,
            const T& data, RGWObjVersionTracker* objv)
  {
    bufferlist bl;
    encode(data, bl);

    return write(dpp, y, pool, oid, exclusive, bl, objv);
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y,
             const rgw_pool& pool, const std::string& oid,
             RGWObjVersionTracker* objv)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }

    librados::ObjectWriteOperation op;
    if (objv) {
      objv->prepare_op_for_write(&op);
    }
    op.remove();

    r = rgw_rados_operate(dpp, ioctx, oid, &op, y);
    if (r >= 0 && objv) {
      objv->apply_write();
    }
    return r;
  }

  int list(const DoutPrefixProvider* dpp, optional_yield y,
           const rgw_pool& pool, const std::string& marker,
           std::string_view prefix, std::span<std::string> entries,
           ListResult& result)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }
    librados::ObjectCursor oc;
    if (!oc.from_str(marker)) {
      ldpp_dout(dpp, 10) << "failed to parse cursor: " << marker << dendl;
      return -EINVAL;
    }
    std::size_t count = 0;
    try {
      auto iter = ioctx.nobjects_begin(oc);
      const auto end = ioctx.nobjects_end();
      for (; count < entries.size() && iter != end; ++iter) {
        std::string oid = iter->get_oid();
        if (oid.starts_with(prefix)) {
          entries[count++] = std::move(oid);
        }
      }
      if (iter == end) {
        result.next.clear();
      } else {
        result.next = iter.get_cursor().to_str();
      }
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 10) << "NObjectIterator exception " << e.what() << dendl;
      return -EIO;
    }
    result.entries = entries.first(count);
    return 0;
  }
};


RadosConfigStore::RadosConfigStore(std::unique_ptr<Impl> impl)
  : impl(std::move(impl))
{
}

RadosConfigStore::~RadosConfigStore() = default;


// Realm
int RadosConfigStore::create_realm(const DoutPrefixProvider* dpp,
                                   optional_yield y, bool exclusive,
                                   const RGWRealm& info,
                                   RGWObjVersionTracker* objv)
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

  // write the realm info
  int r = impl->write(dpp, y, pool, info_oid, exclusive, info, objv);
  if (r < 0) {
    return r;
  }

  // write the realm name
  const auto name = RGWNameToId{info.get_id()};
  RGWObjVersionTracker name_objv;
  name_objv.generate_new_write_ver(dpp->get_cct());

  r = impl->write(dpp, y, pool, name_oid, exclusive, name, &name_objv);
  if (r < 0) {
    (void) impl->remove(dpp, y, pool, info_oid, objv);
    return r;
  }

  // create control object for watch/notify
  const auto control_oid = string_cat_reserve(info_oid, ".control");
  return impl->create(dpp, y, pool, control_oid, true, nullptr);
}

int RadosConfigStore::write_default_realm_id(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string_view realm_id,
                                             RGWObjVersionTracker* objv)
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
                                            RGWObjVersionTracker* objv)
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
                                              RGWObjVersionTracker* objv)
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
                                 RGWObjVersionTracker* objv)
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
      r = impl->read(dpp, y, pool, default_oid, default_info, nullptr);
      if (r < 0) {
        return r;
      }
      realm_id = default_info.default_id;
    } else {
      // look up realm id by name
      const auto name_oid = string_cat_reserve(realm_names_oid_prefix,
                                               realm_name);
      r = impl->read(dpp, y, pool, name_oid, name, nullptr);
      if (r < 0) {
        return r;
      }
      realm_id = name.obj_id;
    }
  }

  const auto info_oid = string_cat_reserve(realm_info_oid_prefix, realm_id);
  return impl->read(dpp, y, pool, info_oid, info, objv);
}

int RadosConfigStore::read_realm_id(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string_view realm_name,
                                    std::string& realm_id)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  RGWNameToId name;

  // look up realm id by name
  const auto name_oid = string_cat_reserve(realm_names_oid_prefix,
                                           realm_name);
  int r = impl->read(dpp, y, pool, name_oid, name, nullptr);
  if (r < 0) {
    return r;
  }
  realm_id = std::move(name.obj_id);
  return 0;
}

int RadosConfigStore::update_realm(const DoutPrefixProvider* dpp,
                                   optional_yield y, const RGWRealm& info,
                                   RGWObjVersionTracker* objv)
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
                                   RGWObjVersionTracker* objv)
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
  new_objv.generate_new_write_ver(dpp->get_cct());
  int r = impl->write(dpp, y, pool, new_oid, true, name, &new_objv);
  if (r < 0) {
    return r;
  }

  // write the info with updated name
  info.set_name(std::string{new_name});
  r = impl->write(dpp, y, pool, info_oid, false, info, objv);
  if (r < 0) {
    // on failure, unlink the new name
    (void) impl->remove(dpp, y, pool, new_oid, &new_objv);
    return r;
  }

  // unlink the old name
  (void) impl->remove(dpp, y, pool, old_oid, nullptr);
  return 0;
}

int RadosConfigStore::delete_realm(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   const RGWRealm& info,
                                   RGWObjVersionTracker* objv)
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
  r = impl->remove(dpp, y, pool, name_oid, nullptr);
  const auto control_oid = string_cat_reserve(info_oid, ".control");
  r = impl->remove(dpp, y, pool, control_oid, nullptr);
  return r;
}

int RadosConfigStore::list_realm_names(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const std::string& marker,
                                       std::span<std::string> entries,
                                       ListResult& result)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  constexpr auto prefix = realm_names_oid_prefix;

  return impl->list(dpp, y, pool, marker, prefix, entries, result);
}


// Period
static std::string get_period_oid(std::string_view period_id, epoch_t epoch)
{
  // omit the epoch for the staging period
  if (period_id.ends_with(period_staging_suffix)) {
    return string_cat_reserve(period_info_oid_prefix, period_id);
  }
  return fmt::format("{}{}.{}", period_info_oid_prefix, period_id, epoch);
}

int RadosConfigStore::create_period(const DoutPrefixProvider* dpp,
                                    optional_yield y, bool exclusive,
                                    const RGWPeriod& info,
                                    RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "period cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_epoch() == 0) {
    ldpp_dout(dpp, 0) << "period cannot have an empty epoch" << dendl;
    return -EINVAL;
  }
  const auto& pool = impl->period_pool;
  const auto info_oid = get_period_oid(info.get_id(), info.get_epoch());
  return impl->write(dpp, y, pool, info_oid, exclusive, info, objv);
}

int RadosConfigStore::write_period_latest_epoch(const DoutPrefixProvider* dpp,
                                                optional_yield y,
                                                bool exclusive,
                                                std::string_view period_id,
                                                epoch_t epoch,
                                                RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->period_pool;
  const auto latest_oid = string_cat_reserve(
      period_info_oid_prefix, period_id,
      name_or_default(dpp->get_cct()->_conf->rgw_period_latest_epoch_info_oid,
                      period_latest_epoch_info_oid));
  const auto latest = RGWPeriodLatestEpochInfo{epoch};
  return impl->write(dpp, y, pool, latest_oid, exclusive, latest, objv);
}

int RadosConfigStore::read_period_latest_epoch(const DoutPrefixProvider* dpp,
                                               optional_yield y,
                                               std::string_view period_id,
                                               epoch_t& epoch,
                                               RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->period_pool;
  const auto latest_oid = string_cat_reserve(
      period_info_oid_prefix, period_id,
      name_or_default(dpp->get_cct()->_conf->rgw_period_latest_epoch_info_oid,
                      period_latest_epoch_info_oid));
  RGWPeriodLatestEpochInfo latest;
  int r = impl->read(dpp, y, pool, latest_oid, latest, objv);
  if (r >= 0) {
    epoch = latest.epoch;
  }
  return r;
}

int RadosConfigStore::read_period(const DoutPrefixProvider* dpp,
                                  optional_yield y,
                                  std::string_view period_id,
                                  std::optional<epoch_t> epoch,
                                  RGWPeriod& info,
                                  RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  int r = 0;
  if (!epoch) {
    epoch = 0;
    r = read_period_latest_epoch(dpp, y, period_id, *epoch, nullptr);
    if (r < 0) {
      return r;
    }
  }

  const auto& pool = impl->period_pool;
  const auto info_oid = get_period_oid(info.get_id(), info.get_epoch());
  return impl->read(dpp, y, pool, info_oid, info, objv);
}

int RadosConfigStore::update_period(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    const RGWPeriod& info,
                                    RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "period cannot have an empty id" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->realm_pool;
  const auto info_oid = get_period_oid(info.get_id(), info.get_epoch());
  return impl->write(dpp, y, pool, info_oid, false, info, objv);
}

int RadosConfigStore::delete_period(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    const RGWPeriod& old_info,
                                    RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;
  return -ENOTSUP;
}

int RadosConfigStore::list_period_ids(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      const std::string& marker,
                                      std::span<std::string> entries,
                                      ListResult& result)
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
