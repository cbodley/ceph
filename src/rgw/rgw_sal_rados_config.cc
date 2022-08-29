#include <system_error>
#include "include/rados/librados.hpp"
#include "common/async/yield_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "rgw_basic_types.h"
#include "rgw_realm_watcher.h"
#include "rgw_sal_rados_config.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"

constexpr std::string_view realm_names_oid_prefix = "realms_names.";
constexpr std::string_view realm_info_oid_prefix = "realms.";
constexpr std::string_view realm_control_oid_suffix = ".control";
constexpr std::string_view default_realm_info_oid = "default.realm";

constexpr std::string_view period_info_oid_prefix = "periods.";
constexpr std::string_view period_latest_epoch_info_oid = ".latest_epoch";
constexpr std::string_view period_staging_suffix = ":staging";

constexpr std::string_view zonegroup_names_oid_prefix = "zonegroups_names.";
constexpr std::string_view zonegroup_info_oid_prefix = "zonegroup_info.";
constexpr std::string_view default_zonegroup_info_oid = "default.zonegroup";

constexpr std::string_view zone_info_oid_prefix = "zone_info.";
constexpr std::string_view zone_names_oid_prefix = "zone_names.";
constexpr std::string_view default_zone_name = "default";

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

// write options that control object creation
enum class Create {
  MustNotExist, // fail with EEXIST if the object already exists
  MayExist, // create if the object didn't exist, overwrite if it did
  MustExist, // fail with ENOENT if the object doesn't exist
};

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

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const rgw_pool& pool, const std::string& oid, Create create,
            const bufferlist& bl, RGWObjVersionTracker* objv)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }

    librados::ObjectWriteOperation op;
    switch (create) {
      case Create::MustNotExist: op.create(true); break;
      case Create::MayExist: op.create(false); break;
      case Create::MustExist: op.assert_exists(); break;
    }
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
            const rgw_pool& pool, const std::string& oid, Create create,
            const T& data, RGWObjVersionTracker* objv)
  {
    bufferlist bl;
    encode(data, bl);

    return write(dpp, y, pool, oid, create, bl, objv);
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
           std::regular_invocable<std::string> auto filter,
           std::span<std::string> entries,
           ListResult<std::string>& result)
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
        std::string entry = filter(iter->get_oid());
        if (!entry.empty()) {
          entries[count++] = std::move(entry);
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

  int notify(const DoutPrefixProvider* dpp, optional_yield y,
             const rgw_pool& pool, const std::string& oid,
             bufferlist& bl, uint64_t timeout_ms)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }
    return rgw_rados_notify(dpp, ioctx, oid, bl, timeout_ms, nullptr, y);
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
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;

  // write the realm info
  int r = impl->write(dpp, y, pool, info_oid, create, info, objv);
  if (r < 0) {
    return r;
  }

  // write the realm name
  const auto name = RGWNameToId{info.get_id()};
  RGWObjVersionTracker name_objv;
  name_objv.generate_new_write_ver(dpp->get_cct());

  r = impl->write(dpp, y, pool, name_oid, create, name, &name_objv);
  if (r < 0) {
    (void) impl->remove(dpp, y, pool, info_oid, objv);
    return r;
  }

  // create control object for watch/notify
  const auto control_oid = string_cat_reserve(info_oid, realm_control_oid_suffix);
  bufferlist empty_bl;
  return impl->write(dpp, y, pool, control_oid, Create::MayExist,
                     empty_bl, nullptr);
}

int RadosConfigStore::write_default_realm_id(const DoutPrefixProvider* dpp,
                                             optional_yield y, bool exclusive,
                                             std::string_view realm_id,
                                             RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  const auto oid = name_or_default(
      dpp->get_cct()->_conf->rgw_default_realm_info_oid,
      default_realm_info_oid);
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = realm_id;

  return impl->write(dpp, y, pool, oid, create, default_info, objv);
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

int RadosConfigStore::overwrite_realm(const DoutPrefixProvider* dpp,
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
  return impl->write(dpp, y, pool, info_oid, Create::MustExist, info, objv);
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
  if (info.get_name().empty() || new_name.empty()) {
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
  int r = impl->write(dpp, y, pool, new_oid, Create::MustNotExist,
                      name, &new_objv);
  if (r < 0) {
    return r;
  }

  // write the info with updated name
  info.set_name(std::string{new_name});
  r = impl->write(dpp, y, pool, info_oid, Create::MustExist, info, objv);
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
  const auto control_oid = string_cat_reserve(info_oid, realm_control_oid_suffix);
  r = impl->remove(dpp, y, pool, control_oid, nullptr);
  return r;
}

int RadosConfigStore::realm_notify_new_period(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              const RGWPeriod& period)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  const auto control_oid = string_cat_reserve(
      realm_info_oid_prefix, period.realm_id, realm_control_oid_suffix);

  bufferlist bl;
  using ceph::encode;
  // push the period to dependent zonegroups/zones
  encode(RGWRealmNotify::ZonesNeedPeriod, bl);
  encode(period, bl);
  // reload the gateway with the new period
  encode(RGWRealmNotify::Reload, bl);

  constexpr uint64_t timeout_ms = 0;
  return impl->notify(dpp, y, pool, control_oid, bl, timeout_ms);
}

int RadosConfigStore::list_realm_names(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const std::string& marker,
                                       std::span<std::string> entries,
                                       ListResult<std::string>& result)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->realm_pool;
  constexpr auto prefix = [] (std::string oid) -> std::string {
      if (!oid.starts_with(realm_names_oid_prefix)) {
        return {};
      }
      return oid.substr(realm_names_oid_prefix.size());
    };
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
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;
  return impl->write(dpp, y, pool, info_oid, create, info, objv);
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
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;
  const auto latest = RGWPeriodLatestEpochInfo{epoch};
  return impl->write(dpp, y, pool, latest_oid, create, latest, objv);
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

int RadosConfigStore::delete_period_latest_epoch(const DoutPrefixProvider* dpp,
                                                 optional_yield y,
                                                 std::string_view period_id,
                                                 RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->period_pool;
  const auto latest_oid = string_cat_reserve(
      period_info_oid_prefix, period_id,
      name_or_default(dpp->get_cct()->_conf->rgw_period_latest_epoch_info_oid,
                      period_latest_epoch_info_oid));

  return impl->remove(dpp, y, pool, latest_oid, objv);
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

int RadosConfigStore::delete_period(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string_view period_id)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;
  const auto& pool = impl->period_pool;

  // read the latest_epoch
  epoch_t latest_epoch = 0;
  RGWObjVersionTracker latest_objv;
  int r = read_period_latest_epoch(dpp, y, period_id, latest_epoch,
                                   &latest_objv);
  if (r < 0 && r != -ENOENT) { // just delete epoch=0 on ENOENT
    ldpp_dout(dpp, 0) << "failed to read latest epoch for period "
        << period_id << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  for (epoch_t epoch = 0; epoch <= latest_epoch; epoch++) {
    const auto info_oid = get_period_oid(period_id, epoch);
    r = impl->remove(dpp, y, pool, info_oid, nullptr);
    if (r < 0 && r != -ENOENT) { // ignore ENOENT
      ldpp_dout(dpp, 0) << "failed to delete period " << info_oid
          << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  return delete_period_latest_epoch(dpp, y, period_id, &latest_objv);
}

int RadosConfigStore::list_period_ids(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      const std::string& marker,
                                      std::span<std::string> entries,
                                      ListResult<std::string>& result)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->period_pool;
  constexpr auto prefix = [] (std::string oid) -> std::string {
      if (!oid.starts_with(period_info_oid_prefix)) {
        return {};
      }
      if (!oid.ends_with(period_latest_epoch_info_oid)) {
        return {};
      }
      // trim the prefix and suffix
      const std::size_t count = oid.size() -
          period_info_oid_prefix.size() -
          period_latest_epoch_info_oid.size();
      return oid.substr(period_info_oid_prefix.size(), count);
    };

  return impl->list(dpp, y, pool, marker, prefix, entries, result);
}


// ZoneGroup
int RadosConfigStore::create_zonegroup(const DoutPrefixProvider* dpp,
                                       optional_yield y, bool exclusive,
                                       const RGWZoneGroup& info,
                                       RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
    return -EINVAL;
  }
  const auto& pool = impl->zonegroup_pool;
  const auto info_oid = string_cat_reserve(zonegroup_info_oid_prefix, info.id);
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;
  return impl->write(dpp, y, pool, info_oid, create, info, objv);
}

int RadosConfigStore::write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                 optional_yield y,
                                                 bool exclusive,
                                                 std::string_view zonegroup_id,
                                                 RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zonegroup_pool;
  const auto oid = name_or_default(
      dpp->get_cct()->_conf->rgw_default_zonegroup_info_oid,
      default_zonegroup_info_oid);
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = zonegroup_id;

  return impl->write(dpp, y, pool, oid, create, default_info, objv);
}

int RadosConfigStore::read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                optional_yield y,
                                                std::string& zonegroup_id,
                                                RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zonegroup_pool;
  const auto oid = name_or_default(
      dpp->get_cct()->_conf->rgw_default_zonegroup_info_oid,
      default_zonegroup_info_oid);

  RGWDefaultSystemMetaObjInfo default_info;
  int r = impl->read(dpp, y, pool, oid, default_info, objv);
  if (r >= 0) {
    zonegroup_id = default_info.default_id;
  }
  return r;
}

int RadosConfigStore::delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                  optional_yield y,
                                                  RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zonegroup_pool;
  const auto oid = name_or_default(
      dpp->get_cct()->_conf->rgw_default_zonegroup_info_oid,
      default_zonegroup_info_oid);

  return impl->remove(dpp, y, pool, oid, objv);
}

int RadosConfigStore::read_zonegroup(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view zonegroup_id,
                                     std::string_view zonegroup_name,
                                     RGWZoneGroup& info,
                                     RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zonegroup_pool;
  RGWDefaultSystemMetaObjInfo default_info;
  RGWNameToId name;

  int r = 0;
  if (zonegroup_id.empty()) {
    if (zonegroup_name.empty()) {
      // read default zonegroup id
      const auto default_oid = name_or_default(
          dpp->get_cct()->_conf->rgw_default_zonegroup_info_oid,
          default_zonegroup_info_oid);
      r = impl->read(dpp, y, pool, default_oid, default_info, nullptr);
      if (r < 0) {
        return r;
      }
      zonegroup_id = default_info.default_id;
    } else {
      // look up zonegroup id by name
      const auto name_oid = string_cat_reserve(zonegroup_names_oid_prefix,
                                               zonegroup_name);
      r = impl->read(dpp, y, pool, name_oid, name, nullptr);
      if (r < 0) {
        return r;
      }
      zonegroup_id = name.obj_id;
    }
  }

  const auto info_oid = string_cat_reserve(zonegroup_info_oid_prefix, zonegroup_id);
  return impl->read(dpp, y, pool, info_oid, info, objv);
}

int RadosConfigStore::overwrite_zonegroup(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          const RGWZoneGroup& info,
                                          RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->zonegroup_pool;
  const auto info_oid = string_cat_reserve(zonegroup_info_oid_prefix, info.get_id());
  return impl->write(dpp, y, pool, info_oid, Create::MustExist, info, objv);
}

int RadosConfigStore::rename_zonegroup(const DoutPrefixProvider* dpp,
                                       optional_yield y, RGWZoneGroup& info,
                                       std::string_view new_name,
                                       RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty() || new_name.empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->zonegroup_pool;
  const auto name = RGWNameToId{info.get_id()};
  const auto info_oid = string_cat_reserve(zonegroup_info_oid_prefix, info.get_id());
  const auto old_oid = string_cat_reserve(zonegroup_names_oid_prefix, info.get_name());
  const auto new_oid = string_cat_reserve(zonegroup_names_oid_prefix, new_name);

  // link the new name
  RGWObjVersionTracker new_objv;
  new_objv.generate_new_write_ver(dpp->get_cct());
  int r = impl->write(dpp, y, pool, new_oid, Create::MustNotExist,
                      name, &new_objv);
  if (r < 0) {
    return r;
  }

  // write the info with updated name
  info.set_name(std::string{new_name});
  r = impl->write(dpp, y, pool, info_oid, Create::MustExist, info, objv);
  if (r < 0) {
    // on failure, unlink the new name
    (void) impl->remove(dpp, y, pool, new_oid, &new_objv);
    return r;
  }

  // unlink the old name
  (void) impl->remove(dpp, y, pool, old_oid, nullptr);
  return 0;
}

int RadosConfigStore::delete_zonegroup(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const RGWZoneGroup& info,
                                       RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zonegroup_pool;
  const auto info_oid = string_cat_reserve(zonegroup_info_oid_prefix, info.get_id());
  int r = impl->remove(dpp, y, pool, info_oid, objv);
  if (r < 0) {
    return r;
  }
  const auto name_oid = string_cat_reserve(zonegroup_names_oid_prefix, info.get_name());
  return impl->remove(dpp, y, pool, name_oid, nullptr);
}

int RadosConfigStore::list_zonegroup_names(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           const std::string& marker,
                                           std::span<std::string> entries,
                                           ListResult<std::string>& result)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zonegroup_pool;
  constexpr auto prefix = [] (std::string oid) -> std::string {
      if (!oid.starts_with(zonegroup_names_oid_prefix)) {
        return {};
      }
      return oid.substr(zonegroup_names_oid_prefix.size());
    };
  return impl->list(dpp, y, pool, marker, prefix, entries, result);
}


// Zone
int RadosConfigStore::create_zone(const DoutPrefixProvider* dpp,
                                  optional_yield y, bool exclusive,
                                  const RGWZoneParams& info,
                                  RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty name" << dendl;
    return -EINVAL;
  }
  const auto& pool = impl->zone_pool;
  const auto info_oid = string_cat_reserve(zone_info_oid_prefix, info.id);
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;
  return impl->write(dpp, y, pool, info_oid, create, info, objv);
}

static std::string default_zone_oid(std::string_view prefix,
                                    std::string_view realm_id)
{
  return fmt::format("{}.{}", prefix, realm_id);
}

int RadosConfigStore::write_default_zone_id(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            bool exclusive,
                                            std::string_view realm_id,
                                            std::string_view zone_id,
                                            RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zone_pool;
  const auto default_oid = default_zone_oid(
      dpp->get_cct()->_conf->rgw_default_zone_info_oid,
      realm_id);

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = zone_id;

  return impl->write(dpp, y, pool, default_oid, create, default_info, objv);
}

int RadosConfigStore::read_default_zone_id(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           std::string_view realm_id,
                                           std::string& zone_id,
                                           RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zone_pool;
  const auto default_oid = default_zone_oid(
      dpp->get_cct()->_conf->rgw_default_zone_info_oid,
      realm_id);

  RGWDefaultSystemMetaObjInfo default_info;
  int r = impl->read(dpp, y, pool, default_oid, default_info, objv);
  if (r >= 0) {
    zone_id = default_info.default_id;
  }
  return r;
}

int RadosConfigStore::delete_default_zone_id(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string_view realm_id,
                                             RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zone_pool;
  const auto default_oid = default_zone_oid(
      dpp->get_cct()->_conf->rgw_default_zone_info_oid,
      realm_id);

  return impl->remove(dpp, y, pool, default_oid, objv);
}

int RadosConfigStore::read_zone(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view zone_id,
                                std::string_view zone_name,
                                RGWZoneParams& info,
                                RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zone_pool;
  RGWDefaultSystemMetaObjInfo default_info;
  RGWNameToId name;

  int r = 0;
  if (zone_id.empty()) {
    if (zone_name.empty()) {
      // read default zone id
      const auto default_oid = default_zone_oid(
          dpp->get_cct()->_conf->rgw_default_zone_info_oid,
          realm_id);
      r = impl->read(dpp, y, pool, default_oid, default_info, nullptr);
      if (r < 0) {
        return r;
      }
      zone_id = default_info.default_id;
    } else {
      // look up zone id by name
      const auto name_oid = string_cat_reserve(zone_names_oid_prefix,
                                               zone_name);
      r = impl->read(dpp, y, pool, name_oid, name, nullptr);
      if (r < 0) {
        return r;
      }
      zone_id = name.obj_id;
    }
  }

  const auto info_oid = string_cat_reserve(zone_info_oid_prefix, zone_id);
  return impl->read(dpp, y, pool, info_oid, info, objv);
}

int RadosConfigStore::overwrite_zone(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     const RGWZoneParams& info,
                                     RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->zone_pool;
  const auto info_oid = string_cat_reserve(zone_info_oid_prefix, info.get_id());
  return impl->write(dpp, y, pool, info_oid, Create::MustExist, info, objv);
}

int RadosConfigStore::rename_zone(const DoutPrefixProvider* dpp,
                                  optional_yield y, RGWZoneParams& info,
                                  std::string_view new_name,
                                  RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty() || new_name.empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->zone_pool;
  const auto name = RGWNameToId{info.get_id()};
  const auto info_oid = string_cat_reserve(zone_info_oid_prefix, info.get_id());
  const auto old_oid = string_cat_reserve(zone_names_oid_prefix, info.get_name());
  const auto new_oid = string_cat_reserve(zone_names_oid_prefix, new_name);

  // link the new name
  RGWObjVersionTracker new_objv;
  new_objv.generate_new_write_ver(dpp->get_cct());
  int r = impl->write(dpp, y, pool, new_oid, Create::MustNotExist,
                      name, &new_objv);
  if (r < 0) {
    return r;
  }

  // write the info with updated name
  info.set_name(std::string{new_name});
  r = impl->write(dpp, y, pool, info_oid, Create::MustExist, info, objv);
  if (r < 0) {
    // on failure, unlink the new name
    (void) impl->remove(dpp, y, pool, new_oid, &new_objv);
    return r;
  }

  // unlink the old name
  (void) impl->remove(dpp, y, pool, old_oid, nullptr);
  return 0;
}

int RadosConfigStore::delete_zone(const DoutPrefixProvider* dpp,
                                  optional_yield y,
                                  const RGWZoneParams& info,
                                  RGWObjVersionTracker* objv)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zone_pool;
  const auto info_oid = string_cat_reserve(zone_info_oid_prefix, info.get_id());
  int r = impl->remove(dpp, y, pool, info_oid, objv);
  if (r < 0) {
    return r;
  }
  const auto name_oid = string_cat_reserve(zone_names_oid_prefix, info.get_name());
  return impl->remove(dpp, y, pool, name_oid, nullptr);
}

int RadosConfigStore::list_zone_names(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      const std::string& marker,
                                      std::span<std::string> entries,
                                      ListResult<std::string>& result)
{
  const auto wrapped = RadosConfigPrefix{*dpp};
  dpp = &wrapped;

  const auto& pool = impl->zone_pool;
  constexpr auto prefix = [] (std::string oid) -> std::string {
      if (!oid.starts_with(zone_names_oid_prefix)) {
        return {};
      }
      return oid.substr(zone_names_oid_prefix.size());
    };
  return impl->list(dpp, y, pool, marker, prefix, entries, result);
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
