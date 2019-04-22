#ifndef CEPH_RGW_SERVICES_RADOS_H
#define CEPH_RGW_SERVICES_RADOS_H

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "include/expected.hpp"

#include "include/RADOS/RADOS.hpp"

#include "common/async/yield_context.h"

#include "rgw/rgw_service.h"


using RGWAccessListFilter =
  std::function<bool(std::string_view, std::string_view)>;

inline auto RGWAccessListFilterPrefix(std::string prefix) {
  return [prefix = std::move(prefix)](std::string_view,
                                      std::string_view key) -> bool {
           return (prefix.compare(key.substr(0, prefix.size())) == 0);
         };
}

class RGWSI_RADOS : public RGWServiceInstance
{
  RADOS::RADOS rados;

public:

  class Pool {
    friend class RGWSI_RADOS;

    CephContext* cct;
    RADOS::RADOS& rados;
    RADOS::IOContext ioc;
    std::string name; // Rather annoying.

    Pool(CephContext* cct,
         RADOS::RADOS& rados,
         int64_t id,
         std::string ns,
         std::string(name)) : cct(cct), rados(rados), ioc(id, ns), name(name) {}

  public:

    class List {
      friend Pool;
      Pool& pool;
      RADOS::EnumerationCursor iter = RADOS::EnumerationCursor::begin();
      RGWAccessListFilter filter;

      List(Pool& pool, RGWAccessListFilter filter) :
        pool(pool), filter(std::move(filter)) {}

    public:
      boost::system::error_code get_next(int max,
                                         std::vector<std::string> *oids,
                                         bool *is_truncated,
                                         optional_yield);
    };

    List list(RGWAccessListFilter filter = nullptr);

  };


private:

  tl::expected<std::int64_t, boost::system::error_code>
  open_pool(std::string_view pool, bool create, optional_yield y);

public:
  RGWSI_RADOS(CephContext *cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc), rados(ioc, cct) {}

  void init() {}

  uint64_t instance_id();

  class Obj {
    friend class RGWSI_RADOS;

    CephContext* cct;
    RADOS::RADOS& rados;
    RADOS::IOContext ioc;
    RADOS::Object obj;
    std::string pname; // Fudge. We need this for now to support
                       // getting an rgw_raw_obj back out.

    Obj(CephContext* cct, RADOS::RADOS& rados, int64_t pool, std::string_view ns,
        std::string_view obj, std::string_view key, std::string_view pname)
      : rados(rados), ioc(pool, ns), obj(obj), pname(pname) {
      if (!key.empty())
        ioc.key(key);
    }

  public:

    Obj(const Obj& rhs)
      : cct(rhs.cct), rados(rhs.rados), ioc(rhs.ioc), obj(rhs.obj),
        pname(rhs.pname) {}
    Obj(Obj&& rhs)
      : cct(rhs.cct), rados(rhs.rados), ioc(std::move(rhs.ioc)),
        obj(std::move(rhs.obj)), pname(std::move(rhs.pname)) {}

    Obj& operator =(const Obj& rhs) {
      ioc = rhs.ioc;
      obj = rhs.obj;
      pname = rhs.pname;
      return *this;
    }
    Obj& operator =(Obj&& rhs) {
      ioc = std::move(rhs.ioc);
      obj = std::move(rhs.obj);
      pname = std::move(rhs.pname);
      return *this;
    }

    rgw_raw_obj get_raw_obj() const {
      return rgw_raw_obj(rgw_pool(pname, std::string(ioc.ns())),
                         std::string(obj),
                         std::string(ioc.key().value_or(std::string_view{})));
    }

    std::string_view get_oid() const {
      return std::string_view(obj);
    }

    boost::system::system_error open(optional_yield y);

    boost::system::error_code operate(RADOS::WriteOp&& op, optional_yield y,
                                      version_t* objver = nullptr);
    boost::system::error_code operate(RADOS::ReadOp&& op,
                                      ceph::buffer::list* bl,
                                      optional_yield y,
                                      version_t* objver = nullptr);
    template<typename CompletionToken>
    auto aio_operate(RADOS::WriteOp&& op, CompletionToken&& token) {
      return rados.execute(obj, ioc, std::move(op),
                           std::forward<CompletionToken>(token));
    }
    template<typename CompletionToken>
    auto aio_operate(RADOS::ReadOp&& op, ceph::buffer::list* bl,
                     CompletionToken&& token) {
      return rados.execute(obj, ioc, std::move(op), bl,
                           std::forward<CompletionToken>(token));
    }

    tl::expected<uint64_t, boost::system::error_code>
    watch(RADOS::RADOS::WatchCB&& f, optional_yield y);
    template<typename CompletionToken>
    auto aio_watch(RADOS::RADOS::WatchCB&& f, CompletionToken&& token) {
      return rados.watch(obj, ioc, nullopt, std::move(f),
                         std::forward<CompletionToken>(token));
    }
    boost::system::error_code unwatch(uint64_t handle,
                                      optional_yield y);
    boost::system::error_code
    notify(bufferlist&& bl,
           std::optional<std::chrono::milliseconds> timeouts,
           bufferlist* pbl, optional_yield y);
    boost::system::error_code notify_ack(uint64_t notify_id, uint64_t cookie, bufferlist&& bl,
                                         optional_yield y);
  };

  boost::system::error_code create_pool(const rgw_pool& p, optional_yield y);
  boost::system::error_code create_pools(const std::vector<rgw_pool>& p,
                                         std::vector<boost::system::error_code>* e,
                                         optional_yield y);

  void watch_flush(optional_yield y);

  tl::expected<Obj, boost::system::error_code>
  obj(const rgw_raw_obj& o, optional_yield y);
  tl::expected<Obj, boost::system::error_code>
  obj(const Pool& p, std::string_view oid, std::string_view loc);

  tl::expected<Pool, boost::system::error_code>
  pool(const rgw_pool& p, optional_yield y);

  friend Obj;
  friend Pool;
  friend Pool::List;
};

#endif
