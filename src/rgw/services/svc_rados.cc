#include "svc_rados.h"

#include "common/errno.h"
#include "common/waiter.h"
#include "osd/osd_types.h"
#include "rgw/rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

tl::expected<std::int64_t, boost::system::error_code>
RGWSI_RADOS::open_pool(std::string_view pool, bool create, optional_yield y) {
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    int64_t id = rados.lookup_pool(std::string(pool), yield[ec]);
    if (ec == boost::system::errc::no_such_file_or_directory && create) {
      rados.create_pool(pool, nullopt, yield[ec]);
      if (ec && ec != boost::system::errc::file_exists) {
        if (ec == boost::system::errc::result_out_of_range) {
          ldout(cct, 0)
            << __func__
            << " ERROR: RADOS::RADOS::create_pool returned " << ec
            << " (this can be due to a pool or placement group misconfiguration, e.g."
            << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
            << dendl;
        }
        return tl::unexpected(ec);
      }
      id = rados.lookup_pool(std::string(pool), yield[ec]);
      if (ec)
        return tl::unexpected(ec);
      rados.enable_application(pool, pg_pool_t::APPLICATION_NAME_RGW, false,
                               yield[ec]);
      if (ec && ec != boost::system::errc::operation_not_supported) {
        return tl::unexpected(ec);
      }
    }
    if (ec)
      return tl::unexpected(ec);
    return id;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  boost::system::error_code ec;
  int64_t id;
  {
    ceph::waiter<boost::system::error_code, int64_t> w;
    rados.lookup_pool(std::string(pool), w.ref());
    std::tie(ec, id) = w.wait();
  }
  if (ec == boost::system::errc::no_such_file_or_directory && create) {
    {
      ceph::waiter<boost::system::error_code> w;
      rados.create_pool(pool, nullopt, w.ref());
      ec = w.wait();
    }
    if (ec && ec != boost::system::errc::file_exists) {
      if (ec == boost::system::errc::result_out_of_range) {
        ldout(cct, 0)
          << __func__
          << " ERROR: RADOS::RADOS::create_pool returned " << ec
          << " (this can be due to a pool or placement group misconfiguration, e.g."
          << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
          << dendl;
      }
      return tl::unexpected(ec);
    }
    {
      ceph::waiter<boost::system::error_code, int64_t> w;
      rados.lookup_pool(std::string(pool), w.ref());
      std::tie(ec, id) = w.wait();
    }
    if (ec)
      return tl::unexpected(ec);
    {
      ceph::waiter<boost::system::error_code> w;
      rados.enable_application(pool, pg_pool_t::APPLICATION_NAME_RGW, false,
                               w.ref());
      ec = w.wait();
    }
    if (ec && ec != boost::system::errc::operation_not_supported) {
      return tl::unexpected(ec);
    }
  }
  if (ec)
    return tl::unexpected(ec);
  return id;
}

uint64_t RGWSI_RADOS::instance_id()
{
  return rados.instance_id();
}

boost::system::error_code RGWSI_RADOS::Obj::operate(RADOS::WriteOp&& op,
                                                    optional_yield y,
                                                    version_t* objver)
{
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    rados.execute(obj, ioc, std::move(op), yield[ec], objver);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ceph::waiter<boost::system::error_code> w;
  rados.execute(obj, ioc, std::move(op), w.ref(), objver);
  return w.wait();
}

boost::system::error_code RGWSI_RADOS::Obj::operate(RADOS::ReadOp&& op,
                                                    ceph::buffer::list* bl,
                                                    optional_yield y,
                                                    version_t* objver)
{
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    rados.execute(obj, ioc, std::move(op), bl, yield[ec], objver);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ceph::waiter<boost::system::error_code> w;
  rados.execute(obj, ioc, std::move(op), bl, w.ref(), objver);
  return w.wait();
}

tl::expected<uint64_t, boost::system::error_code>
RGWSI_RADOS::Obj::watch(RADOS::RADOS::WatchCB&& f, optional_yield y) {
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    auto handle = rados.watch(obj, ioc, nullopt, std::move(f),
                              yield[ec]);
    if (ec)
      return tl::unexpected(ec);
    else
      return handle;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ceph::waiter<boost::system::error_code, uint64_t> w;
  rados.watch(obj, ioc, nullopt, std::move(f), w.ref());
  auto [ec, handle] = w.wait();
  if (ec)
    return tl::unexpected(ec);
  else
    return handle;
}

boost::system::error_code RGWSI_RADOS::Obj::unwatch(uint64_t handle,
                                                    optional_yield y)
{
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    rados.unwatch(handle, ioc, yield[ec]);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ceph::waiter<boost::system::error_code> w;
  rados.unwatch(handle, ioc, w.ref());
  return w.wait();
}

boost::system::error_code
RGWSI_RADOS::Obj::notify(bufferlist&& bl,
                         std::optional<std::chrono::milliseconds> timeout,
                         bufferlist* pbl, optional_yield y) {
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    auto rbl = rados.notify(obj, ioc, std::move(bl), timeout, yield[ec]);
    if (pbl)
      *pbl = std::move(rbl);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ceph::waiter<boost::system::error_code, bufferlist> w;
  rados.notify(obj, ioc, std::move(bl), timeout, w.ref());
  auto [ec, rbl] = w.wait();
  if (pbl)
    *pbl = std::move(rbl);
  return ec;
}

boost::system::error_code
RGWSI_RADOS::Obj::notify_ack(uint64_t notify_id, uint64_t cookie,
                             bufferlist&& bl,
                             optional_yield y) {
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    rados.notify_ack(obj, ioc, notify_id, cookie, std::move(bl), yield[ec]);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ceph::waiter<boost::system::error_code> w;
  rados.notify_ack(obj, ioc, notify_id, cookie, std::move(bl), w.ref());
  return w.wait();
}

boost::system::error_code RGWSI_RADOS::create_pool(const rgw_pool& p,
                                                   optional_yield y)
{
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    rados.create_pool(p.name, nullopt, yield[ec]);
    if (ec) {
      if (ec == boost::system::errc::result_out_of_range) {
        ldout(cct, 0)
          << __func__
          << " ERROR: RADOS::RADOS::create_pool returned " << ec
          << " (this can be due to a pool or placement group misconfiguration, e.g."
          << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
          << dendl;
      }
      return ec;
    }
    rados.enable_application(p.name, pg_pool_t::APPLICATION_NAME_RGW, false,
                             yield[ec]);
    if (ec && ec != boost::system::errc::operation_not_supported) {
      return ec;
    }
    return {};
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  boost::system::error_code ec;
  {
    ceph::waiter<boost::system::error_code> w;
    rados.create_pool(p.name, nullopt, w.ref());
    ec = w.wait();
  }
  if (ec) {
    if (ec == boost::system::errc::result_out_of_range) {
      ldout(cct, 0)
        << __func__
        << " ERROR: RADOS::RADOS::create_pool returned " << ec
        << " (this can be due to a pool or placement group misconfiguration, e.g."
        << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
        << dendl;
    }
    return ec;
  }
  {
    ceph::waiter<boost::system::error_code> w;
    rados.enable_application(p.name, pg_pool_t::APPLICATION_NAME_RGW, false,
                             w.ref());
    ec = w.wait();
  }
  if (ec && ec != boost::system::errc::operation_not_supported) {
    return ec;
  }

  return {};
}

boost::system::error_code
RGWSI_RADOS::create_pools(const std::vector<rgw_pool>& pools,
                          std::vector<boost::system::error_code>* e,
                          optional_yield y) {
  // TODO: Figure out how to queue up a whole bunch of operations on a
  // single coroutine rather than serializing them.
  boost::system::error_code tec;

  if (e)
    e->clear();
  for (const auto& p : pools) {
    auto ec = create_pool(p, y);
    if (ec)
      tec = ec;
    if (e)
      e->push_back(ec);
  }
  return tec;
}

RGWSI_RADOS::Pool::List RGWSI_RADOS::Pool::list(RGWAccessListFilter filter) {
  return List(*this, std::move(filter));
}

boost::system::error_code
RGWSI_RADOS::Pool::List::get_next(int max,
                                  std::vector<std::string>* oids,
                                  bool* is_truncated,
                                  optional_yield y)
{
  if (iter == RADOS::EnumerationCursor::begin())
    return ceph::to_error_code(ENOENT);

  std::vector<RADOS::EnumeratedObject> ls;
  boost::system::error_code ec;

#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    pool.rados.enumerate_objects(pool.ioc, iter,
                                 RADOS::EnumerationCursor::end(),
                                 max, {}, &ls, &iter,
                                 yield[ec]);
    }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(pool.cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  if (!y) {
    ceph::waiter<boost::system::error_code> w;
    pool.rados.enumerate_objects(pool.ioc, iter,
                                 RADOS::EnumerationCursor::end(),
                                 max, {}, &ls, &iter,
                                 w.ref());
    ec = w.wait();
  }

  if (ec)
    return ec;

  for (auto& e : ls) {
    auto& oid = e.oid;
    ldout(pool.cct, 20) << "Pool::get_ext: got " << oid << dendl;
    if (filter && filter(oid, oid))
      continue;

    oids->push_back(std::move(oid));
  }

  if (is_truncated)
    *is_truncated = (iter != RADOS::EnumerationCursor::end());
  return {};
}

tl::expected<RGWSI_RADOS::Obj, boost::system::error_code>
RGWSI_RADOS::obj(const rgw_raw_obj& o, optional_yield y) {
  auto r = open_pool(o.pool.name, true, y);
  if (r)
    return Obj(cct, rados, *r, o.pool.ns, o.oid, o.loc, o.pool.name);
  else
    return tl::unexpected(r.error());
}
tl::expected<RGWSI_RADOS::Obj, boost::system::error_code>
RGWSI_RADOS::obj(const RGWSI_RADOS::Pool& p, std::string_view oid,
                 std::string_view loc) {
  return Obj(cct, rados, p.ioc.pool(), p.ioc.ns(), oid, loc, p.name);
}

tl::expected<RGWSI_RADOS::Pool, boost::system::error_code>
RGWSI_RADOS::pool(const rgw_pool& p, optional_yield y) {
  auto r = open_pool(p.name, true, y);
  if (r)
    return Pool(cct, rados, *r, p.ns, p.name);
  else
    return tl::unexpected(r.error());
}

void RGWSI_RADOS::watch_flush(optional_yield y) {
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    rados.flush_watch(yield);
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
  return;
#endif
  ceph::waiter<void> w;
  rados.flush_watch(w.ref());
  w.wait();
}
