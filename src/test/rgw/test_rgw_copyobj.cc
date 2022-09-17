// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_aio_throttle.h"
#include "cls/refcount/cls_refcount_ops.h"
#include "cls/refcount/cls_refcount_client.h"

#include <optional>
#include <string>
#include "include/scope_guard.h"
#include "test/librados/test_cxx.h"

#include <spawn/spawn.hpp>
#include <gtest/gtest.h>

using namespace std;

class RefcountReadCtx : public librados::ObjectOperationCompletion {
  list<string> *entries;
public:
  RefcountReadCtx(list<string> *_entries) : entries(_entries) {}
  void handle_completion(int r, bufferlist& outbl) override {
    if (r >= 0) {
      cls_refcount_read_ret ret;
      try {
        auto iter = outbl.cbegin();
        decode(ret, iter);
        if (entries) {
          *entries = std::move(ret.refs);
        }
      } catch (ceph::buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_refcount_read(librados::ObjectReadOperation& op, list<string> *refs, bool implicit_ref)
{

  bufferlist inbl;
  cls_refcount_read_op call;
  call.implicit_ref = implicit_ref;
  encode(call, inbl);

  op.exec("refcount", "read", inbl, new RefcountReadCtx(refs));
}

static librados::ObjectWriteOperation *new_write_op() {
  return new librados::ObjectWriteOperation();
}

static librados::ObjectReadOperation *new_read_op() {
  return new librados::ObjectReadOperation();
}

struct RadosEnv : public ::testing::Environment {
 public:
  static constexpr auto poolname = "ceph_test_rgw_copyobj";

  static std::optional<RGWSI_RADOS> rados;

  void SetUp() override {
    rados.emplace(g_ceph_context);
    const NoDoutPrefix no_dpp(g_ceph_context, 1);
    ASSERT_EQ(0, rados->start(null_yield, &no_dpp));
    int r = rados->pool({poolname}).create(&no_dpp);
    if (r == -EEXIST)
      r = 0;
    ASSERT_EQ(0, r);
  }
  void TearDown() override {
    rados->shutdown();
    rados.reset();
  }
};
std::optional<RGWSI_RADOS> RadosEnv::rados;

auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

// test fixture for global setup/teardown
class RadosFixture : public ::testing::Test {
 protected:
  RGWSI_RADOS::Obj make_obj(const std::string& oid) {
    auto obj = RadosEnv::rados->obj({{RadosEnv::poolname}, oid});
    const NoDoutPrefix no_dpp(g_ceph_context, 1);
    ceph_assert_always(0 == obj.open(&no_dpp));
    return obj;
  }
};

using Aio_Throttle = RadosFixture;

namespace rgw {

TEST_F(Aio_Throttle, RefCopySeccess)
{
  auto obj = make_obj(__PRETTY_FUNCTION__);

  boost::asio::io_context context;
  spawn::spawn(context,
    [&] (yield_context yield) {
      auto aio = rgw::make_throttle(10, optional_yield(context, yield));
      rgw::AioResultList all_results;

      /* create obj */
      librados::ObjectWriteOperation *write_op = new_write_op();
      write_op->create(false);
      rgw::AioResultList completed = aio->get(obj, rgw::Aio::librados_op(std::move(*write_op), optional_yield(context, yield)), 1, 0);
      all_results.splice(all_results.end(), completed);
      delete write_op;

      /* take first reference, that is to say copy once */
      write_op = new_write_op();
      cls_refcount_get(*write_op, "tag1", true);
      completed = aio->get(obj, rgw::Aio::librados_op(std::move(*write_op), optional_yield(context, yield)), 1, 0);
      all_results.splice(all_results.end(), completed);
      delete write_op;

      /* take second reference, that is copy again */
      write_op = new_write_op();
      cls_refcount_get(*write_op, "tag2", true);
      completed = aio->get(obj, rgw::Aio::librados_op(std::move(*write_op), optional_yield(context, yield)), 1, 0);
      all_results.splice(all_results.end(), completed);
      delete write_op;

      librados::ObjectReadOperation *read_op = new_read_op();
      list<string> refs;
      cls_refcount_read(*read_op, &refs, true);
      completed = aio->get(obj, rgw::Aio::librados_op(std::move(*read_op), optional_yield(context, yield)), 1, 0);
      all_results.splice(all_results.end(), completed);
      delete read_op;

      completed = aio->drain();
      all_results.splice(all_results.end(), completed);
      ASSERT_EQ(0, rgw::check_for_errors(all_results));

      /* together with wildcard_tag, there should be 3 reference in total */
      ASSERT_EQ(3, (int)refs.size());
    });
  context.run();
}

TEST_F(Aio_Throttle, RefCopyFail)
{
  auto obj = make_obj(__PRETTY_FUNCTION__);

  boost::asio::io_context context;
  spawn::spawn(context,
    [&] (yield_context yield) {
      auto aio = rgw::make_throttle(10, optional_yield(context, yield));
      rgw::AioResultList all_results;

      /* create obj */
      librados::ObjectWriteOperation *write_op = new_write_op();
      write_op->create(false);
      rgw::AioResultList completed = aio->get(obj, rgw::Aio::librados_op(std::move(*write_op), optional_yield(context, yield)), 1, 0);
      completed = aio->drain();
      ASSERT_EQ(0, rgw::check_for_errors(completed));
      completed.clear();
      delete write_op;

      /* copy once */
      write_op = new_write_op();
      cls_refcount_get(*write_op, "tag1", true);
      completed = aio->get(obj, rgw::Aio::librados_op(std::move(*write_op), optional_yield(context, yield)), 1, 0);
      all_results.splice(all_results.end(), completed);
      delete write_op;

      /* copy again */
      write_op = new_write_op();
      cls_refcount_get(*write_op, "tag2", true);
      completed = aio->get(obj, rgw::Aio::librados_op(std::move(*write_op), optional_yield(context, yield)), 1, 0);
      all_results.splice(all_results.end(), completed);
      delete write_op;

      completed = aio->drain();
      all_results.splice(all_results.end(), completed);
      ASSERT_EQ(0, rgw::check_for_errors(all_results));

      librados::ObjectReadOperation *read_op = new_read_op();
      list<string> refs;
      cls_refcount_read(*read_op, &refs, true);
      aio->get(obj, rgw::Aio::librados_op(std::move(*read_op), optional_yield(context, yield)), 1, 0);
      delete read_op;
      aio->drain();

      /* together with wildcard_tag, there should be 3 reference in total */
      ASSERT_EQ(3, (int)refs.size());

      /* inject an error, so to test the rollback logic in copy_obj */
      ASSERT_EQ(2, all_results.size());
      AioResultEntry bad_entry;
      bad_entry.result = -ENODATA;
      all_results.push_back(bad_entry);
      int tag_num = 1;
      for (auto& r : all_results) {
        if (r.result < 0) {
          continue; // skip errors
        }
        write_op = new_write_op();
        cls_refcount_put(*write_op, "tag"+std::to_string(tag_num), true);
        aio->get(r.obj, rgw::Aio::librados_op(std::move(*write_op), optional_yield(context, yield)), 1, 0);
        delete write_op;
        tag_num++;
      }
      read_op = new_read_op();
      cls_refcount_read(*read_op, &refs, true);
      aio->get(obj, rgw::Aio::librados_op(std::move(*read_op), optional_yield(context, yield)), 1, 0);
      delete read_op;

      completed = aio->drain();
      ASSERT_EQ(0, rgw::check_for_errors(completed));

      /* only wildcard_tag left */
      ASSERT_EQ(1, (int)refs.size());

    });
    context.run();
}

} // namespace rgw

