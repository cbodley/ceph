#include <errno.h>

#include "cls/version/cls_version_client.h"
#include "include/rados/librados.hpp"
#include "common/waiter.h"


using namespace librados;


void cls_version_set(librados::ObjectWriteOperation& op, obj_version& objv)
{
  bufferlist in;
  cls_version_set_op call;
  call.objv = objv;
  encode(call, in);
  op.exec("version", "set", in);
}

void cls_version_inc(librados::ObjectWriteOperation& op)
{
  bufferlist in;
  cls_version_inc_op call;
  encode(call, in);
  op.exec("version", "inc", in);
}

void cls_version_inc(librados::ObjectWriteOperation& op, obj_version& objv, VersionCond cond)
{
  bufferlist in;
  cls_version_inc_op call;
  call.objv = objv;
  
  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "inc_conds", in);
}

void cls_version_check(librados::ObjectOperation& op, obj_version& objv, VersionCond cond)
{
  bufferlist in;
  cls_version_check_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "check_conds", in);
}

class VersionReadCtx : public ObjectOperationCompletion {
  obj_version *objv;
public:
  explicit VersionReadCtx(obj_version *_objv) : objv(_objv) {}
  void handle_completion(int r, bufferlist& outbl) override {
    if (r >= 0) {
      cls_version_read_ret ret;
      try {
        auto iter = outbl.cbegin();
        decode(ret, iter);
	*objv = ret.objv;
      } catch (buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_version_read(librados::ObjectReadOperation& op, obj_version *objv)
{
  bufferlist inbl;
  op.exec("version", "read", inbl, new VersionReadCtx(objv));
}

int cls_version_read(librados::IoCtx& io_ctx, string& oid, obj_version *ver)
{
  bufferlist in, out;
  int r = io_ctx.exec(oid, "version", "read", in, out);
  if (r < 0)
    return r;

  cls_version_read_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  *ver = ret.objv;

  return r;
}

// New World Order

void cls_version_set(RADOS::WriteOp& op, obj_version& objv)
{
  bufferlist in;
  cls_version_set_op call;
  call.objv = objv;
  encode(call, in);
  op.exec("version", "set", in);
}

void cls_version_inc(RADOS::WriteOp& op)
{
  bufferlist in;
  cls_version_inc_op call;
  encode(call, in);
  op.exec("version", "inc", in);
}

void cls_version_inc(RADOS::WriteOp& op, obj_version& objv, VersionCond cond)
{
  bufferlist in;
  cls_version_inc_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "inc_conds", in);
}

void cls_version_check(RADOS::ReadOp& op, obj_version& objv, VersionCond cond)
{
  bufferlist in;
  cls_version_check_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "check_conds", in, nullptr);
}

void cls_version_check(RADOS::WriteOp& op, obj_version& objv, VersionCond cond)
{
  bufferlist in;
  cls_version_check_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "check_conds", in);
}

void cls_version_read(RADOS::ReadOp& op, obj_version *objv)
{
  bufferlist inbl;
  op.exec("version", "read", inbl,
          [objv](boost::system::error_code ec,
                 const bufferlist& bl) {
            cls_version_read_ret ret;
            if (!ec) {
              try {
                auto iter = bl.cbegin();
                decode(ret, iter);
                *objv = ret.objv;
              } catch (const ceph::buffer::error& err) {
                // nothing we can do about it atm
              }
            }
          });
}

tl::expected<obj_version, boost::system::error_code>
cls_version_read(RADOS::RADOS& rados, const RADOS::Object& obj,
		 const RADOS::IOContext& ioc, optional_yield y) {
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    boost::system::error_code ec;
    auto& yield = y.get_yield_context();
    bufferlist out;
    RADOS::ReadOp op;
    op.exec("version", "read", {}, &out);
    rados.execute(obj, ioc, std::move(op),  nullptr, yield[ec]);
    if (ec)
      return tl::unexpected(ec);
    cls_version_read_ret ret;
    try {
      auto iter = out.cbegin();
      decode(ret, iter);
    } catch (const ceph::buffer::error& err) {
      return tl::unexpected(err.code());
    }
    return ret.objv;
  }
#endif

  bufferlist out;
  ceph::waiter<boost::system::error_code> w;
  RADOS::ReadOp op;
  op.exec("version", "read", {}, &out);
  rados.execute(obj, ioc, std::move(op), nullptr, w.ref());
  auto ec = w.wait();
  if (ec)
    return tl::unexpected(ec);
  cls_version_read_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (const ceph::buffer::error& err) {
    return tl::unexpected(err.code());
  }
  return ret.objv;
}
