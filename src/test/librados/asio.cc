// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "librados/librados_asio.h"
#include <gtest/gtest.h>

#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#include <boost/asio/spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/optional.hpp>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

librados::Rados rados;
librados::IoCtx io;
librados::IoCtx snapio; // writes to snapio fail with -EROFS

using optional_work = boost::optional<boost::asio::io_service::work>;

TEST(AsioRados, AsyncReadCallback)
{
  boost::asio::io_service service;

  optional_work work1{service};
  auto success_cb = [&] (boost::system::error_code ec, bufferlist bl) {
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
    work1.reset();
  };
  librados::async_read(io, "exist", 256, 0, success_cb);

  optional_work work2{service};
  auto failure_cb = [&] (boost::system::error_code ec, bufferlist bl) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    work2.reset();
  };
  librados::async_read(io, "noexist", 256, 0, failure_cb);

  service.run();
}

TEST(AsioRados, AsyncReadFuture)
{
  std::future<bufferlist> f1 = librados::async_read(io, "exist", 256, 0,
                                                    boost::asio::use_future);
  std::future<bufferlist> f2 = librados::async_read(io, "noexist", 256, 0,
                                                    boost::asio::use_future);
  EXPECT_NO_THROW({
    auto bl = f1.get();
    EXPECT_EQ("hello", bl.to_str());
  });
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST(AsioRados, AsyncReadYield)
{
  boost::asio::io_service service;

  optional_work work1{service};
  auto success_cr = [&] (boost::asio::yield_context yield) {
    boost::system::error_code ec;
    auto bl = librados::async_read(io, "exist", 256, 0, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
    work1.reset();
  };
  boost::asio::spawn(service, success_cr);

  optional_work work2{service};
  auto failure_cr = [&] (boost::asio::yield_context yield) {
    boost::system::error_code ec;
    auto bl = librados::async_read(io, "noexist", 256, 0, yield[ec]);
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    work2.reset();
  };
  boost::asio::spawn(service, failure_cr);

  service.run();
}

TEST(AsioRados, AsyncWriteCallback)
{
  boost::asio::io_service service;

  bufferlist bl;
  bl.append("hello");

  optional_work work1{service};
  auto success_cb = [&] (boost::system::error_code ec) {
    EXPECT_FALSE(ec);
    work1.reset();
  };
  librados::async_write(io, "exist", bl, bl.length(), 0, success_cb);

  optional_work work2{service};
  auto failure_cb = [&] (boost::system::error_code ec) {
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    work2.reset();
  };
  librados::async_write(snapio, "exist", bl, bl.length(), 0, failure_cb);

  service.run();
}

TEST(AsioRados, AsyncWriteFuture)
{
  bufferlist bl;
  bl.append("hello");

  auto f1 = librados::async_write(io, "exist", bl, bl.length(), 0,
                                  boost::asio::use_future);
  auto f2 = librados::async_write(snapio, "exist", bl, bl.length(), 0,
                                  boost::asio::use_future);

  EXPECT_NO_THROW(f1.get());
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST(AsioRados, AsyncWriteYield)
{
  boost::asio::io_service service;

  bufferlist bl;
  bl.append("hello");

  optional_work work1{service};
  auto success_cr = [&] (boost::asio::yield_context yield) {
    boost::system::error_code ec;
    librados::async_write(io, "exist", bl, bl.length(), 0, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
    work1.reset();
  };
  boost::asio::spawn(service, success_cr);

  optional_work work2{service};
  auto failure_cr = [&] (boost::asio::yield_context yield) {
    boost::system::error_code ec;
    librados::async_write(snapio, "exist", bl, bl.length(), 0, yield[ec]);
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    work2.reset();
  };
  boost::asio::spawn(service, failure_cr);

  service.run();
}

TEST(AsioRados, AsyncReadOperationCallback)
{
  boost::asio::io_service service;
  optional_work work1{service};
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    auto success_cb = [&] (boost::system::error_code ec, bufferlist bl) {
      EXPECT_FALSE(ec);
      EXPECT_EQ("hello", bl.to_str());
      work1.reset();
    };
    librados::async_operate(io, "exist", &op, 0, success_cb);
  }
  optional_work work2{service};
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    auto failure_cb = [&] (boost::system::error_code ec, bufferlist bl) {
      EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
      work2.reset();
    };
    librados::async_operate(io, "noexist", &op, 0, failure_cb);
  }
  service.run();
}

TEST(AsioRados, AsyncReadOperationFuture)
{
  std::future<bufferlist> f1;
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    f1 = librados::async_operate(io, "exist", &op, 0,
                                 boost::asio::use_future);
  }
  std::future<bufferlist> f2;
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    f2 = librados::async_operate(io, "noexist", &op, 0,
                                 boost::asio::use_future);
  }
  EXPECT_NO_THROW({
    auto bl = f1.get();
    EXPECT_EQ("hello", bl.to_str());
  });
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST(AsioRados, AsyncReadOperationYield)
{
  boost::asio::io_service service;

  optional_work work1{service};
  auto success_cr = [&] (boost::asio::yield_context yield) {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    boost::system::error_code ec;
    auto bl = librados::async_operate(io, "exist", &op, 0, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
    work1.reset();
  };
  boost::asio::spawn(service, success_cr);

  optional_work work2{service};
  auto failure_cr = [&] (boost::asio::yield_context yield) {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    boost::system::error_code ec;
    auto bl = librados::async_operate(io, "noexist", &op, 0, yield[ec]);
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    work2.reset();
  };
  boost::asio::spawn(service, failure_cr);

  service.run();
}

TEST(AsioRados, AsyncWriteOperationCallback)
{
  boost::asio::io_service service;

  bufferlist bl;
  bl.append("hello");

  optional_work work1{service};
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    auto success_cb = [&] (boost::system::error_code ec) {
      EXPECT_FALSE(ec);
      work1.reset();
    };
    librados::async_operate(io, "exist", &op, 0, success_cb);
  }
  optional_work work2{service};
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    auto failure_cb = [&] (boost::system::error_code ec) {
      EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
      work2.reset();
    };
    librados::async_operate(snapio, "exist", &op, 0, failure_cb);
  }
  service.run();
}

TEST(AsioRados, AsyncWriteOperationFuture)
{
  bufferlist bl;
  bl.append("hello");

  std::future<void> f1;
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    f1 = librados::async_operate(io, "exist", &op, 0,
                                 boost::asio::use_future);
  }
  std::future<void> f2;
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    f2 = librados::async_operate(snapio, "exist", &op, 0,
                                 boost::asio::use_future);
  }
  EXPECT_NO_THROW(f1.get());
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST(AsioRados, AsyncWriteOperationYield)
{
  boost::asio::io_service service;

  bufferlist bl;
  bl.append("hello");

  optional_work work1{service};
  auto success_cr = [&] (boost::asio::yield_context yield) {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    boost::system::error_code ec;
    librados::async_operate(io, "exist", &op, 0, yield[ec]);
    EXPECT_FALSE(ec);
    work1.reset();
  };
  boost::asio::spawn(service, success_cr);

  optional_work work2{service};
  auto failure_cr = [&] (boost::asio::yield_context yield) {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    boost::system::error_code ec;
    librados::async_operate(snapio, "exist", &op, 0, yield[ec]);
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    work2.reset();
  };
  boost::asio::spawn(service, failure_cr);

  service.run();
}

#include <boost/asio/yield.hpp>

TEST(AsioRados, AsyncReadCoroutine)
{
  class read_cr : public boost::asio::coroutine {
    librados::IoCtx &io;
    const std::string& oid;
    size_t len;
    uint64_t off;
    std::function<void(boost::system::error_code, bufferlist)> cb;
   public:
    read_cr(librados::IoCtx &io, const std::string& oid,
            size_t len, uint64_t off,
            std::function<void(boost::system::error_code, bufferlist)> cb)
      : io(io), oid(oid), len(len), off(off), cb(cb)
    {}
    void operator()(boost::system::error_code ec = boost::system::error_code(),
                    bufferlist bl = bufferlist()) {
      reenter(this) {
        yield librados::async_read(io, oid, len, off, *this);
        cb(ec, bl);
      }
    }
  };

  boost::asio::io_service service;
  optional_work work1{service};
  auto success_cb = [&] (boost::system::error_code ec, bufferlist bl) {
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
    work1.reset();
  };
  service.post(read_cr(io, "exist", 256, 0, success_cb));

  optional_work work2{service};
  auto failure_cb = [&] (boost::system::error_code ec, bufferlist bl) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    work2.reset();
  };
  service.post(read_cr(io, "noexist", 256, 0, failure_cb));

  service.run();
}

TEST(AsioRados, AsyncWriteCoroutine)
{
  class write_cr : public boost::asio::coroutine {
    librados::IoCtx &io;
    const std::string& oid;
    bufferlist bl;
    std::function<void(boost::system::error_code)> cb;
   public:
    write_cr(librados::IoCtx &io, const std::string& oid, bufferlist bl,
             std::function<void(boost::system::error_code)> cb)
      : io(io), oid(oid), bl(bl), cb(cb)
    {}
    void operator()(boost::system::error_code ec = boost::system::error_code()) {
      reenter(this) {
        yield librados::async_write(io, oid, bl, bl.length(), 0, *this);
        cb(ec);
      }
    }
  };

  boost::asio::io_service service;

  bufferlist bl;
  bl.append("hello");

  optional_work work1{service};
  auto success_cb = [&] (boost::system::error_code ec) {
    EXPECT_FALSE(ec);
    work1.reset();
  };
  service.post(write_cr(io, "exist", bl, success_cb));

  optional_work work2{service};
  auto failure_cb = [&] (boost::system::error_code ec) {
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    work2.reset();
  };
  service.post(write_cr(snapio, "exist", bl, failure_cb));

  service.run();
}

TEST(AsioRados, AsyncReadOperationCoroutine)
{
  class read_op_cr : public boost::asio::coroutine {
    librados::IoCtx &io;
    const std::string& oid;
    std::function<void(boost::system::error_code, bufferlist)> cb;
   public:
    read_op_cr(librados::IoCtx &io, const std::string& oid,
               std::function<void(boost::system::error_code, bufferlist)> cb)
      : io(io), oid(oid), cb(cb)
    {}
    void operator()(boost::system::error_code ec = boost::system::error_code(),
                    bufferlist bl = bufferlist()) {
      reenter(this) {
        yield {
          librados::ObjectReadOperation op;
          op.read(0, 0, nullptr, nullptr);
          librados::async_operate(io, oid, &op, 0, *this);
        }
        cb(ec, bl);
      }
    }
  };

  boost::asio::io_service service;
  optional_work work1{service};
  auto success_cb = [&] (boost::system::error_code ec, bufferlist bl) {
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
    work1.reset();
  };
  service.post(read_op_cr(io, "exist", success_cb));

  optional_work work2{service};
  auto failure_cb = [&] (boost::system::error_code ec, bufferlist bl) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    work2.reset();
  };
  service.post(read_op_cr(io, "noexist", failure_cb));

  service.run();
}

TEST(AsioRados, AsyncWriteOperationCoroutine)
{
  class write_op_cr : public boost::asio::coroutine {
    librados::IoCtx &io;
    const std::string& oid;
    bufferlist bl;
    std::function<void(boost::system::error_code)> cb;
   public:
    write_op_cr(librados::IoCtx &io, const std::string& oid, bufferlist bl,
                std::function<void(boost::system::error_code)> cb)
      : io(io), oid(oid), bl(bl), cb(cb)
    {}
    void operator()(boost::system::error_code ec = boost::system::error_code()) {
      reenter(this) {
        yield {
          librados::ObjectWriteOperation op;
          op.write_full(bl);
          librados::async_operate(io, oid, &op, 0, *this);
        }
        cb(ec);
      }
    }
  };

  boost::asio::io_service service;

  bufferlist bl;
  bl.append("hello");

  optional_work work1{service};
  auto success_cb = [&] (boost::system::error_code ec) {
    EXPECT_FALSE(ec);
    work1.reset();
  };
  service.post(write_op_cr(io, "exist", bl, success_cb));

  optional_work work2{service};
  auto failure_cb = [&] (boost::system::error_code ec) {
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    work2.reset();
  };
  service.post(write_op_cr(snapio, "exist", bl, failure_cb));

  service.run();
}

#include <boost/asio/unyield.hpp>

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  ldout(cct.get(), 0) << "connecting..." << dendl;
  int r = rados.init_with_context(cct.get());
  if (r < 0) {
    lderr(cct.get()) << "Rados::init_with_context() failed with "
        << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }
  r = rados.connect();
  if (r < 0) {
    lderr(cct.get()) << "Rados::connect() failed with "
        << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }
  // open/create test pool
  constexpr auto pool = "unittest_librados_asio";
  r = rados.ioctx_create(pool, io);
  if (r == -ENOENT) {
    ldout(cct.get(), 0) << "creating pool \"unittest_librados_asio\"" << dendl;
    r = rados.pool_create(pool);
    if (r == -EEXIST) {
      r = 0;
    } else if (r < 0) {
      lderr(cct.get()) << "Rados::pool_create() failed with "
          << cpp_strerror(r) << dendl;
      return EXIT_FAILURE;
    }
    r = rados.ioctx_create(pool, io);
  }
  if (r < 0) {
    lderr(cct.get()) << "Rados::ioctx_create() failed with "
        << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }
  r = rados.ioctx_create(pool, snapio);
  if (r < 0) {
    lderr(cct.get()) << "Rados::ioctx_create() failed with "
        << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }
  snapio.snap_set_read(1);
  // initialize object
  bufferlist bl;
  bl.append("hello");
  r = io.write_full("exist", bl);
  if (r < 0) {
    lderr(cct.get()) << "IoCtx::write_full(\"exist\", \"hello\") failed with "
        << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
