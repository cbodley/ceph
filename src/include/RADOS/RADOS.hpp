// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <cstddef>
#include <memory>
#include <tuple>
#include <string_view>
#include <type_traits>

#include <boost/asio.hpp>

class CephContext;

namespace RADOS
{
class RADOS
{
public:
  static constexpr std::tuple<uint32_t, uint32_t, uint32_t> version() {
    return {0, 0, 1};
  }

  RADOS(boost::asio::io_context& ioctx);
  RADOS(boost::asio::io_context& ioctx, std::string_view id);
  RADOS(boost::asio::io_context& ioctx, std::string_view name, std::string_view cluster);
  RADOS(boost::asio::io_context& ioctx, CephContext* cct);

  CephContext* cct();

private:
  static constexpr std::size_t impl_size = 512 * 8;
  std::aligned_storage_t<impl_size> impl;
};
}
