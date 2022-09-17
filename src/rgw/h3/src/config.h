// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <memory>

#include <h3/h3.h>

struct quiche_config;
struct quiche_h3_config;

namespace rgw::h3 {

struct config_deleter { void operator()(quiche_config* config); };
using config_ptr = std::unique_ptr<quiche_config, config_deleter>;

struct h3_config_deleter { void operator()(quiche_h3_config* h3); };
using h3_config_ptr = std::unique_ptr<quiche_h3_config, h3_config_deleter>;


class ConfigImpl : public Config {
  config_ptr config;
  h3_config_ptr h3_config;
 public:
  explicit ConfigImpl(config_ptr config, h3_config_ptr h3_config) noexcept
    : config(std::move(config)), h3_config(std::move(h3_config))
  {}

  quiche_config* get_config() const { return config.get(); }
  quiche_h3_config* get_h3_config() const { return h3_config.get(); }
};

} // namespace rgw::h3
