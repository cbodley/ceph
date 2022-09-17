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

#include <h3/h3.h>
#include "config.h"
#include "listener.h"

extern "C" {

/// Create a Listener on the given udp socket.
auto create_h3_listener(rgw::h3::Config& config,
                        rgw::h3::Observer& observer,
                        rgw::h3::Listener::executor_type ex,
                        rgw::h3::StreamHandler& on_new_stream,
                        rgw::h3::udp_socket socket)
    -> std::unique_ptr<rgw::h3::Listener>
{
  auto& cfg = static_cast<rgw::h3::ConfigImpl&>(config);
  return std::make_unique<rgw::h3::ListenerImpl>(
      observer, ex, cfg, std::move(socket), on_new_stream);
}

} // extern "C"
