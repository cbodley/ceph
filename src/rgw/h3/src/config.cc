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

#include <quiche.h>

#include <h3/h3.h>
#include "config.h"
#include "message.h"

namespace rgw::h3 {

void config_deleter::operator()(quiche_config* config)
{
  ::quiche_config_free(config);
}

void h3_config_deleter::operator()(quiche_h3_config* h3)
{
  ::quiche_h3_config_free(h3);
}

void configure(const Options& o,
               quiche_config* config,
               quiche_h3_config* h3config)
{
  static constexpr std::string_view alpn = "\x02h3";
  ::quiche_config_set_application_protos(config,
      (uint8_t *) alpn.data(), alpn.size());

  if (o.log_callback) {
    ::quiche_enable_debug_logging(o.log_callback, o.log_arg);
  }

  ::quiche_config_set_max_idle_timeout(config, o.conn_idle_timeout.count());
  ::quiche_config_set_max_recv_udp_payload_size(config, max_datagram_size);
  ::quiche_config_set_max_send_udp_payload_size(config, max_datagram_size);
  ::quiche_config_set_initial_max_data(config, o.conn_max_data);
  ::quiche_config_set_initial_max_stream_data_bidi_local(
      config, o.stream_max_data_bidi_local);
  ::quiche_config_set_initial_max_stream_data_bidi_remote(
      config, o.stream_max_data_bidi_remote);
  ::quiche_config_set_initial_max_stream_data_uni(
      config, o.stream_max_data_uni);
  ::quiche_config_set_initial_max_streams_bidi(config, o.conn_max_streams_bidi);
  ::quiche_config_set_initial_max_streams_uni(config, o.conn_max_streams_uni);
  ::quiche_config_set_disable_active_migration(config, true);

  // ssl configuration
  int r = ::quiche_config_load_cert_chain_from_pem_file(
      config, o.ssl_certificate_path);
  if (r < 0) {
    throw std::runtime_error("failed to load ssl cert chain");
  }
  r = ::quiche_config_load_priv_key_from_pem_file(
      config, o.ssl_private_key_path);
  if (r < 0) {
    throw std::runtime_error("failed to load private key");
  }
}

} // namespace rgw::h3

extern "C" {

auto create_h3_config(const rgw::h3::Options& options)
    -> std::unique_ptr<rgw::h3::Config>
{
  rgw::h3::config_ptr config{::quiche_config_new(QUICHE_PROTOCOL_VERSION)};
  rgw::h3::h3_config_ptr h3config{::quiche_h3_config_new()};

  rgw::h3::configure(options, config.get(), h3config.get());
  return std::make_unique<rgw::h3::ConfigImpl>(
      std::move(config), std::move(h3config));
}

} // extern "C"
