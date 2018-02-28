// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) Red Hat, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>

#include <atomic>
#include <thread>
#include <vector>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/ceph_time.h"
#include "common/Throttle.h"

static int usage(const char *name)
{
  std::cerr << "Usage: " << name << " [options]\n\n"
      "Options:\n"
      "  --throttle <count>  maximum concurrent operations [default: 8]\n"
      "  --threads <count>   number of work threads [default: 16]\n"
      "  --delay <msec>      time in milliseconds to hold the throttle [default: 10]\n"
      "  --duration <sec>    total run time in seconds [default: 10]\n"
      << std::flush;
  return EXIT_FAILURE;
}

int main(int argc, char **argv)
{
  std::vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  size_t throttle_count = 8;
  ceph::timespan delay = std::chrono::milliseconds{10};
  ceph::timespan duration = std::chrono::seconds{10};
  size_t thread_count = 16;

  std::string val;
  for (auto i = args.begin(); i != args.end();) {
    if (ceph_argparse_flag(args, i, "-h", "--help", nullptr)) {
      return usage(argv[0]);
    } else if (ceph_argparse_witharg(args, i, &val, "--throttle", nullptr)) {
      try {
        throttle_count = std::stoull(val);
        if (throttle_count == 0)
          throw std::runtime_error("must be greater than zero");
      } catch (const std::exception& e) {
        std::cerr << "Invalid argument --throttle=" << val << ": " << e.what() << std::endl;
        return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--threads", nullptr)) {
      try {
        thread_count = std::stoull(val);
        if (thread_count == 0)
          throw std::runtime_error("must be greater than zero");
      } catch (const std::exception& e) {
        std::cerr << "Invalid argument --threads=" << val << ": " << e.what() << std::endl;
        return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--delay", nullptr)) {
      try {
        auto msec = std::stoll(val);
        if (msec <= 0)
          throw std::runtime_error("must be greater than zero");
        delay = std::chrono::milliseconds{msec};
      } catch (const std::exception& e) {
        std::cerr << "Invalid argument --delay=" << val << ": " << e.what() << std::endl;
        return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--duration", nullptr)) {
      try {
        auto sec = std::stoll(val);
        if (sec <= 0)
          throw std::runtime_error("must be greater than zero");
        duration = std::chrono::seconds{sec};
      } catch (const std::exception& e) {
        std::cerr << "Invalid argument --duration=" << val << ": " << e.what() << std::endl;
        return EXIT_FAILURE;
      }
    } else {
      ++i;
    }
  }

  Throttle throttle(g_ceph_context, "bench_throttle", throttle_count, false);
  std::atomic<bool> done{false};

  auto thread_worker = [&throttle, &done, delay] {
      while (!done) {
        throttle.get();
        std::this_thread::sleep_for(delay);
        throttle.put();
      }
    };

  std::vector<std::thread> threads;
  threads.reserve(thread_count);

  for (size_t i = 0; i < thread_count; i++) {
    threads.emplace_back(thread_worker);
  }

  std::cout << "Started with throttle=" << throttle_count
      << " threads=" << thread_count << " delay=" << delay
      << " duration=" << duration << std::endl;

  std::this_thread::sleep_for(duration);
  done = true;

  for (auto& thread : threads) {
    thread.join();
  }

  std::cout << "Done" << std::endl;
  return 0;
}
