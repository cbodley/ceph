// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <benchmark/benchmark.h>
#include "include/str_list.h"
#include "include/random.h"


// generate an input string with 'count' tokens, each with a variable length
static std::string generate_input(size_t count, size_t min_len, size_t max_len)
{
  std::string input;
  for (size_t i = 0; i < count; i++) {
    if (i > 0) input.append(1, ' ');
    const auto len = ceph::util::generate_random_number(min_len, max_len);
    input.append(len, 'A');
  }
  return input;
}

// generate tokens small enough for small string optimization
static const std::string small_strings = generate_input(10, 4, 12);

// generate tokens too big for small string optimization
static const std::string large_strings = generate_input(10, 36, 48);


static void BenchStrList(benchmark::State& state, const std::string& input)
{
  std::list<std::string> tokens;
  for (auto _ : state) {
    get_str_list(input, tokens);
  }
}

static void BenchStrVec(benchmark::State& state, const std::string& input)
{
  std::vector<std::string> tokens;
  for (auto _ : state) {
    get_str_vec(input, tokens);
  }
}

BENCHMARK_CAPTURE(BenchStrList, SmallStrList, small_strings);
BENCHMARK_CAPTURE(BenchStrList, LargeStrList, large_strings);

BENCHMARK_CAPTURE(BenchStrVec, SmallStrVec, small_strings);
BENCHMARK_CAPTURE(BenchStrVec, LargeStrVec, large_strings);

BENCHMARK_MAIN();
