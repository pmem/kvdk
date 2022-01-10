#include <cassert>
#include <cctype>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>

#include <immintrin.h>
#include <x86intrin.h>

#include "utils/rand64.hpp"
#include "utils/zipf.hpp"
#include "utils/range_iterator.hpp"

struct PaddedEngine
{
  extd::xorshift_engine gen;
  char padding[64];
};

struct PaddedRangeIterators
{
  extd::range_iterator<std::uint64_t> gen;
  PaddedRangeIterators(std::uint64_t lo, std::uint64_t hi) : gen{lo, hi}{}
  char padding[64];
};
