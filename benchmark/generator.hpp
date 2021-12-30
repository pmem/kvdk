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

#include "extd/rand64.hpp"
#include "extd/zipf.hpp"

inline uint64_t fast_random_64() {
  thread_local unsigned long long state = 0;
  static std::atomic_uint64_t seed_gen{1};
  if (state == 0) {
    state = seed_gen.fetch_add(1);
  }
  uint64_t x = state; /* The state must be seeded with a nonzero value. */
  x ^= x >> 12;       // a
  x ^= x << 25;       // b
  x ^= x >> 27;       // c
  state = x;
  return x * 0x2545F4914F6CDD1D;
}

inline double fast_random_double() {
  return (double)fast_random_64() / UINT64_MAX;
}

class Generator {
public:
  virtual uint64_t Next() = 0;
};

class ConstantGenerator : public Generator {
public:
  ConstantGenerator(uint64_t num) : num_(num) {}

  uint64_t Next() override { return num_; }

private:
  uint64_t num_;
};

class UniformGenerator : public Generator {
public:
  UniformGenerator(uint64_t max_num, int scale = 64)
      : base_(max_num / scale), scale_(scale), gen_cnt_(0) {
    for (uint64_t i = 0; i < base_.size(); i++) {
      base_[i] = i + 1;
    }
    std::shuffle(base_.begin(), base_.end(), std::mt19937_64());
  }

  virtual uint64_t Next() override {
    auto next = gen_cnt_.fetch_add(1, std::memory_order_relaxed);
    auto index = next % base_.size();
    auto cur_scale = (next / base_.size()) % scale_;
    return base_[index] + base_.size() * cur_scale;
  }

private:
  std::vector<uint64_t> base_;
  uint64_t scale_;
  std::atomic<uint64_t> gen_cnt_;
};

class RangeIterator {
public:
  // Yield Number in range [lower, upper), by step.
  RangeIterator(std::uint64_t lower, std::uint64_t upper,
                std::uint64_t step = 1)
      : lower_{lower}, upper_{upper}, step_{step}, curr_{lower_} {}

  uint64_t Yield() {
    std::uint64_t old = curr_;
    curr_ += step_;
    if (curr_ < upper_) {
      return old;
    } else {
      // Sleep 100ms, slow down generation
      std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(100));
      return curr_;
    }
  }

private:
  std::uint64_t lower_;
  std::uint64_t upper_;
  std::uint64_t step_;
  std::uint64_t curr_;
  char padding[64];
};

class MultiThreadingRangeIterator : public Generator {
public:
  MultiThreadingRangeIterator(size_t n_thread, std::uint64_t lower,
                              std::uint64_t upper, std::uint64_t step = 1)
      : ranges_{}, id_pool_{0} {
    size_t n = (upper - lower) / n_thread;
    for (size_t i = 0; i < n_thread; i++) {
      size_t lo = lower + i * n;
      size_t hi = (i < n_thread - 1) ? lo + n : upper;
      ranges_.emplace_back(RangeIterator{lo, hi, step});
    }
  }

  virtual std::uint64_t Next() override {
    thread_local std::uint64_t id = id_pool_.fetch_add(1);
    assert(id < ranges_.size());
    return ranges_[id].Yield();
  }

private:
  std::vector<RangeIterator> ranges_;
  std::atomic_uint64_t id_pool_;
};

class RandomGenerator : public Generator {
public:
  RandomGenerator(uint64_t max) : max_(max) {}

  uint64_t Next() override { return fast_random_64() % max_ + 1; }

private:
  uint64_t max_;
};

class ZipfianGenerator : public Generator {
public:
  ZipfianGenerator(size_t n_thread, uint64_t max, double s = 0.99)
      : zipf_dist{max, s}, engines(n_thread), id_pool_{0} {}

  uint64_t Next() override {
    thread_local std::uint64_t id = id_pool_.fetch_add(1);
    return zipf_dist(engines[id].engine);
  }

private:
  extd::zipfian_distribution<std::uint64_t> const zipf_dist;
  struct PaddedEngine
  {
    extd::xorshift_engine engine;
    char padding[64];
  };

  std::vector<PaddedEngine> engines;
  std::atomic_uint64_t id_pool_;
};