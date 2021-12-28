#include <algorithm>
#include <assert.h>
#include <atomic>
#include <ctype.h>
#include <random>

#include "zipf.hpp"

inline uint64_t fast_random_64() {
  static std::mt19937_64 generator;
  thread_local uint64_t seed = 0;
  if (seed == 0) {
    seed = generator();
  }
  uint64_t x = seed; /* The state must be seeded with a nonzero value. */
  x ^= x >> 12;      // a
  x ^= x << 25;      // b
  x ^= x >> 27;      // c
  seed = x;
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

class RandomGenerator : public Generator {
public:
  RandomGenerator(uint64_t max) : max_(max) {}

  uint64_t Next() override { return fast_random_64() % max_ + 1; }

private:
  uint64_t max_;
};

class ZipfianGenerator : public Generator {
public:
  ZipfianGenerator(uint64_t max) : ZipfianGenerator(max, kZipfianConst) {}

  uint64_t Next() override {
    //    assert(num >= 2 && num < kMaxNumItems);
    if (max_ > n_for_zeta_) { // Recompute zeta_n and eta
      RaiseZeta(max_);
      eta_ = Eta();
    }

    double u = fast_random_double();
    double uz = u * zeta_n_;

    if (uz < 1.0) {
      last_value_ = 0;
    } else if (uz < 1.0 + std::pow(0.5, theta_)) {
      last_value_ = 1;
    } else {
      last_value_ = base_ + max_ * std::pow(eta_ * u - eta_ + 1, alpha_);
    }

    return std::max(base_, last_value_);
  }

private:
  extstd::zipfian_distribution<std::uint64_t> const zipf_dist;
};