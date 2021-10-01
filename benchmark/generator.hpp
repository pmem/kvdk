#include <algorithm>
#include <assert.h>
#include <atomic>
#include <ctype.h>
#include <random>

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
  constexpr static const double kZipfianConst = 0.99;
  //  static const uint64_t kMaxNumItems = (UINT64_MAX);

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
  ZipfianGenerator(uint64_t max, double zipfian_const)
      : max_(max), base_(1), theta_(zipfian_const), zeta_n_(0), n_for_zeta_(0) {
    //    assert(max_ >= 2 && max_ < kMaxNumItems);
    zeta_2_ = Zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    RaiseZeta(max_);
    eta_ = Eta();

    Next();
  }

  void RaiseZeta(uint64_t num) {
    assert(num >= n_for_zeta_);
    zeta_n_ = Zeta(n_for_zeta_, num, theta_, zeta_n_);
    n_for_zeta_ = num;
  }

  double Eta() {
    return (1 - std::pow(2.0 / max_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_);
  }

  static double Zeta(uint64_t last_num, uint64_t cur_num, double theta,
                     double last_zeta) {
    double zeta = last_zeta;
    for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
      zeta += 1 / std::pow(i, theta);
    }
    return zeta;
  }

  static double Zeta(uint64_t num, double theta) {
    return Zeta(0, num, theta, 0);
  }

  uint64_t max_;
  uint64_t base_; /// Min number of items to generate

  // Computed parameters for generating the distribution
  double theta_, zeta_n_, eta_, alpha_, zeta_2_;
  uint64_t n_for_zeta_; /// Number of items used to compute zeta_n
  uint64_t last_value_;
};