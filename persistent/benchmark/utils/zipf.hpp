#pragma once

#include <cassert>
#include <random>

namespace extd {

// Param: s, N
// P(X=k) = k^{-s}/\sum_{i=1}^N i^{-s} = k^{-s}/zeta(N;s)
template <typename IntType = int>
class zipfian_distribution {
 public:
  using result_type = IntType;
  using float_type = double;

  static constexpr float_type default_s = 0.99;
  static constexpr size_t precalc_n = 8;
  static constexpr float_type half = 0.5;

 private:
  float_type zeta_k[precalc_n]{};

  result_type const N;
  float_type const s;

  float_type const r = 1 - s;
  float_type const inv_r = 1 / r;
  float_type const a = precalc_n + 0.5;
  float_type const a_exp_r = std::pow(a, r);
  float_type const zeta_n{zeta(N)};  // Last to initialize!

 public:
  inline zipfian_distribution(result_type n, float_type e = default_s)
      : N{n}, s{e} {
    for (size_t i = 0; i < precalc_n; i++) {
      zeta_k[i] = zeta(i + 1);
    }
  }

  template <typename UniformBitRandomGenerator>
  inline result_type operator()(UniformBitRandomGenerator& g) const {
    using GeneratorIntType = typename UniformBitRandomGenerator::result_type;
    static constexpr GeneratorIntType min = UniformBitRandomGenerator::min();
    static constexpr GeneratorIntType max = UniformBitRandomGenerator::max();
    GeneratorIntType r = g();
    float_type sum = (r - min) * zeta_n / (max - min);
    assert(0 <= sum && sum <= zeta_n);
    return inv_zeta(sum);
  }

  inline float_type probability(result_type k) const {
    if (k < 1 || k > N) {
      return 0.0;
    }
    return std::pow(k, -s) / zeta_n;
  }

 private:
  // zeta(k) = sum_{i=1}^{k} i^{-s}
  inline constexpr float_type zeta(result_type k) const {
    constexpr result_type acc_n = 100;
    if (k <= acc_n) {
      float_type sum = 0;
      for (result_type i = 1; i <= k; i++) {
        sum += std::pow(i, -s);
      }
      return sum;
    } else {
      float_type a = acc_n + half;
      float_type b = k + half;
      if (s != 1.0) {
        return zeta(acc_n) + (std::pow(b, r) - std::pow(a, r)) / r;
      } else {
        return zeta(acc_n) + (std::log(b) - std::log(a));
      }
    }
  }

  // (k = inv_zeta(sum)) <=> (zeta(k) <= sum < zeta(k+1))
  inline result_type inv_zeta(float_type sum) const {
    for (size_t i = 0; i < precalc_n; i++) {
      if (sum <= zeta_k[i]) {
        return i + 1;
      }
    }
    // integral == \int_a^b zeta(x)dx
    float_type integral = sum - zeta_k[precalc_n - 1];
    assert(integral > 0);

    if (s != 1.0) {
      float_type b = std::pow(integral * r + a_exp_r, inv_r);
      assert(a < b && b <= N + half);
      return std::floor(b + half);
    } else {
      float_type b = a * std::exp(integral);
      assert(a < b && b <= N + half);
      return std::floor(b + half);
    }
  }
};

}  // namespace extd
