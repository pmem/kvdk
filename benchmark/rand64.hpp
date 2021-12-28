#include <cassert>

#include <limits>
#include <random>
#include <stdexcept>

#include <immintrin.h>
#include <x86intrin.h>

namespace extstd {
class fast_u64_random_engine {
public:
  using result_type = std::uint64_t;

  inline explicit fast_u64_random_engine() { reseed(); }

  inline explicit fast_u64_random_engine(std::uint64_t seed) : s{seed} {
    assert(s != 0);
  }

  inline result_type operator()() {
    update_state();
    return s * magic;
  }

  constexpr static result_type min() {
    return std::numeric_limits<result_type>::min();
  }

  constexpr static result_type max() {
    return std::numeric_limits<result_type>::max();
  }

private:
  result_type s;
  char padding[64];

  static constexpr result_type magic = 0x2545F4914F6CDD1D;

  void reseed() {
    size_t failures = 0;
    unsigned long long sink;
    while (_rdseed64_step(&sink) != 1 || sink == 0) {
      ++failures;
      if (failures > 10000)
        throw std::runtime_error{"Fail to seed the engine"};
    }
    s = sink;
  }

  void update_state() {
    if (s == 0) {
      reseed();
    }
    s ^= (s >> 12);
    s ^= (s << 25);
    s ^= (s >> 27);
  }
};

} // namespace extstd
