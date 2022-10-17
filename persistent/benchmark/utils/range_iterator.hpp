#pragma once

#include <cassert>
#include <cstdint>
#include <type_traits>

namespace extd {
template <typename IntType,
          typename std::enable_if<std::is_integral<IntType>::value,
                                  bool>::type = true>
// Yield Number in range [lower, upper), by step.
class range_iterator {
 public:
  range_iterator(IntType lo, IntType hi, IntType s = 1)
      : lower{lo}, upper{hi}, step{s}, curr{lower} {}

  IntType operator()() {
    IntType old = curr;
    curr += step;
    assert(old < upper);
    return old;
  }

 private:
  IntType const lower;
  IntType const upper;
  IntType const step;
  IntType curr;
};

}  // namespace extd
