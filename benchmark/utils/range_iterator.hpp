#ifndef EXTD_RANGE_ITERATOR_HPP
#define EXTD_RANGE_ITERATOR_HPP

#include <cstdint>
#include <cassert>

#include <type_traits>

namespace extd
{
template<typename IntType, typename std::enable_if<std::is_integral<IntType>::value, bool>::type = true>
// Yield Number in range [lower, upper), by step.
class range_iterator {
public:
  range_iterator(IntType lo, IntType hi, IntType s = 1)
      : lower{lo}, upper{hi}, step{s}, curr{s} {}

  IntType operator()() 
  {
    IntType old = curr;
    curr += step;
    assert(curr < upper);
    return old;    
  }

private:
  IntType lower;
  IntType upper;
  IntType step;
  IntType curr;
};

}

#endif // EXTD_RANGE_ITERATOR_HPP