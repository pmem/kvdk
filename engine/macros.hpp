#pragma once

#include <iomanip>
#include <stdexcept>
#include <string>

#define to_hex(x)                                                          \
  std::hex << std::setfill('0') << std::setw(sizeof(decltype(x)) * 2) << x \
           << std::dec

#ifndef KVDK_DEBUG_LEVEL
#pragma GCC warning "KVDK_DEBUG_LEVEL not defined, defaulted to 0"
#define KVDK_DEBUG_LEVEL 0
#endif

#define kvdk_assert(cond, msg)                                           \
  {                                                                      \
    if (KVDK_DEBUG_LEVEL > 0 && !(cond)) {                               \
      throw std::runtime_error{__FILE__ ":" + std::to_string(__LINE__) + \
                               ":\t" + std::string{msg}};                \
    }                                                                    \
  }
