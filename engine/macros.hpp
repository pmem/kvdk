#pragma once

#include <iomanip>
#include <stdexcept>
#include <string>

#define to_hex(x)                                                          \
  std::hex << std::setfill('0') << std::setw(sizeof(decltype(x)) * 2) << x \
           << std::dec

#if KVDK_DEBUG_LEVEL > 0
#define kvdk_assert(cond, msg)                                           \
  {                                                                      \
    if (!(cond)) {                                                       \
      throw std::runtime_error{__FILE__ ":" + std::to_string(__LINE__) + \
                               ":\t" + std::string{msg}};                \
    }                                                                    \
  }
#else
#define kvdk_assert(cond, msg) \
  {                            \
    (void)(cond);              \
    (void)(msg);               \
  }
#endif
