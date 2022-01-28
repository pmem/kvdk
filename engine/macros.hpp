#pragma once

#define to_hex(x)                                                              \
  std::hex << std::setfill('0') << std::setw(sizeof(decltype(x)) * 2) << x     \
           << std::dec

#if DEBUG_LEVEL > 0
#define kvdk_assert(cond, msg)                                                 \
  {                                                                            \
    if (!(cond))                                                               \
      throw std::runtime_error{msg};                                           \
  }
#else
#define kvdk_assert(cond, msg)                                                 \
  {}
#endif