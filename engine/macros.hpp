#pragma once

#define to_hex(x)                                                              \
  std::hex << std::setfill('0') << std::setw(sizeof(decltype(x)) * 2) << x     \
           << std::dec

#define kvdk_assert(cond, msg)                                                 \
  {                                                                            \
    assert((cond) && msg);                                                     \
    if (!(cond))                                                               \
      throw std::runtime_error{msg};                                           \
  }
