#pragma once

#include <iomanip>
#include <stdexcept>
#include <string>

#include "kvdk/types.hpp"

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

namespace KVDK_NAMESPACE {
inline Status ExceptionToStatus(std::exception const& ex) {
  if (dynamic_cast<std::bad_alloc const*>(&ex)) return Status::MemoryOverflow;
  if (dynamic_cast<std::out_of_range const*>(&ex)) return Status::OutOfRange;
  if (dynamic_cast<std::invalid_argument const*>(&ex))
    return Status::InvalidArgument;

  return Status::Abort;
}

#define KVDK_TRY try

#define KVDK_HANDLE_EXCEPTIONS       \
  catch (std::exception const& ex) { \
    return ExceptionToStatus(ex);    \
  }                                  \
  catch (...) {                      \
    return Status::Abort;            \
  }

}  // namespace KVDK_NAMESPACE
