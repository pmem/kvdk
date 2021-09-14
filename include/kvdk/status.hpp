#include "namespace.hpp"

namespace KVDK_NAMESPACE {

enum class Status : unsigned char {
  Ok = 1,
  NotFound,
  MemoryOverflow,
  PmemOverflow,
  NotSupported,
  MapError,
  BatchOverflow,
  TooManyWriteThreads,
  InvalidDataSize,
  IOError,
  InvalidConfiguration,
};

}