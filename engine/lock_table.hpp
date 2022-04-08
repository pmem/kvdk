#pragma once

#include <mutex>
#include <vector>

namespace KVDK_NAMESPACE {

class LockTable {
  using HashType = std::uint64_t;
  std::vector<std::recursive_mutex> mutexes;

 public:
  LockTable(size_t n) : mutexes{n} {}

  void Lock(HashType hash) { mutexes[hash % mutexes.size()].lock(); }

  bool TryLock(HashType hash) {
    return mutexes[hash % mutexes.size()].try_lock();
  }

  void Unlock(HashType hash) { mutexes[hash % mutexes.size()].unlock(); }
};

}  // namespace KVDK_NAMESPACE
