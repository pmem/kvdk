#pragma once

#include <mutex>
#include <vector>

namespace KVDK_NAMESPACE {

class LockTable {
  using HashType = std::uint64_t;
  using MutexType = std::recursive_mutex;
  std::vector<MutexType> mutexes;

 public:
  LockTable(size_t n) : mutexes{n} {}

  void Lock(HashType hash) { Mutex(hash)->lock(); }

  bool TryLock(HashType hash) { return Mutex(hash)->try_lock(); }

  void Unlock(HashType hash) { Mutex(hash)->unlock(); }

  void MultiLock(std::vector<HashType> const& hashes) {
    std::vector<HashType> sorted{hashes};
    std::sort(sorted.begin(), sorted.end());
    for (HashType hash : sorted) {
      Lock(hash);
    }
  }

  void MultiLock(std::initializer_list<HashType> hashes) {
    MultiLock(std::vector<HashType>{hashes});
  }

  void MultiUnlock(std::vector<HashType> const& hashes) {
    for (HashType hash : hashes) {
      Unlock(hash);
    }
  }

  void MultiUnlock(std::initializer_list<HashType> hashes) {
    MultiUnlock(std::vector<HashType>{hashes});
  }

  MutexType* Mutex(HashType hash) { return &mutexes[hash % mutexes.size()]; }
};

}  // namespace KVDK_NAMESPACE
