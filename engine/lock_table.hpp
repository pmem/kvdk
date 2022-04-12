#pragma once

#include <mutex>
#include <vector>

namespace KVDK_NAMESPACE {

class LockTable {
  using HashType = std::uint64_t;
  using MutexType = std::recursive_mutex;
  using ULockType = std::unique_lock<MutexType>;
  std::vector<MutexType> mutexes;

 public:
  LockTable(size_t n) : mutexes{n} {}

  void Lock(HashType hash) { Mutex(hash)->lock(); }

  void Unlock(HashType hash) { Mutex(hash)->unlock(); }

  void MultiLock(std::vector<HashType> const& hashes) {
    std::vector<HashType> sorted{hashes};
    sort(sorted);
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

  std::vector<ULockType> MultiGuard(std::vector<HashType> const& hashes) {
    std::vector<HashType> sorted{hashes};
    sort(sorted);
    std::vector<ULockType> guard;
    for (HashType hash : sorted) {
      guard.emplace_back(*Mutex(hash));
    }
    return guard;
  }

  void MultiGuard(std::initializer_list<HashType> hashes) {
    MultiGuard(std::vector<HashType>{hashes});
  }

  MutexType* Mutex(HashType hash) { return &mutexes[hash % mutexes.size()]; }

 private:
  void sort(std::vector<HashType>& hashes) {
    size_t N = mutexes.size();
    std::sort(hashes.begin(), hashes.end(),
              [&](HashType lhs, HashType rhs) { return (lhs % N < rhs % N); });
  }
};

}  // namespace KVDK_NAMESPACE
