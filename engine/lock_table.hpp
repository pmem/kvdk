#pragma once

#include <mutex>
#include <vector>

#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

class LockTable {
 public:
  using HashType = std::uint64_t;
  using MutexType = SpinMutex;
  using ULockType = std::unique_lock<MutexType>;
  using GuardType = std::vector<ULockType>;

 private:
  std::vector<MutexType> mutexes;

 public:
  LockTable(size_t n) : mutexes{n} {}

  std::unique_lock<MutexType> AcquireLock(HashType hash) {
    return std::unique_lock<MutexType>(*Mutex(hash));
  }

  void Lock(HashType hash) { Mutex(hash)->lock(); }

  void Unlock(HashType hash) { Mutex(hash)->unlock(); }

  void MultiLock(std::vector<HashType> const& hashes) {
    auto sorted = rearrange(hashes);
    for (HashType hash : sorted) {
      Lock(hash);
    }
  }

  void MultiLock(std::initializer_list<HashType> hashes) {
    MultiLock(std::vector<HashType>{hashes});
  }

  void MultiUnlock(std::vector<HashType> const& hashes) {
    auto sorted = rearrange(hashes);
    for (HashType hash : sorted) {
      Unlock(hash);
    }
  }

  void MultiUnlock(std::initializer_list<HashType> hashes) {
    MultiUnlock(std::vector<HashType>{hashes});
  }

  GuardType MultiGuard(std::vector<HashType> const& hashes) {
    auto sorted = rearrange(hashes);
    GuardType guard;
    for (HashType hash : sorted) {
      guard.emplace_back(*Mutex(hash));
    }
    return guard;
  }

  GuardType MultiGuard(std::initializer_list<HashType> hashes) {
    return MultiGuard(std::vector<HashType>{hashes});
  }

  MutexType* Mutex(HashType hash) { return &mutexes[hash % mutexes.size()]; }

 private:
  std::vector<HashType> rearrange(std::vector<HashType> const& hashes) {
    std::vector<HashType> ret{hashes};
    size_t N = mutexes.size();
    for (auto& hash : ret) {
      hash %= N;
    }
    std::sort(ret.begin(), ret.end());
    size_t new_sz = std::unique(ret.begin(), ret.end()) - ret.begin();
    ret.resize(new_sz);
    return ret;
  }
};

}  // namespace KVDK_NAMESPACE
