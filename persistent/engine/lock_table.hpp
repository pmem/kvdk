#pragma once

#include <mutex>
#include <vector>

#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

// This class manages a table of locks, which indexed by hash value. It is able
// to lock multiple locks in the table by sequence to avoid dead lock
class LockTable {
 public:
  using HashValueType = std::uint64_t;
  using MutexType = SpinMutex;
  using ULockType = std::unique_lock<MutexType>;
  using MultiGuardType = std::vector<ULockType>;

  LockTable(size_t n) : mutexes_{n} {}

  std::unique_lock<MutexType> AcquireLock(HashValueType hash) {
    return std::unique_lock<MutexType>(*Mutex(hash));
  }

  void Lock(HashValueType hash) { Mutex(hash)->lock(); }

  void Unlock(HashValueType hash) { Mutex(hash)->unlock(); }

  void MultiLock(std::vector<HashValueType> const& hashes) {
    auto sorted = rearrange(hashes);
    for (HashValueType hash : sorted) {
      Lock(hash);
    }
  }

  void MultiLock(std::initializer_list<HashValueType> hashes) {
    MultiLock(std::vector<HashValueType>{hashes});
  }

  void MultiUnlock(std::vector<HashValueType> const& hashes) {
    auto sorted = rearrange(hashes);
    for (HashValueType hash : sorted) {
      Unlock(hash);
    }
  }

  void MultiUnlock(std::initializer_list<HashValueType> hashes) {
    MultiUnlock(std::vector<HashValueType>{hashes});
  }

  MultiGuardType MultiGuard(std::vector<HashValueType> const& hashes) {
    auto sorted = rearrange(hashes);
    MultiGuardType guard;
    for (HashValueType hash : sorted) {
      guard.emplace_back(*Mutex(hash));
    }
    return guard;
  }

  MultiGuardType MultiGuard(std::initializer_list<HashValueType> hashes) {
    return MultiGuard(std::vector<HashValueType>{hashes});
  }

  MutexType* Mutex(HashValueType hash) {
    return &mutexes_[hash % mutexes_.size()];
  }

 private:
  std::vector<HashValueType> rearrange(
      std::vector<HashValueType> const& hashes) {
    std::vector<HashValueType> ret{hashes};
    size_t N = mutexes_.size();
    for (auto& hash : ret) {
      hash %= N;
    }
    std::sort(ret.begin(), ret.end());
    size_t new_sz = std::unique(ret.begin(), ret.end()) - ret.begin();
    ret.resize(new_sz);
    return ret;
  }

  std::vector<MutexType> mutexes_;
};

}  // namespace KVDK_NAMESPACE
