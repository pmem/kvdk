/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include "../alias.hpp"
#include "../thread_manager.hpp"
#include "../utils/utils.hpp"
#include "kvdk/configs.hpp"

namespace KVDK_NAMESPACE {
constexpr TimeStampType kMaxTimestamp = UINT64_MAX;

struct SnapshotImpl : public Snapshot {
  explicit SnapshotImpl(const TimeStampType& t) : timestamp(t) {}

  SnapshotImpl() = default;

  TimeStampType GetTimestamp() const { return timestamp; }

  TimeStampType timestamp = kMaxTimestamp;
  SnapshotImpl* prev = nullptr;
  SnapshotImpl* next = nullptr;
};

// A SnapshotList is a linked-list of global snapshots, new older snapshot is
// linked at the tail of list
class SnapshotList {
 public:
  SnapshotList() : head_() {
    head_.prev = &head_;
    head_.next = &head_;
  }

  SnapshotImpl* New(TimeStampType ts) {
    SnapshotImpl* impl = new SnapshotImpl(ts);
    impl->prev = &head_;
    impl->next = head_.next;
    head_.next->prev = impl;
    head_.next = impl;
    return impl;
  }

  void Delete(const SnapshotImpl* impl) {
    impl->prev->next = impl->next;
    impl->next->prev = impl->prev;
    delete impl;
  }

  TimeStampType OldestSnapshotTS() {
    return empty() ? kMaxTimestamp : head_.prev->GetTimestamp();
  }

  ~SnapshotList() {
    SnapshotImpl* curr = head_.next;
    while (curr != &head_) {
      SnapshotImpl* tmp = curr->next;
      delete curr;
      curr = tmp;
    }
  }

 private:
  bool empty() { return head_.prev == &head_; }

  SnapshotImpl head_;
};

// VersionController manages snapshots and timestamp of a KVDK instance
// The snapshots include temporal snapshots that cached by each access thread of
// kvdk instance, and a global snapshot list that actively created by user
class VersionController {
 public:
  // LocalSnapshotHolder is used by kvdk functions such as Get(), SortedGet() and
  // HashGet() internally to guarantee lockless reads will always read out valid
  // data. LocalSnapshotHolder is thread_local, and one thread can hold atmost
  // one at same time.
  class LocalSnapshotHolder {
    VersionController* owner_{nullptr};
    TimeStampType ts_{};

   public:
    LocalSnapshotHolder(VersionController* o) : owner_{o} {
      owner_->HoldLocalSnapshot();
      ts_ = owner_->version_thread_cache_[access_thread.id]
                .holding_snapshot.timestamp;
    };
    LocalSnapshotHolder(LocalSnapshotHolder const&) = delete;
    LocalSnapshotHolder& operator=(LocalSnapshotHolder const&) = delete;
    LocalSnapshotHolder(LocalSnapshotHolder&& other) {
      *this = std::move(other);
    }
    LocalSnapshotHolder& operator=(LocalSnapshotHolder&& other) {
      kvdk_assert(owner_ == nullptr, "");
      kvdk_assert(other.owner_ != nullptr, "");
      std::swap(owner_, other.owner_);
      std::swap(ts_, other.ts_);
      return *this;
    }
    ~LocalSnapshotHolder() {
      if (owner_ != nullptr) {
        owner_->ReleaseLocalSnapshot();
      }
    }
    TimeStampType Timestamp() { return ts_; }
  };

  // GlobalSnapshotHolder is hold internally by iterators.
  // Create GlobalSnapshotHolder is more costly then InteralToken.
  class GlobalSnapshotHolder {
    VersionController* owner_{nullptr};
    SnapshotImpl* snap_{nullptr};

   public:
    GlobalSnapshotHolder(VersionController* o) : owner_{o} {
      snap_ = owner_->NewGlobalSnapshot();
    };
    GlobalSnapshotHolder(GlobalSnapshotHolder const&) = delete;
    GlobalSnapshotHolder& operator=(GlobalSnapshotHolder const&) = delete;
    GlobalSnapshotHolder(GlobalSnapshotHolder&& other) {
      *this = std::move(other);
    }
    GlobalSnapshotHolder& operator=(GlobalSnapshotHolder&& other) {
      kvdk_assert(owner_ == nullptr, "");
      kvdk_assert(other.owner_ != nullptr, "");
      kvdk_assert(snap_ == nullptr, "");
      kvdk_assert(other.snap_ != nullptr, "");
      std::swap(owner_, other.owner_);
      std::swap(snap_, other.snap_);
      return *this;
    }
    ~GlobalSnapshotHolder() {
      if (owner_ != nullptr) {
        owner_->ReleaseSnapshot(snap_);
      }
    }
    TimeStampType Timestamp() { return snap_->GetTimestamp(); }
  };

 public:
  VersionController(uint64_t max_access_threads)
      : version_thread_cache_(max_access_threads) {}

  void Init(uint64_t base_timestamp) {
    tsc_on_startup_ = rdtsc();
    base_timestamp_ = base_timestamp;
    UpdatedOldestSnapshot();
  }

  LocalSnapshotHolder GetLocalSnapshotHolder() {
    return LocalSnapshotHolder{this};
  }

  std::shared_ptr<GlobalSnapshotHolder> GetGlobalSnapshotToken() {
    return std::make_shared<GlobalSnapshotHolder>(this);
  }

  inline void HoldLocalSnapshot() {
    kvdk_assert(access_thread.id >= 0 && static_cast<size_t>(access_thread.id) <
                                             version_thread_cache_.size(),
                "Uninitialized thread in NewLocalSnapshot");
    kvdk_assert(
        version_thread_cache_[access_thread.id].holding_snapshot.timestamp ==
            kMaxTimestamp,
        "Previous LocalSnapshot not released yet!");
    version_thread_cache_[access_thread.id].holding_snapshot.timestamp =
        GetCurrentTimestamp();
  }

  inline void ReleaseLocalSnapshot() {
    kvdk_assert(access_thread.id >= 0 && static_cast<size_t>(access_thread.id) <
                                             version_thread_cache_.size(),
                "Uninitialized thread in ReleaseLocalSnapshot");
    version_thread_cache_[access_thread.id].holding_snapshot.timestamp =
        kMaxTimestamp;
  }

  inline const SnapshotImpl& GetLocalSnapshot(size_t thread_num) {
    kvdk_assert(thread_num < version_thread_cache_.size(),
                "Wrong thread num in GetLocalSnapshot");
    return version_thread_cache_[thread_num].holding_snapshot;
  }

  inline const SnapshotImpl& GetLocalSnapshot() {
    kvdk_assert(access_thread.id >= 0 && static_cast<size_t>(access_thread.id) <
                                             version_thread_cache_.size(),
                "Uninitialized thread in GetLocalSnapshot");
    return version_thread_cache_[access_thread.id].holding_snapshot;
  }

  // Create a new global snapshot
  SnapshotImpl* NewGlobalSnapshot() {
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    return global_snapshots_.New(GetCurrentTimestamp());
  }

  // Release a global snapshot, it should be created by this instance
  void ReleaseSnapshot(const SnapshotImpl* impl) {
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    global_snapshots_.Delete(impl);
  }

  inline TimeStampType GetCurrentTimestamp() {
    auto res = rdtsc() - tsc_on_startup_ + base_timestamp_;
    return res;
  }

  TimeStampType OldestSnapshotTS() { return oldest_snapshot_.GetTimestamp(); }

  // Update recorded oldest snapshot up to state by iterating every thread
  // holding snapshot
  void UpdatedOldestSnapshot() {
    TimeStampType ts = GetCurrentTimestamp();
    for (size_t i = 0; i < version_thread_cache_.size(); i++) {
      auto& tc = version_thread_cache_[i];
      ts = std::min(tc.holding_snapshot.GetTimestamp(), ts);
    }
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    oldest_snapshot_.timestamp =
        std::min(ts, global_snapshots_.OldestSnapshotTS());
  }

 private:
  // Each access thread of the instance hold its own local snapshot in thread
  // cache to avoid thread contention
  struct alignas(64) VersionThreadCache {
    VersionThreadCache() : holding_snapshot(kMaxTimestamp) {}

    SnapshotImpl holding_snapshot;
    char padding[64 - sizeof(holding_snapshot)];
  };

  Array<VersionThreadCache> version_thread_cache_;
  SnapshotList global_snapshots_;
  SpinMutex global_snapshots_lock_;
  // Known oldest snapshot of the instance, there is delay with the actual
  // oldest snapshot until call UpdatedOldestSnapshot()
  SnapshotImpl oldest_snapshot_;

  // These two used to get current timestamp of the instance
  // version_base_: The newest timestamp on instance closing last time
  // tsc_on_startup_: The CPU tsc on instance start up
  uint64_t base_timestamp_;
  uint64_t tsc_on_startup_;
};

class CheckPoint {
 public:
  void MakeCheckpoint(const Snapshot* snapshot) {
    checkpoint_ts = static_cast<const SnapshotImpl*>(snapshot)->GetTimestamp();
  }

  void MaybeRelease(const Snapshot* releasing_snapshot) {
    if (static_cast<const SnapshotImpl*>(releasing_snapshot)->GetTimestamp() ==
        checkpoint_ts) {
      Release();
    }
  }

  void Release() { checkpoint_ts = 0; }

  TimeStampType CheckpointTS() { return checkpoint_ts; }

  bool Valid() { return checkpoint_ts > 0; }

 private:
  TimeStampType checkpoint_ts;
};

}  // namespace KVDK_NAMESPACE