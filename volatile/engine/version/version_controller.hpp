/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "../alias.hpp"
#include "../allocator.hpp"
#include "../thread_manager.hpp"
#include "kvdk/volatile/configs.hpp"

namespace KVDK_NAMESPACE {
constexpr TimestampType kMaxTimestamp = UINT64_MAX;

struct SnapshotImpl : public Snapshot {
  explicit SnapshotImpl(const TimestampType& t) : timestamp(t) {}

  SnapshotImpl() = default;

  TimestampType GetTimestamp() const { return timestamp; }

  TimestampType timestamp = kMaxTimestamp;
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

  SnapshotImpl* New(TimestampType ts) {
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

  TimestampType OldestSnapshotTS() {
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
  // LocalSnapshotHolder is used by kvdk functions such as Get(), SortedGet()
  // and HashGet() internally to guarantee lockless reads will always read out
  // valid data. LocalSnapshotHolder is thread_local, and one thread can hold
  // atmost one at same time.
  class LocalSnapshotHolder {
    VersionController* owner_{nullptr};
    TimestampType ts_{};

   public:
    LocalSnapshotHolder(VersionController* o) : owner_{o} {
      owner_->HoldLocalSnapshot();
      ts_ = owner_
                ->version_thread_cache_[ThreadManager::ThreadID() %
                                        owner_->version_thread_cache_.size()]
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
    TimestampType Timestamp() { return ts_; }
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
    TimestampType Timestamp() { return snap_->GetTimestamp(); }
  };

  class BatchWriteToken {
    VersionController* owner_{nullptr};
    TimestampType ts_{kMaxTimestamp};

   public:
    BatchWriteToken(VersionController* o)
        : owner_{o}, ts_{owner_->GetCurrentTimestamp()} {
      ts_ = owner_->GetCurrentTimestamp();
      auto& tc =
          owner_->version_thread_cache_[ThreadManager::ThreadID() %
                                        owner_->version_thread_cache_.size()];
      kvdk_assert(tc.batch_write_ts == kMaxTimestamp, "");
      tc.batch_write_ts = ts_;
    }
    BatchWriteToken(BatchWriteToken const&) = delete;
    BatchWriteToken& operator=(BatchWriteToken const&) = delete;
    BatchWriteToken(BatchWriteToken&& other) { swap(other); }
    BatchWriteToken& operator=(BatchWriteToken&& other) {
      swap(other);
      return *this;
    }
    ~BatchWriteToken() {
      if (owner_ != nullptr) {
        auto& tc =
            owner_->version_thread_cache_[ThreadManager::ThreadID() %
                                          owner_->version_thread_cache_.size()];
        tc.batch_write_ts = kMaxTimestamp;
      }
    }
    TimestampType Timestamp() { return ts_; }

   private:
    void swap(BatchWriteToken& other) {
      kvdk_assert(owner_ == nullptr, "");
      kvdk_assert(other.owner_ != nullptr, "");
      kvdk_assert(ts_ == kMaxTimestamp, "");
      kvdk_assert(other.ts_ != kMaxTimestamp, "");
      std::swap(owner_, other.owner_);
      std::swap(ts_, other.ts_);
    }
  };

 public:
  VersionController(uint64_t max_access_threads)
      : version_thread_cache_(max_access_threads) {}

  void Init(uint64_t base_timestamp) {
    tsc_on_startup_ = rdtsc();
    base_timestamp_ = base_timestamp;
    UpdateLocalOldestSnapshot();
  }

  LocalSnapshotHolder GetLocalSnapshotHolder() {
    return LocalSnapshotHolder{this};
  }

  BatchWriteToken GetBatchWriteToken() { return BatchWriteToken{this}; }

  std::shared_ptr<GlobalSnapshotHolder> GetGlobalSnapshotToken() {
    return std::make_shared<GlobalSnapshotHolder>(this);
  }

  inline void HoldLocalSnapshot() {
    kvdk_assert(ThreadManager::ThreadID() >= 0,
                "Uninitialized thread in NewLocalSnapshot");
    kvdk_assert(version_thread_cache_[ThreadManager::ThreadID() %
                                      version_thread_cache_.size()]
                        .holding_snapshot.timestamp == kMaxTimestamp,
                "Previous LocalSnapshot not released yet!");
    version_thread_cache_[ThreadManager::ThreadID() %
                          version_thread_cache_.size()]
        .holding_snapshot.timestamp = GetCurrentTimestamp();
  }

  inline void ReleaseLocalSnapshot() {
    kvdk_assert(ThreadManager::ThreadID() >= 0,
                "Uninitialized thread in ReleaseLocalSnapshot");
    version_thread_cache_[ThreadManager::ThreadID() %
                          version_thread_cache_.size()]
        .holding_snapshot.timestamp = kMaxTimestamp;
  }

  inline const SnapshotImpl& GetLocalSnapshot(size_t thread_num) {
    kvdk_assert(thread_num < version_thread_cache_.size(),
                "Wrong thread num in GetLocalSnapshot");
    return version_thread_cache_[thread_num].holding_snapshot;
  }

  inline const SnapshotImpl& GetLocalSnapshot() {
    kvdk_assert(ThreadManager::ThreadID() >= 0,
                "Uninitialized thread in GetLocalSnapshot");
    return version_thread_cache_[ThreadManager::ThreadID() %
                                 version_thread_cache_.size()]
        .holding_snapshot;
  }

  // Create a new global snapshot
  SnapshotImpl* NewGlobalSnapshot(bool may_block = true) {
    TimestampType ts = GetCurrentTimestamp();
    if (may_block) {
      for (size_t i = 0; i < version_thread_cache_.size(); i++) {
        while (ts >= version_thread_cache_[i].batch_write_ts) {
          _mm_pause();
        }
      }
    } else {
      for (size_t i = 0; i < version_thread_cache_.size(); i++) {
        TimestampType batch_ts = version_thread_cache_[i].batch_write_ts;
        ts = std::min(ts, batch_ts - 1);
      }
    }

    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    SnapshotImpl* new_global_snapshot = global_snapshots_.New(ts);
    global_oldest_snapshot_ts_ = global_snapshots_.OldestSnapshotTS();
    return new_global_snapshot;
  }

  // Release a global snapshot, it should be created by this instance
  void ReleaseSnapshot(const SnapshotImpl* impl) {
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    global_snapshots_.Delete(impl);
    global_oldest_snapshot_ts_ = global_snapshots_.OldestSnapshotTS();
  }

  inline TimestampType GetCurrentTimestamp() {
    auto res = rdtsc() - tsc_on_startup_ + base_timestamp_;
    return res;
  }

  TimestampType LocalOldestSnapshotTS() {
    return local_oldest_snapshot_.GetTimestamp();
  }

  TimestampType GlobalOldestSnapshotTs() {
    return global_oldest_snapshot_ts_.load();
  }

  // Update recorded oldest snapshot up to state by iterating every thread
  // holding snapshot
  void UpdateLocalOldestSnapshot() {
    // update local oldest snapshot
    TimestampType ts = GetCurrentTimestamp();
    for (size_t i = 0; i < version_thread_cache_.size(); i++) {
      auto& tc = version_thread_cache_[i];
      ts = std::min(tc.holding_snapshot.GetTimestamp(), ts);
    }
    local_oldest_snapshot_.timestamp = ts;
  }

 private:
  // Each access thread of the instance hold its own local snapshot in thread
  // cache to avoid thread contention
  struct alignas(64) VersionThreadCache {
    VersionThreadCache() : holding_snapshot(kMaxTimestamp) {}

    SnapshotImpl holding_snapshot;
    TimestampType batch_write_ts{kMaxTimestamp};
    char padding[64 - sizeof(holding_snapshot)];
  };

  Array<VersionThreadCache> version_thread_cache_;
  SnapshotList global_snapshots_;
  SpinMutex global_snapshots_lock_;
  // Known oldest snapshot of the instance, there is delay with the actual
  // oldest snapshot until call UpdatedOldestSnapshot()
  SnapshotImpl local_oldest_snapshot_{kMaxTimestamp};
  std::atomic<TimestampType> global_oldest_snapshot_ts_{kMaxTimestamp};

  // These two used to get current timestamp of the instance
  // version_base_: The newest timestamp on instance closing last time
  // tsc_on_startup_: The CPU tsc on instance start up
  uint64_t base_timestamp_;
  uint64_t tsc_on_startup_;
};

class CheckPoint {
 public:
  void MakeCheckpoint(const Snapshot* snapshot) {
    checkpoint_ts_ = static_cast<const SnapshotImpl*>(snapshot)->GetTimestamp();
  }

  void MaybeRelease(const Snapshot* releasing_snapshot) {
    if (static_cast<const SnapshotImpl*>(releasing_snapshot)->GetTimestamp() ==
        checkpoint_ts_) {
      Release();
    }
  }

  void Release() { checkpoint_ts_ = 0; }

  TimestampType CheckpointTS() { return checkpoint_ts_; }

  bool Valid() { return checkpoint_ts_ > 0; }

 private:
  TimestampType checkpoint_ts_;
};

}  // namespace KVDK_NAMESPACE