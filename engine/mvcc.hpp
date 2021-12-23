/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "alias.hpp"
#include "kvdk/configs.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {
constexpr TimestampType kMaxTimestamp = UINT64_MAX;

struct SnapshotImpl : public Snapshot {
  explicit SnapshotImpl(const TimestampType &t)
      : timestamp(t), next(nullptr), prev(nullptr) {}

  SnapshotImpl() : SnapshotImpl(kMaxTimestamp) {}

  TimestampType GetTimestamp() const { return timestamp; }

  TimestampType timestamp;
  SnapshotImpl *prev;
  SnapshotImpl *next;
};

class SnapshotList {
public:
  SnapshotList() : head_() {
    head_.prev = &head_;
    head_.next = &head_;
  }

  SnapshotImpl *New(TimestampType ts) {
    SnapshotImpl *impl = new SnapshotImpl(ts);
    impl->prev = &head_;
    impl->next = head_.next;
    head_.next->prev = impl;
    head_.next = impl;
    return impl;
  }

  void Delete(const SnapshotImpl *impl) {
    impl->prev->next = impl->next;
    impl->next->prev = impl->prev;
    delete impl;
  }

  TimestampType OldestSnapshotTS() {
    return empty() ? kMaxTimestamp : head_.prev->GetTimestamp();
  }

private:
  bool empty() { return head_.prev == &head_; }

  SnapshotImpl head_;
};

// set snapshot to "new_ts", and set it to kMaxTimestamp on destroy
struct SnapshotSetter {
public:
  explicit SnapshotSetter(SnapshotImpl &snapshot, TimestampType new_ts)
      : snapshot_(snapshot) {
    snapshot.timestamp = new_ts;
  }

  ~SnapshotSetter() { snapshot_.timestamp = kMaxTimestamp; }

private:
  SnapshotImpl &snapshot_;
};

class VersionController {
public:
  VersionController(uint64_t max_write_threads)
      : thread_cache_(max_write_threads) {}

  void Init(uint64_t version_base) {
    tsc_on_startup_ = get_cpu_tsc();
    version_base_ = version_base;
    UpdatedOldestSnapshot();
  }

  inline SnapshotImpl &ThreadHoldingSnapshot(size_t thread_num) {
    assert(thread_num < thread_cache_.size());
    return thread_cache_[thread_num].holding_snapshot;
  }

  // Create a new global snapshot
  SnapshotImpl *NewSnapshot() {
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    return global_snapshots_.New(GetCurrentTimestamp());
  }

  // Release a global snapshot, it should be created by this instance
  void ReleaseSnapshot(const SnapshotImpl *impl) {
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    global_snapshots_.Delete(impl);
  }

  inline TimestampType GetCurrentTimestamp() {
    auto res = get_cpu_tsc() - tsc_on_startup_ + version_base_;
    return res;
  }

  TimestampType OldestSnapshotTS() { return oldest_snapshot_.GetTimestamp(); }

  // Update recorded oldest snapshot up to state by iterating every thread
  // holding snapshot
  void UpdatedOldestSnapshot() {
    TimestampType ts = GetCurrentTimestamp();
    for (size_t i = 0; i < thread_cache_.size(); i++) {
      auto &tc = thread_cache_[i];
      ts = std::min(tc.holding_snapshot.GetTimestamp(), ts);
    }
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    oldest_snapshot_.timestamp =
        std::min(ts, global_snapshots_.OldestSnapshotTS());
  }

private:
  // Each access thread of the instance hold its own local snapshot in thread
  // cache to avoid thread contention
  struct alignas(64) ThreadCache {
    ThreadCache() : holding_snapshot(kMaxTimestamp) {}

    SnapshotImpl holding_snapshot;
  };

  inline uint64_t get_cpu_tsc() {
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)lo) | (((uint64_t)hi) << 32);
  }

  Array<ThreadCache> thread_cache_;
  SnapshotList global_snapshots_;
  SpinMutex global_snapshots_lock_;
  // Known oldest snapshot of the instance, there is delay with the actual
  // oldest snapshot until call UpdatedOldestSnapshot()
  SnapshotImpl oldest_snapshot_;

  // These two used to get current timestamp of the instance
  // version_base_: The latest timestamp on instance start up
  // tsc_on_startup_: The CPU tsc on instance start up
  uint64_t version_base_;
  uint64_t tsc_on_startup_;
};

} // namespace KVDK_NAMESPACE