/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "alias.hpp"
#include "kvdk/configs.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {
constexpr TimeStampType kMaxTimestamp = UINT64_MAX;

struct SnapshotImpl : public Snapshot {
  explicit SnapshotImpl(const TimeStampType &t)
      : timestamp(t), next(nullptr), prev(nullptr) {}

  SnapshotImpl() : SnapshotImpl(kMaxTimestamp) {}

  TimeStampType GetTimestamp() override { return timestamp; }

  TimeStampType timestamp;
  SnapshotImpl *prev;
  SnapshotImpl *next;
};

class SnapshotList {
public:
  SnapshotList() : head_() {
    head_.prev = &head_;
    head_.next = &head_;
  }

  SnapshotImpl *New(TimeStampType ts) {
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

  TimeStampType OldestSnapshotTS() {
    return empty() ? kMaxTimestamp : head_.prev->GetTimestamp();
  }

private:
  bool empty() { return head_.prev == &head_; }

  SnapshotImpl head_;
};

// set snapshot to "new_ts", and set it to kMaxTimestamp on destroy
struct SnapshotSetter {
public:
  explicit SnapshotSetter(SnapshotImpl &snapshot, TimeStampType new_ts)
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

  inline SnapshotImpl &HoldingSnapshot() {
    assert(write_thread.id >= 0);
    return thread_cache_[write_thread.id].holding_snapshot;
  }

  SnapshotImpl *MakeSnapshot() {
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    return global_snapshots_.New(CurrentTimestamp());
  }

  void ReleaseSnapshot(const SnapshotImpl *impl) {
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    global_snapshots_.Delete(impl);
  }

  inline TimeStampType CurrentTimestamp() {
    auto res = get_cpu_tsc() - tsc_on_startup_ + version_base_;
    return res;
  }

  TimeStampType OldestSnapshot() { return oldest_snapshot_.GetTimestamp(); }

  void UpdatedOldestSnapshot() {
    TimeStampType ts = CurrentTimestamp();
    for (size_t i = 0; i < thread_cache_.size(); i++) {
      auto &tc = thread_cache_[i];
      ts = std::min(tc.holding_snapshot.GetTimestamp(), ts);
    }
    std::lock_guard<SpinMutex> lg(global_snapshots_lock_);
    oldest_snapshot_.timestamp =
        std::min(ts, global_snapshots_.OldestSnapshotTS());
  }

private:
  struct alignas(64) ThreadCache {
    ThreadCache() : holding_snapshot(kMaxTimestamp) {}

    SnapshotImpl holding_snapshot;
  };

  inline uint64_t get_cpu_tsc() {
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)lo) | (((uint64_t)hi) << 32);
  }

  // Known oldest holding snapshot, there is delay existing until call
  // UpdatedOldestSnapshot()
  SnapshotImpl oldest_snapshot_;
  SnapshotList global_snapshots_;
  SpinMutex global_snapshots_lock_;

  uint64_t version_base_;
  uint64_t tsc_on_startup_;
  Array<ThreadCache> thread_cache_;
};

} // namespace KVDK_NAMESPACE