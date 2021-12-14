/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "alias.hpp"
#include "kvdk/configs.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {
constexpr TimeStampType kMaxTimestamp = UINT64_MAX;

struct SnapshotImpl : public Snapshot {
  explicit SnapshotImpl(const TimeStampType &t) : timestamp(t) {}

  SnapshotImpl() : timestamp(kMaxTimestamp) {}

  TimeStampType GetTimestamp() override { return timestamp; }

  TimeStampType timestamp;
};

// set snapshot to "new_ts", and set it to kMaxTimestamp on destruct
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

struct ChainedSnapshot {
  SnapshotImpl snapshot;

  ChainedSnapshot *prev;
  ChainedSnapshot *next;
};

class VersionController {
public:
  VersionController(uint64_t max_write_threads)
      : thread_cache_(max_write_threads) {}

  void Init(uint64_t version_base) {
    ts_on_startup_ = get_cpu_tsc();
    version_base_ = version_base;
    UpdatedOldestSnapshot();
  }

  inline SnapshotImpl &HoldingSnapshot() {
    assert(write_thread.id >= 0);
    return thread_cache_[write_thread.id].holding_snapshot;
  }

  inline TimeStampType CurrentTimestamp() {
    auto res = get_cpu_tsc() - ts_on_startup_ + version_base_;
    return res;
  }

  TimeStampType OldestSnapshot() { return oldest_snapshot_.GetTimestamp(); }

  void UpdatedOldestSnapshot() {
    TimeStampType ts = CurrentTimestamp();
    for (size_t i = 0; i < thread_cache_.size(); i++) {
      auto &tc = thread_cache_[i];
      ts = std::min(tc.holding_snapshot.GetTimestamp(), ts);
    }
    oldest_snapshot_.timestamp = ts;
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

  uint64_t version_base_;
  uint64_t ts_on_startup_;
  SnapshotImpl oldest_snapshot_;
  Array<ThreadCache> thread_cache_;
};

} // namespace KVDK_NAMESPACE