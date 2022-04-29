/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <vector>

#include "../alias.hpp"
#include "../collection.hpp"
#include "../hash_table.hpp"
#include "../thread_manager.hpp"
#include "../thread_pool.hpp"
#include "../utils/utils.hpp"
#include "kvdk/configs.hpp"
#include "version_controller.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

struct OldDataRecord {
  void* pmem_data_record;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely clear index of this OldDataRecord, and free its space
  TimeStampType release_time;
};

// We only use this for skiplist now
struct OutdatedCollection {
  OutdatedCollection(Collection* c, PointerType type, TimeStampType rt)
      : collection(c, type), release_time(rt) {}
  PointerWithTag<Collection, PointerType> collection;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely destroy the collection
  TimeStampType release_time;
};

struct PendingFreeSpaceEntries {
  std::vector<SpaceEntry> entries;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely free these entries
  TimeStampType release_time;
};

struct PendingFreeSpaceEntry {
  SpaceEntry entry;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely free this entry
  TimeStampType release_time;
};

// OldRecordsCleaner is used to clean old version PMem records of kvdk
//
// To support multi-version machenism and consistent backup of kvdk,
// the updated/deleted records need to be ramained for a while until they are
// not refered by any snapshot
class OldRecordsCleaner {
 public:
  OldRecordsCleaner(KVEngine* kv_engine, uint32_t max_access_threads)
      : kv_engine_(kv_engine), cleaner_thread_cache_(max_access_threads) {
    assert(kv_engine_ != nullptr);
  }

  void PushToPendingFree(void* addr, TimeStampType ts);
  bool TryFreePendingSpace(
      const PendingFreeSpaceEntries& pending_free_space_entries);
  void PushToCache(const OldDataRecord& old_data_record);
  void PushToCache(const OutdatedCollection& outdated_collection);
  void PushToGlobal(std::deque<OutdatedCollection>&& outdated_collections);
  void TryCleanCachedOldRecords(size_t num_limit_clean);
  void TryCleanDataRecords();
  void TryGlobalClean();
  uint64_t NumCachedOldRecords() {
    // TODO jiayu: calculate length of outdated collection
    assert(access_thread.id >= 0);
    auto& tc = cleaner_thread_cache_[access_thread.id];
    return tc.old_data_records.size();
  }

  void PushToTaskQueue(
      const std::vector<std::pair<void*, PointerType>>& outdated_records);

 private:
  SpaceEntry PurgeStringRecord(void* pmem_record);

  SpaceEntry PurgeSortedRecord(SkiplistNode* dram_node, void* pmem_record);

 private:
  struct CleanerThreadCache {
    std::deque<OldDataRecord> old_data_records{};
    std::deque<PendingFreeSpaceEntry> pending_free_space_entries{};
    std::deque<OutdatedCollection> outdated_collections{};
    SpinMutex old_records_lock;
  };
  const uint64_t kLimitCachedDeleteRecords = 10000;

  void maybeUpdateOldestSnapshot();
  // Purge a old data record and free space
  SpaceEntry purgeOldDataRecord(const OldDataRecord& old_data_record);

  KVEngine* kv_engine_;

  Array<CleanerThreadCache> cleaner_thread_cache_;

  SpinMutex lock_;
  std::vector<std::deque<OldDataRecord>> global_old_data_records_;
  std::deque<PendingFreeSpaceEntries> global_pending_free_space_entries_;
  std::vector<std::deque<OutdatedCollection>> global_outdated_collections_;
  TimeStampType clean_all_data_record_ts_{0};

  ThreadPool thread_pool_{4};
};

}  // namespace KVDK_NAMESPACE
