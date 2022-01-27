/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <vector>

#include "../hash_table.hpp"
#include "../thread_manager.hpp"
#include "../utils/utils.hpp"
#include "kvdk/configs.hpp"
#include "kvdk/namespace.hpp"
#include "version_controller.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

struct OldDataRecord {
  void *pmem_data_record;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely clear index of this OldDataRecord, and free its space
  TimeStampType release_time;
};

struct OldDeleteRecord {
  void *pmem_delete_record;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely clear index of this OldDeleteRecord, and transfer it to
  // PendingFreeSpaceEntries
  TimeStampType release_time;
  // We need ref to hash entry for clear index of delete record
  HashEntry *hash_entry_ref;
  SpinMutex *hash_entry_lock;
};

struct PendingFreeSpaceEntries {
  std::vector<SpaceEntry> entries;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely free these entries
  TimeStampType release_time;
};

// OldRecordsCleaner is used to clean old version PMem records of kvdk
//
// To support multi-version machenism and consistent backup of kvdk,
// the updated/deleted records need to be ramained for a while until they are
// not refered by any snapshot
class OldRecordsCleaner {
public:
  OldRecordsCleaner(KVEngine *kv_engine, uint32_t max_access_threads)
      : kv_engine_(kv_engine), cleaner_thread_cache_(max_access_threads) {
    assert(kv_engine_ != nullptr);
  }

  void Push(const OldDataRecord &old_data_record);
  void Push(const OldDeleteRecord &old_delete_record);
  // Try to clean global old records
  void TryGlobalClean();
  void TryCleanCachedOldRecords(size_t num_limit_clean);
  uint64_t NumCachedOldRecords() {
    assert(access_thread.id >= 0);
    auto &tc = cleaner_thread_cache_[access_thread.id];
    return tc.old_delete_records.size() + tc.old_data_records.size();
  }

private:
  struct CleanerThreadCache {
    std::deque<OldDeleteRecord> old_delete_records{};
    std::deque<OldDataRecord> old_data_records{};
    SpinMutex old_records_lock;
  };
  const uint64_t kLimitCachedDeleteRecords = 1000000;

  void maybeUpdateOldestSnapshot();
  SpaceEntry purgeOldDataRecord(const OldDataRecord &old_data_record);
  SpaceEntry purgeOldDeleteRecord(const OldDeleteRecord &old_delete_record);

  KVEngine *kv_engine_;

  Array<CleanerThreadCache> cleaner_thread_cache_;

  std::vector<std::deque<OldDataRecord>> global_old_data_records_;
  std::vector<std::deque<OldDeleteRecord>> global_old_delete_records_;
  std::deque<PendingFreeSpaceEntries> pending_free_space_entries_;
  TimeStampType clean_all_data_record_ts_{0};
};
} // namespace KVDK_NAMESPACE
