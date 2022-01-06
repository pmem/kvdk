/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <vector>

#include "../hash_table.hpp"
#include "../thread_manager.hpp"
#include "../utils.hpp"
#include "kvdk/configs.hpp"
#include "kvdk/namespace.hpp"
#include "version_controller.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

struct OldDataRecord {
  void *pmem_data_record;
  TimestampType newer_version_timestamp;
};

struct OldDeleteRecord {
  void *pmem_delete_record;
  TimestampType newer_version_timestamp;
  // We need ref to hash entry for clear index of delete record
  HashEntry *hash_entry_ref;
  SpinMutex *hash_entry_lock;
};

struct PendingFreeSpaceEntries {
  std::vector<SpaceEntry> entries;
  TimestampType free_ts;
};

// Used for freeing old version records, this is required for MVCC
class OldRecordsCleaner {
public:
  OldRecordsCleaner(KVEngine *kv_engine, uint32_t max_access_threads)
      : kv_engine_(kv_engine), thread_cache_(max_access_threads) {
    assert(kv_engine_ != nullptr);
  }
  void NotifyBGCleaner() { background_cleaner_cv_.notify_all(); }
  void Push(OldDataRecord &&old_data_record);
  void Push(OldDeleteRecord &&old_delete_record);
  // Try to clean all old records
  void TryCleanAll();
  // Run in background to clean old records regularly
  void BackgroundCleaner();
  void CleanCachedOldRecords();

private:
  struct ThreadCache {
    std::deque<OldDeleteRecord> old_delete_records{};
    std::deque<OldDataRecord> old_data_records{};
    SpinMutex old_records_lock;
  };

  void maybeUpdateOldestSnapshot();
  SpaceEntry purgeOldDataRecord(const OldDataRecord &old_data_record);
  SpaceEntry purgeOldDeleteRecord(const OldDeleteRecord &old_delete_record);

  KVEngine *kv_engine_;

  Array<ThreadCache> thread_cache_;
  std::condition_variable_any background_cleaner_cv_;
  bool bg_free_thread_processing_{false};

  std::vector<std::deque<OldDataRecord>> bg_free_data_records_;
  std::vector<std::deque<OldDeleteRecord>> bg_free_delete_records_;
  std::deque<PendingFreeSpaceEntries> bg_free_space_entries_;
};
} // namespace KVDK_NAMESPACE
