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

// OldRecordsCleaner is used to clean old version PMem records of kvdk
//
// To support multi-version machenism and consistent backup of kvdk,
// the updated/deleted records need to be ramained for a while until they are
// not refered by any snapshot
class OldRecordsCleaner {
public:
  OldRecordsCleaner(KVEngine *kv_engine, uint32_t max_access_threads)
      : kv_engine_(kv_engine), thread_cache_(max_access_threads) {
    assert(kv_engine_ != nullptr);
  }

  void Push(OldDataRecord &&old_data_record);
  void Push(OldDeleteRecord &&old_delete_record);
  // Try to clean all old records
  void TryCleanAll();
  void TryCleanCachedOldRecords();
  uint64_t NumCachedOldRecords() {
    assert(access_thread.id >= 0);
    auto &tc = thread_cache_[access_thread.id];
    return tc.old_delete_records.size() + tc.old_data_records.size();
  }

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

  std::vector<std::deque<OldDataRecord>> global_old_data_records_;
  std::vector<std::deque<OldDeleteRecord>> global_old_delete_records_;
  std::deque<PendingFreeSpaceEntries> pending_free_space_entries_;
};
} // namespace KVDK_NAMESPACE
