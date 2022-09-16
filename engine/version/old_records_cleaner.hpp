/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "../alias.hpp"
#include "../collection.hpp"
#include "../hash_table.hpp"
#include "../kv_engine_cleaner.hpp"
#include "../thread_manager.hpp"
#include "../utils/utils.hpp"
#include "kvdk/configs.hpp"
#include "version_controller.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

class IDeleter {
 public:
  // Called by OldRecordsCleaner for actual deletion.
  virtual void Delete(void* obj) = 0;
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

  // Warning: not thread safe. Must be called during kv_engine initialization.
  void RegisterDelayDeleter(IDeleter& deleter);

  void DelayDelete(IDeleter& deleter, void* obj);

  // Try to clean global old records
  void TryGlobalClean();

 private:
  using PendingQueue = std::deque<std::pair<TimestampType, void*>>;

  struct CleanerThreadCache {
    std::deque<PendingFreeSpaceEntry> pending_free_space_entries{};
    std::vector<PendingQueue> local_queues_;
    SpinMutex old_records_lock;
  };
  const uint64_t kLimitCachedDeleteRecords = 1000000;

  void maybeUpdateOldestSnapshot();

  // Try purging some entries from a locked PendingQueue with associated
  // deleter.
  void tryPurge(IDeleter& deleter, PendingQueue& pending_kvs, size_t lim);

  KVEngine* kv_engine_;

  Array<CleanerThreadCache> cleaner_thread_cache_;

  std::deque<PendingFreeSpaceEntry> global_pending_free_space_entries_;

  std::unordered_map<IDeleter*, size_t> delay_deleters_;
  std::vector<PendingQueue> global_queues_;
};

}  // namespace KVDK_NAMESPACE
