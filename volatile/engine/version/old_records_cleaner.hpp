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
#include "../kv_engine_cleaner.hpp"
#include "../thread_manager.hpp"
#include "../utils/utils.hpp"
#include "kvdk/volatile/configs.hpp"
#include "version_controller.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

// OldRecordsCleaner is used to clean old version data records of kvdk
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

  // Try to clean global old records
  void TryGlobalClean();

 private:
  struct CleanerThreadCache {
    std::deque<PendingFreeSpaceEntry> pending_free_space_entries{};
    SpinMutex old_records_lock;
  };
  const uint64_t kLimitCachedDeleteRecords = 1000000;

  void maybeUpdateOldestSnapshot();

  KVEngine* kv_engine_;

  Array<CleanerThreadCache> cleaner_thread_cache_;

  std::deque<PendingFreeSpaceEntry> global_pending_free_space_entries_;
};

}  // namespace KVDK_NAMESPACE
