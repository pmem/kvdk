/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <tuple>
#include <vector>

#include "../alias.hpp"
#include "../collection.hpp"
#include "../hash_table.hpp"
#include "../kv_engine_cleaner.hpp"
#include "../thread_manager.hpp"
#include "../utils/utils.hpp"
#include "../experimental/vhash_kv.hpp"
#include "kvdk/configs.hpp"
#include "version_controller.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

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

  /// TODO: unify DelayDelete with static polymorphism
  /// It cannot be done with dynamic polymorphism as sometimes
  /// we cannot afford the memory usage by virtual function pointer
  /// and if we use shared memory dynamic polymorphism is impossible.
  template<typename KVType>
  void DelayDelete(KVType* kv);
  // Try to clean global old records
  void TryGlobalClean();

 private:
  struct CleanerThreadCache {
    std::deque<PendingFreeSpaceEntry> pending_free_space_entries{};
    std::deque<std::pair<TimeStampType, VHashKV*>> pending_vhash_kvs;
    SpinMutex old_records_lock;
  };
  const uint64_t kLimitCachedDeleteRecords = 1000000;

  void maybeUpdateOldestSnapshot();

  template<typename Deleter, typename PendingQueue>
  void tryDelete(Deleter del, PendingQueue& pending_kvs, size_t lim);

  KVEngine* kv_engine_;

  Array<CleanerThreadCache> cleaner_thread_cache_;

  std::deque<PendingFreeSpaceEntry> global_pending_free_space_entries_;
  std::deque<std::pair<TimeStampType, VHashKV*>> global_pending_vhash_kvs;
};

}  // namespace KVDK_NAMESPACE
