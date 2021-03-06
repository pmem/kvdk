/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */
#pragma once

#include <stdio.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "alias.hpp"
#include "collection.hpp"
#include "hash_table.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

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

struct PendingPurgeStrRecords {
  PendingPurgeStrRecords(std::vector<StringRecord*>&& _records,
                         TimeStampType _release_time)
      : records(_records), release_time(_release_time) {}

  std::vector<StringRecord*> records;
  TimeStampType release_time;
};

struct PendingPurgeDLRecords {
  PendingPurgeDLRecords(std::vector<DLRecord*>&& _records,
                        TimeStampType _release_time)
      : records(_records), release_time(_release_time) {}

  std::vector<DLRecord*> records;
  TimeStampType release_time;
};

struct PendingCleanRecords {
  using ListPtr = std::unique_ptr<List>;
  using HashListPtr = std::unique_ptr<HashList>;

  std::deque<std::pair<TimeStampType, ListPtr>> outdated_lists;
  std::deque<std::pair<TimeStampType, HashListPtr>> outdated_hash_lists;
  std::deque<std::pair<TimeStampType, Skiplist*>> outdated_skip_lists;
  std::deque<PendingPurgeStrRecords> pending_purge_strings;
  std::deque<PendingPurgeDLRecords> pending_purge_dls;
  std::deque<Skiplist*> no_index_skiplists;
  size_t Size() {
    return outdated_lists.size() + outdated_hash_lists.size() +
           outdated_skip_lists.size() + pending_purge_strings.size() +
           pending_purge_dls.size() + no_index_skiplists.size();
  }
};

class KVEngine;

class Cleaner {
 public:
  static constexpr int64_t kSlotBlockUnit = 1024;
  static constexpr double kWakeUpThreshold = 0.1;

  Cleaner(KVEngine* kv_engine, int64_t max_cleaner_threads)
      : kv_engine_(kv_engine),
        max_thread_num_(max_cleaner_threads),
        close_(false),
        start_slot_(0),
        live_thread_num_(0),
        workers_(max_cleaner_threads) {
    for (size_t thread_id = 0; thread_id < max_thread_num_; ++thread_id) {
      idled_workers_.push_back(thread_id);
    }
  }

  ~Cleaner() { CloseAllWorkers(); }

  void StartClean();
  void CloseAllWorkers() {
    close_ = true;
    for (size_t i = 0; i < workers_.size(); ++i) {
      if (workers_[i].worker.joinable()) {
        workers_[i].finish = true;
        workers_[i].worker.join();
      }
    }
  }
  void AdjustThread(size_t advice_thread_num);
  size_t ActiveThreadNum() { return live_thread_num_.load(); }

 private:
  struct ThreadWorker {
    std::atomic_bool finish{true};
    std::thread worker;
  };
  KVEngine* kv_engine_;
  PendingCleanRecords pending_clean_records_;

  size_t max_thread_num_;
  size_t min_thread_num_ = 1;
  std::atomic_bool close_;
  std::atomic_int64_t start_slot_;
  std::atomic<size_t> live_thread_num_;
  std::vector<ThreadWorker> workers_;
  std::deque<size_t> idled_workers_;
  std::deque<size_t> actived_workers_;

 private:
  void doCleanWork(size_t thread_id);
  void mainWorker();
};

}  // namespace KVDK_NAMESPACE