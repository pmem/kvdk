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
  TimestampType release_time;
};

struct PendingFreeSpaceEntry {
  SpaceEntry entry;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely free this entry
  TimestampType release_time;
};

struct PendingPurgeStrRecords {
  PendingPurgeStrRecords(std::vector<StringRecord*>&& _records,
                         TimestampType _release_time)
      : records(_records), release_time(_release_time) {}

  std::vector<StringRecord*> records;
  TimestampType release_time;
};

struct PendingPurgeDLRecords {
  PendingPurgeDLRecords(std::vector<DLRecord*>&& _records,
                        TimestampType _release_time)
      : records(_records), release_time(_release_time) {}

  std::vector<DLRecord*> records;
  TimestampType release_time;
};

struct PendingCleanRecords {
  std::deque<std::pair<TimestampType, List*>> outdated_lists;
  std::deque<std::pair<TimestampType, HashList*>> outdated_hlists;
  std::deque<std::pair<TimestampType, Skiplist*>> outdated_skiplists;
  std::deque<PendingPurgeStrRecords> pending_purge_strings;
  std::deque<PendingPurgeDLRecords> pending_purge_dls;
  std::deque<Skiplist*> no_hash_skiplists;
  std::deque<List*> valid_lists;
  size_t Size() {
    return outdated_lists.size() + outdated_hlists.size() +
           outdated_skiplists.size() + pending_purge_strings.size() +
           pending_purge_dls.size() + no_hash_skiplists.size() +
           valid_lists.size();
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
        min_thread_num_(1),
        close_(false),
        start_slot_(0),
        active_clean_workers_(0),
        main_worker_([this]() { this->mainWork(); }),
        clean_workers_(max_thread_num_ - 1 /*1 for main worker*/) {
    for (auto& w : clean_workers_) {
      w.Init([this]() { this->cleanWork(); });
    }
  }

  ~Cleaner() { Close(); }

  void Start() {
    if (!close_ && min_thread_num_ > 0) {
      main_worker_.Run();
    }
  }

  void Close() {
    close_ = true;
    main_worker_.Join();
    for (auto& w : clean_workers_) {
      w.Join();
    }
  }

  void AdjustCleanWorkers(size_t advice_workers_num);

  size_t ActiveThreadNum() { return active_clean_workers_.load() + 1; }

  double SearchOutdatedCollections();
  void FetchOutdatedCollections(PendingCleanRecords& pending_clean_records);

 private:
  class Worker {
   public:
    ~Worker() { Join(); }

    Worker(std::function<void()> f) { Init(f); }

    Worker() = default;

    void Init(std::function<void()> f) {
      Join();
      func = f;
      std::unique_lock<SpinMutex> ul(spin);
      join_ = false;
      keep_work_ = false;

      auto work = [&]() {
        while (true) {
          {
            std::unique_lock<SpinMutex> ul(spin);
            while (!keep_work_ && !join_) {
              cv.wait(ul);
            }
            if (join_) {
              return;
            }
          }
          func();
        }
      };
      worker_thread = std::thread(work);
    }

    void Run() {
      std::unique_lock<SpinMutex> ul(spin);
      keep_work_ = true;
      cv.notify_all();
    }

    void Stop() {
      std::unique_lock<SpinMutex> ul(spin);
      keep_work_ = false;
    }

    void Join() {
      {
        std::unique_lock<SpinMutex> ul(spin);
        keep_work_ = false;
        join_ = true;
        cv.notify_all();
      }
      if (worker_thread.joinable()) {
        worker_thread.join();
      }
    }

   private:
    bool keep_work_;
    bool join_;
    std::condition_variable_any cv;
    SpinMutex spin;
    std::thread worker_thread;
    std::function<void(void)> func;
  };

  KVEngine* kv_engine_;

  size_t max_thread_num_;
  size_t min_thread_num_;
  std::atomic<bool> close_;
  std::atomic<int64_t> start_slot_;
  std::atomic<size_t> active_clean_workers_;
  Worker main_worker_;
  std::vector<Worker> clean_workers_;

  struct OutDatedCollections {
    struct TimeStampCmp {
     public:
      bool operator()(const std::pair<Collection*, TimestampType> a,
                      const std::pair<Collection*, TimestampType> b) const {
        if (a.second < b.second) return true;
        if (a.second == b.second && a.first->ID() < b.first->ID()) return true;
        return false;
      }
    };
    using ListQueue = std::set<std::pair<List*, TimestampType>, TimeStampCmp>;
    using HashListQueue =
        std::set<std::pair<HashList*, TimestampType>, TimeStampCmp>;

    using SkiplistQueue =
        std::set<std::pair<Skiplist*, TimestampType>, TimeStampCmp>;

    SpinMutex queue_mtx;
    ListQueue lists;
    HashListQueue hashlists;
    SkiplistQueue skiplists;
    double increase_ratio = 0;
    ~OutDatedCollections();
  };

  OutDatedCollections outdated_collections_;

 private:
  void cleanWork();
  void mainWork();
};

}  // namespace KVDK_NAMESPACE