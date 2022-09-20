/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <unordered_set>

#include "alias.hpp"
#include "kvdk/engine.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

class ThreadManager;
static std::atomic<int64_t> next_manager_id_{0};

struct Thread {
 public:
  Thread() : id(-1), manager_id(-1), thread_manager(nullptr) {}
  ~Thread();
  void Release();
  int id;
  int64_t manager_id;
  std::shared_ptr<ThreadManager> thread_manager;
};

class ThreadManager : public std::enable_shared_from_this<ThreadManager> {
 public:
  ThreadManager(uint32_t max_threads)
      : manager_id_(next_manager_id_.fetch_add(1, std::memory_order_relaxed)),
        max_threads_(max_threads),
        next_thread_id_(0) {}
  Status MaybeRegisterThread(Thread& t);
  bool Registered(const Thread& t);

  void Release(Thread& t);

 private:
  int64_t manager_id_;
  uint32_t max_threads_;
  std::atomic<uint32_t> next_thread_id_;
  std::unordered_set<uint32_t> usable_id_;
  SpinMutex spin_;
};

extern thread_local Thread access_thread;

}  // namespace KVDK_NAMESPACE