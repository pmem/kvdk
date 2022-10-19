/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <unordered_set>

#include "alias.hpp"
#include "kvdk/volatile/engine.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

class ThreadManager;

struct Thread {
 public:
  Thread() : id(-1), manager(nullptr) {}
  int64_t id;
  std::shared_ptr<ThreadManager> manager;

  ~Thread();
};

extern thread_local Thread this_thread;

class ThreadManager : public std::enable_shared_from_this<ThreadManager> {
 public:
  static ThreadManager* Get() { return manager_.get(); }
  static int64_t ThreadID() {
    Get()->MaybeInitThread(this_thread);
    return this_thread.id;
  }
  void MaybeInitThread(Thread& t);
  void Release(Thread& t);

 private:
  ThreadManager() : ids_(0), recycle_id_(), spin_() {}

  static std::shared_ptr<ThreadManager> manager_;
  std::atomic<int64_t> ids_;
  std::unordered_set<uint32_t> recycle_id_;
  SpinMutex spin_;
};

}  // namespace KVDK_NAMESPACE