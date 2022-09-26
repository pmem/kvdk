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

struct Thread {
 public:
  Thread() : id(-1) {}
  ~Thread();
  void Release();
  int64_t id;
};

class ThreadManager : public std::enable_shared_from_this<ThreadManager> {
 public:
  static ThreadManager* Get() { return &manager_; }
  Status MaybeInitThread(Thread& t);

  void Release(const Thread& t);

 private:
  ThreadManager() : ids_(0) {}

  static ThreadManager manager_;
  std::atomic<int64_t> ids_;
  std::unordered_set<int64_t> usable_id_;
  SpinMutex spin_;
};

extern thread_local Thread access_thread;

}  // namespace KVDK_NAMESPACE