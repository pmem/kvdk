/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <mutex>

#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {

Thread::~Thread() {
  if (thread_manager) {
    (thread_manager)->Release(*this);
  }
}

Status ThreadManager::MaybeInitThread(Thread &t) {
  if (t.id < 0) {
    if (!usable_id_.empty()) {
      std::lock_guard<SpinMutex> lg(spin_);
      if (!usable_id_.empty()) {
        auto it = usable_id_.begin();
        t.id = *it;
        usable_id_.erase(it);
        t.thread_manager = shared_from_this();
        return Status::Ok;
      }
    }
    int id = ids_.fetch_add(1, std::memory_order_relaxed);
    if (id >= max_threads_) {
      return Status::TooManyWriteThreads;
    }
    t.id = id;
    t.thread_manager = shared_from_this();
  }
  return Status::Ok;
}

void ThreadManager::Release(const Thread &t) {
  std::lock_guard<SpinMutex> lg(spin_);
  usable_id_.insert(t.id);
}

thread_local Thread write_thread;

} // namespace KVDK_NAMESPACE
