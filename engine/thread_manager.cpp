/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "thread_manager.hpp"

#include <mutex>

namespace KVDK_NAMESPACE {

ThreadManager ThreadManager::manager_;

void Thread::Release() {
    id = -1;
}

Thread::~Thread() { Release(); }

Status ThreadManager::MaybeInitThread(Thread& t) {
  if (t.id < 0) {
    if (!usable_id_.empty()) {
      std::lock_guard<SpinMutex> lg(spin_);
      if (!usable_id_.empty()) {
        auto it = usable_id_.begin();
        t.id = *it;
        usable_id_.erase(it);
        return Status::Ok;
      }
    }
    int id = ids_.fetch_add(1, std::memory_order_relaxed);
    t.id = id;
  }
  return Status::Ok;
}

thread_local Thread access_thread;

}  // namespace KVDK_NAMESPACE
