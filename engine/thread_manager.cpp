/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "thread_manager.hpp"

#include <mutex>

namespace KVDK_NAMESPACE {

constexpr size_t kMaxRecycleID = 1024;

std::shared_ptr<ThreadManager> ThreadManager::manager_(new ThreadManager);

Thread::~Thread() {
  if (manager != nullptr) {
    manager->Release(*this);
  }
}

void ThreadManager::MaybeInitThread(Thread& t) {
  if (t.id < 0) {
    if (!recycle_id_.empty()) {
      std::lock_guard<SpinMutex> lg(spin_);
      if (!recycle_id_.empty()) {
        auto it = recycle_id_.begin();
        t.manager = shared_from_this();
        t.id = *it;
        recycle_id_.erase(it);
        return;
      }
    }
    int id = ids_.fetch_add(1, std::memory_order_relaxed);
    t.manager = shared_from_this();
    t.id = id;
  }
}

void ThreadManager::Release(Thread& t) {
  if (t.manager.get() == this && t.id >= 0 &&
      recycle_id_.size() < kMaxRecycleID) {
    std::lock_guard<SpinMutex> lg(spin_);
    recycle_id_.insert(t.id);
  }
  t.id = -1;
  t.manager = nullptr;
}

thread_local Thread this_thread;

}  // namespace KVDK_NAMESPACE
