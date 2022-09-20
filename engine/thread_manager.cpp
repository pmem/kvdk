/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "thread_manager.hpp"

#include <mutex>

namespace KVDK_NAMESPACE {

void Thread::Release() {
  if (thread_manager) {
    thread_manager->Release(*this);
  }
}

Thread::~Thread() { Release(); }

Status ThreadManager::MaybeRegisterThread(Thread& t) {
  if (!Registered(t)) {
    t.Release();
    if (!usable_id_.empty()) {
      std::lock_guard<SpinMutex> lg(spin_);
      if (!usable_id_.empty()) {
        auto it = usable_id_.begin();
        t.thread_manager = shared_from_this();
        t.id = *it;
        t.manager_id = manager_id_;
        usable_id_.erase(it);
        return Status::Ok;
      }
    }
    int id = next_thread_id_.fetch_add(1, std::memory_order_relaxed);
    if (static_cast<unsigned>(id) >= max_threads_) {
      return Status::TooManyAccessThreads;
    }
    t.thread_manager = shared_from_this();
    t.id = id;
    t.manager_id = manager_id_;
  }
  return Status::Ok;
}

bool ThreadManager::Registered(const Thread& t) {
  return t.thread_manager.get() == this && t.manager_id == manager_id_ &&
         t.id >= 0;
}

void ThreadManager::Release(Thread& t) {
  if (Registered(t)) {
    assert(static_cast<unsigned>(t.id) < max_threads_);
    std::lock_guard<SpinMutex> lg(spin_);
    usable_id_.insert(t.id);
    t.thread_manager = nullptr;
    t.id = -1;
    t.manager_id = -1;
  }
}

thread_local Thread access_thread;

}  // namespace KVDK_NAMESPACE
