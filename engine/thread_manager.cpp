/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "thread_manager.hpp"

#include <mutex>

namespace KVDK_NAMESPACE {

ThreadManager ThreadManager::manager_;

void ThreadManager::MaybeInitThread(Thread& t) {
  if (t.id < 0) {
    int id = ids_.fetch_add(1, std::memory_order_relaxed);
    t.id = id;
  }
}

thread_local Thread this_thread;

}  // namespace KVDK_NAMESPACE
