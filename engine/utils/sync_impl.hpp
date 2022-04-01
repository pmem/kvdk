/* Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 * This source code is licensed under both the GPLv2 (found in the
 * COPYING file in the root directory) and Apache 2.0 License
 * (found in the LICENSE.Apache file in the root directory).
 */

/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <assert.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../alias.hpp"

#if KVDK_DEBUG_LEVEL > 0
namespace KVDK_NAMESPACE {

struct SyncPointPair {
  std::string producer;
  std::string consumer;
};

struct SyncImpl {
  SyncImpl() : ready_(false) {}
  void LoadDependency(const std::vector<SyncPointPair>& dependencies) {
    std::lock_guard<std::mutex> lock(mutex_);
    consumers_.clear();
    producers_.clear();
    cleared_points_.clear();
    for (const auto& dependency : dependencies) {
      consumers_[dependency.producer].push_back(dependency.consumer);
      producers_[dependency.consumer].push_back(dependency.producer);
      point_table_.insert(dependency.consumer);
      point_table_.insert(dependency.producer);
    }
    cv_.notify_all();
  }

  void EnableProcessing() { ready_ = true; }

  void DisableProcessing() { ready_ = false; }

  void Process(const std::string& point, void* func_arg) {
    if (!ready_) {
      return;
    }

    if (point_table_.find(point) == point_table_.end()) {
      return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    while (!IsClearedAllproducers(point)) {
      cv_.wait(lock);
    }

    auto callback_pair = callbacks_.find(point);
    if (callback_pair != callbacks_.end()) {
      num_callbacks_running_++;
      mutex_.unlock();
      callback_pair->second(func_arg);
      mutex_.lock();
      num_callbacks_running_--;
    }
    cleared_points_.insert(point);

    cv_.notify_all();
  }

  void SetCallBack(const std::string& point,
                   const std::function<void(void*)>& callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callbacks_[point] = callback;
    point_table_.insert(point);
  }

  void ClearAllCallBacks() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (num_callbacks_running_ > 0) {
      cv_.wait(lock);
    }
    callbacks_.clear();
  }

  bool IsClearedAllproducers(const std::string& point) {
    for (const std::string& producer : producers_[point]) {
      if (cleared_points_.find(producer) == cleared_points_.end()) return false;
    }
    return true;
  }

  void ClearDependTrace() {
    std::lock_guard<std::mutex> lock(mutex_);
    cleared_points_.clear();
  }

  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    cleared_points_.clear();
    consumers_.clear();
    producers_.clear();
    callbacks_.clear();
    point_table_.clear();
    num_callbacks_running_ = 0;
  }

  virtual ~SyncImpl() {}

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> ready_;
  std::unordered_set<std::string> point_table_;
  int num_callbacks_running_ = 0;

  std::unordered_map<std::string, std::vector<std::string>> consumers_;
  std::unordered_map<std::string, std::vector<std::string>> producers_;
  std::unordered_set<std::string> cleared_points_;
  std::unordered_map<std::string, std::function<void(void*)>> callbacks_;
};

}  // namespace KVDK_NAMESPACE

#endif