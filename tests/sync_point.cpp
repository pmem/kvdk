
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 * Copyright (c) 2011 The Rocksdb Authors. All rights reserved.
 */

#include "test_sync_point.h"

#ifndef NDEBUG
namespace KVDK_NAMESPACE {
namespace TEST_UTILS {
SyncPoint *SyncPoint::GetInstance() {
  static SyncPoint sync_point;
  return &sync_point;
}

SyncPoint::SyncPoint() : sync_impl_(new SyncImpl()) {}

SyncPoint::~SyncPoint() { delete sync_impl_; }

void SyncImpl::LoadDependency(const std::vector<SyncPointPair> &dependencies) {
  std::lock_guard<std::mutex> lock(mutex_);
  successors_.clear();
  predecessors_.clear();
  cleared_points_.clear();
  for (const auto &dependency : dependencies) {
    successors_[dependency.predecessor].push_back(dependency.successor);
    predecessors_[dependency.successor].push_back(dependency.predecessor);
    point_table_.insert(dependency.successor);
    point_table_.insert(dependency.predecessor);
  }
  cv_.notify_all();
}

void SyncImpl::Process(const std::string& point, void* cb_arg) {
  if (!ready_) {
    return;
  }
  
  if (!point_table_.find(point)) {
    return;
  }

  // Take heap hit.
  std::unique_lock<std::mutex> lock(mutex_);
  auto thread_id = std::this_thread::get_id();

  auto marker_iter = markers_.find(point_string);
  if (marker_iter != markers_.end()) {
    for (auto& marked_point : marker_iter->second) {
      marked_thread_id_.emplace(marked_point, thread_id);
      point_filter_.Add(marked_point);
    }
  }

  if (DisabledByMarker(point_string, thread_id)) {
    return;
  }

  while (!PredecessorsAllCleared(point_string)) {
    cv_.wait(lock);
    if (DisabledByMarker(point_string, thread_id)) {
      return;
    }
  }

  auto callback_pair = callbacks_.find(point_string);
  if (callback_pair != callbacks_.end()) {
    num_callbacks_running_++;
    mutex_.unlock();
    callback_pair->second(cb_arg);
    mutex_.lock();
    num_callbacks_running_--;
  }
  cleared_points_.insert(point_string);
  cv_.notify_all();
}

} // namespace TEST_UTILS
} // namespace KVDK_NAMESPACE
#endif
