/* Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 * This source code is licensed under both the GPLv2 (found in the
 * COPYING file in the root directory) and Apache 2.0 License
 * (found in the LICENSE.Apache file in the root directory).
 */

/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "sync_impl.hpp"

#if KVDK_DEBUG_LEVEL > 0

namespace KVDK_NAMESPACE {

/* SyncPoint Guide:
 * Developer could specify sync points in the codebase by TEST_SYNC_POINT.
 * Each sync point represents a position in the execution stream of a thread.
 * In the uint test, developer can set the relationship between the sync point
 * by LoadDependency to reproduce a desired interleave of threads execution.
 * Also can set the execution of the sync point by SetCallBack. Please see the
 * example in the unit tests.
 */
class SyncPoint {
 public:
  static SyncPoint* GetInstance();
  SyncPoint(const SyncPoint&) = delete;
  SyncPoint& operator=(const SyncPoint&) = delete;

  ~SyncPoint();

  void LoadDependency(const std::vector<SyncPointPair>& dependencies);
  void EnableProcessing();
  void DisableProcessing();

  void SetCallBack(const std::string& point,
                   const std::function<void(void*)>& callback);

  void ClearAllCallBacks();

  void ClearDependTrace();

  void Process(const std::string& point, void* func_arg = nullptr);

  void Reset();

 private:
  SyncPoint();
  SyncImpl* sync_impl_;
};

}  // namespace KVDK_NAMESPACE

#define TEST_SYNC_POINT(x) KVDK_NAMESPACE::SyncPoint::GetInstance()->Process(x)
#define TEST_SYNC_POINT_CALLBACK(x, y) \
  KVDK_NAMESPACE::SyncPoint::GetInstance()->Process(x, y)

#else
#define TEST_SYNC_POINT(x)
#define TEST_SYNC_POINT_CALLBACK(x, y)
#endif