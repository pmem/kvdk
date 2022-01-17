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

#include "sync_impl.hpp"

#if DEBUG_LEVEL > 0

namespace KVDK_NAMESPACE {
class SyncPoint {
public:
  static SyncPoint *GetInstance();
  SyncPoint(const SyncPoint &) = delete;
  SyncPoint &operator=(const SyncPoint &) = delete;

  ~SyncPoint();

  void LoadDependency(const std::vector<SyncPointPair> &dependencies);
  void EnableProcessing();
  void DisableProcessing();

  void SetCallBack(const std::string &point,
                   const std::function<void(void *)> &callback);

  void ClearAllCallBacks();

  void ClearDependTrace();

  void Process(const std::string &point, void *func_arg = nullptr);

  void Init();

private:
  SyncPoint();
  SyncImpl *sync_impl_;
};

} // namespace KVDK_NAMESPACE

#define TEST_SYNC_POINT(x) KVDK_NAMESPACE::SyncPoint::GetInstance()->Process(x)
#define TEST_SYNC_POINT_CALLBACK(x, y)                                         \
  KVDK_NAMESPACE::SyncPoint::GetInstance()->Process(x, y)

#else
#define TEST_SYNC_POINT(x)
#define TEST_SYNC_POINT_CALLBACK(x, y)
#endif