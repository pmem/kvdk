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

namespace KVDK_NAMESPACE {

class SyncPoint {
public:
  static SyncPoint *GetInstance();
  SyncPoint(const SyncPoint &) = delete;
  SyncPoint &operator=(const SyncPoint &) = delete;

  ~SyncPoint();

  void LoadDependency(const std::vector<SyncPointPair> &dependencies) {
    sync_impl_->LoadDependency(dependencies);
  }

  void EnableProcessing() { sync_impl_->EnableProcessing(); }

  void DisableProcessing() { sync_impl_->DisableProcessing(); }

  void SetCallBack(const std::string &point,
                   const std::function<void(void *)> &callback) {
    sync_impl_->SetCallBack(point, callback);
  }

  void ClearAllCallBacks() { sync_impl_->ClearAllCallBacks(); }

  void ClearDependTrace() { sync_impl_->ClearDependTrace(); }

  void Process(const std::string &point, void *func_arg = nullptr);

  void Init() { sync_impl_->Init(); }

private:
  SyncPoint();
  SyncImpl *sync_impl_;
};

SyncPoint *SyncPoint::GetInstance() {
  static SyncPoint sync_point;
  return &sync_point;
}
SyncPoint::SyncPoint() : sync_impl_(new SyncImpl){};

SyncPoint::~SyncPoint() { delete sync_impl_; }

void SyncPoint::Process(const std::string &point, void *func_arg) {
  sync_impl_->Process(point, func_arg);
}

} // namespace KVDK_NAMESPACE

#define TEST_SYNC_POINT(x) KVDK_NAMESPACE::SyncPoint::GetInstance()->Process(x)
#define TEST_SYNC_POINT_CALLBACK(x, y)                                         \
  KVDK_NAMESPACE::SyncPoint::GetInstance()->Process(x, y)
