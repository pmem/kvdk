#include "sync_point.hpp"

#if DEBUG_LEVEL > 0

namespace KVDK_NAMESPACE {
SyncPoint *SyncPoint::GetInstance() {
  static SyncPoint sync_point;
  return &sync_point;
}
SyncPoint::SyncPoint() : sync_impl_(new SyncImpl){};

SyncPoint::~SyncPoint() { delete sync_impl_; }

void SyncPoint::Process(const std::string &point, void *func_arg) {
  sync_impl_->Process(point, func_arg);
}

void SyncPoint::LoadDependency(const std::vector<SyncPointPair> &dependencies) {
  sync_impl_->LoadDependency(dependencies);
}

void SyncPoint::EnableProcessing() { sync_impl_->EnableProcessing(); }

void SyncPoint::DisableProcessing() { sync_impl_->DisableProcessing(); }

void SyncPoint::SetCallBack(const std::string &point,
                            const std::function<void(void *)> &callback) {
  sync_impl_->SetCallBack(point, callback);
}

void SyncPoint::ClearAllCallBacks() { sync_impl_->ClearAllCallBacks(); }

void SyncPoint::ClearDependTrace() { sync_impl_->ClearDependTrace(); }

void SyncPoint::Init() { sync_impl_->Init(); }

} // namespace KVDK_NAMESPACE

#endif
