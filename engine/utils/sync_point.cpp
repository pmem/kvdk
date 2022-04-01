/* Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 * This source code is licensed under both the GPLv2 (found in the
 * COPYING file in the root directory) and Apache 2.0 License
 * (found in the LICENSE.Apache file in the root directory).
 */

/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "sync_point.hpp"

#if KVDK_DEBUG_LEVEL > 0

namespace KVDK_NAMESPACE {
SyncPoint* SyncPoint::GetInstance() {
  static SyncPoint sync_point;
  return &sync_point;
}
SyncPoint::SyncPoint() : sync_impl_(new SyncImpl){};

SyncPoint::~SyncPoint() { delete sync_impl_; }

void SyncPoint::Process(const std::string& point, void* func_arg) {
  sync_impl_->Process(point, func_arg);
}

void SyncPoint::LoadDependency(const std::vector<SyncPointPair>& dependencies) {
  sync_impl_->LoadDependency(dependencies);
}

void SyncPoint::EnableProcessing() { sync_impl_->EnableProcessing(); }

void SyncPoint::DisableProcessing() { sync_impl_->DisableProcessing(); }

void SyncPoint::SetCallBack(const std::string& point,
                            const std::function<void(void*)>& callback) {
  sync_impl_->SetCallBack(point, callback);
}

void SyncPoint::ClearAllCallBacks() { sync_impl_->ClearAllCallBacks(); }

void SyncPoint::ClearDependTrace() { sync_impl_->ClearDependTrace(); }

void SyncPoint::Reset() { sync_impl_->Reset(); }

}  // namespace KVDK_NAMESPACE

#endif
