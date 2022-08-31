/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "transaction_impl.hpp"

#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {
TransactionImpl::TransactionImpl(KVEngine* engine) : engine_(engine) {
  kvdk_assert(engine != nullptr, "");
  batch_ = engine_->WriteBatchCreate();
}
}  // namespace KVDK_NAMESPACE