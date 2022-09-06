/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "transaction_impl.hpp"

#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {
// To avoid deadlock in transaction, we abort lock key in
// kLockTimeoutMilliseconds and return Timeout in write operations
constexpr int64_t kLockTimeoutMilliseconds = 10;

TransactionImpl::TransactionImpl(KVEngine* engine) : engine_(engine) {
  kvdk_assert(engine_ != nullptr, "");
  batch_.reset(
      dynamic_cast<WriteBatchImpl*>(engine_->WriteBatchCreate().release()));
  kvdk_assert(batch_ != nullptr, "");
}

Status TransactionImpl::StringPut(const std::string& key,
                                  const std::string& value) {
  auto hash_table = engine_->GetHashTable();
  if (!TryLock(hash_table->GetLock(key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->StringPut(key, value);
  return Status::Ok;
}

Status TransactionImpl::StringDelete(const std::string& key) {
  auto hash_table = engine_->GetHashTable();
  if (!TryLock(hash_table->GetLock(key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->StringDelete(key);
  return Status::Ok;
}

Status TransactionImpl::StringGet(const std::string& key, std::string* value) {
  auto hash_table = engine_->GetHashTable();
  if (!TryLock(hash_table->GetLock(key))) {
    status_ = Status::Timeout;
    return status_;
  }

  auto op = batch_->StringGet(key);
  if (op != nullptr) {
    if (op->op == WriteOp::Delete) {
      return Status::NotFound;
    } else {
      value->assign(op->value);
      return Status::Ok;
    }
  } else {
    return engine_->Get(key, value);
  }
}

bool TransactionImpl::TryLock(SpinMutex* spin) {
  auto iter = locked_.find(spin);
  if (iter == locked_.end()) {
    auto now = TimeUtils::millisecond_time();
    while (!spin->try_lock()) {
      if (TimeUtils::millisecond_time() - now > kLockTimeoutMilliseconds) {
        return false;
      }
    }
    locked_.insert(spin);
    return true;
  } else {
    return true;
  }
}

Status TransactionImpl::Commit() { return engine_->CommitTransaction(this); }
void TransactionImpl::Rollback() {
  for (SpinMutex* kv : locked_) {
    kv->unlock();
  }
  for (SpinMutex* s : locked_) {
    s->unlock();
  }
  locked_.clear();
  string_kv_.clear();
  sorted_kv_.clear();
  hash_kv_.clear();
}
}  // namespace KVDK_NAMESPACE