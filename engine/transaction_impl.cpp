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
  if (!tryLock(hash_table->GetLock(key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->StringPut(key, value);
  return Status::Ok;
}

Status TransactionImpl::StringDelete(const std::string& key) {
  auto hash_table = engine_->GetHashTable();
  if (!tryLock(hash_table->GetLock(key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->StringDelete(key);
  return Status::Ok;
}

Status TransactionImpl::StringGet(const std::string& key, std::string* value) {
  auto op = batch_->StringGet(key);
  if (op != nullptr) {
    if (op->op == WriteOp::Delete) {
      return Status::NotFound;
    } else {
      value->assign(op->value);
      return Status::Ok;
    }
  } else {
    auto hash_table = engine_->GetHashTable();
    if (!tryLock(hash_table->GetLock(key))) {
      status_ = Status::Timeout;
      return status_;
    }
    return engine_->Get(key, value);
  }
}

Status TransactionImpl::SortedPut(const std::string& collection,
                                  const std::string& key,
                                  const std::string& value) {
  auto hash_table = engine_->GetHashTable();
  // TODO not hold collection lock in transaciton(r/w lock?)
  if (!tryLock(hash_table->GetLock(collection))) {
    status_ = Status::Timeout;
    return status_;
  }
  auto lookup_result =
      hash_table->Lookup<false>(collection, RecordType::SortedRecord);
  if (lookup_result.s != Status::Ok) {
    kvdk_assert(lookup_result.s == Status::NotFound, "");
    return lookup_result.s;
  }
  Skiplist* skiplist = lookup_result.entry.GetIndex().skiplist;
  auto internal_key = skiplist->InternalKey(key);
  if (!tryLock(hash_table->GetLock(internal_key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->SortedPut(collection, key, value);
  return Status::Ok;
}

Status TransactionImpl::SortedDelete(const std::string& collection,
                                     const std::string& key) {
  auto hash_table = engine_->GetHashTable();
  // TODO not hold collection lock in transaciton(r/w lock?)
  if (!tryLock(hash_table->GetLock(collection))) {
    status_ = Status::Timeout;
    return status_;
  }
  auto lookup_result =
      hash_table->Lookup<false>(collection, RecordType::SortedRecord);
  if (lookup_result.s != Status::Ok) {
    kvdk_assert(lookup_result.s == Status::NotFound, "");
    return Status::Ok;
  }
  Skiplist* skiplist = lookup_result.entry.GetIndex().skiplist;
  auto internal_key = skiplist->InternalKey(key);
  if (!tryLock(hash_table->GetLock(internal_key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->SortedDelete(collection, key);
  return Status::Ok;
}

Status TransactionImpl::SortedGet(const std::string& collection,
                                  const std::string& key, std::string* value) {
  auto op = batch_->SortedGet(collection, key);
  if (op != nullptr) {
    if (op->op == WriteOp::Delete) {
      return Status::NotFound;
    } else {
      value->assign(op->value);
      return Status::Ok;
    }
  } else {
    auto hash_table = engine_->GetHashTable();
    // TODO not hold collection lock in transaciton(r/w lock?)
    if (!tryLock(hash_table->GetLock(collection))) {
      status_ = Status::Timeout;
      return status_;
    }
    auto lookup_result =
        hash_table->Lookup<false>(collection, RecordType::SortedRecord);
    if (lookup_result.s != Status::Ok) {
      GlobalLogger.Debug("collection not found\n");
      kvdk_assert(lookup_result.s == Status::NotFound, "");
      return lookup_result.s;
    }
    Skiplist* skiplist = lookup_result.entry.GetIndex().skiplist;
    auto internal_key = skiplist->InternalKey(key);
    if (!tryLock(hash_table->GetLock(internal_key))) {
      status_ = Status::Timeout;
      return status_;
    }

    GlobalLogger.Debug("skiplist get\n");
    return skiplist->Get(key, value);
  }
}

Status TransactionImpl::HashPut(const std::string& collection,
                                const std::string& key,
                                const std::string& value) {
  return Status::Ok;
}

Status TransactionImpl::HashDelete(const std::string& collection,
                                   const std::string& key) {
  return Status::Ok;
}

Status TransactionImpl::HashGet(const std::string& collection,
                                const std::string& key, std::string* value) {
  return Status::Ok;
}

bool TransactionImpl::tryLock(SpinMutex* spin) {
  auto iter = locked_.find(spin);
  if (iter == locked_.end()) {
    if (tryLockImpl(spin)) {
      locked_.insert(spin);
      return true;
    }
    return false;
  } else {
    return true;
  }
}

bool TransactionImpl::tryLockImpl(SpinMutex* spin) {
  auto now = TimeUtils::millisecond_time();
  while (!spin->try_lock()) {
    if (TimeUtils::millisecond_time() - now > kLockTimeoutMilliseconds) {
      return false;
    }
  }
  return true;
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