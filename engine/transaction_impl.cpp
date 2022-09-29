/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "transaction_impl.hpp"

#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {
// To avoid deadlock in transaction, we abort locking key in
// random(kLockTimeoutMicrosecondsMin,kLockTimeoutMicrosecondsMax) and
// return Timeout in write operations
constexpr int64_t kLockTimeoutMicrosecondsMin = 5000;
constexpr int64_t kLockTimeoutMicrosecondsMax = 15000;

TransactionImpl::TransactionImpl(KVEngine* engine)
    : engine_(engine), timeout_(randomTimeout()) {
  kvdk_assert(engine_ != nullptr, "");
  batch_.reset(
      dynamic_cast<WriteBatchImpl*>(engine_->WriteBatchCreate().release()));
  kvdk_assert(batch_ != nullptr, "");
}

TransactionImpl::~TransactionImpl() { Rollback(); }

Status TransactionImpl::StringPut(const StringView key,
                                  const StringView value) {
  auto hash_table = engine_->GetHashTable();
  if (!tryLock(hash_table->GetLock(key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->StringPut(key, value);
  return Status::Ok;
}

Status TransactionImpl::StringDelete(const StringView key) {
  auto hash_table = engine_->GetHashTable();
  if (!tryLock(hash_table->GetLock(key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->StringDelete(key);
  return Status::Ok;
}

Status TransactionImpl::StringGet(const StringView key, std::string* value) {
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

Status TransactionImpl::SortedPut(const StringView collection,
                                  const StringView key,
                                  const StringView value) {
  acquireCollectionTransaction();
  auto hash_table = engine_->GetHashTable();
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

Status TransactionImpl::SortedDelete(const StringView collection,
                                     const StringView key) {
  acquireCollectionTransaction();
  auto hash_table = engine_->GetHashTable();
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

  batch_->SortedDelete(collection, key);
  return Status::Ok;
}

Status TransactionImpl::SortedGet(const StringView collection,
                                  const StringView key, std::string* value) {
  auto op = batch_->SortedGet(collection, key);
  if (op != nullptr) {
    if (op->op == WriteOp::Delete) {
      return Status::NotFound;
    } else {
      value->assign(op->value);
      return Status::Ok;
    }
  } else {
    acquireCollectionTransaction();
    auto hash_table = engine_->GetHashTable();
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

    return skiplist->Get(key, value);
  }
}

Status TransactionImpl::HashPut(const StringView collection,
                                const StringView key, const StringView value) {
  acquireCollectionTransaction();
  auto hash_table = engine_->GetHashTable();
  auto lookup_result =
      hash_table->Lookup<false>(collection, RecordType::HashRecord);
  if (lookup_result.s != Status::Ok) {
    kvdk_assert(lookup_result.s == Status::NotFound, "");
    return lookup_result.s;
  }
  HashList* hlist = lookup_result.entry.GetIndex().hlist;
  auto internal_key = hlist->InternalKey(key);
  if (!tryLock(hash_table->GetLock(internal_key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->HashPut(collection, key, value);
  return Status::Ok;
}

Status TransactionImpl::HashDelete(const StringView collection,
                                   const StringView key) {
  acquireCollectionTransaction();
  auto hash_table = engine_->GetHashTable();
  auto lookup_result =
      hash_table->Lookup<false>(collection, RecordType::HashRecord);
  if (lookup_result.s != Status::Ok) {
    kvdk_assert(lookup_result.s == Status::NotFound, "");
    return lookup_result.s;
  }
  HashList* hlist = lookup_result.entry.GetIndex().hlist;
  auto internal_key = hlist->InternalKey(key);
  if (!tryLock(hash_table->GetLock(internal_key))) {
    status_ = Status::Timeout;
    return status_;
  }

  batch_->HashDelete(collection, key);
  return Status::Ok;
}

Status TransactionImpl::HashGet(const StringView collection,
                                const StringView key, std::string* value) {
  auto op = batch_->HashGet(collection, key);
  if (op != nullptr) {
    if (op->op == WriteOp::Delete) {
      return Status::NotFound;
    } else {
      value->assign(op->value);
      return Status::Ok;
    }
  } else {
    acquireCollectionTransaction();
    auto hash_table = engine_->GetHashTable();
    auto lookup_result =
        hash_table->Lookup<false>(collection, RecordType::HashRecord);
    if (lookup_result.s != Status::Ok) {
      kvdk_assert(lookup_result.s == Status::NotFound, "");
      return lookup_result.s;
    }
    HashList* hlist = lookup_result.entry.GetIndex().hlist;
    auto internal_key = hlist->InternalKey(key);
    if (!tryLock(hash_table->GetLock(internal_key))) {
      status_ = Status::Timeout;
      return status_;
    }

    return hlist->Get(key, value);
  }
}

void TransactionImpl::acquireCollectionTransaction() {
  if (ct_token_ == nullptr) {
    ct_token_ = engine_->AcquireCollectionTransactionLock();
  }
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
  auto now = TimeUtils::microseconds_time();
  while (!spin->try_lock()) {
    if (TimeUtils::microseconds_time() - now > timeout_) {
      return false;
    }
  }
  return true;
}

Status TransactionImpl::Commit() {
  Status s = engine_->CommitTransaction(this);
  Rollback();
  return s;
}

void TransactionImpl::Rollback() {
  for (SpinMutex* s : locked_) {
    s->unlock();
  }
  locked_.clear();
  string_kv_.clear();
  sorted_kv_.clear();
  hash_kv_.clear();
  batch_->Clear();
  ct_token_ = nullptr;
}

void TransactionImpl::SetLockTimeout(int64_t microseconds) {
  timeout_ = microseconds;
}

int64_t TransactionImpl::randomTimeout() {
  return fast_random_64() %
             (kLockTimeoutMicrosecondsMax - kLockTimeoutMicrosecondsMin) +
         kLockTimeoutMicrosecondsMin;
}
}  // namespace KVDK_NAMESPACE