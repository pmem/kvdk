/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <condition_variable>
#include <memory>
#include <unordered_map>

#include "alias.hpp"
#include "kvdk/persistent/transaction.hpp"
#include "write_batch_impl.hpp"

namespace KVDK_NAMESPACE {

class KVEngine;

// Collections of in processing transaction should not be created or
// destroyed, we use this for communication between collection related
// transactions and collection create/destroy threads
class CollectionTransactionCV {
 public:
  struct TransactionToken {
    TransactionToken(CollectionTransactionCV* cv) : cv_(cv) {
      kvdk_assert(cv_ != nullptr, "");
      cv_->AcquireTransaction();
    }

    TransactionToken(const TransactionToken&) = delete;
    TransactionToken(TransactionToken&&) = delete;

    ~TransactionToken() { cv_->FinishTransaction(); }

   private:
    CollectionTransactionCV* cv_;
  };

  struct CollectionToken {
    CollectionToken(CollectionTransactionCV* cv) : cv_(cv) {
      kvdk_assert(cv_ != nullptr, "");
      cv_->AcquireCollection();
    }

    ~CollectionToken() { cv_->FinishCollection(); }

    CollectionToken(const CollectionToken&) = delete;
    CollectionToken(CollectionToken&&) = delete;

   private:
    CollectionTransactionCV* cv_;
  };

  CollectionTransactionCV() = default;

  CollectionTransactionCV(const CollectionTransactionCV&) = delete;

  void AcquireTransaction() {
    std::unique_lock<SpinMutex> ul(spin_);
    while (processing_collection_ > 0) {
      cv_.wait(ul);
    }
    kvdk_assert(processing_collection_ == 0, "");
    processing_transaction_++;
  }

  void AcquireCollection() {
    std::unique_lock<SpinMutex> ul(spin_);
    // collection create/destroy has higher priority than txn as it's faster,
    // so we add processing cnt here to forbit hungry
    processing_collection_++;
    while (processing_transaction_ > 0) {
      cv_.wait(ul);
    }
    kvdk_assert(processing_transaction_ == 0, "");
  }

  void FinishTransaction() {
    std::unique_lock<SpinMutex> ul(spin_);
    if (--processing_transaction_ == 0) {
      cv_.notify_all();
    }
  }

  void FinishCollection() {
    std::unique_lock<SpinMutex> ul(spin_);
    if (--processing_collection_ == 0) {
      cv_.notify_all();
    }
  }

 private:
  std::condition_variable_any cv_;
  SpinMutex spin_;
  int processing_collection_ = 0;
  int processing_transaction_ = 0;
};

class TransactionImpl final : public Transaction {
 public:
  TransactionImpl(KVEngine* engine);
  ~TransactionImpl() final;
  Status StringPut(const StringView key, const StringView value) final;
  Status StringDelete(const StringView key) final;
  Status StringGet(const StringView key, std::string* value) final;
  Status SortedPut(const StringView collection, const StringView key,
                   const StringView value) final;
  Status SortedDelete(const StringView collection, const StringView key) final;
  Status SortedGet(const StringView collection, const StringView key,
                   std::string* value) final;
  Status HashPut(const StringView collection, const StringView key,
                 const StringView value) final;
  Status HashDelete(const StringView collection, const StringView key) final;
  Status HashGet(const StringView collection, const StringView key,
                 std::string* value) final;
  Status Commit() final;
  void Rollback() final;
  Status InternalStatus() final { return status_; }

  // This used by kv engine
  WriteBatchImpl* GetBatch() { return batch_.get(); }

  // Set lock time out while acquiring a key lock in transaction, if <0,
  // operations will immediately return timeout while failed to lock a key
  void SetLockTimeout(int64_t micro_seconds);

 private:
  struct KVOp {
    WriteOp op;
    std::string value;
  };

  bool tryLock(SpinMutex* spin);
  bool tryLockImpl(SpinMutex* spin);
  void acquireCollectionTransaction();
  int64_t randomTimeout();

  KVEngine* engine_;
  Status status_;
  std::unordered_map<std::string, std::unordered_map<std::string, KVOp>>
      sorted_kv_;
  std::unordered_map<std::string, std::unordered_map<std::string, KVOp>>
      hash_kv_;
  std::unordered_map<std::string, KVOp> string_kv_;
  std::unique_ptr<WriteBatchImpl> batch_;
  // TODO use std::unique_lock
  std::unordered_set<SpinMutex*> locked_;
  std::unique_ptr<CollectionTransactionCV::TransactionToken> ct_token_;
  int64_t timeout_;
};
}  // namespace KVDK_NAMESPACE