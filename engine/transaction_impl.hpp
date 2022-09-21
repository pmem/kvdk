/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <condition_variable>
#include <memory>
#include <unordered_map>

#include "alias.hpp"
#include "kvdk/transaction.hpp"
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
  Status StringPut(const std::string& key, const std::string& value) final;
  Status StringDelete(const std::string& key) final;
  Status StringGet(const std::string& key, std::string* value) final;
  Status SortedPut(const std::string& collection, const std::string& key,
                   const std::string& value) final;
  Status SortedDelete(const std::string& collection,
                      const std::string& key) final;
  Status SortedGet(const std::string& collection, const std::string& key,
                   std::string* value) final;
  Status HashPut(const std::string& collection, const std::string& key,
                 const std::string& value) final;
  Status HashDelete(const std::string& collection,
                    const std::string& key) final;
  Status HashGet(const std::string& collection, const std::string& key,
                 std::string* value) final;
  Status Commit() final;
  void Rollback() final;
  Status InternalStatus() final { return status_; }

  // This used by kv engine
  WriteBatchImpl* GetBatch() { return batch_.get(); }

 private:
  struct KVOp {
    WriteOp op;
    std::string value;
  };

  bool tryLock(SpinMutex* spin);
  bool tryLockImpl(SpinMutex* spin);
  void acquireCollectionTransaction();

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
};
}  // namespace KVDK_NAMESPACE