/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <memory>
#include <unordered_map>

#include "alias.hpp"
#include "kvdk/transaction.hpp"
#include "write_batch_impl.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

class TransactionImpl final : public Transaction {
 public:
  TransactionImpl(KVEngine* engine);
  Status StringPut(const std::string& key, const std::string& value) final;
  Status StringDelete(const std::string& key) final;
  Status StringGet(const std::string& key, std::string* value) final;
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

  bool TryLock(SpinMutex* spin);

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
};
}  // namespace KVDK_NAMESPACE