/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <memory>

#include "kvdk/transaction.hpp"
#include "kvdk/write_batch.hpp"

class KVEngine;

namespace KVDK_NAMESPACE {
class TransactionImpl final : public Transaction {
 public:
  TransactionImpl(KVEngine* engine);
  void StringPut(const std::string& key, const std::string& value) final;
  void StringDelete(const std::string& key) final;

 private:
  std::unique_ptr<WriteBatch> batch_;
  KVEngine* engine_;
};
}  // namespace KVDK_NAMESPACE