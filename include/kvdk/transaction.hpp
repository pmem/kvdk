/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <string>

#include "types.hpp"

namespace KVDK_NAMESPACE {
// This struct is used to do transaction operations. A transaction struct is
// assotiated with a kvdk instance
class Transaction {
 public:
  // Put a STRING-type KV to the transaction
  //
  // Return:
  // Status::Ok on success
  // Status::Timeout on conflict and long-time lock contention
  virtual Status StringPut(const StringView key, const StringView value) = 0;
  // Delete a STRING-type key to the transaction
  //
  // Return:
  // Status::Ok on success
  // Status::Timeout on conflict and long-time lock contention
  virtual Status StringDelete(const StringView key) = 0;
  // Get value of a STRING-type KV. It will first get from the transaction
  // operations (Put/Delete), then the kvdk instance of the transaction
  //
  // Return:
  // Status::Ok on success and store value to "*value"
  // Status::NotFound if key not existed or be deleted by this transaction
  // Status::Timeout on conflict and long-time lock contention
  virtual Status StringGet(const StringView key, std::string* value) = 0;
  // Put a KV of sorted collection to the transaction
  //
  // Return:
  // Status::Ok on success
  // Status::NotFound if collection does not exist
  // Status::Timeout on conflict and long-time lock contention
  virtual Status SortedPut(const StringView collection, const StringView key,
                           const StringView value) = 0;
  // Delete a KV from sorted collection to the trnasaction.
  //
  // Return:
  // Status::Ok on success
  // Status::NotFound if collection does not exist
  // Status::Timeout on conflict and long-time lock contention
  virtual Status SortedDelete(const StringView collection,
                              const StringView key) = 0;
  // Get value of a KV from sorted collection. It will first get from the
  // transaction operations (Put/Delete), then the kvdk instance of the
  // transaction
  //
  // Return:
  // Status::Ok and store value to "*value" on success
  // Status::NotFound if collection or key does not exist
  // Status::Timeout on conflict and long-time lock contention
  virtual Status SortedGet(const StringView collection, const StringView key,
                           std::string* value) = 0;
  // Put a KV of hash collection to the transaction
  //
  // Return:
  // Status::Ok on success
  // Status::NotFound if collection does not exist
  // Status::Timeout on conflict and long-time lock contention
  virtual Status HashPut(const StringView collection, const StringView key,
                         const StringView value) = 0;
  // Delete a KV from hash collection to the transaction
  //
  // Return:
  // Status::Ok on success
  // Status::NotFound if collection does not exist
  // Status::Timeout on conflict and long-time lock contention
  virtual Status HashDelete(const StringView collection,
                            const StringView key) = 0;

  // Get value of a KV from hash collection. It will first get from the
  // transaction operations (Put/Delete), then the kvdk instance of the
  // transaction
  //
  // Return:
  // Status::Ok and store value to "*value" on success
  // Status::NotFound if collection or key does not exist
  // Status::Timeout on conflict and long-time lock contention
  virtual Status HashGet(const StringView collection, const StringView key,
                         std::string* value) = 0;

  // Commit all operations of the transaction to the kvdk instance, and
  // release all locks it holds
  //
  // Return:
  // Status::Ok on success, all operations will be persistent on the instance
  // Status::PMemOverflow/Status::MemoryOverflow if PMem/DRAM exhausted
  virtual Status Commit() = 0;

  // Rollback all operations of the transaction, release locks it holds
  virtual void Rollback() = 0;

  // Return status of the last transaction operation
  virtual Status InternalStatus() = 0;

  virtual ~Transaction() = default;
};
}  // namespace KVDK_NAMESPACE