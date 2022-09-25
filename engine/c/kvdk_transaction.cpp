/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include "kvdk_c.hpp"

extern "C" {
KVDKTransaction* KVDKTransactionCreate(KVDKEngine* engine) {
  KVDKTransaction* txn = new KVDKTransaction{};
  txn->rep = engine->rep->TransactionCreate();
  return txn;
}
void KVDKTransactionDestory(KVDKTransaction* txn) { delete txn; }
KVDKStatus KVDKTransactionStringPut(KVDKTransaction* txn, char const* key_data,
                                    size_t key_len, char const* val_data,
                                    size_t val_len) {
  return txn->rep->StringPut(std::string(key_data, key_len),
                             std::string(val_data, val_len));
}
KVDKStatus KVDKTransactionStringDelete(KVDKTransaction* txn,
                                       char const* key_data, size_t key_len) {
  return txn->rep->StringDelete(std::string(key_data, key_len));
}
KVDKStatus KVDKTransactionSortedPut(KVDKTransaction* txn,
                                    char const* collection,
                                    size_t collection_len, char const* key_data,
                                    size_t key_len, char const* val_data,
                                    size_t val_len) {
  return txn->rep->SortedPut(std::string(collection, collection_len),
                             std::string(key_data, key_len),
                             std::string(val_data, val_len));
}

KVDKStatus KVDKTransactionSortedDelete(KVDKTransaction* txn,
                                       char const* collection,
                                       size_t collection_len,
                                       char const* key_data, size_t key_len) {
  return txn->rep->SortedDelete(std::string(collection, collection_len),
                                std::string(key_data, key_len));
}
KVDKStatus KVDKTransactionHashPut(KVDKTransaction* txn, char const* collection,
                                  size_t collection_len, char const* key_data,
                                  size_t key_len, char const* val_data,
                                  size_t val_len) {
  return txn->rep->HashPut(std::string(collection, collection_len),
                           std::string(key_data, key_len),
                           std::string(val_data, val_len));
}
KVDKStatus KVDKTransactionHashDelete(KVDKTransaction* txn,
                                     char const* collection,
                                     size_t collection_len,
                                     char const* key_data, size_t key_len) {
  return txn->rep->HashDelete(std::string(collection, collection_len),
                              std::string(key_data, key_len));
}
KVDKStatus KVDKTransactionCommit(KVDKTransaction* txn) {
  return txn->rep->Commit();
}
void KVDKTransactionRollback(KVDKTransaction* txn) {
  return txn->rep->Rollback();
}

}  // extern "C"
