/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include "kvdk_c.hpp"

extern "C" {
KVDKWriteBatch* KVDKWriteBatchCreate(KVDKEngine* engine) {
  KVDKWriteBatch* batch = new KVDKWriteBatch{};
  batch->rep = engine->rep->WriteBatchCreate();
  return batch;
}

void KVDKWriteBatchDestory(KVDKWriteBatch* batch) { delete batch; }

void KVDKWRiteBatchClear(KVDKWriteBatch* batch) { batch->rep->Clear(); }

void KVDKWriteBatchStringPut(KVDKWriteBatch* batch, char const* key_data,
                             size_t key_len, char const* val_data,
                             size_t val_len) {
  batch->rep->StringPut(std::string{key_data, key_len},
                        std::string{val_data, val_len});
}

void KVDKWriteBatchStringDelete(KVDKWriteBatch* batch, char const* key_data,
                                size_t key_len) {
  batch->rep->StringDelete(std::string{key_data, key_len});
}

void KVDKWriteBatchSortedPut(KVDKWriteBatch* batch, char const* key_data,
                             size_t key_len, char const* field_data,
                             size_t field_len, char const* val_data,
                             size_t val_len) {
  batch->rep->SortedPut(std::string{key_data, key_len},
                        std::string{field_data, field_len},
                        std::string{val_data, val_len});
}

void KVDKWriteBatchSortedDelete(KVDKWriteBatch* batch, char const* key_data,
                                size_t key_len, char const* field_data,
                                size_t field_len) {
  batch->rep->SortedDelete(std::string{key_data, key_len},
                           std::string{field_data, field_len});
}

void KVDKWriteBatchHashPut(KVDKWriteBatch* batch, char const* key_data,
                           size_t key_len, char const* field_data,
                           size_t field_len, char const* val_data,
                           size_t val_len) {
  batch->rep->HashPut(std::string{key_data, key_len},
                      std::string{field_data, field_len},
                      std::string{val_data, val_len});
}

void KVDKWriteBatchHashDelete(KVDKWriteBatch* batch, char const* key_data,
                              size_t key_len, char const* field_data,
                              size_t field_len) {
  batch->rep->HashDelete(std::string{key_data, key_len},
                         std::string{field_data, field_len});
}

KVDKStatus KVDKBatchWrite(KVDKEngine* engine, KVDKWriteBatch const* batch) {
  return engine->rep->BatchWrite(batch->rep);
}

}  // extern "C"
