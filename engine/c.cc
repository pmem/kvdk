/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "kvdk/configs.hpp"
#include "kvdk/engine.h"
#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"
#include "kvdk/write_batch.hpp"

using kvdk::Configs;
using kvdk::Engine;
using kvdk::Iterator;
using kvdk::Status;
using kvdk::WriteBatch;

extern "C" {
struct KVDKConfigs {
  Configs rep;
};
struct KVDKEngine {
  Engine *rep;
};
struct KVDKWriteBatch {
  WriteBatch rep;
};
struct KVDKIterator {
  Iterator *rep;
};

static char *CopyStringToChar(const std::string &str) {
  char *result = reinterpret_cast<char *>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

static const std::string EnumToString(Status s) {
  switch (s) {
  case Status::NotFound:
    return "NotFound Key!";
  case Status::BatchOverflow:
    return "BatchOverflow!";
  case Status::InvalidConfiguration:
    return "InvalidConfiguration!";
  case Status::IOError:
    return "IOError!";
  case Status::MapError:
    return "MapError!";
  case Status::MemoryOverflow:
    return "MapOverflow!";
  case Status::NotSupported:
    return "NotSupported, maybe implemented in the future!";
  case Status::PmemOverflow:
    return "PmemOverflow!";
  case Status::TooManyWriteThreads:
    return "TooManyWriteThreads!";
  case Status::InvalidDataSize:
    return "InvalidDataSize!";
  default:
    return "Ok";
  }
}

static bool SaveError(char **errptr, const Status &s) {
  if (s == Status::Ok) {
    return false;
  } else if (*errptr == nullptr) {
    *errptr = strdup(EnumToString(s).c_str());
  } else {
    free(*errptr);
    *errptr = strdup(EnumToString(s).c_str());
  }
  return true;
}

KVDKConfigs *KVDKCreateConfigs() { return new KVDKConfigs; }

void KVDKUserConfigs(KVDKConfigs *kv_config, uint64_t max_write_threads,
                     uint64_t pmem_file_size, unsigned char populate_pmem_space,
                     uint32_t pmem_block_size, uint64_t pmem_segment_blocks,
                     uint32_t hash_bucket_size, uint64_t hash_bucket_num,
                     uint32_t num_buckets_per_slot) {
  kv_config->rep.max_write_threads = max_write_threads;
  kv_config->rep.hash_bucket_num = hash_bucket_num;
  kv_config->rep.hash_bucket_size = hash_bucket_size;
  kv_config->rep.num_buckets_per_slot = num_buckets_per_slot;
  kv_config->rep.pmem_block_size = pmem_block_size;
  kv_config->rep.pmem_file_size = pmem_file_size;
  kv_config->rep.pmem_segment_blocks = pmem_segment_blocks;
  kv_config->rep.populate_pmem_space = populate_pmem_space;
}

void KVDKConigsDestory(KVDKConfigs *kv_config) { delete kv_config; }

KVDKEngine *KVDKOpen(const char *name, const KVDKConfigs *config,
                     FILE *log_file, char **error) {
  Engine *engine;
  if (SaveError(error, Engine::Open(std::string(name), &engine, config->rep,
                                    log_file)))
    engine = nullptr;
  KVDKEngine *kv_engine = new KVDKEngine;
  kv_engine->rep = engine;
  return kv_engine;
}

void KVDKReleaseWriteThread(KVDKEngine *engine) {
  engine->rep->ReleaseWriteThread();
}

void KVDKCloseEngine(KVDKEngine *engine) {
  delete engine->rep;
  delete engine;
}

void KVDKRemovePMemContents(const char *name) {
  std::string res = "rm -rf " + std::string(name) + "\n";
  int ret __attribute__((unused)) = system(res.c_str());
}

KVDKWriteBatch *KVDKWriteBatchCreate(void) { return new KVDKWriteBatch; }

void KVDKWriteBatchDelete(KVDKWriteBatch *wb, const char *key, size_t key_len) {
  wb->rep.Delete(std::string(key, key_len));
}

void KVDKWriteBatchPut(KVDKWriteBatch *wb, const char *key, size_t key_len,
                       const char *value, size_t value_len) {
  wb->rep.Put(std::string(key, key_len), std::string(value, value_len));
}

void KVDKWrite(KVDKEngine *engine, const KVDKWriteBatch *batch, char **error) {
  SaveError(error, engine->rep->BatchWrite(batch->rep));
}

void KVDKWriteBatchDestory(KVDKWriteBatch *wb) { delete wb; }

char *KVDKGet(KVDKEngine *engine, const char *key, size_t key_len,
              size_t *val_len, char **error) {
  std::string val_str;
  char *val = nullptr;
  Status s = engine->rep->Get(pmem::obj::string_view(key, key_len), &val_str);
  if (s == Status::Ok) {
    *val_len = val_str.size();
    val = CopyStringToChar(val_str);
  } else {
    *val_len = 0;
    SaveError(error, s);
  }
  return val;
}

void KVDKSet(KVDKEngine *engine, const char *key, size_t key_len,
             const char *val, size_t val_len, char **error) {
  SaveError(error, engine->rep->Set(pmem::obj::string_view(key, key_len),
                                    pmem::obj::string_view(val, val_len)));
}

void KVDKDelete(KVDKEngine *engine, const char *key, size_t key_len,
                char **error) {
  SaveError(error, engine->rep->Delete(pmem::obj::string_view(key, key_len)));
}

void KVDKSortedSet(KVDKEngine *engine, const char *collection,
                   size_t collection_len, const char *key, size_t key_len,
                   const char *val, size_t val_len, char **error) {
  SaveError(error, engine->rep->SSet(
                       pmem::obj::string_view(collection, collection_len),
                       pmem::obj::string_view(key, key_len),
                       pmem::obj::string_view(val, val_len)));
}

char *KVDKSortedGet(KVDKEngine *engine, const char *collection,
                    size_t collection_len, const char *key, size_t key_len,
                    size_t *val_len, char **error) {
  std::string val_str;
  char *val = nullptr;
  Status s =
      engine->rep->SGet(pmem::obj::string_view(collection, collection_len),
                        pmem::obj::string_view(key, key_len), &val_str);
  if (s == Status::Ok) {

    *val_len = val_str.size();
    val = CopyStringToChar(val_str);
  } else {
    *val_len = 0;
    SaveError(error, s);
  }
  return val;
}

void KVDKSortedDelete(KVDKEngine *engine, const char *collection,
                      size_t collection_len, const char *key, size_t key_len,
                      char **error) {
  SaveError(error, engine->rep->SDelete(
                       pmem::obj::string_view(collection, collection_len),
                       pmem::obj::string_view(key, key_len)));
}

KVDKIterator *KVDKCreateIterator(KVDKEngine *engine, const char *collection) {
  KVDKIterator *result = new KVDKIterator;
  result->rep = (engine->rep->NewSortedIterator(std::string(collection))).get();
  assert(result->rep != nullptr && "Create Sorted Iterator Failed!");
  return result;
}

void KVDKIterSeekToFirst(KVDKIterator *iter) { iter->rep->SeekToFirst(); }

void KVDKIterSeek(KVDKIterator *iter, const char *key) {
  iter->rep->Seek(std::string(key));
}

unsigned char KVDKIterValid(KVDKIterator *iter) { return iter->rep->Valid(); }

void KVDKIterNext(KVDKIterator *iter) { iter->rep->Next(); }

void KVDKIterPre(KVDKIterator *iter) { iter->rep->Prev(); }

const char *KVDKIterKey(KVDKIterator *iter) { return iter->rep->Key().data(); }

const char *KVDKIterValue(KVDKIterator *iter) {
  return iter->rep->Value().data();
}

void KVDKIterDestory(KVDKIterator *iter) { delete iter; }
}
