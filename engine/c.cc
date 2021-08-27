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

static void StatusAssert(Status s) {
  switch (s) {
  case Status::NotFound:
    assert(0 && "NotFound Key!");
  case Status::BatchOverflow:
    assert(0 && "BatchOverflow!");
  case Status::InvalidConfiguration:
    assert(0 && "InvalidConfiguration!");
  case Status::IOError:
    assert(0 && "IOError!");
  case Status::MapError:
    assert(0 && "MapError!");
  case Status::MemoryOverflow:
    assert(0 && "MapOverflow!");
  case Status::NotSupported:
    assert(0 && "NotSupported, maybe implemented in the future!");
  case Status::PmemOverflow:
    assert(0 && "PmemOverflow!");
  case Status::TooManyWriteThreads:
    assert(0 && "TooManyWriteThreads!");
  case Status::InvalidDataSize:
    assert(0 && "InvalidDataSize!");
  default:
    return;
  }
}

static char *CopyStringToChar(const std::string &str) {
  char *result = reinterpret_cast<char *>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

KVDKConfigs *KVDKCreateConfigs() { return new KVDKConfigs; }

void KVDKUserConfigs(KVDKConfigs *kv_config,
                     uint64_t max_write_threads /*= 48*/,
                     uint64_t pmem_file_size /*= 256ULL << 30*/,
                     unsigned char populate_pmem_space /*= true*/,
                     uint32_t pmem_block_size /*=64*/,
                     uint64_t pmem_segment_blocks /*= 2 * 1024 * 1024*/,
                     uint32_t hash_bucket_size /* = 128*/,
                     uint64_t hash_bucket_num /*= (1 << 27)*/,
                     uint32_t num_buckets_per_slot /*= (1 << 4)*/) {
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
                     FILE *log_file) {
  Engine *engine;
  StatusAssert(Engine::Open(std::string(name), &engine, config->rep, log_file));
  KVDKEngine *result = new KVDKEngine;
  result->rep = engine;
  return result;
}

void KVDKCloseEngine(KVDKEngine *engine) {
  delete engine->rep;
  delete engine;
}

void KVDKRemovePMemContents(const char *name) {
  std::string res = "rm -rf " + std::string(name) + "\n";
  system(res.c_str());
}

KVDKWriteBatch *KVDKWriteBatchCreate(void) { return new KVDKWriteBatch; }

void KVDKWriteBatchDelete(KVDKWriteBatch *wb, const char *key) {
  wb->rep.Delete(std::string(key));
}

void KVDKWriteBatchPut(KVDKWriteBatch *wb, const char *key, const char *value) {
  wb->rep.Put(std::string(key), std::string(value));
}

void KVDKWrite(KVDKEngine *engine, const KVDKWriteBatch *batch) {
  StatusAssert(engine->rep->BatchWrite(batch->rep));
}

void KVDKWriteBatchDestory(KVDKWriteBatch *wb) { delete wb; }

char *KVDKGet(KVDKEngine *engine, const char *key) {
  char *res = nullptr;
  std::string val_str;
  StatusAssert(engine->rep->Get(std::string(key), &val_str));
  res = CopyStringToChar(val_str);
  return res;
}

void KVDKSet(KVDKEngine *engine, const char *key, const char *val) {
  StatusAssert(engine->rep->Set(std::string(key), std::string(val)));
}

void KVDKDelete(KVDKEngine *engine, const char *key) {
  StatusAssert(engine->rep->Delete(std::string(key)));
}

void KVDKSortedSet(KVDKEngine *engine, const char *collection, const char *key,
                   const char *val) {
  StatusAssert(engine->rep->SSet(std::string(collection), std::string(key),
                                 std::string(val)));
}

char *KVDKSortedGet(KVDKEngine *engine, const char *collection,
                    const char *key) {
  char *res = nullptr;
  std::string val_str;
  StatusAssert(
      engine->rep->SGet(std::string(collection), std::string(key), &val_str));
  res = CopyStringToChar(val_str);
  return res;
}

void KVDKSortedDelete(KVDKEngine *engine, const char *collection,
                      const char *key) {
  StatusAssert(engine->rep->SDelete(std::string(collection), std::string(key)));
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
