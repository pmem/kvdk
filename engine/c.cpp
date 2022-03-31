/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "alias.hpp"
#include "kvdk/configs.hpp"
#include "kvdk/engine.h"
#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"
#include "kvdk/status.h"
#include "kvdk/write_batch.hpp"
using kvdk::StringView;

using kvdk::Configs;
using kvdk::Engine;
using kvdk::Iterator;
using kvdk::Snapshot;
using kvdk::SortedCollectionConfigs;
using kvdk::WriteBatch;
using kvdk::WriteOptions;

extern "C" {
struct KVDKConfigs {
  Configs rep;
};
struct KVDKEngine {
  std::unique_ptr<Engine> rep;
};
struct KVDKWriteBatch {
  WriteBatch rep;
};
struct KVDKIterator {
  KVDKIterType type;
  Iterator* sorted_iter;
  std::shared_ptr<Iterator> hash_iter;
};
struct KVDKSnapshot {
  Snapshot* rep;
};
struct KVDKWriteOptions {
  WriteOptions rep;
};

struct KVDKSortedCollectionConfigs {
  SortedCollectionConfigs rep;
};

static char* CopyStringToChar(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

KVDKConfigs* KVDKCreateConfigs() { return new KVDKConfigs; }

void KVDKSetConfigs(KVDKConfigs* kv_config, uint64_t max_access_threads,
                    uint64_t pmem_file_size, unsigned char populate_pmem_space,
                    uint32_t pmem_block_size, uint64_t pmem_segment_blocks,
                    uint32_t hash_bucket_size, uint64_t hash_bucket_num,
                    uint32_t num_buckets_per_slot) {
  kv_config->rep.max_access_threads = max_access_threads;
  kv_config->rep.hash_bucket_num = hash_bucket_num;
  kv_config->rep.hash_bucket_size = hash_bucket_size;
  kv_config->rep.num_buckets_per_slot = num_buckets_per_slot;
  kv_config->rep.pmem_block_size = pmem_block_size;
  kv_config->rep.pmem_file_size = pmem_file_size;
  kv_config->rep.pmem_segment_blocks = pmem_segment_blocks;
  kv_config->rep.populate_pmem_space = populate_pmem_space;
}

void KVDKDestroyConfigs(KVDKConfigs* kv_config) { delete kv_config; }

KVDKWriteOptions* KVDKCreateWriteOptions(void) { return new KVDKWriteOptions; }

void KVDKDestroyWriteOptions(KVDKWriteOptions* kv_options) {
  delete kv_options;
}

void KVDKWriteOptionsSetTTLTime(KVDKWriteOptions* kv_options,
                                int64_t ttl_time) {
  kv_options->rep.ttl_time = ttl_time;
}

KVDK_LIBRARY_API void KVDKWriteOptionsSetKeyExist(KVDKWriteOptions* kv_options,
                                                  unsigned char key_exist) {
  kv_options->rep.key_exist = key_exist;
}

KVDKSortedCollectionConfigs* KVDKCreateSortedCollectionConfigs() {
  return new KVDKSortedCollectionConfigs;
}

void KVDKSetSortedCollectionConfigs(KVDKSortedCollectionConfigs* configs,
                                    const char* comp_func_name,
                                    size_t comp_func_len) {
  configs->rep.comparator_name = std::string(comp_func_name, comp_func_len);
}

void KVDKDestroySortedCollectionConfigs(KVDKSortedCollectionConfigs* configs) {
  delete configs;
}

KVDKStatus KVDKOpen(const char* name, const KVDKConfigs* config, FILE* log_file,
                    KVDKEngine** kv_engine) {
  Engine* engine;
  KVDKStatus s =
      Engine::Open(std::string(name), &engine, config->rep, log_file);
  if (s != KVDKStatus::Ok) {
    *kv_engine = nullptr;
    return s;
  }
  *kv_engine = new KVDKEngine;
  (*kv_engine)->rep.reset(engine);
  return s;
}

void KVDKReleaseAccessThread(KVDKEngine* engine) {
  engine->rep->ReleaseAccessThread();
}

KVDKSnapshot* KVDKGetSnapshot(KVDKEngine* engine, int make_checkpoint) {
  KVDKSnapshot* snapshot = new KVDKSnapshot;
  snapshot->rep = engine->rep->GetSnapshot(make_checkpoint);
  return snapshot;
}

void KVDKReleaseSnapshot(KVDKEngine* engine, KVDKSnapshot* snapshot) {
  engine->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

void KVDKCloseEngine(KVDKEngine* engine) { delete engine; }

void KVDKRemovePMemContents(const char* name) {
  std::string res = "rm -rf " + std::string(name) + "\n";
  int ret __attribute__((unused)) = system(res.c_str());
}

int KVDKRegisterCompFunc(KVDKEngine* engine, const char* compara_name,
                         size_t compara_len,
                         int (*compare)(const char* src, size_t src_len,
                                        const char* target,
                                        size_t target_len)) {
  auto comp_func = [compare](const StringView& src,
                             const StringView& target) -> int {
    return compare(src.data(), src.size(), target.data(), target.size());
  };
  return engine->rep->RegisterComparator(StringView(compara_name, compara_len),
                                         comp_func);
}

KVDKStatus KVDKCreateSortedCollection(KVDKEngine* engine,
                                      const char* collection_name,
                                      size_t collection_len,
                                      KVDKSortedCollectionConfigs* configs) {
  KVDKStatus s = engine->rep->CreateSortedCollection(
      StringView(collection_name, collection_len), configs->rep);
  if (s != KVDKStatus::Ok) {
    return s;
  }
  return s;
}

KVDKWriteBatch* KVDKWriteBatchCreate(void) { return new KVDKWriteBatch; }

void KVDKWriteBatchDelete(KVDKWriteBatch* wb, const char* key, size_t key_len) {
  wb->rep.Delete(std::string(key, key_len));
}

void KVDKWriteBatchPut(KVDKWriteBatch* wb, const char* key, size_t key_len,
                       const char* value, size_t value_len) {
  wb->rep.Put(std::string(key, key_len), std::string(value, value_len));
}

KVDKStatus KVDKWrite(KVDKEngine* engine, const KVDKWriteBatch* batch) {
  return engine->rep->BatchWrite(batch->rep);
}

void KVDKWriteBatchDestory(KVDKWriteBatch* wb) { delete wb; }

KVDKStatus KVDKGet(KVDKEngine* engine, const char* key, size_t key_len,
                   size_t* val_len, char** val) {
  std::string val_str;

  *val = nullptr;
  KVDKStatus s = engine->rep->Get(StringView(key, key_len), &val_str);
  if (s != KVDKStatus::Ok) {
    *val_len = 0;
    return s;
  }
  *val_len = val_str.size();
  *val = CopyStringToChar(val_str);
  return s;
}

KVDKStatus KVDKSet(KVDKEngine* engine, const char* key, size_t key_len,
                   const char* val, size_t val_len,
                   const KVDKWriteOptions* write_option) {
  return engine->rep->Set(StringView(key, key_len), StringView(val, val_len),
                          write_option->rep);
}

KVDKStatus KVDKModify(KVDKEngine* engine, const char* key, size_t key_len,
                      char* new_value, size_t* new_value_len,
                      void (*modify)(const char*, size_t, char*, size_t*),
                      const KVDKWriteOptions* write_option) {
  auto modify_func = [&](StringView value) {
    modify(value.data(), value.size(), new_value, new_value_len);
    return std::string(new_value, *new_value_len);
  };
  std::string modify_result;
  KVDKStatus s = engine->rep->Modify(StringView(key, key_len), &modify_result,
                                     modify_func, write_option->rep);
  assert(s != KVDKStatus::Ok ||
         (modify_result.size() == *new_value_len &&
          memcmp(modify_result.data(), new_value, modify_result.size()) == 0));
  return s;
}

KVDKStatus KVDKDelete(KVDKEngine* engine, const char* key, size_t key_len) {
  return engine->rep->Delete(StringView(key, key_len));
}

KVDKStatus KVDKSortedSet(KVDKEngine* engine, const char* collection,
                         size_t collection_len, const char* key, size_t key_len,
                         const char* val, size_t val_len) {
  return engine->rep->SSet(StringView(collection, collection_len),
                           StringView(key, key_len), StringView(val, val_len));
}

KVDKStatus KVDKSortedGet(KVDKEngine* engine, const char* collection,
                         size_t collection_len, const char* key, size_t key_len,
                         size_t* val_len, char** val) {
  std::string val_str;

  *val = nullptr;
  KVDKStatus s = engine->rep->SGet(StringView(collection, collection_len),
                                   StringView(key, key_len), &val_str);
  if (s != KVDKStatus::Ok) {
    *val_len = 0;
    return s;
  }
  *val_len = val_str.size();
  *val = CopyStringToChar(val_str);
  return s;
}

KVDKStatus KVDKSortedDelete(KVDKEngine* engine, const char* collection,
                            size_t collection_len, const char* key,
                            size_t key_len) {
  return engine->rep->SDelete(StringView(collection, collection_len),
                              StringView(key, key_len));
}

KVDKStatus KVDKHashSet(KVDKEngine* engine, const char* collection,
                       size_t collection_len, const char* key, size_t key_len,
                       const char* val, size_t val_len) {
  return engine->rep->HSet(StringView(collection, collection_len),
                           StringView(key, key_len), StringView(val, val_len));
}

KVDKStatus KVDKHashDelete(KVDKEngine* engine, const char* collection,
                          size_t collection_len, const char* key,
                          size_t key_len) {
  return engine->rep->HDelete(StringView(collection, collection_len),
                              StringView(key, key_len));
}

KVDKStatus KVDKHashGet(KVDKEngine* engine, const char* collection,
                       size_t collection_len, const char* key, size_t key_len,
                       size_t* val_len, char** val) {
  std::string val_str;
  *val = nullptr;
  KVDKStatus s = engine->rep->HGet(StringView(collection, collection_len),
                                   StringView(key, key_len), &val_str);
  if (s != KVDKStatus::Ok) {
    *val_len = 0;
    return s;
  }
  *val_len = val_str.size();
  *val = CopyStringToChar(val_str);
  return s;
}

KVDKStatus KVDKLPush(KVDKEngine* engine, const char* collection,
                     size_t collection_len, const char* key, size_t key_len) {
  return engine->rep->ListPush(StringView(collection, collection_len),
                               Engine::ListPosition::Left,
                               StringView(key, key_len));
}

KVDKStatus KVDKRPush(KVDKEngine* engine, const char* collection,
                     size_t collection_len, const char* key, size_t key_len) {
  return engine->rep->ListPush(StringView(collection, collection_len),
                               Engine::ListPosition::Right,
                               StringView(key, key_len));
}

KVDKStatus KVDKLPop(KVDKEngine* engine, const char* collection,
                    size_t collection_len, char** key, size_t* key_len) {
  std::string str;
  *key = nullptr;
  KVDKStatus s = engine->rep->ListPop(StringView(collection, collection_len),
                                      Engine::ListPosition::Left, &str);
  if (s != KVDKStatus::Ok) {
    *key_len = 0;
    return s;
  }
  *key_len = str.size();
  *key = CopyStringToChar(str);
  return s;
}

KVDKStatus KVDKRPop(KVDKEngine* engine, const char* collection,
                    size_t collection_len, char** key, size_t* key_len) {
  std::string str;
  *key = nullptr;
  KVDKStatus s = engine->rep->ListPop(StringView(collection, collection_len),
                                      Engine::ListPosition::Right, &str);
  if (s != KVDKStatus::Ok) {
    *key_len = 0;
    return s;
  }
  *key_len = str.size();
  *key = CopyStringToChar(str);
  return s;
}

KVDKIterator* KVDKCreateUnorderedIterator(KVDKEngine* engine,
                                          const char* collection,
                                          size_t collection_len) {
  KVDKIterator* result = new KVDKIterator;
  result->hash_iter =
      engine->rep->NewUnorderedIterator(std::string(collection));
  if (result->hash_iter == nullptr) {
    delete result;
    return nullptr;
  }
  result->type = HASH;
  return result;
}

KVDKIterator* KVDKCreateSortedIterator(KVDKEngine* engine,
                                       const char* collection,
                                       size_t collection_len,
                                       KVDKSnapshot* snapshot) {
  KVDKIterator* result = new KVDKIterator;
  result->sorted_iter = (engine->rep->NewSortedIterator(
      std::string(collection), snapshot ? snapshot->rep : nullptr));
  if (result->sorted_iter == nullptr) {
    delete result;
    return nullptr;
  }
  result->type = SORTED;
  return result;
}

void KVDKDestroyIterator(KVDKEngine* engine, KVDKIterator* iterator) {
  switch (iterator->type) {
    case SORTED: {
      engine->rep->ReleaseSortedIterator(iterator->sorted_iter);
      break;
    }
    case HASH: {
      break;
    }
    default: {
      std::abort();
    }
  }
  delete iterator;
}

void KVDKIterSeekToFirst(KVDKIterator* iter) {
  switch (iter->type) {
    case SORTED: {
      iter->sorted_iter->SeekToFirst();
      break;
    }
    case HASH: {
      iter->hash_iter->SeekToFirst();
      break;
    }
    default: {
      std::abort();
    }
  }
}

void KVDKIterSeekToLast(KVDKIterator* iter) {
  switch (iter->type) {
    case SORTED: {
      iter->sorted_iter->SeekToLast();
      break;
    }
    case HASH: {
      iter->hash_iter->SeekToLast();
      break;
    }
    default: {
      std::abort();
    }
  }
}

void KVDKIterSeek(KVDKIterator* iter, const char* str, size_t str_len) {
  switch (iter->type) {
    case SORTED: {
      iter->sorted_iter->Seek(std::string{str, str_len});
      break;
    }
    case HASH: {
      iter->hash_iter->Seek(std::string{str, str_len});
      break;
    }
    default: {
      std::abort();
    }
  }
}

unsigned char KVDKIterValid(KVDKIterator* iter) {
  switch (iter->type) {
    case SORTED: {
      return iter->sorted_iter->Valid();
      break;
    }
    case HASH: {
      iter->hash_iter->Valid();
      break;
    }
    default: {
      std::abort();
    }
  }
}

void KVDKIterNext(KVDKIterator* iter) {
  switch (iter->type) {
    case SORTED: {
      iter->sorted_iter->Next();
      break;
    }
    case HASH: {
      iter->hash_iter->Next();
      break;
    }
    default: {
      std::abort();
    }
  }
}

void KVDKIterPrev(KVDKIterator* iter) {
  switch (iter->type) {
    case SORTED: {
      iter->sorted_iter->Prev();
      break;
    }
    case HASH: {
      iter->hash_iter->Prev();
      break;
    }
    default: {
      std::abort();
    }
  }
}

char* KVDKIterKey(KVDKIterator* iter, size_t* key_len) {
  std::string key_str;
  switch (iter->type) {
    case SORTED: {
      key_str = iter->sorted_iter->Key();
      break;
    }
    case HASH: {
      key_str = iter->hash_iter->Key();
      break;
    }
    default: {
      std::abort();
    }
  }
  *key_len = key_str.size();
  return CopyStringToChar(key_str);
}

char* KVDKIterValue(KVDKIterator* iter, size_t* val_len) {
  std::string val_str;
  switch (iter->type) {
    case SORTED: {
      val_str = iter->sorted_iter->Value();
      break;
    }
    case HASH: {
      val_str = iter->hash_iter->Value();
      break;
    }
    default: {
      std::abort();
    }
  }
  *val_len = val_str.size();
  return CopyStringToChar(val_str);
}

KVDKStatus KVDKExpire(KVDKEngine* engine, const char* str, size_t str_len,
                      int64_t ttl_time) {
  return engine->rep->Expire(std::string(str, str_len), ttl_time);
}

KVDKStatus KVDKGetTTL(KVDKEngine* engine, const char* str, size_t str_len,
                      int64_t* ttl_time) {
  return engine->rep->GetTTL(std::string(str, str_len), ttl_time);
}
}
