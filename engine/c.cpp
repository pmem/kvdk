/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "c/kvdk_c.hpp"

extern "C" {
KVDKConfigs* KVDKCreateConfigs() { return new KVDKConfigs; }

void KVDKSetConfigs(KVDKConfigs* kv_config, uint64_t max_access_threads,
                    uint64_t pmem_file_size, unsigned char populate_pmem_space,
                    uint32_t pmem_block_size, uint64_t pmem_segment_blocks,
                    uint64_t hash_bucket_num, uint32_t num_buckets_per_slot) {
  kv_config->rep.max_access_threads = max_access_threads;
  kv_config->rep.hash_bucket_num = hash_bucket_num;
  kv_config->rep.num_buckets_per_slot = num_buckets_per_slot;
  kv_config->rep.pmem_block_size = pmem_block_size;
  kv_config->rep.pmem_file_size = pmem_file_size;
  kv_config->rep.pmem_segment_blocks = pmem_segment_blocks;
  kv_config->rep.populate_pmem_space = populate_pmem_space;
}

void KVDKConfigRegisterCompFunc(KVDKConfigs* kv_config,
                                const char* compara_name, size_t compara_len,
                                int (*compare)(const char* src, size_t src_len,
                                               const char* target,
                                               size_t target_len)) {
  auto comp_func = [compare](const StringView& src,
                             const StringView& target) -> int {
    return compare(src.data(), src.size(), target.data(), target.size());
  };
  kv_config->rep.comparator.RegisterComparator(
      StringView(compara_name, compara_len), comp_func);
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

KVDKSortedCollectionConfigs* KVDKCreateSortedCollectionConfigs() {
  return new KVDKSortedCollectionConfigs;
}

void KVDKSetSortedCollectionConfigs(KVDKSortedCollectionConfigs* configs,
                                    const char* comp_func_name,
                                    size_t comp_func_len,
                                    int index_with_hashtable) {
  configs->rep.comparator_name = std::string(comp_func_name, comp_func_len);
  configs->rep.index_with_hashtable = index_with_hashtable;
}

void KVDKDestroySortedCollectionConfigs(KVDKSortedCollectionConfigs* configs) {
  delete configs;
}

KVDKStatus KVDKOpen(const char* name, const KVDKConfigs* config, FILE* log_file,
                    KVDKEngine** kv_engine) {
  Engine* engine;
  KVDKStatus s =
      Engine::Open(std::string(name), &engine, config->rep, log_file);
  *kv_engine = nullptr;
  if (s != KVDKStatus::Ok) {
    return s;
  }
  *kv_engine = new KVDKEngine;
  (*kv_engine)->rep.reset(engine);
  return s;
}

KVDKStatus KVDKBackup(KVDKEngine* engine, char* backup_path,
                      size_t backup_path_len, KVDKSnapshot* snapshot) {
  return engine->rep->Backup(StringView(backup_path, backup_path_len),
                             snapshot->rep);
}

KVDKStatus KVDKRestore(const char* name, const char* backup_log,
                       const KVDKConfigs* config, FILE* log_file,
                       KVDKEngine** kv_engine) {
  Engine* engine;
  KVDKStatus s = Engine::Restore(std::string(name), std::string(backup_log),
                                 &engine, config->rep, log_file);
  if (s == KVDKStatus::Ok) {
    *kv_engine = new KVDKEngine;
    (*kv_engine)->rep.reset(engine);
  }
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

KVDKStatus KVDKSortedCreate(KVDKEngine* engine, const char* collection_name,
                            size_t collection_len,
                            KVDKSortedCollectionConfigs* configs) {
  KVDKStatus s = engine->rep->SortedCreate(
      StringView(collection_name, collection_len), configs->rep);
  if (s != KVDKStatus::Ok) {
    return s;
  }
  return s;
}

extern KVDKStatus KVDKSortedDestroy(KVDKEngine* engine,
                                    const char* collection_name,
                                    size_t collection_len) {
  return engine->rep->SortedDestroy(
      StringView(collection_name, collection_len));
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

KVDKStatus KVDKPut(KVDKEngine* engine, const char* key, size_t key_len,
                   const char* val, size_t val_len,
                   const KVDKWriteOptions* write_option) {
  return engine->rep->Put(StringView(key, key_len), StringView(val, val_len),
                          write_option->rep);
}

KVDKStatus KVDKModify(KVDKEngine* engine, const char* key, size_t key_len,
                      KVDKModifyFunc modify_func, void* modify_args,
                      KVDKFreeFunc free_func,
                      const KVDKWriteOptions* write_option) {
  auto cpp_modify_func = [&](const std::string* old_value,
                             std::string* new_value, void* args) {
    char* nv;
    size_t nv_len;
    auto result =
        modify_func(old_value ? old_value->data() : nullptr,
                    old_value ? old_value->size() : 0, &nv, &nv_len, args);
    if (result == KVDK_MODIFY_WRITE) {
      assert(nv != nullptr);
      new_value->assign(nv, nv_len);
    }
    if (nv != nullptr && free_func != nullptr) {
      free_func(nv);
    }
    return kvdk::ModifyOperation(result);
  };
  KVDKStatus s = engine->rep->Modify(StringView(key, key_len), cpp_modify_func,
                                     modify_args, write_option->rep);
  return s;
}

KVDKStatus KVDKDelete(KVDKEngine* engine, const char* key, size_t key_len) {
  return engine->rep->Delete(StringView(key, key_len));
}

KVDKStatus KVDKSortedSize(KVDKEngine* engine, const char* collection,
                          size_t collection_len, size_t* size) {
  return engine->rep->SortedSize(StringView(collection, collection_len), size);
}

KVDKStatus KVDKSortedPut(KVDKEngine* engine, const char* collection,
                         size_t collection_len, const char* key, size_t key_len,
                         const char* val, size_t val_len) {
  return engine->rep->SortedPut(StringView(collection, collection_len),
                                StringView(key, key_len),
                                StringView(val, val_len));
}

KVDKStatus KVDKSortedGet(KVDKEngine* engine, const char* collection,
                         size_t collection_len, const char* key, size_t key_len,
                         size_t* val_len, char** val) {
  std::string val_str;

  *val = nullptr;
  KVDKStatus s = engine->rep->SortedGet(StringView(collection, collection_len),
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
  return engine->rep->SortedDelete(StringView(collection, collection_len),
                                   StringView(key, key_len));
}

KVDKSortedIterator* KVDKKVDKSortedIteratorCreate(KVDKEngine* engine,
                                                 const char* collection,
                                                 size_t collection_len,
                                                 KVDKSnapshot* snapshot) {
  KVDKSortedIterator* result = new KVDKSortedIterator;
  result->rep =
      (engine->rep->NewSortedIterator(StringView{collection, collection_len},
                                      snapshot ? snapshot->rep : nullptr));
  if (!result->rep) {
    delete result;
    return nullptr;
  }
  return result;
}

void KVDKSortedIteratorDestroy(KVDKEngine* engine,
                               KVDKSortedIterator* iterator) {
  if (iterator != nullptr) {
    engine->rep->ReleaseSortedIterator(iterator->rep);
  }
  delete iterator;
}

void KVDKSortedIteratorSeekToFirst(KVDKSortedIterator* iter) {
  iter->rep->SeekToFirst();
}

void KVDKKVDKSortedIteratorSeekToLast(KVDKSortedIterator* iter) {
  iter->rep->SeekToLast();
}

void KVDKSortedIteratorSeek(KVDKSortedIterator* iter, const char* str,
                            size_t str_len) {
  iter->rep->Seek(std::string(str, str_len));
}

unsigned char KVDKSortedIteratorValid(KVDKSortedIterator* iter) {
  return iter->rep->Valid();
}

void KVDKSortedIteratorNext(KVDKSortedIterator* iter) { iter->rep->Next(); }

void KVDKSortedIteratorPrev(KVDKSortedIterator* iter) { iter->rep->Prev(); }

void KVDKSortedIteratorKey(KVDKSortedIterator* iter, char** key,
                           size_t* key_len) {
  std::string key_str = iter->rep->Key();
  *key_len = key_str.size();
  *key = CopyStringToChar(key_str);
}

void KVDKSortedIteratorValue(KVDKSortedIterator* iter, char** value,
                             size_t* val_len) {
  std::string val_str = iter->rep->Value();
  *val_len = val_str.size();
  *value = CopyStringToChar(val_str);
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

// List
extern "C" {
KVDKStatus KVDKListCreate(KVDKEngine* engine, char const* key_data,
                          size_t key_len) {
  return engine->rep->ListCreate(StringView{key_data, key_len});
}
KVDKStatus KVDKListDestroy(KVDKEngine* engine, char const* key_data,
                           size_t key_len) {
  return engine->rep->ListDestroy(StringView{key_data, key_len});
}
KVDKStatus KVDKListLength(KVDKEngine* engine, char const* key_data,
                          size_t key_len, size_t* len) {
  return engine->rep->ListLength(StringView{key_data, key_len}, len);
}

KVDKStatus KVDKListPushFront(KVDKEngine* engine, char const* key_data,
                             size_t key_len, char const* elem_data,
                             size_t elem_len) {
  return engine->rep->ListPushFront(StringView{key_data, key_len},
                                    StringView{elem_data, elem_len});
}

KVDKStatus KVDKListPushBack(KVDKEngine* engine, char const* key_data,
                            size_t key_len, char const* elem_data,
                            size_t elem_len) {
  return engine->rep->ListPushBack(StringView{key_data, key_len},
                                   StringView{elem_data, elem_len});
}

KVDKStatus KVDKListPopFront(KVDKEngine* engine, char const* key_data,
                            size_t key_len, char** elem_data,
                            size_t* elem_len) {
  *elem_data = nullptr;
  *elem_len = 0;
  std::string buffer;
  KVDKStatus s =
      engine->rep->ListPopFront(StringView{key_data, key_len}, &buffer);
  if (s == KVDKStatus::Ok) {
    *elem_data = CopyStringToChar(buffer);
    *elem_len = buffer.size();
  }
  return s;
}

KVDKStatus KVDKListPopBack(KVDKEngine* engine, char const* key_data,
                           size_t key_len, char** elem_data, size_t* elem_len) {
  *elem_data = nullptr;
  *elem_len = 0;
  std::string buffer;
  KVDKStatus s =
      engine->rep->ListPopBack(StringView{key_data, key_len}, &buffer);
  if (s == KVDKStatus::Ok) {
    *elem_data = CopyStringToChar(buffer);
    *elem_len = buffer.size();
  }
  return s;
}

KVDKStatus KVDKListInsertBefore(KVDKEngine* engine, KVDKListIterator* pos,
                                char const* elem_data, size_t elem_len) {
  return engine->rep->ListInsertBefore(pos->rep,
                                       StringView{elem_data, elem_len});
}

KVDKStatus KVDKListInsertAfter(KVDKEngine* engine, KVDKListIterator* pos,
                               char const* elem_data, size_t elem_len) {
  return engine->rep->ListInsertAfter(pos->rep,
                                      StringView{elem_data, elem_len});
}

KVDKStatus KVDKListErase(KVDKEngine* engine, KVDKListIterator* pos) {
  return engine->rep->ListErase(pos->rep);
}

KVDKStatus KVDKListPut(KVDKEngine* engine, KVDKListIterator* pos,
                       char const* elem_data, size_t elem_len) {
  return engine->rep->ListPut(pos->rep, StringView{elem_data, elem_len});
}

KVDKListIterator* KVDKListIteratorCreate(KVDKEngine* engine,
                                         char const* key_data, size_t key_len) {
  auto rep = engine->rep->ListCreateIterator(StringView{key_data, key_len});
  if (rep == nullptr) {
    return nullptr;
  }
  KVDKListIterator* iter = new KVDKListIterator;
  iter->rep.swap(rep);
  return iter;
}

void KVDKListIteratorDestroy(KVDKListIterator* iter) { delete iter; }

void KVDKListIteratorPrev(KVDKListIterator* iter) { iter->rep->Prev(); }

void KVDKListIteratorNext(KVDKListIterator* iter) { iter->rep->Next(); }

void KVDKListIteratorSeekToFirst(KVDKListIterator* iter) {
  iter->rep->SeekToFirst();
}

void KVDKListIteratorSeekToLast(KVDKListIterator* iter) {
  iter->rep->SeekToLast();
}

void KVDKListIteratorSeekPos(KVDKListIterator* iter, long pos) {
  iter->rep->Seek(pos);
}

void KVDKListIteratorPrevElem(KVDKListIterator* iter, char const* elem_data,
                              size_t elem_len) {
  iter->rep->Prev(StringView{elem_data, elem_len});
}

void KVDKListIteratorNextElem(KVDKListIterator* iter, char const* elem_data,
                              size_t elem_len) {
  iter->rep->Next(StringView{elem_data, elem_len});
}

void KVDKListIteratorSeekToFirstElem(KVDKListIterator* iter,
                                     char const* elem_data, size_t elem_len) {
  iter->rep->SeekToFirst(StringView{elem_data, elem_len});
}

void KVDKListIteratorSeekToLastElem(KVDKListIterator* iter,
                                    char const* elem_data, size_t elem_len) {
  iter->rep->SeekToLast(StringView{elem_data, elem_len});
}

int KVDKListIteratorIsValid(KVDKListIterator* iter) {
  bool valid = iter->rep->Valid();
  return (valid ? 1 : 0);
}

void KVDKListIteratorGetValue(KVDKListIterator* iter, char** elem_data,
                              size_t* elem_len) {
  *elem_data = nullptr;
  *elem_len = 0;
  std::string buffer = iter->rep->Value();
  *elem_data = CopyStringToChar(buffer);
  *elem_len = buffer.size();
}
}