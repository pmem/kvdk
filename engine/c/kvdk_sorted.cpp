/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include "kvdk_c.hpp"

extern "C" {
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

KVDKStatus KVDKSortedDestroy(KVDKEngine* engine, const char* collection_name,
                             size_t collection_len) {
  return engine->rep->SortedDestroy(
      StringView(collection_name, collection_len));
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
                                                 KVDKSnapshot* snapshot,
                                                 KVDKStatus* s) {
  KVDKSortedIterator* result = new KVDKSortedIterator;
  result->rep =
      (engine->rep->NewSortedIterator(StringView{collection, collection_len},
                                      snapshot ? snapshot->rep : nullptr, s));
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

}  // extern "C"
