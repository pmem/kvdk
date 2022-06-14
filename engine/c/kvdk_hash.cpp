/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <cassert>
#include <cstring>

#include "kvdk_c.hpp"

extern "C" {
KVDKStatus KVDKHashCreate(KVDKEngine* engine, char const* key_data,
                          size_t key_len) {
  return engine->rep->HashCreate(StringView{key_data, key_len});
}
KVDKStatus KVDKHashDestroy(KVDKEngine* engine, char const* key_data,
                           size_t key_len) {
  return engine->rep->HashDestroy(StringView{key_data, key_len});
}

KVDKStatus KVDKHashLength(KVDKEngine* engine, char const* key_data,
                          size_t key_len, size_t* len) {
  return engine->rep->HashLength(StringView{key_data, key_len}, len);
}

KVDKStatus KVDKHashGet(KVDKEngine* engine, const char* key_data, size_t key_len,
                       const char* field_data, size_t field_len,
                       char** val_data, size_t* val_len) {
  std::string val_str;
  *val_data = nullptr;
  KVDKStatus s =
      engine->rep->HashGet(StringView(key_data, key_len),
                           StringView(field_data, field_len), &val_str);
  if (s != KVDKStatus::Ok) {
    *val_len = 0;
    return s;
  }
  *val_len = val_str.size();
  *val_data = CopyStringToChar(val_str);
  return s;
}

KVDKStatus KVDKHashPut(KVDKEngine* engine, const char* key_data, size_t key_len,
                       const char* field_data, size_t field_len,
                       const char* val_data, size_t val_len) {
  return engine->rep->HashPut(StringView(key_data, key_len),
                              StringView(field_data, field_len),
                              StringView(val_data, val_len));
}

KVDKStatus KVDKHashDelete(KVDKEngine* engine, const char* key_data,
                          size_t key_len, const char* field_data,
                          size_t field_len) {
  return engine->rep->HashDelete(StringView(key_data, key_len),
                                 StringView(field_data, field_len));
}

extern KVDKStatus KVDKHashModify(KVDKEngine* engine, const char* key_data,
                                 size_t key_len, const char* field_data,
                                 size_t field_len, KVDKModifyFunc modify_func,
                                 void* args, KVDKFreeFunc free_func) {
  auto ModifyFunc = [&](const std::string* old_value, std::string* new_value,
                        void* arg) {
    int op;
    char* new_val_data;
    size_t new_val_len;
    if (old_value != nullptr) {
      op = modify_func(old_value->data(), old_value->size(), &new_val_data,
                       &new_val_len, arg);
    } else {
      op = modify_func(nullptr, 0, &new_val_data, &new_val_len, arg);
    }
    if (op == KVDK_MODIFY_WRITE) {
      assert(new_val_data != nullptr);
      new_value->assign(new_val_data, new_val_len);
    }
    if (free_func != nullptr && new_val_data != nullptr) {
      free_func(new_val_data);
    }
    return static_cast<kvdk::ModifyOperation>(op);
  };
  KVDKStatus s = engine->rep->HashModify(StringView{key_data, key_len},
                                         StringView{field_data, field_len},
                                         ModifyFunc, args);
  return s;
}

KVDKHashIterator* KVDKHashIteratorCreate(KVDKEngine* engine,
                                         char const* key_data, size_t key_len,
                                         KVDKStatus* s) {
  auto iter = engine->rep->HashCreateIterator(StringView{key_data, key_len}, s);
  if (iter == nullptr) {
    return nullptr;
  }
  KVDKHashIterator* hash_iter = new KVDKHashIterator;
  hash_iter->rep.swap(iter);
  return hash_iter;
}

void KVDKHashIteratorDestroy(KVDKHashIterator* iter) { delete iter; }

void KVDKHashIteratorPrev(KVDKHashIterator* iter) { iter->rep->Prev(); }

void KVDKHashIteratorNext(KVDKHashIterator* iter) { iter->rep->Next(); }

void KVDKHashIteratorSeekToFirst(KVDKHashIterator* iter) {
  iter->rep->SeekToFirst();
}

void KVDKHashIteratorSeekToLast(KVDKHashIterator* iter) {
  iter->rep->SeekToLast();
}

int KVDKHashIteratorIsValid(KVDKHashIterator* iter) {
  bool valid = iter->rep->Valid();
  return (valid ? 1 : 0);
}

void KVDKHashIteratorGetValue(KVDKHashIterator* iter, char** value_data,
                              size_t* value_len) {
  *value_data = nullptr;
  *value_len = 0;
  std::string buffer = iter->rep->Value();
  *value_data = CopyStringToChar(buffer);
  *value_len = buffer.size();
}

void KVDKHashIteratorGetKey(KVDKHashIterator* iter, char** field_data,
                            size_t* field_len) {
  *field_data = nullptr;
  *field_len = 0;
  std::string buffer = iter->rep->Key();
  *field_data = CopyStringToChar(buffer);
  *field_len = buffer.size();
}

int KVDKHashIteratorMatchKey(KVDKHashIterator* iter, KVDKRegex const* re) {
  return iter->rep->MatchKey(re->rep) ? 1 : 0;
}

}  // extern "C"
