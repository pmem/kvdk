/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include "kvdk_c.hpp"
extern "C" {
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

}  // extern "C"
