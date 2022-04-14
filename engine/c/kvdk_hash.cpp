#include <cstring>

#include "kvdk_c.hpp"

extern "C" {
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

KVDKStatus KVDKHashSet(KVDKEngine* engine, const char* key_data, size_t key_len,
                       const char* field_data, size_t field_len,
                       const char* val_data, size_t val_len) {
  return engine->rep->HashSet(StringView(key_data, key_len),
                              StringView(field_data, field_len),
                              StringView(val_data, val_len));
}

KVDKStatus KVDKHashDelete(KVDKEngine* engine, const char* key_data,
                          size_t key_len, const char* field_data,
                          size_t field_len) {
  return engine->rep->HashDelete(StringView(key_data, key_len),
                                 StringView(field_data, field_len));
}

KVDKHashIterator* KVDKHashIteratorCreate(KVDKEngine* engine,
                                         char const* key_data, size_t key_len) {
  auto rep = engine->rep->HashCreateIterator(StringView{key_data, key_len});
  if (rep == nullptr) {
    return nullptr;
  }
  KVDKHashIterator* iter = new KVDKHashIterator;
  iter->rep.swap(rep);
  return iter;
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

void KVDKHashIteratorGetValue(KVDKHashIterator* iter, char** elem_data,
                              size_t* elem_len) {
  *elem_data = nullptr;
  *elem_len = 0;
  std::string buffer = iter->rep->Value();
  *elem_data = CopyStringToChar(buffer);
  *elem_len = buffer.size();
}

void KVDKHashIteratorGetKey(KVDKHashIterator* iter, char** elem_data,
                            size_t* elem_len) {
  *elem_data = nullptr;
  *elem_len = 0;
  std::string buffer = iter->rep->Key();
  *elem_data = CopyStringToChar(buffer);
  *elem_len = buffer.size();
}

}  // extern "C"
