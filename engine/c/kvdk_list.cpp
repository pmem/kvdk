/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include "kvdk_c.hpp"
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

KVDKStatus KVDKListReplace(KVDKEngine* engine, KVDKListIterator* pos,
                           char const* elem_data, size_t elem_len) {
  return engine->rep->ListReplace(pos->rep, StringView{elem_data, elem_len});
}

KVDKStatus KVDKListBatchPushFront(KVDKEngine* engine, char const* key_data,
                                  size_t key_len, char const* const* elems_data,
                                  size_t const* elems_len, size_t elems_cnt) {
  std::vector<StringView> elems;
  for (size_t i = 0; i < elems_cnt; i++) {
    elems.emplace_back(elems_data[i], elems_len[i]);
  }
  return engine->rep->ListBatchPushFront(StringView{key_data, key_len}, elems);
}

KVDKStatus KVDKListBatchPushBack(KVDKEngine* engine, char const* key_data,
                                 size_t key_len, char const* const* elems_data,
                                 size_t const* elems_len, size_t elems_cnt) {
  std::vector<StringView> elems;
  for (size_t i = 0; i < elems_cnt; i++) {
    elems.emplace_back(elems_data[i], elems_len[i]);
  }
  return engine->rep->ListBatchPushBack(StringView{key_data, key_len}, elems);
}

KVDKStatus KVDKListBatchPopFront(KVDKEngine* engine, char const* key_data,
                                 size_t key_len, size_t n,
                                 void (*cb)(char const* elem_data,
                                            size_t elem_len, void* args),
                                 void* args) {
  std::vector<std::string> elems;
  KVDKStatus s =
      engine->rep->ListBatchPopFront(StringView{key_data, key_len}, n, &elems);
  if (s != KVDKStatus::Ok) {
    return s;
  }
  for (auto const& elem : elems) {
    cb(elem.data(), elem.size(), args);
  }
  return s;
}

KVDKStatus KVDKListBatchPopBack(KVDKEngine* engine, char const* key_data,
                                size_t key_len, size_t n,
                                void (*cb)(char const* elem_data,
                                           size_t elem_len, void* args),
                                void* args) {
  std::vector<std::string> elems;
  KVDKStatus s =
      engine->rep->ListBatchPopBack(StringView{key_data, key_len}, n, &elems);
  if (s != KVDKStatus::Ok) {
    return s;
  }
  for (auto const& elem : elems) {
    cb(elem.data(), elem.size(), args);
  }
  return s;
}

KVDKStatus KVDKListMove(KVDKEngine* engine, char const* src_data,
                        size_t src_len, int src_pos, char const* dst_data,
                        size_t dst_len, int dst_pos, char** elem_data,
                        size_t* elem_len) {
  std::string elem;
  KVDKStatus s =
      engine->rep->ListMove(StringView{src_data, src_len}, src_pos,
                            StringView{dst_data, dst_len}, dst_pos, &elem);
  *elem_data = CopyStringToChar(elem);
  *elem_len = elem.size();
  return s;
}

KVDKListIterator* KVDKListIteratorCreate(KVDKEngine* engine,
                                         char const* key_data, size_t key_len,
                                         KVDKStatus* s) {
  auto rep = engine->rep->ListCreateIterator(StringView{key_data, key_len}, s);
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
