/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "status.h"
#include "types.h"

typedef struct KVDKEngine KVDKEngine;
typedef struct KVDKConfigs KVDKConfigs;
typedef struct KVDKWriteOptions KVDKWriteOptions;
typedef struct KVDKWriteBatch KVDKWriteBatch;
typedef struct KVDKSortedIterator KVDKSortedIterator;
typedef struct KVDKListIterator KVDKListIterator;
typedef struct KVDKHashIterator KVDKHashIterator;
typedef struct KVDKSnapshot KVDKSnapshot;
typedef struct KVDKSortedCollectionConfigs KVDKSortedCollectionConfigs;

KVDKConfigs* KVDKCreateConfigs(void);
extern void KVDKSetConfigs(KVDKConfigs* kv_config, uint64_t max_access_threads,
                           uint64_t pmem_file_size,
                           unsigned char populate_pmem_space,
                           uint32_t pmem_block_size,
                           uint64_t pmem_segment_blocks,
                           uint32_t hash_bucket_size, uint64_t hash_bucket_num,
                           uint32_t num_buckets_per_slot);
extern void KVDKDestroyConfigs(KVDKConfigs* kv_config);

extern KVDKWriteOptions* KVDKCreateWriteOptions(void);
extern void KVDKDestroyWriteOptions(KVDKWriteOptions*);
extern void KVDKWriteOptionsSetTTLTime(KVDKWriteOptions*, int64_t);
extern void KVDKWriteOptionsSetKeyExist(KVDKWriteOptions*, unsigned char);

extern KVDKSortedCollectionConfigs* KVDKCreateSortedCollectionConfigs();
extern void KVDKSetSortedCollectionConfigs(KVDKSortedCollectionConfigs* configs,
                                           const char* comp_func_name,
                                           size_t comp_func_len);
extern void KVDKDestroySortedCollectionConfigs(
    KVDKSortedCollectionConfigs* configs);

extern KVDKStatus KVDKOpen(const char* name, const KVDKConfigs* config,
                           FILE* log_file, KVDKEngine** engine);
extern void KVDKReleaseAccessThread(KVDKEngine* engine);
extern void KVDKCloseEngine(KVDKEngine* engine);
extern void KVDKRemovePMemContents(const char* name);
extern KVDKSnapshot* KVDKGetSnapshot(KVDKEngine* engine, int make_checkpoint);
extern void KVDKReleaseSnapshot(KVDKEngine* engine, KVDKSnapshot* snapshot);

extern int KVDKRegisterCompFunc(KVDKEngine* engine, const char* compara_name,
                                size_t compara_len,
                                int (*compare)(const char* src, size_t src_len,
                                               const char* target,
                                               size_t target_len));

// Create Sorted Collection
extern KVDKStatus KVDKCreateSortedCollection(
    KVDKEngine* engine, const char* collection_name, size_t collection_len,
    KVDKSortedCollectionConfigs* configs);

// For BatchWrite
extern KVDKWriteBatch* KVDKWriteBatchCreate(void);
extern void KVDKWriteBatchDestory(KVDKWriteBatch*);
extern void KVDKWriteBatchDelete(KVDKWriteBatch*, const char* key,
                                 size_t key_len);
extern void KVDKWriteBatchPut(KVDKWriteBatch*, const char* key, size_t key_len,
                              const char* value, size_t value_len);
extern KVDKStatus KVDKWrite(KVDKEngine* engine, const KVDKWriteBatch* batch);

// For Anonymous Global Collection
extern KVDKStatus KVDKGet(KVDKEngine* engine, const char* key, size_t key_len,
                          size_t* val_len, char** val);
extern KVDKStatus KVDKSet(KVDKEngine* engine, const char* key, size_t key_len,
                          const char* val, size_t val_len,
                          const KVDKWriteOptions* write_option);
extern KVDKStatus KVDKDelete(KVDKEngine* engine, const char* key,
                             size_t key_len);

// Modify value of existing key in the engine
//
// * modify_func: customized function to modify existing value of key. See
// definition of KVDKModifyFunc (types.h) for more details.
// * modify_args: customized arguments of modify_func.
// * free_func: function to free allocated space for new value in
// modify_func, pall NULL if not need to free
//
// Return Ok if modify success.
// Return Abort if modify function abort modifying.
// Return other non-Ok status on any error.
extern KVDKStatus KVDKModify(KVDKEngine* engine, const char* key,
                             size_t key_len, KVDKModifyFunc modify_func,
                             void* modify_args, KVDKFreeFunc free_func,
                             const KVDKWriteOptions* write_option);

// For Named Global Collection
extern KVDKStatus KVDKSortedSet(KVDKEngine* engine, const char* collection,
                                size_t collection_len, const char* key,
                                size_t key_len, const char* val,
                                size_t val_len);
extern KVDKStatus KVDKSortedDelete(KVDKEngine* engine, const char* collection,
                                   size_t collection_len, const char* key,
                                   size_t key_len);
extern KVDKStatus KVDKSortedGet(KVDKEngine* engine, const char* collection,
                                size_t collection_len, const char* key,
                                size_t key_len, size_t* val_len, char** val);

/// Hash //////////////////////////////////////////////////////////////////////
extern KVDKStatus KVDKHashLength(KVDKEngine* engine, char const* key_data,
                                 size_t key_len, size_t* len);
extern KVDKStatus KVDKHashGet(KVDKEngine* engine, const char* key_data,
                              size_t key_len, const char* field_data,
                              size_t field_len, char** val_data,
                              size_t* val_len);
extern KVDKStatus KVDKHashSet(KVDKEngine* engine, const char* key_data,
                              size_t key_len, const char* field_data,
                              size_t field_len, const char* val_data,
                              size_t val_len);
extern KVDKStatus KVDKHashDelete(KVDKEngine* engine, const char* key_data,
                                 size_t key_len, const char* field_data,
                                 size_t field_len);
/// HashIterator //////////////////////////////////////////////////////////////
extern KVDKHashIterator* KVDKHashIteratorCreate(KVDKEngine* engine,
                                                char const* key_data,
                                                size_t key_len);
extern void KVDKHashIteratorDestroy(KVDKHashIterator* iter);
extern void KVDKHashIteratorPrev(KVDKHashIterator* iter);
extern void KVDKHashIteratorNext(KVDKHashIterator* iter);
extern void KVDKHashIteratorSeekToFirst(KVDKHashIterator* iter);
extern void KVDKHashIteratorSeekToLast(KVDKHashIterator* iter);
extern int KVDKHashIteratorIsValid(KVDKHashIterator* iter);
extern void KVDKHashIteratorGetKey(KVDKHashIterator* iter, char** elem_data,
                                   size_t* elem_len);
extern void KVDKHashIteratorGetValue(KVDKHashIterator* iter, char** elem_data,
                                     size_t* elem_len);

/// List //////////////////////////////////////////////////////////////////////

extern KVDKStatus KVDKListLength(KVDKEngine* engine, char const* key_data,
                                 size_t key_len, size_t* len);
extern KVDKStatus KVDKListPushFront(KVDKEngine* engine, char const* key_data,
                                    size_t key_len, char const* elem_data,
                                    size_t elem_len);
extern KVDKStatus KVDKListPushBack(KVDKEngine* engine, char const* key_data,
                                   size_t key_len, char const* elem_data,
                                   size_t elem_len);
extern KVDKStatus KVDKListPopFront(KVDKEngine* engine, char const* key_data,
                                   size_t key_len, char** elem_data,
                                   size_t* elem_len);
extern KVDKStatus KVDKListPopBack(KVDKEngine* engine, char const* key_data,
                                  size_t key_len, char** elem_data,
                                  size_t* elem_len);
extern KVDKStatus KVDKListInsertBefore(KVDKEngine* engine,
                                       KVDKListIterator* pos,
                                       char const* elem_data, size_t elem_len);
extern KVDKStatus KVDKListInsertAfter(KVDKEngine* engine, KVDKListIterator* pos,
                                      char const* elem_data, size_t elem_len);
extern KVDKStatus KVDKListErase(KVDKEngine* engine, KVDKListIterator* pos);
extern KVDKStatus KVDKListSet(KVDKEngine* engine, KVDKListIterator* pos,
                              char const* elem_data, size_t elem_len);
/// ListIterator //////////////////////////////////////////////////////////////
extern KVDKListIterator* KVDKListIteratorCreate(KVDKEngine* engine,
                                                char const* key_data,
                                                size_t key_len);
extern void KVDKListIteratorDestroy(KVDKListIterator* iter);
extern void KVDKListIteratorPrev(KVDKListIterator* iter);
extern void KVDKListIteratorNext(KVDKListIterator* iter);
extern void KVDKListIteratorPrevElem(KVDKListIterator* iter,
                                     char const* elem_data, size_t elem_len);
extern void KVDKListIteratorNextElem(KVDKListIterator* iter,
                                     char const* elem_data, size_t elem_len);
extern void KVDKListIteratorSeekToFirst(KVDKListIterator* iter);
extern void KVDKListIteratorSeekToLast(KVDKListIterator* iter);
extern void KVDKListIteratorSeekToFirstElem(KVDKListIterator* iter,
                                            char const* elem_data,
                                            size_t elem_len);
extern void KVDKListIteratorSeekToLastElem(KVDKListIterator* iter,
                                           char const* elem_data,
                                           size_t elem_len);
extern void KVDKListIteratorSeekPos(KVDKListIterator* iter, long pos);
extern int KVDKListIteratorIsValid(KVDKListIterator* iter);
extern void KVDKListIteratorGetValue(KVDKListIterator* iter, char** elem_data,
                                     size_t* elem_len);

extern KVDKSortedIterator* KVDKKVDKSortedIteratorCreate(KVDKEngine* engine,
                                                        const char* collection,
                                                        size_t collection_len,
                                                        KVDKSnapshot* snapshot);
extern void KVDKSortedIteratorDestroy(KVDKEngine* engine,
                                      KVDKSortedIterator* iterator);
extern void KVDKSortedIteratorSeekToFirst(KVDKSortedIterator* iter);
extern void KVDKKVDKSortedIteratorSeekToLast(KVDKSortedIterator* iter);
extern void KVDKSortedIteratorSeek(KVDKSortedIterator* iter, const char* str,
                                   size_t str_len);
extern void KVDKSortedIteratorNext(KVDKSortedIterator* iter);
extern void KVDKSortedIteratorPrev(KVDKSortedIterator* iter);
extern unsigned char KVDKSortedIteratorValid(KVDKSortedIterator* iter);
extern void KVDKSortedIteratorKey(KVDKSortedIterator* iter, char** key,
                                  size_t* key_len);
extern void KVDKSortedIteratorValue(KVDKSortedIterator* iter, char** value,
                                    size_t* val_len);

/* ttl_time is negetive or positive number, If ttl_time == INT64_MAX,
 * the key is persistent; If ttl_time <=0, the key is expired immediately.
 */
extern KVDKStatus KVDKExpire(KVDKEngine* engine, const char* str,
                             size_t str_len, int64_t ttl_time);
/* ttl_time is INT64_MAX and return Status::Ok if the key is persist
 * ttl_time is 0 and return Status::NotFound if the key is expired or doesnot
 * exist.
 * ttl_time is certain positive number and return Status::Ok if the key hasn't
 * expired and exist.
 */
extern KVDKStatus KVDKGetTTL(KVDKEngine* engine, const char* str,
                             size_t str_len, int64_t* ttl_time);

#ifdef __cplusplus
} /* end extern "C" */
#endif  // extern "C"
