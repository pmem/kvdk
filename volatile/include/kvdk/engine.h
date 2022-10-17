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
typedef struct KVDKRegex KVDKRegex;

extern KVDKRegex* KVDKRegexCreate(char const* data, size_t len);
extern void KVDKRegexDestroy(KVDKRegex* re);

extern KVDKConfigs* KVDKCreateConfigs(void);
extern void KVDKSetConfigs(KVDKConfigs* kv_config, uint64_t max_access_threads,
                           uint64_t pmem_file_size,
                           unsigned char populate_pmem_space,
                           uint32_t pmem_block_size,
                           uint64_t pmem_segment_blocks,
                           uint64_t hash_bucket_num,
                           uint32_t num_buckets_per_slot);
extern void KVDKConfigRegisterCompFunc(
    KVDKConfigs* kv_config, const char* compara_name, size_t compara_len,
    int (*compare)(const char* src, size_t src_len, const char* target,
                   size_t target_len));
extern void KVDKDestroyConfigs(KVDKConfigs* kv_config);

extern KVDKWriteOptions* KVDKCreateWriteOptions(void);
extern void KVDKDestroyWriteOptions(KVDKWriteOptions*);
extern void KVDKWriteOptionsSetTTLTime(KVDKWriteOptions*, int64_t);
extern void KVDKWriteOptionsSetUpdateTTL(KVDKWriteOptions* kv_options,
                                         int update_ttl);

extern KVDKSortedCollectionConfigs* KVDKCreateSortedCollectionConfigs();
extern void KVDKSetSortedCollectionConfigs(KVDKSortedCollectionConfigs* configs,
                                           const char* comp_func_name,
                                           size_t comp_func_len,
                                           int index_with_hashtable);
extern void KVDKDestroySortedCollectionConfigs(
    KVDKSortedCollectionConfigs* configs);

extern KVDKStatus KVDKOpen(const char* name, const KVDKConfigs* config,
                           FILE* log_file, KVDKEngine** engine);
extern KVDKStatus KVDKBackup(KVDKEngine* engine, const char* backup_path,
                             size_t backup_path_len, KVDKSnapshot* snapshot);
extern KVDKStatus KVDKRestore(const char* name, const char* backup_log,
                              const KVDKConfigs* config, FILE* log_file,
                              KVDKEngine** engine);
extern void KVDKCloseEngine(KVDKEngine* engine);
extern void KVDKRemovePMemContents(const char* name);
extern KVDKSnapshot* KVDKGetSnapshot(KVDKEngine* engine, int make_checkpoint);
extern void KVDKReleaseSnapshot(KVDKEngine* engine, KVDKSnapshot* snapshot);

extern int KVDKRegisterCompFunc(KVDKEngine* engine, const char* compara_name,
                                size_t compara_len,
                                int (*compare)(const char* src, size_t src_len,
                                               const char* target,
                                               size_t target_len));

// For BatchWrite
extern KVDKWriteBatch* KVDKWriteBatchCreate(KVDKEngine* engine);
extern void KVDKWriteBatchDestory(KVDKWriteBatch* batch);
extern void KVDKWRiteBatchClear(KVDKWriteBatch* batch);
extern void KVDKWriteBatchStringPut(KVDKWriteBatch* batch, char const* key_data,
                                    size_t key_len, char const* val_data,
                                    size_t val_len);
extern void KVDKWriteBatchStringDelete(KVDKWriteBatch* batch,
                                       char const* key_data, size_t key_len);
extern void KVDKWriteBatchSortedPut(KVDKWriteBatch* batch, char const* key_data,
                                    size_t key_len, char const* field_data,
                                    size_t field_len, char const* val_data,
                                    size_t val_len);
extern void KVDKWriteBatchSortedDelete(KVDKWriteBatch* batch,
                                       char const* key_data, size_t key_len,
                                       char const* field_data,
                                       size_t field_len);
extern void KVDKWriteBatchHashPut(KVDKWriteBatch* batch, char const* key_data,
                                  size_t key_len, char const* field_data,
                                  size_t field_len, char const* val_data,
                                  size_t val_len);
extern void KVDKWriteBatchHashDelete(KVDKWriteBatch* batch,
                                     char const* key_data, size_t key_len,
                                     char const* field_data, size_t field_len);
extern KVDKStatus KVDKBatchWrite(KVDKEngine* engine,
                                 KVDKWriteBatch const* batch);

// For Anonymous Global Collection
extern KVDKStatus KVDKGet(KVDKEngine* engine, const char* key, size_t key_len,
                          size_t* val_len, char** val);
extern KVDKStatus KVDKPut(KVDKEngine* engine, const char* key, size_t key_len,
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

/// Sorted
/// //////////////////////////////////////////////////////////////////////
extern KVDKStatus KVDKSortedCreate(KVDKEngine* engine,
                                   const char* collection_name,
                                   size_t collection_len,
                                   KVDKSortedCollectionConfigs* configs);
extern KVDKStatus KVDKSortedDestroy(KVDKEngine* engine,
                                    const char* collection_name,
                                    size_t collection_len);
extern KVDKStatus KVDKSortedSize(KVDKEngine* engine, const char* collection,
                                 size_t collection_len, size_t* size);
extern KVDKStatus KVDKSortedPut(KVDKEngine* engine, const char* collection,
                                size_t collection_len, const char* key,
                                size_t key_len, const char* val,
                                size_t val_len);
extern KVDKStatus KVDKSortedDelete(KVDKEngine* engine, const char* collection,
                                   size_t collection_len, const char* key,
                                   size_t key_len);
extern KVDKStatus KVDKSortedGet(KVDKEngine* engine, const char* collection,
                                size_t collection_len, const char* key,
                                size_t key_len, size_t* val_len, char** val);
extern KVDKSortedIterator* KVDKSortedIteratorCreate(KVDKEngine* engine,
                                                    const char* collection,
                                                    size_t collection_len,
                                                    KVDKSnapshot* snapshot,
                                                    KVDKStatus* s);
extern void KVDKSortedIteratorDestroy(KVDKEngine* engine,
                                      KVDKSortedIterator* iterator);
extern void KVDKSortedIteratorSeekToFirst(KVDKSortedIterator* iter);
extern void KVDKSortedIteratorSeekToLast(KVDKSortedIterator* iter);
extern void KVDKSortedIteratorSeek(KVDKSortedIterator* iter, const char* str,
                                   size_t str_len);
extern void KVDKSortedIteratorNext(KVDKSortedIterator* iter);
extern void KVDKSortedIteratorPrev(KVDKSortedIterator* iter);
extern unsigned char KVDKSortedIteratorValid(KVDKSortedIterator* iter);
extern void KVDKSortedIteratorKey(KVDKSortedIterator* iter, char** key,
                                  size_t* key_len);
extern void KVDKSortedIteratorValue(KVDKSortedIterator* iter, char** value,
                                    size_t* val_len);

/// Hash //////////////////////////////////////////////////////////////////////
extern KVDKStatus KVDKHashCreate(KVDKEngine* engine, char const* key_data,
                                 size_t key_len);
extern KVDKStatus KVDKHashDestroy(KVDKEngine* engine, char const* key_data,
                                  size_t key_len);
extern KVDKStatus KVDKHashLength(KVDKEngine* engine, char const* key_data,
                                 size_t key_len, size_t* len);
extern KVDKStatus KVDKHashGet(KVDKEngine* engine, const char* key_data,
                              size_t key_len, const char* field_data,
                              size_t field_len, char** val_data,
                              size_t* val_len);
extern KVDKStatus KVDKHashPut(KVDKEngine* engine, const char* key_data,
                              size_t key_len, const char* field_data,
                              size_t field_len, const char* val_data,
                              size_t val_len);
extern KVDKStatus KVDKHashDelete(KVDKEngine* engine, const char* key_data,
                                 size_t key_len, const char* field_data,
                                 size_t field_len);
extern KVDKStatus KVDKHashModify(KVDKEngine* engine, const char* key_data,
                                 size_t key_len, const char* field_data,
                                 size_t field_len, KVDKModifyFunc modify_func,
                                 void* args, KVDKFreeFunc free_func);

/// HashIterator //////////////////////////////////////////////////////////////
extern KVDKHashIterator* KVDKHashIteratorCreate(KVDKEngine* engine,
                                                char const* key_data,
                                                size_t key_len,
                                                KVDKSnapshot* snapshot,
                                                KVDKStatus* s);
extern void KVDKHashIteratorDestroy(KVDKEngine* engine, KVDKHashIterator* iter);
extern void KVDKHashIteratorPrev(KVDKHashIterator* iter);
extern void KVDKHashIteratorNext(KVDKHashIterator* iter);
extern void KVDKHashIteratorSeekToFirst(KVDKHashIterator* iter);
extern void KVDKHashIteratorSeekToLast(KVDKHashIterator* iter);
extern int KVDKHashIteratorIsValid(KVDKHashIterator* iter);
extern void KVDKHashIteratorGetKey(KVDKHashIterator* iter, char** field_data,
                                   size_t* field_len);
extern void KVDKHashIteratorGetValue(KVDKHashIterator* iter, char** value_data,
                                     size_t* value_len);
extern int KVDKHashIteratorMatchKey(KVDKHashIterator* iter,
                                    KVDKRegex const* re);

/// List //////////////////////////////////////////////////////////////////////
extern KVDKStatus KVDKListCreate(KVDKEngine* engine, char const* key_data,
                                 size_t key_len);
extern KVDKStatus KVDKListDestroy(KVDKEngine* engine, char const* key_data,
                                  size_t key_len);
extern KVDKStatus KVDKListSize(KVDKEngine* engine, char const* key_data,
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
extern KVDKStatus KVDKListBatchPushFront(KVDKEngine* engine,
                                         char const* key_data, size_t key_len,
                                         char const* const* elems_data,
                                         size_t const* elems_len,
                                         size_t elems_cnt);
extern KVDKStatus KVDKListBatchPushBack(KVDKEngine* engine,
                                        char const* key_data, size_t key_len,
                                        char const* const* elems_data,
                                        size_t const* elems_len,
                                        size_t elems_cnt);
extern KVDKStatus KVDKListBatchPopFront(
    KVDKEngine* engine, char const* key_data, size_t key_len, size_t n,
    void (*cb)(char const* elem_data, size_t elem_len, void* args), void* args);
extern KVDKStatus KVDKListBatchPopBack(
    KVDKEngine* engine, char const* key_data, size_t key_len, size_t n,
    void (*cb)(char const* elem_data, size_t elem_len, void* args), void* args);
extern KVDKStatus KVDKListMove(KVDKEngine* engine, char const* src_data,
                               size_t src_len, int src_pos,
                               char const* dst_data, size_t dst_len,
                               int dst_pos, char** elem_data, size_t* elem_len);
extern KVDKStatus KVDKListInsertAt(KVDKEngine* engine, char const* list_name,
                                   size_t list_name_len, char const* elem_data,
                                   size_t elem_len, long index);
extern KVDKStatus KVDKListInsertBefore(KVDKEngine* engine,
                                       char const* list_name,
                                       size_t list_name_len,
                                       char const* elem_data, size_t elem_len,
                                       char const* pos_elem,
                                       size_t pos_elem_len);
extern KVDKStatus KVDKListInsertAfter(KVDKEngine* engine, char const* list_name,
                                      size_t list_name_len,
                                      char const* elem_data, size_t elem_len,
                                      char const* pos_elem,
                                      size_t pos_elem_len);
extern KVDKStatus KVDKListErase(KVDKEngine* engine, char const* list_name,
                                size_t list_len, long index, char** elem_data,
                                size_t* elem_len);
extern KVDKStatus KVDKListReplace(KVDKEngine* engine, char const* list_name,
                                  size_t list_name_len, long index,
                                  char const* elem, size_t elem_len);
/// ListIterator //////////////////////////////////////////////////////////////
extern KVDKListIterator* KVDKListIteratorCreate(KVDKEngine* engine,
                                                char const* key_data,
                                                size_t key_len, KVDKStatus* s);
extern void KVDKListIteratorDestroy(KVDKEngine* engine, KVDKListIterator* iter);
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

extern KVDKStatus KVDKTypeOf(KVDKEngine* engine, char const* key_data,
                             size_t key_len, KVDKValueType* type);

#ifdef __cplusplus
} /* end extern "C" */
#endif  // extern "C"
