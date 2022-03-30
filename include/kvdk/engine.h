/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#ifdef _WIN32
#ifdef KVDK_DLL
#ifdef KVDK_LIBRARY_EXPORTS
#define KVDK_LIBRARY_API __declspec(dllimport)
#else
#define KVDK_LIBRARY_API __declspec(dllexport)
#endif
#else
#define KVDK_LIBRARY_API
#endif
#else
#define KVDK_LIBRARY_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "status.h"

typedef struct KVDKEngine KVDKEngine;
typedef struct KVDKConfigs KVDKConfigs;
typedef struct KVDKWriteOptions KVDKWriteOptions;
typedef struct KVDKWriteBatch KVDKWriteBatch;
typedef struct KVDKIterator KVDKIterator;
typedef struct KVDKSnapshot KVDKSnapshot;
typedef struct KVDKSortedCollectionConfigs KVDKSortedCollectionConfigs;

typedef enum { SORTED, HASH } KVDKIterType;

extern KVDK_LIBRARY_API KVDKConfigs* KVDKCreateConfigs(void);
extern KVDK_LIBRARY_API void KVDKSetConfigs(
    KVDKConfigs* kv_config, uint64_t max_access_threads,
    uint64_t pmem_file_size, unsigned char populate_pmem_space,
    uint32_t pmem_block_size, uint64_t pmem_segment_blocks,
    uint32_t hash_bucket_size, uint64_t hash_bucket_num,
    uint32_t num_buckets_per_slot);
extern KVDK_LIBRARY_API void KVDKDestroyConfigs(KVDKConfigs* kv_config);

KVDK_LIBRARY_API KVDKWriteOptions* KVDKCreateWriteOptions(void);
KVDK_LIBRARY_API void KVDKDestroyWriteOptions(KVDKWriteOptions*);
KVDK_LIBRARY_API void KVDKWriteOptionsSetTTLTime(KVDKWriteOptions*, int64_t);
KVDK_LIBRARY_API void KVDKWriteOptionsSetKeyExist(KVDKWriteOptions*,
                                                  unsigned char);

extern KVDK_LIBRARY_API KVDKSortedCollectionConfigs*
KVDKCreateSortedCollectionConfigs();
extern KVDK_LIBRARY_API void KVDKSetSortedCollectionConfigs(
    KVDKSortedCollectionConfigs* configs, const char* comp_func_name,
    size_t comp_func_len);
extern KVDK_LIBRARY_API void KVDKDestroySortedCollectionConfigs(
    KVDKSortedCollectionConfigs* configs);

extern KVDK_LIBRARY_API KVDKStatus KVDKOpen(const char* name,
                                            const KVDKConfigs* config,
                                            FILE* log_file,
                                            KVDKEngine** engine);
extern KVDK_LIBRARY_API void KVDKReleaseAccessThread(KVDKEngine* engine);
extern KVDK_LIBRARY_API void KVDKCloseEngine(KVDKEngine* engine);
extern KVDK_LIBRARY_API void KVDKRemovePMemContents(const char* name);
extern KVDK_LIBRARY_API KVDKSnapshot* KVDKGetSnapshot(KVDKEngine* engine,
                                                      int make_checkpoint);
extern KVDK_LIBRARY_API void KVDKReleaseSnapshot(KVDKEngine* engine,
                                                 KVDKSnapshot* snapshot);

extern KVDK_LIBRARY_API int KVDKRegisterCompFunc(
    KVDKEngine* engine, const char* compara_name, size_t compara_len,
    int (*compare)(const char* src, size_t src_len, const char* target,
                   size_t target_len));

// Create Sorted Collection
extern KVDK_LIBRARY_API KVDKStatus KVDKCreateSortedCollection(
    KVDKEngine* engine, const char* collection_name, size_t collection_len,
    KVDKSortedCollectionConfigs* configs);

// For BatchWrite
extern KVDK_LIBRARY_API KVDKWriteBatch* KVDKWriteBatchCreate(void);
extern KVDK_LIBRARY_API void KVDKWriteBatchDestory(KVDKWriteBatch*);
extern KVDK_LIBRARY_API void KVDKWriteBatchDelete(KVDKWriteBatch*,
                                                  const char* key,
                                                  size_t key_len);
extern KVDK_LIBRARY_API void KVDKWriteBatchPut(KVDKWriteBatch*, const char* key,
                                               size_t key_len,
                                               const char* value,
                                               size_t value_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKWrite(KVDKEngine* engine,
                                             const KVDKWriteBatch* batch);

// For Anonymous Global Collection
extern KVDK_LIBRARY_API KVDKStatus KVDKGet(KVDKEngine* engine, const char* key,
                                           size_t key_len, size_t* val_len,
                                           char** val);
extern KVDK_LIBRARY_API KVDKStatus
KVDKSet(KVDKEngine* engine, const char* key, size_t key_len, const char* val,
        size_t val_len, const KVDKWriteOptions* write_option);
extern KVDK_LIBRARY_API KVDKStatus KVDKDelete(KVDKEngine* engine,
                                              const char* key, size_t key_len);
// Modify value of existing "key" according to modify function, and update
// existing value with modify result
//
// modify: a function to modify existing value. old_val is existing value, store
// modify result in new_val
//
// Return Ok if key exists and update old_val to new_value successful
extern KVDK_LIBRARY_API KVDKStatus
KVDKModify(KVDKEngine* engine, const char* key, size_t key_len, char* new_value,
           size_t* new_value_len,
           void (*modify)(const char* old_val, size_t old_val_len,
                          char* new_val, size_t* new_val_len),
           const KVDKWriteOptions* write_option);

// For Named Global Collection
extern KVDK_LIBRARY_API KVDKStatus
KVDKSortedSet(KVDKEngine* engine, const char* collection, size_t collection_len,
              const char* key, size_t key_len, const char* val, size_t val_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKSortedDelete(KVDKEngine* engine,
                                                    const char* collection,
                                                    size_t collection_len,
                                                    const char* key,
                                                    size_t key_len);
extern KVDK_LIBRARY_API KVDKStatus
KVDKSortedGet(KVDKEngine* engine, const char* collection, size_t collection_len,
              const char* key, size_t key_len, size_t* val_len, char** val);

// For Hash Collection
extern KVDK_LIBRARY_API KVDKStatus KVDKHashSet(KVDKEngine* engine,
                                               const char* collection,
                                               size_t collection_len,
                                               const char* key, size_t key_len,
                                               const char* val, size_t val_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKHashDelete(KVDKEngine* engine,
                                                  const char* collection,
                                                  size_t collection_len,
                                                  const char* key,
                                                  size_t key_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKHashGet(KVDKEngine* engine,
                                               const char* collection,
                                               size_t collection_len,
                                               const char* key, size_t key_len,
                                               size_t* val_len, char** val);

// For Queue
extern KVDK_LIBRARY_API KVDKStatus KVDKLPush(KVDKEngine* engine,
                                             const char* collection,
                                             size_t collection_len,
                                             const char* key, size_t key_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKLPop(KVDKEngine* engine,
                                            const char* collection,
                                            size_t collection_len, char** key,
                                            size_t* key_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKRPush(KVDKEngine* engine,
                                             const char* collection,
                                             size_t collection_len,
                                             const char* key, size_t key_len);

extern KVDK_LIBRARY_API KVDKStatus KVDKRPop(KVDKEngine* engine,
                                            const char* collection,
                                            size_t collection_len, char** key,
                                            size_t* key_len);

extern KVDK_LIBRARY_API KVDKIterator* KVDKCreateUnorderedIterator(
    KVDKEngine* engine, const char* collection, size_t collection_len);
extern KVDK_LIBRARY_API KVDKIterator* KVDKCreateSortedIterator(
    KVDKEngine* engine, const char* collection, size_t collection_len,
    KVDKSnapshot* snapshot);
extern KVDK_LIBRARY_API void KVDKDestroyIterator(KVDKEngine* engine,
                                                 KVDKIterator* iterator);
extern KVDK_LIBRARY_API void KVDKIterSeekToFirst(KVDKIterator* iter);
extern KVDK_LIBRARY_API void KVDKIterSeekToLast(KVDKIterator* iter);
extern KVDK_LIBRARY_API void KVDKIterSeek(KVDKIterator* iter, const char* str,
                                          size_t str_len);
extern KVDK_LIBRARY_API void KVDKIterNext(KVDKIterator* iter);
extern KVDK_LIBRARY_API void KVDKIterPre(KVDKIterator* iter);
extern KVDK_LIBRARY_API unsigned char KVDKIterValid(KVDKIterator* iter);
extern KVDK_LIBRARY_API const char* KVDKIterKey(KVDKIterator* iter,
                                                size_t* key_len);
extern KVDK_LIBRARY_API const char* KVDKIterValue(KVDKIterator* iter,
                                                  size_t* val_len);

// For Expire
/* ttl_time is negetive or positive number, If ttl_time == INT64_MAX,
 * the key is persistent; If ttl_time <=0, the key is expired immediately.
 */
KVDK_LIBRARY_API KVDKStatus KVDKExpire(KVDKEngine* engine, const char* str,
                                       size_t str_len, int64_t ttl_time);
/* ttl_time is INT64_MAX and return Status::Ok if the key is persist
 * ttl_time is 0 and return Status::NotFound if the key is expired or doesnot
 * exist.
 * ttl_time is certain positive number and return Status::Ok if the key hasn't
 * expired and exist.
 */
KVDK_LIBRARY_API KVDKStatus KVDKGetTTL(KVDKEngine* engine, const char* str,
                                       size_t str_len, int64_t* ttl_time);

#ifdef __cplusplus
} /* end extern "C" */
#endif
