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
typedef struct KVDKWriteBatch KVDKWriteBatch;
typedef struct KVDKIterator KVDKIterator;
typedef struct KVDKCollection KVDKCollection;

typedef enum { SORTED, HASH } KVDKIterType;

extern KVDK_LIBRARY_API KVDKConfigs *KVDKCreateConfigs(void);
extern KVDK_LIBRARY_API void
KVDKUserConfigs(KVDKConfigs *kv_config, uint64_t max_access_threads,
                uint64_t pmem_file_size, unsigned char populate_pmem_space,
                uint32_t pmem_block_size, uint64_t pmem_segment_blocks,
                uint32_t hash_bucket_size, uint64_t hash_bucket_num,
                uint32_t num_buckets_per_slot);
extern KVDK_LIBRARY_API void KVDKConfigsDestory(KVDKConfigs *kv_config);

extern KVDK_LIBRARY_API KVDKStatus KVDKOpen(const char *name,
                                            const KVDKConfigs *config,
                                            FILE *log_file,
                                            KVDKEngine **engine);
extern KVDK_LIBRARY_API void KVDKReleaseAccessThread(KVDKEngine *engine);
extern KVDK_LIBRARY_API void KVDKCloseEngine(KVDKEngine *engine);
extern KVDK_LIBRARY_API void KVDKRemovePMemContents(const char *name);

extern KVDK_LIBRARY_API void
KVDKRegisterCompFunc(KVDKEngine *engine, const char *compara_name,
                     size_t compara_len,
                     int (*compare)(const char *src, size_t src_len,
                                    const char *target, size_t target_len));

// Create Sorted Collection
extern KVDK_LIBRARY_API KVDKStatus KVDKCreateSortedCollection(
    KVDKEngine *engine, KVDKCollection **sorted_collection,
    const char *collection_name, size_t collection_len,
    const char *compara_name, size_t compara_len);

extern KVDK_LIBRARY_API void
KVDKDestorySortedCollection(KVDKCollection *collection);

// For BatchWrite
extern KVDK_LIBRARY_API KVDKWriteBatch *KVDKWriteBatchCreate(void);
extern KVDK_LIBRARY_API void KVDKWriteBatchDestory(KVDKWriteBatch *);
extern KVDK_LIBRARY_API void
KVDKWriteBatchDelete(KVDKWriteBatch *, const char *key, size_t key_len);
extern KVDK_LIBRARY_API void KVDKWriteBatchPut(KVDKWriteBatch *,
                                               const char *key, size_t key_len,
                                               const char *value,
                                               size_t value_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKWrite(KVDKEngine *engine,
                                             const KVDKWriteBatch *batch);

// For Anonymous Global Collection
extern KVDK_LIBRARY_API KVDKStatus KVDKGet(KVDKEngine *engine, const char *key,
                                           size_t key_len, size_t *val_len,
                                           char **val);
extern KVDK_LIBRARY_API KVDKStatus KVDKSet(KVDKEngine *engine, const char *key,
                                           size_t key_len, const char *val,
                                           size_t val_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKDelete(KVDKEngine *engine,
                                              const char *key, size_t key_len);

// For Named Global Collection
extern KVDK_LIBRARY_API KVDKStatus
KVDKSortedSet(KVDKEngine *engine, const char *collection, size_t collection_len,
              const char *key, size_t key_len, const char *val, size_t val_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKSortedDelete(KVDKEngine *engine,
                                                    const char *collection,
                                                    size_t collection_len,
                                                    const char *key,
                                                    size_t key_len);
extern KVDK_LIBRARY_API KVDKStatus
KVDKSortedGet(KVDKEngine *engine, const char *collection, size_t collection_len,
              const char *key, size_t key_len, size_t *val_len, char **val);

// For Hash Collection
extern KVDK_LIBRARY_API KVDKStatus KVDKHashSet(KVDKEngine *engine,
                                               const char *collection,
                                               size_t collection_len,
                                               const char *key, size_t key_len,
                                               const char *val, size_t val_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKHashDelete(KVDKEngine *engine,
                                                  const char *collection,
                                                  size_t collection_len,
                                                  const char *key,
                                                  size_t key_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKHashGet(KVDKEngine *engine,
                                               const char *collection,
                                               size_t collection_len,
                                               const char *key, size_t key_len,
                                               size_t *val_len, char **val);

// For Queue
extern KVDK_LIBRARY_API KVDKStatus KVDKLPush(KVDKEngine *engine,
                                             const char *collection,
                                             size_t collection_len,
                                             const char *key, size_t key_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKLPop(KVDKEngine *engine,
                                            const char *collection,
                                            size_t collection_len, char **key,
                                            size_t *key_len);
extern KVDK_LIBRARY_API KVDKStatus KVDKRPush(KVDKEngine *engine,
                                             const char *collection,
                                             size_t collection_len,
                                             const char *key, size_t key_len);

extern KVDK_LIBRARY_API KVDKStatus KVDKRPop(KVDKEngine *engine,
                                            const char *collection,
                                            size_t collection_len, char **key,
                                            size_t *key_len);

extern KVDK_LIBRARY_API KVDKIterator *
KVDKCreateIterator(KVDKEngine *engine, const char *collection,
                   size_t collection_len, KVDKIterType iter_type);
extern KVDK_LIBRARY_API void KVDKIterDestory(KVDKIterator *iter);
extern KVDK_LIBRARY_API void KVDKIterSeekToFirst(KVDKIterator *iter);
extern KVDK_LIBRARY_API void KVDKIterSeekToLast(KVDKIterator *iter);
extern KVDK_LIBRARY_API void KVDKIterSeek(KVDKIterator *iter, const char *str,
                                          size_t str_len);
extern KVDK_LIBRARY_API void KVDKIterNext(KVDKIterator *iter);
extern KVDK_LIBRARY_API void KVDKIterPre(KVDKIterator *iter);
extern KVDK_LIBRARY_API unsigned char KVDKIterValid(KVDKIterator *iter);
extern KVDK_LIBRARY_API const char *KVDKIterKey(KVDKIterator *iter,
                                                size_t *key_len);
extern KVDK_LIBRARY_API const char *KVDKIterValue(KVDKIterator *iter,
                                                  size_t *val_len);

#ifdef __cplusplus
} /* end extern "C" */
#endif
