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

typedef struct KVDKEngine KVDKEngine;
typedef struct KVDKConfigs KVDKConfigs;
typedef struct KVDKWriteBatch KVDKWriteBatch;
typedef struct KVDKIterator KVDKIterator;

typedef enum { Sucess, Fail } KVDKCommonStatus;

extern KVDK_LIBRARY_API KVDKConfigs *KVDKCreateConfigs(void);
extern KVDK_LIBRARY_API void
KVDKUserConfigs(KVDKConfigs *kv_config, uint64_t max_write_threads,
                uint64_t pmem_file_size, unsigned char populate_pmem_space,
                uint32_t pmem_block_size, uint64_t pmem_segment_blocks,
                uint32_t hash_bucket_size, uint64_t hash_bucket_num,
                uint32_t num_buckets_per_slot);
extern KVDK_LIBRARY_API void KVDKConigsDestory(KVDKConfigs *kv_config);

extern KVDK_LIBRARY_API KVDKEngine *KVDKOpen(const char *name,
                                             const KVDKConfigs *config,
                                             FILE *log_file, char **error);
extern KVDK_LIBRARY_API void KVDKCloseEngine(KVDKEngine *engine);
extern KVDK_LIBRARY_API void KVDKRemovePMemContents(const char *name);
// For BatchWrite
extern KVDK_LIBRARY_API KVDKWriteBatch *KVDKWriteBatchCreate(void);
extern KVDK_LIBRARY_API void KVDKWriteBatchDestory(KVDKWriteBatch *);
extern KVDK_LIBRARY_API void
KVDKWriteBatchDelete(KVDKWriteBatch *, const char *key, size_t key_len);
extern KVDK_LIBRARY_API void KVDKWriteBatchPut(KVDKWriteBatch *,
                                               const char *key, size_t key_len,
                                               const char *value,
                                               size_t value_len);
extern KVDK_LIBRARY_API void
KVDKWrite(KVDKEngine *engine, const KVDKWriteBatch *batch, char **error);

// For Anonymous Global Collection
extern KVDK_LIBRARY_API char *KVDKGet(KVDKEngine *engine, const char *key,
                                      size_t key_len, size_t *val_len,
                                      char **error);
extern KVDK_LIBRARY_API void KVDKSet(KVDKEngine *engine, const char *key,
                                     size_t key_len, const char *val,
                                     size_t val_len, char **error);
extern KVDK_LIBRARY_API void KVDKDelete(KVDKEngine *engine, const char *key,
                                        size_t key_len, char **error);

// For Named Global Collection
extern KVDK_LIBRARY_API void
KVDKSortedSet(KVDKEngine *engine, const char *collection, size_t collection_len,
              const char *key, size_t key_len, const char *val, size_t val_len,
              char **error);
extern KVDK_LIBRARY_API void KVDKSortedDelete(KVDKEngine *engine,
                                              const char *collection,
                                              size_t collection_len,
                                              const char *key, size_t key_len,
                                              char **error);
extern KVDK_LIBRARY_API char *
KVDKSortedGet(KVDKEngine *engine, const char *collection, size_t collection_len,
              const char *key, size_t key_len, size_t *val_len, char **error);

extern KVDK_LIBRARY_API KVDKIterator *
KVDKCreateIterator(KVDKEngine *engine, const char *collection);
extern KVDK_LIBRARY_API void KVDKIterDestory(KVDKIterator *iter);
extern KVDK_LIBRARY_API void KVDKIterSeekToFirst(KVDKIterator *iter);
extern KVDK_LIBRARY_API void KVDKIterSeek(KVDKIterator *iter, const char *key);
extern KVDK_LIBRARY_API void KVDKIterNext(KVDKIterator *iter);
extern KVDK_LIBRARY_API void KVDKIterPre(KVDKIterator *iter);
extern KVDK_LIBRARY_API unsigned char KVDKIterValid(KVDKIterator *iter);
extern KVDK_LIBRARY_API const char *KVDKIterKey(KVDKIterator *iter);
extern KVDK_LIBRARY_API const char *KVDKIterValue(KVDKIterator *iter);

#ifdef __cplusplus
} /* end extern "C" */
#endif
