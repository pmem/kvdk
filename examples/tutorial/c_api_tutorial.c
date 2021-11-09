/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kvdk/engine.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>

// The KVDK instance is mounted as a directory
// /mnt/pmem0/tutorial_kvdk_example.
// Modify this path if necessary.
const char *pmem_path = "/mnt/pmem0/tutorial_kvdk_example";

int main() {
  // Initialize a KVDK instance.
  KVDKConfigs *kvdk_configs = KVDKCreateConfigs();
  KVDKUserConfigs(kvdk_configs, 48, 1ull << 20, 1u, 64u, 1ull << 8, 128u,
                  1ull << 10, 1 << 4);

  const char *engine_path = "/mnt/pmem0/tutorial_kvdk_example";
  // Purge old KVDK instance
  KVDKRemovePMemContents(engine_path);
  // open engine
  KVDKEngine *kvdk_engine;
  assert(KVDKOpen(engine_path, kvdk_configs, stdout, &kvdk_engine) == Ok);
  // Reads and Writes on Anonymous Global Collection
  {
    const char *key1 = "key1";
    const char *key2 = "key2";
    const char *value1 = "value1";
    const char *value2 = "value2";
    char *read_v1;
    char *read_v2;
    size_t key1_len = strlen(key1);
    size_t key2_len = strlen(key2);
    size_t value1_len = strlen(value1);
    size_t value2_len = strlen(value2);
    size_t read_v1_len, read_v2_len;
    assert(KVDKSet(kvdk_engine, key1, key1_len, value1, value1_len) == Ok);
    assert(KVDKSet(kvdk_engine, key2, key1_len, value2, value2_len) == Ok);
    assert(KVDKGet(kvdk_engine, key1, key1_len, &read_v1_len, &read_v1) == Ok);
    assert(strcmp(read_v1, value1) == 0);
    assert(KVDKSet(kvdk_engine, key1, key1_len, value2, value2_len) == Ok);
    assert(KVDKGet(kvdk_engine, key1, key1_len, &read_v1_len, &read_v1) == Ok);
    assert(strcmp(read_v1, value2) == 0);
    assert(KVDKGet(kvdk_engine, key2, key2_len, &read_v2_len, &read_v2) == Ok);
    assert(strcmp(read_v2, value2) == 0);
    assert(KVDKDelete(kvdk_engine, key1, key1_len) == Ok);
    assert(KVDKDelete(kvdk_engine, key2, key2_len) == Ok);
    free(read_v1);
    free(read_v2);
    printf("Successfully performed Get, Set, Delete operations on anonymous "
           "global collection.\n");
  }
  // Reads and Writes on Named Collection
  {
    const char *collection1 = "collection1";
    const char *collection2 = "collection2";
    const char *key1 = "key1";
    const char *key2 = "key2";
    const char *value1 = "value1";
    const char *value2 = "value2";
    char *read_v1;
    char *read_v2;
    size_t read_v1_len, read_v2_len;
    assert(KVDKSortedSet(kvdk_engine, collection1, strlen(collection1), key1,
                         strlen(key1), value1, strlen(value1)) == Ok);
    assert(KVDKSortedSet(kvdk_engine, collection2, strlen(collection2), key2,
                         strlen(key2), value2, strlen(value2)) == Ok);
    assert(KVDKSortedGet(kvdk_engine, collection1, strlen(collection1), key1,
                         strlen(key1), &read_v1_len, &read_v1) == Ok);
    assert(strcmp(read_v1, value1) == 0);
    assert(KVDKSortedSet(kvdk_engine, collection1, strlen(collection1), key1,
                         strlen(key1), value2, strlen(value2)) == Ok);
    assert(KVDKSortedGet(kvdk_engine, collection1, strlen(collection1), key1,
                         strlen(key1), &read_v1_len, &read_v1) == Ok);
    assert(strcmp(read_v1, value2) == 0);
    assert(KVDKSortedGet(kvdk_engine, collection2, strlen(collection2), key2,
                         strlen(key2), &read_v2_len, &read_v2) == Ok);
    assert(strcmp(read_v2, value2) == 0);
    assert(KVDKSortedDelete(kvdk_engine, collection1, strlen(collection1), key1,
                            strlen(key1)) == Ok);
    assert(KVDKSortedDelete(kvdk_engine, collection2, strlen(collection2), key2,
                            strlen(key2)) == Ok);
    free(read_v1);
    free(read_v2);
    printf("Successfully performed SortedGet, SortedSet, SortedDelete "
           "operations on named "
           "collections.\n");
  }
  // Iterating a Sorted Named Collection
  {
    const char *nums[10] = {"4", "5", "0", "2", "9", "1", "3", "8", "6", "7"};
    const char *sorted_nums[10] = {"0", "1", "2", "3", "4",
                                   "5", "6", "7", "8", "9"};
    const char *sorted_collection = "sorted_collection";
    for (int i = 0; i < 10; ++i) {
      char key[10] = "key", value[10] = "value";
      strcat(key, nums[i]);
      strcat(value, nums[i]);
      assert(KVDKSortedSet(kvdk_engine, sorted_collection,
                           strlen(sorted_collection), key, strlen(key), value,
                           strlen(value)) == Ok);
    }
    // create sorted iterator
    KVDKIterator *kvdk_iter =
        KVDKCreateIterator(kvdk_engine, sorted_collection, SORTED);
    KVDKIterSeekToFirst(kvdk_iter);
    // Iterate through range ["key1", "key8").
    const char *beg = "key1";
    const char *end = "key8";

    int i = 1;
    KVDKIterSeek(kvdk_iter, beg);
    for (KVDKIterSeek(kvdk_iter, beg);
         KVDKIterValid(kvdk_iter) && (strcmp(KVDKIterKey(kvdk_iter), end) <= 0);
         KVDKIterNext(kvdk_iter)) {
      char key[10] = "key", value[10] = "value";
      strcat(key, sorted_nums[i]);
      strcat(value, sorted_nums[i]);
      assert(strcmp(KVDKIterKey(kvdk_iter), key) == 0);
      assert(strcmp(KVDKIterValue(kvdk_iter), value) == 0);
      ++i;
    }
    assert(i == 9);

    // Iterate through range ["key8", "key1").
    beg = "key8";
    end = "key1";

    i = 8;
    for (KVDKIterSeek(kvdk_iter, beg);
         KVDKIterValid(kvdk_iter) && (strcmp(KVDKIterKey(kvdk_iter), end) >= 0);
         KVDKIterPre(kvdk_iter)) {
      char key[10] = "key", value[10] = "value";
      strcat(key, sorted_nums[i]);
      strcat(value, sorted_nums[i]);
      assert(strcmp(KVDKIterKey(kvdk_iter), key) == 0);
      assert(strcmp(KVDKIterValue(kvdk_iter), value) == 0);
      --i;
    }
    assert(i == 0);
    printf("Successfully iterated through a sorted named collections.\n");
    KVDKIterDestory(kvdk_iter);
  }
  // BatchWrite on Anonymous Global Collection
  {
    const char *key1 = "key1";
    const char *key2 = "key2";
    const char *value1 = "value1";
    const char *value2 = "value2";
    char *read_v1;
    char *read_v2;
    size_t read_v1_len, read_v2_len;
    KVDKWriteBatch *kvdk_wb = KVDKWriteBatchCreate();
    KVDKWriteBatchPut(kvdk_wb, key1, strlen(key1), value1, strlen(value1));
    KVDKWriteBatchPut(kvdk_wb, key2, strlen(key2), value2, strlen(value2));
    KVDKWriteBatchDelete(kvdk_wb, key1, strlen(key1));
    KVDKWriteBatchPut(kvdk_wb, key1, strlen(key1), value2, strlen(value1));
    assert(KVDKWrite(kvdk_engine, kvdk_wb) == Ok);
    assert(KVDKGet(kvdk_engine, key1, strlen(key1), &read_v1_len, &read_v1) ==
           Ok);
    assert(strcmp(read_v1, value2) == 0);
    assert(KVDKGet(kvdk_engine, key2, strlen(key2), &read_v2_len, &read_v2) ==
           Ok);
    assert(strcmp(read_v2, value2) == 0);
    printf(
        "Successfully performed BatchWrite on anonymous global collection.\n");
    KVDKWriteBatchDestory(kvdk_wb);
    free(read_v1);
    free(read_v2);
  }

  // HashMap Collection
  {
    const char *nums[10] = {"9", "5", "2", "0", "7", "3", "1", "8", "6", "4"};
    const char *hash_collection = "hash_collection";
    for (int i = 0; i < 10; ++i) {
      char key[10] = "key", value[10] = "value";
      strcat(key, nums[i]);
      strcat(value, nums[i]);
      assert(KVDKHashSet(kvdk_engine, hash_collection, strlen(hash_collection),
                         key, strlen(key), value, strlen(value)) == Ok);
      size_t val_len;
      char *val;
      assert(KVDKHashGet(kvdk_engine, hash_collection, strlen(hash_collection),
                         key, strlen(key), &val_len, &val) == Ok);
      assert(strcmp(val, value) == 0);
      free(val);
    }

    assert(KVDKHashDelete(kvdk_engine, hash_collection, strlen(hash_collection),
                          "key8", strlen("key8")) == Ok);
    // create sorted iterator
    KVDKIterator *kvdk_iter =
        KVDKCreateIterator(kvdk_engine, hash_collection, HASH);
    int cnt = 0;
    for (KVDKIterSeekToFirst(kvdk_iter); KVDKIterValid(kvdk_iter);
         KVDKIterNext(kvdk_iter)) {
      ++cnt;
    }
    assert(cnt == 9);

    cnt = 0;
    for (KVDKIterSeekToLast(kvdk_iter); KVDKIterValid(kvdk_iter);
         KVDKIterPre(kvdk_iter)) {
      ++cnt;
    }
    printf("Successfully performed Get Set Delete Iterate on HashList.\n");
    assert(cnt == 9);
    KVDKIterDestory(kvdk_iter);
  }
  KVDKConfigsDestory(kvdk_configs);
  KVDKCloseEngine(kvdk_engine);
  return 0;
}