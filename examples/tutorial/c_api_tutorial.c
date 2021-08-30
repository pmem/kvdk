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
  char *error = NULL;
  // Purge old KVDK instance
  KVDKRemovePMemContents(engine_path);
  // open engine
  KVDKEngine *kvdk_engine = KVDKOpen(engine_path, kvdk_configs, stdout, &error);
  assert(!error);
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
    KVDKSet(kvdk_engine, key1, key1_len, value1, value1_len, &error);
    assert(!error);
    KVDKSet(kvdk_engine, key2, key1_len, value2, value2_len, &error);
    assert(!error);
    read_v1 = KVDKGet(kvdk_engine, key1, key1_len, &read_v1_len, &error);
    assert(!error);
    assert(strcmp(read_v1, value1) == 0);
    KVDKSet(kvdk_engine, key1, key1_len, value2, value2_len, &error);
    read_v1 = KVDKGet(kvdk_engine, key1, key1_len, &read_v1_len, &error);
    assert(!error);
    assert(strcmp(read_v1, value2) == 0);
    read_v2 = KVDKGet(kvdk_engine, key2, key2_len, &read_v2_len, &error);
    assert(strcmp(read_v2, value2) == 0);
    KVDKDelete(kvdk_engine, key1, key1_len, &error);
    assert(!error);
    KVDKDelete(kvdk_engine, key2, key2_len, &error);
    assert(!error);
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
    KVDKSortedSet(kvdk_engine, collection1, strlen(collection1), key1,
                  strlen(key1), value1, strlen(value1), &error);
    assert(!error);
    KVDKSortedSet(kvdk_engine, collection2, strlen(collection2), key2,
                  strlen(key2), value2, strlen(value2), &error);
    read_v1 = KVDKSortedGet(kvdk_engine, collection1, strlen(collection1), key1,
                            strlen(key1), &read_v1_len, &error);
    assert(!error);
    assert(strcmp(read_v1, value1) == 0);
    KVDKSortedSet(kvdk_engine, collection1, strlen(collection1), key1,
                  strlen(key1), value2, strlen(value2), &error);
    assert(!error);
    read_v1 = KVDKSortedGet(kvdk_engine, collection1, strlen(collection1), key1,
                            strlen(key1), &read_v1_len, &error);
    assert(!error);
    assert(strcmp(read_v1, value2) == 0);
    read_v2 = KVDKSortedGet(kvdk_engine, collection2, strlen(collection2), key2,
                            strlen(key2), &read_v2_len, &error);
    assert(!error);
    assert(strcmp(read_v2, value2) == 0);
    KVDKSortedDelete(kvdk_engine, collection1, strlen(collection1), key1,
                     strlen(key1), &error);
    assert(!error);
    KVDKSortedDelete(kvdk_engine, collection2, strlen(collection2), key2,
                     strlen(key2), &error);
    assert(!error);
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
      KVDKSortedSet(kvdk_engine, sorted_collection, strlen(sorted_collection),
                    key, strlen(key), value, strlen(value), &error);
      assert(!error);
    }
    // create sorted iterator
    KVDKIterator *kvdk_iter =
        KVDKCreateIterator(kvdk_engine, sorted_collection);
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
    KVDKWrite(kvdk_engine, kvdk_wb, &error);
    assert(!error);
    read_v1 = KVDKGet(kvdk_engine, key2, strlen(key2), &read_v1_len, &error);
    assert(!error);
    assert(strcmp(read_v2, value2) == 0);
    printf(
        "Successfully performed BatchWrite on anonymous global collection.\n");
    KVDKWriteBatchDestory(kvdk_wb);
    free(read_v1);
    free(read_v2);
  }
  free(error);
  KVDKConigsDestory(kvdk_configs);
  KVDKCloseEngine(kvdk_engine);
  return 0;
}