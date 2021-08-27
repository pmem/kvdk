/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kvdk/engine.h"
#include <cstdlib>
#include <cstring>

// The KVDK instance is mounted as a directory
// /mnt/pmem0/tutorial_kvdk_example.
// Modify this path if necessary.
const char *pmem_path = "/mnt/pmem0/tutorial_kvdk_example";

int main() {
  // Initialize a KVDK instance.
  KVDKConfigs *kvdk_configs = KVDKCreateConfigs();
  KVDKUserConfigs(kvdk_configs, pmem_file_size = 1ull << 20);

  const char engine_path = "/mnt/pmem0/tutorial_kvdk_example";
  // Purge old KVDK instance
  KVDKRemovePMemContents(engine_path);
  // open engine
  KVDKEnigne *kvdk_engine = KVDKOpen(engine_path, kvdk_configs, stdout);
  // Reads and Writes on Anonymous Global Collection
  {
    const char *key1 = "key1", key2 = "key2";
    const char *value1 = "value1", value2 = "value2";
    char *read_v1, read_v2;
    KVDKSet(kvdk_engine, key1, value1);
    KVDKSet(kvdk_engine, key2, value2);
    KVDKGet(kvdk_engine, key1, read_v1);
    assert(strcmp(read_v1, value1) == 0);
    KVDKSet(kvdk_engine, key1, value2);
    KVDKGet(kvdk_engine, key1, read_v1);
    assert(strcmp(read_v1, value2) == 0);
    KVDKGet(kvdk_engine, key2, read_v2);
    assert(strcmp(read_v2, value2) == 0);
    KVDKDelete(kvdk_engine, key1);
    KVDKDelete(kvdk_engine, key2);
    printf("Successfully performed Get, Set, Delete operations on anonymous "
           "global collection.\n");
  }
  // Reads and Writes on Named Collection
  {
    const char *collection1 = "collection1", collection2 = "collection2";
    const char *key1 = "key1", key2 = "key2";
    const char *value1 = "value1", value2 = "value2";
    char *read_v1, read_v2;
    KVDKSortedSet(kvdk_engine, collection1, key1, value1);
    KVDKSortedSet(kvdk_engine, collection2, key2, value2);
    KVDKSortedGet(kvdk_engine, collection1, key1, read_v1);
    assert(strcmp(read_v1, value1) == 0);
    KVDKSortedSet(kvdk_engine, collection1, key1, value2);
    KVDKSortedGet(kvdk_engine, collection1, key1, read_v1);
    assert(strcmp(read_v1, value2) == 0);
    KVDKSortedGet(kvdk_engine, collection2, key2, read_v2);
    assert(strcmp(read_v2, value2) == 0);
    KVDKSortedDelete(kvdk_engine, collection1, key1);
    KVDKSortedDelete(kvdk_engine, collection2, key2);
    printf("Successfully performed SortedGet, SortedSet, SortedDelete "
           "operations on named "
           "collections.\n");
  }
  // Iterating a Sorted Named Collection
  {
    char nums = {"4", "5", "0", "2", "9", "1", "3", "8", "6", "7"};
    const char *sorted_collection = "sorted_collection";
    for (int i = 0; i < 10; ++i) {
      char *key = std::strcat("key", nums[i]);
      char *value = std::strcat("value", nums[i]);
      KVDKSortedSet(kvdk_engine, collection1, key, value);
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
         KVDKIterValid(kvdk_iter) && KVDKIterKey(kvdk_iter) < end;
         KVDKIterNext(kvdk_iter)) {
      assert(strcmp(KVDKIterKey(kvdk_iter), strcat("key", itoa(i))) == 0);
      assert(strcmp(KVDKIterValue(kvdk_iter), strcat("key", itoa(i)) == 0);
      ++i;
    }
    assert(i == 8);

    // Iterate through range ["key8", "key1").
    const char *beg = "key8";
    const char *end = "key1";

    int i = 8;
    KVDKIterSeek(kvdk_iter, beg);
    for (KVDKIterSeek(kvdk_iter, beg);
         KVDKIterValid(kvdk_iter) && KVDKIterKey(kvdk_iter) > end;
         KVDKIterPre(kvdk_iter)) {
      assert(strcmp(KVDKIterKey(kvdk_iter), strcat("key", itoa(i))) == 0);
      assert(strcmp(KVDKIterValue(kvdk_iter), strcat("key", itoa(i)) == 0);
      --i;
    }
    assert(i == 0);
    printf("Successfully iterated through a sorted named collections.\n");
    KVDKIterDestory(kvdk_iter);
  }
  // BatchWrite on Anonymous Global Collection
  {
    const char *key1 = "key1", key2 = "key2";
    const char *value1 = "value1", value2 = "value2";
    char *read_v1, read_v2;
    KVDKWriteBatch *kvdk_wb = KVDKWriteBatchCreate();
    KVDKWriteBatchPut(kvdk_wb, key1, value1);
    KVDKWriteBatchPut(kvdk_wb, key2, value2);
    KVDKWriteBatchDelete(kvdk_wb, key1);
    KVDKWrite(kvdk_engine, kvdk_wb);
    KVDKGet(kvdk_engine, key2, read_v2);
    assert(strcmp(read_v2, value2) == 0);
    printf(
        "Successfully performed BatchWrite on anonymous global collection.\n");
    KVDKWriteBatchDestory(kvdk_wb);
  }
  KVDKConigsDestory(kvdk_configs);
  KVDKCloseEngine(kvdk_engine);
  return 0;
}