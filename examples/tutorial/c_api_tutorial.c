/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "kvdk/engine.h"

// The KVDK instance is mounted as a directory
// /mnt/pmem0/tutorial_kvdk_example.
// Modify this path if necessary.
const char* pmem_path = "/mnt/pmem0/tutorial_kvdk_example";

static int CmpCompare(const char* a, size_t alen, const char* b, size_t blen) {
  int n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  if (r == 0) {
    if (alen < blen)
      r = -1;
    else if (alen > blen)
      r = +1;
  }
  return r;
}

void AnonymousCollectionExample(KVDKEngine* kvdk_engine) {
  const char* key1 = "key1";
  const char* key2 = "key2";
  const char* value1 = "value1";
  const char* value2 = "value2";
  char* read_v1;
  char* read_v2;
  size_t key1_len = strlen(key1);
  size_t key2_len = strlen(key2);
  size_t value1_len = strlen(value1);
  size_t value2_len = strlen(value2);
  size_t read_v1_len, read_v2_len;
  int cmp;
  KVDKWriteOptions* write_option = KVDKCreateWriteOptions();
  KVDKStatus s =
      KVDKSet(kvdk_engine, key1, key1_len, value1, value1_len, write_option);
  assert(s == Ok);
  s = KVDKSet(kvdk_engine, key2, key1_len, value2, value2_len, write_option);
  assert(s == Ok);
  s = KVDKGet(kvdk_engine, key1, key1_len, &read_v1_len, &read_v1);
  assert(s == Ok);
  cmp = CmpCompare(read_v1, read_v1_len, value1, value1_len);
  assert(cmp == 0);
  s = KVDKSet(kvdk_engine, key1, key1_len, value2, value2_len, write_option);
  assert(s == Ok);
  s = KVDKGet(kvdk_engine, key1, key1_len, &read_v1_len, &read_v1);
  assert(s == Ok);
  cmp = CmpCompare(read_v1, read_v1_len, value2, value2_len);
  assert(cmp == 0);
  s = KVDKGet(kvdk_engine, key2, key2_len, &read_v2_len, &read_v2);
  assert(s == Ok);
  cmp = CmpCompare(read_v2, read_v2_len, value2, value2_len);
  assert(cmp == 0);
  s = KVDKDelete(kvdk_engine, key1, key1_len);
  assert(s == Ok);
  s = KVDKDelete(kvdk_engine, key2, key2_len);
  assert(s == Ok);
  free(read_v1);
  free(read_v2);
  printf(
      "Successfully performed Get, Set, Delete operations on anonymous "
      "global collection.\n");
}

// Reads and Writes on Named Sorted Collection
void SortedCollectionExample(KVDKEngine* kvdk_engine) {
  const char* collection1 = "collection1";
  const char* collection2 = "collection2";
  const char* key1 = "key1";
  const char* key2 = "key2";
  const char* value1 = "value1";
  const char* value2 = "value2";
  char* read_v1;
  char* read_v2;
  size_t read_v1_len, read_v2_len;
  int cmp;

  KVDKSortedCollectionConfigs* s_configs = KVDKCreateSortedCollectionConfigs();
  KVDKStatus s = KVDKCreateSortedCollection(kvdk_engine, collection1,
                                            strlen(collection1), s_configs);
  assert(s == Ok);
  s = KVDKCreateSortedCollection(kvdk_engine, collection2, strlen(collection2),
                                 s_configs);
  assert(s == Ok);
  s = KVDKSortedSet(kvdk_engine, collection1, strlen(collection1), key1,
                    strlen(key1), value1, strlen(value1));
  assert(s == Ok);
  s = KVDKSortedSet(kvdk_engine, collection2, strlen(collection2), key2,
                    strlen(key2), value2, strlen(value2));
  assert(s == Ok);
  s = KVDKSortedGet(kvdk_engine, collection1, strlen(collection1), key1,
                    strlen(key1), &read_v1_len, &read_v1);
  assert(s == Ok);
  cmp = CmpCompare(read_v1, read_v1_len, value1, strlen(value1));
  assert(cmp == 0);
  s = KVDKSortedSet(kvdk_engine, collection1, strlen(collection1), key1,
                    strlen(key1), value2, strlen(value2));
  assert(s == Ok);
  s = KVDKSortedGet(kvdk_engine, collection1, strlen(collection1), key1,
                    strlen(key1), &read_v1_len, &read_v1);
  assert(s == Ok);
  cmp = CmpCompare(read_v1, read_v1_len, value2, strlen(value2));
  assert(cmp == 0);
  s = KVDKSortedGet(kvdk_engine, collection2, strlen(collection2), key2,
                    strlen(key2), &read_v2_len, &read_v2);
  assert(s == Ok);
  cmp = CmpCompare(read_v2, read_v2_len, value2, strlen(value2));
  assert(cmp == 0);
  s = KVDKSortedDelete(kvdk_engine, collection1, strlen(collection1), key1,
                       strlen(key1));
  assert(s == Ok);
  s = KVDKSortedDelete(kvdk_engine, collection2, strlen(collection2), key2,
                       strlen(key2));
  assert(s == Ok);
  free(read_v1);
  free(read_v2);
  KVDKDestroySortedCollectionConfigs(s_configs);
  printf(
      "Successfully performed SortedGet, SortedSet, SortedDelete "
      "operations on named "
      "collections.\n");
}

void SortedCollectinIterExample(KVDKEngine* kvdk_engine) {
  const char* nums[10] = {"4", "5", "0", "2", "9", "1", "3", "8", "6", "7"};
  const char* sorted_nums[10] = {"0", "1", "2", "3", "4",
                                 "5", "6", "7", "8", "9"};
  const char* sorted_collection = "sorted_collection";
  KVDKSortedCollectionConfigs* s_configs = KVDKCreateSortedCollectionConfigs();
  KVDKStatus s = KVDKCreateSortedCollection(
      kvdk_engine, sorted_collection, strlen(sorted_collection), s_configs);
  assert(s == Ok);
  for (int i = 0; i < 10; ++i) {
    char key[10] = "key", value[10] = "value";
    strcat(key, nums[i]);
    strcat(value, nums[i]);
    s = KVDKSortedSet(kvdk_engine, sorted_collection, strlen(sorted_collection),
                      key, strlen(key), value, strlen(value));
    assert(s == Ok);
  }
  // create sorted iterator
  KVDKIterator* kvdk_iter = KVDKCreateSortedIterator(
      kvdk_engine, sorted_collection, strlen(sorted_collection), NULL);
  KVDKIterSeekToFirst(kvdk_iter);
  // Iterate through range ["key1", "key8").
  const char* beg = "key1";
  const char* end = "key8";

  int i = 1;
  for (KVDKIterSeek(kvdk_iter, beg, strlen(beg)); KVDKIterValid(kvdk_iter);
       KVDKIterNext(kvdk_iter)) {
    char expected_key[10] = "key", expected_value[10] = "value";
    strcat(expected_key, sorted_nums[i]);
    strcat(expected_value, sorted_nums[i]);
    size_t key_len, val_len;
    const char* key_res = KVDKIterKey(kvdk_iter, &key_len);
    const char* val_res = KVDKIterValue(kvdk_iter, &val_len);
    if (CmpCompare(key_res, key_len, end, strlen(end)) > 0) {
      break;
    }
    int cmp = CmpCompare(key_res, key_len, expected_key, strlen(expected_key));
    assert(cmp == 0);
    cmp = CmpCompare(val_res, val_len, expected_value, strlen(expected_value));
    assert(cmp == 0);
    ++i;
  }
  assert(i == 9);

  // Iterate through range ["key8", "key1").
  beg = "key8";
  end = "key1";

  i = 8;
  for (KVDKIterSeek(kvdk_iter, beg, strlen(beg)); KVDKIterValid(kvdk_iter);
       KVDKIterPre(kvdk_iter)) {
    char expected_key[10] = "key", expected_value[10] = "value";
    strcat(expected_key, sorted_nums[i]);
    strcat(expected_value, sorted_nums[i]);
    size_t key_len, val_len;
    const char* key_res = KVDKIterKey(kvdk_iter, &key_len);
    const char* val_res = KVDKIterValue(kvdk_iter, &val_len);
    if (CmpCompare(key_res, key_len, end, strlen(end)) < 0) {
      break;
    }
    int cmp = CmpCompare(key_res, key_len, expected_key, strlen(expected_key));
    assert(cmp == 0);
    cmp = CmpCompare(val_res, val_len, expected_value, strlen(expected_value));
    assert(cmp == 0);
    --i;
  }
  assert(i == 0);
  printf("Successfully iterated through a sorted named collections.\n");
  KVDKDestroyIterator(kvdk_engine, kvdk_iter);
  KVDKDestroySortedCollectionConfigs(s_configs);
}

int score_cmp(const char* a, size_t a_len, const char* b, size_t b_len) {
  double scorea = atof(a);
  double scoreb = atof(b);
  if (scorea == scoreb)
    return 0;
  else if (scorea < scoreb)
    return 1;
  else
    return -1;
}

void CompFuncForSortedCollectionExample(KVDKEngine* kvdk_engine) {
  const char* collection = "collection0";
  struct number_kv {
    const char* number_key;
    const char* value;
  };

  struct number_kv array[5] = {
      {"100", "a"}, {"50", "c"}, {"40", "d"}, {"30", "b"}, {"90", "f"}};

  struct number_kv expected_array[5] = {
      {"100", "a"}, {"90", "f"}, {"50", "c"}, {"40", "d"}, {"30", "b"}};

  // regitser compare function
  const char* comp_name = "double_comp";
  KVDKRegisterCompFunc(kvdk_engine, comp_name, strlen(comp_name), score_cmp);
  // create sorted collection
  KVDKSortedCollectionConfigs* s_configs = KVDKCreateSortedCollectionConfigs();
  KVDKSetSortedCollectionConfigs(s_configs, comp_name, strlen(comp_name));
  KVDKStatus s = KVDKCreateSortedCollection(kvdk_engine, collection,
                                            strlen(collection), s_configs);
  assert(s == Ok);
  for (int i = 0; i < 5; ++i) {
    s = KVDKSortedSet(kvdk_engine, collection, strlen(collection),
                      array[i].number_key, strlen(array[i].number_key),
                      array[i].value, strlen(array[i].value));
    assert(s == Ok);
  }
  KVDKIterator* iter = KVDKCreateSortedIterator(kvdk_engine, collection,
                                                strlen(collection), NULL);

  assert(iter != NULL);

  int i = 0;
  for (KVDKIterSeekToFirst(iter); KVDKIterValid(iter); KVDKIterNext(iter)) {
    size_t key_len, value_len;
    const char* key = KVDKIterKey(iter, &key_len);
    const char* value = KVDKIterValue(iter, &value_len);
    if (CmpCompare(key, key_len, expected_array[i].number_key,
                   strlen(expected_array[i].number_key)) != 0) {
      printf("sort key error, current key: %s , but expected key: %s\n", key,
             expected_array[i].number_key);
    }
    if (CmpCompare(value, value_len, expected_array[i].value,
                   strlen(expected_array[i].value)) != 0) {
      printf("sort value error, current value: %s , but expected value: %s\n",
             value, expected_array[i].value);
    }
    ++i;
  }
  KVDKDestroyIterator(kvdk_engine, iter);
  printf("Successfully collections sorted by number.\n");
  KVDKDestroySortedCollectionConfigs(s_configs);
}

void BatchWriteAnonCollectionExample(KVDKEngine* kvdk_engine) {
  const char* key1 = "key1";
  const char* key2 = "key2";
  const char* value1 = "value1";
  const char* value2 = "value2";
  char* read_v1;
  char* read_v2;
  size_t read_v1_len, read_v2_len;
  KVDKWriteBatch* kvdk_wb = KVDKWriteBatchCreate();
  KVDKWriteBatchPut(kvdk_wb, key1, strlen(key1), value1, strlen(value1));
  KVDKWriteBatchPut(kvdk_wb, key2, strlen(key2), value2, strlen(value2));
  KVDKWriteBatchDelete(kvdk_wb, key1, strlen(key1));
  KVDKWriteBatchPut(kvdk_wb, key1, strlen(key1), value2, strlen(value1));
  KVDKStatus s = KVDKWrite(kvdk_engine, kvdk_wb);
  assert(s == Ok);
  s = KVDKGet(kvdk_engine, key1, strlen(key1), &read_v1_len, &read_v1);
  assert(s == Ok);
  int cmp = CmpCompare(read_v1, read_v1_len, value2, strlen(value2));
  assert(cmp == 0);
  s = KVDKGet(kvdk_engine, key2, strlen(key2), &read_v2_len, &read_v2);
  assert(s == Ok);
  cmp = CmpCompare(read_v2, read_v2_len, value2, strlen(value2));
  assert(cmp == 0);
  printf("Successfully performed BatchWrite on anonymous global collection.\n");
  KVDKWriteBatchDestory(kvdk_wb);
  free(read_v1);
  free(read_v2);
}

void HashesCollectionExample(KVDKEngine* kvdk_engine) {
  const char* nums[10] = {"9", "5", "2", "0", "7", "3", "1", "8", "6", "4"};
  const char* hash_collection = "hash_collection";
  KVDKStatus s;
  for (int i = 0; i < 10; ++i) {
    char key[10] = "key", value[10] = "value";
    strcat(key, nums[i]);
    strcat(value, nums[i]);
    s = KVDKHashSet(kvdk_engine, hash_collection, strlen(hash_collection), key,
                    strlen(key), value, strlen(value));
    assert(s == Ok);
    size_t val_len;
    char* val;
    s = KVDKHashGet(kvdk_engine, hash_collection, strlen(hash_collection), key,
                    strlen(key), &val_len, &val);
    assert(s == Ok);
    int cmp = CmpCompare(val, val_len, value, strlen(value));
    assert(cmp == 0);
    free(val);
  }

  s = KVDKHashDelete(kvdk_engine, hash_collection, strlen(hash_collection),
                     "key8", strlen("key8"));
  assert(s == Ok);
  // create sorted iterator
  KVDKIterator* kvdk_iter = KVDKCreateUnorderedIterator(
      kvdk_engine, hash_collection, strlen(hash_collection));
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
  KVDKDestroyIterator(kvdk_engine, kvdk_iter);
}

void ListsCollectionExample(KVDKEngine* kvdk_engine) {
  const char* nums[10] = {"9", "5", "2", "0", "7", "3", "1", "8", "6", "4"};
  const char* list_collection = "list_collection";
  for (int i = 0; i < 10; ++i) {
    char key[10] = "key";
    strcat(key, nums[i]);
    KVDKStatus s = KVDKLPush(kvdk_engine, list_collection,
                             strlen(list_collection), key, strlen(key));
    assert(s == Ok);
    size_t key_len_res;
    char* key_res;
    s = KVDKLPop(kvdk_engine, list_collection, strlen(list_collection),
                 &key_res, &key_len_res);
    assert(s == Ok);
    int cmp = CmpCompare(key_res, key_len_res, key, strlen(key));
    assert(cmp == 0);
    free(key_res);
  }

  for (int i = 0; i < 10; ++i) {
    char key[10] = "value";
    strcat(key, nums[i]);
    KVDKStatus s = KVDKRPush(kvdk_engine, list_collection,
                             strlen(list_collection), key, strlen(key));
    assert(s == Ok);
    size_t key_len_res;
    char* key_res;
    s = KVDKRPop(kvdk_engine, list_collection, strlen(list_collection),
                 &key_res, &key_len_res);
    assert(s == Ok);
    int cmp = CmpCompare(key_res, key_len_res, key, strlen(key));
    assert(cmp == 0);
    free(key_res);
  }
  printf("Successfully performed RPush RPop LPush LPop on Lists.\n");
}

int main() {
  // Initialize a KVDK instance.
  KVDKConfigs* kvdk_configs = KVDKCreateConfigs();
  KVDKSetConfigs(kvdk_configs, 48, 1ull << 20, 1u, 64u, 1ull << 8, 128u,
                 1ull << 10, 1 << 4);

  const char* engine_path = "/mnt/pmem0/tutorial_kvdk_example";
  // Purge old KVDK instance
  KVDKRemovePMemContents(engine_path);
  // open engine
  KVDKEngine* kvdk_engine;
  KVDKStatus s = KVDKOpen(engine_path, kvdk_configs, stdout, &kvdk_engine);
  assert(s == Ok);

  // Anonymous Global Collection Example
  AnonymousCollectionExample(kvdk_engine);

  // Named Sorted Collection Example
  SortedCollectionExample(kvdk_engine);

  // Sorted Named Collection Example
  SortedCollectinIterExample(kvdk_engine);

  CompFuncForSortedCollectionExample(kvdk_engine);

  // BatchWrite on Anonymous Global Collection Example
  BatchWriteAnonCollectionExample(kvdk_engine);

  // Hashes Collection Example
  HashesCollectionExample(kvdk_engine);

  // Listes Collection Example
  ListsCollectionExample(kvdk_engine);

  KVDKDestroyConfigs(kvdk_configs);
  KVDKCloseEngine(kvdk_engine);
  return 0;
}