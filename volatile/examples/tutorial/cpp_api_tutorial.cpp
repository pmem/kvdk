/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "kvdk/volatile/engine.hpp"

#define DEBUG  // For assert

using StringView = pmem::obj::string_view;
using kvdk::Snapshot;

// The KVDK instance is mounted as a directory
// /mnt/pmem0/tutorial_kvdk_example.
// Modify this path if necessary.
const char* pmem_path = "/mnt/pmem0/tutorial_kvdk_example";

kvdk::Status status;
kvdk::Engine* engine = nullptr;

static void test_anon_coll() {
  std::string key1{"key1"};
  std::string key2{"key2"};
  std::string value1{"value1"};
  std::string value2{"value2"};
  std::string v;
  kvdk::WriteOptions write_options;

  // Insert key1-value1
  status = engine->Put(key1, value1, write_options);
  assert(status == kvdk::Status::Ok);

  // Get value1 by key1
  status = engine->Get(key1, &v);
  assert(status == kvdk::Status::Ok);
  assert(v == value1);

  // Update key1-value1 to key1-value2
  status = engine->Put(key1, value2, write_options);
  assert(status == kvdk::Status::Ok);

  // Get value2 by key1
  status = engine->Get(key1, &v);
  assert(status == kvdk::Status::Ok);
  assert(v == value2);

  // Insert key2-value2
  status = engine->Put(key2, value2, write_options);
  assert(status == kvdk::Status::Ok);

  // Delete key1-value2
  status = engine->Delete(key1);
  assert(status == kvdk::Status::Ok);

  // Delete key2-value2
  status = engine->Delete(key2);
  assert(status == kvdk::Status::Ok);

  printf(
      "Successfully performed Get, Put, Delete operations on anonymous "
      "global collection.\n");
  return;
}

static void test_named_coll() {
  std::string collection1{"my_collection_1"};
  std::string collection2{"my_collection_2"};
  std::string key1{"key1"};
  std::string key2{"key2"};
  std::string value1{"value1"};
  std::string value2{"value2"};
  std::string v;
  status = engine->SortedCreate(collection1);
  assert(status == kvdk::Status::Ok);

  status = engine->SortedCreate(collection2);
  assert(status == kvdk::Status::Ok);
  // Insert key1-value1 into "my_collection_1".
  // Implicitly create a collection named "my_collection_1" in which
  // key1-value1 is stored.
  status = engine->SortedPut(collection1, key1, value1);
  assert(status == kvdk::Status::Ok);

  // Get value1 by key1 in collection "my_collection_1"
  status = engine->SortedGet(collection1, key1, &v);
  assert(status == kvdk::Status::Ok);
  assert(v == value1);

  // Insert key1-value2 into "my_collection_2".
  // Implicitly create a collection named "my_collection_2" in which
  // key1-value2 is stored.
  status = engine->SortedPut(collection2, key1, value2);
  assert(status == kvdk::Status::Ok);

  // Get value2 by key1 in collection "my_collection_2"
  status = engine->SortedGet(collection2, key1, &v);
  assert(status == kvdk::Status::Ok);
  assert(v == value2);

  // Get value1 by key1 in collection "my_collection_1"
  // key1-value2 is stored in "my_collection_2"
  // Thus key1-value1 stored in "my_collection_1" is unaffected by operation
  // engine->SortedPut(collection2, key1, value2).
  status = engine->SortedGet(collection1, key1, &v);
  assert(status == kvdk::Status::Ok);
  assert(v == value1);

  // Insert key2-value2 into collection "my_collection_2"
  // Collection "my_collection_2" already exists and no implicit collection
  // creation occurs.
  status = engine->SortedPut(collection2, key2, value2);
  assert(status == kvdk::Status::Ok);

  // Delete key1-value1 in collection "my_collection_1"
  // Although "my_collection_1" has no elements now, the collection itself is
  // not deleted though.
  status = engine->SortedDelete(collection1, key1);
  assert(status == kvdk::Status::Ok);

  printf(
      "Successfully performed SortedGet, SortedPut, SortedDelete operations on "
      "named "
      "collections.\n");
  return;
}

static void test_iterator() {
  std::string sorted_collection{"my_sorted_collection"};
  // Create Sorted Collection
  status = engine->SortedCreate(sorted_collection);
  assert(status == kvdk::Status::Ok);
  // Create toy keys and values.
  std::vector<std::pair<std::string, std::string>> kv_pairs;
  for (int i = 0; i < 10; ++i) {
    kv_pairs.emplace_back(
        std::make_pair("key" + std::to_string(i), "value" + std::to_string(i)));
  }
  std::shuffle(kv_pairs.begin(), kv_pairs.end(), std::mt19937{42});
  // Print out kv_pairs to check if they are really shuffled.
  printf("The shuffled kv-pairs are:\n");
  for (const auto& kv : kv_pairs)
    printf("%s\t%s\n", kv.first.c_str(), kv.second.c_str());

  // Populate collection "my_sorted_collection" with keys and values.
  // kv_pairs are not necessarily sorted, but kv-pairs in collection
  // "my_sorted_collection" are sorted.
  for (int i = 0; i < 10; ++i) {
    // Collection "my_sorted_collection" is implicitly created in first
    // iteration
    status = engine->SortedPut(sorted_collection, kv_pairs[i].first,
                               kv_pairs[i].second);
    assert(status == kvdk::Status::Ok);
  }
  // Sort kv_pairs for checking the order of "my_sorted_collection".
  std::sort(kv_pairs.begin(), kv_pairs.end());

  // Iterate through collection "my_sorted_collection"
  auto iter = engine->SortedIteratorCreate(sorted_collection);
  if (!iter) {
    fprintf(stderr, "Seek error\n");
    return;
  }
  iter->SeekToFirst();
  {
    int i = 0;
    while (iter->Valid()) {
      assert(iter->Key() == kv_pairs[i].first);
      assert(iter->Value() == kv_pairs[i].second);
      iter->Next();
      ++i;
    }
  }

  // Iterate through range ["key1", "key8").
  std::string beg{"key1"};
  std::string end{"key8"};
  {
    int i = 1;
    iter->Seek(beg);
    for (iter->Seek(beg); iter->Valid() && iter->Key() < end; iter->Next()) {
      assert(iter->Key() == kv_pairs[i].first);
      assert(iter->Value() == kv_pairs[i].second);
      ++i;
    }
  }

  // Reversely iterate through range ["key8", "key1").
  beg = "key8";
  end = "key1";
  {
    int i = 8;
    for (iter->Seek(beg); iter->Valid() && iter->Key() > end; iter->Prev()) {
      assert(iter->Key() == kv_pairs[i].first);
      assert(iter->Value() == kv_pairs[i].second);
      --i;
    }
  }

  printf("Successfully iterated through a sorted named collections.\n");
  engine->SortedIteratorRelease(iter);
  return;
}

static void test_batch_write() {
  std::string key1{"key1"};
  std::string key2{"key2"};
  std::string value1{"value1"};
  std::string value2{"value2"};
  std::string v;

  auto batch = engine->WriteBatchCreate();
  batch->StringPut(key1, value1);
  batch->StringPut(key1, value2);
  batch->StringPut(key2, value2);
  batch->StringDelete(key2);

  // If the batch is successfully written, there should be only key1-value2 in
  // anonymous global collection.
  status = engine->BatchWrite(batch);
  assert(status == kvdk::Status::Ok);

  // Get value2 by key1
  status = engine->Get(key1, &v);
  assert(status == kvdk::Status::Ok);
  assert(v == value2);

  // Get value2 by key1
  status = engine->Get(key2, &v);
  assert(status == kvdk::Status::NotFound);
  // v is unchanged, but it is invalid. Always Check kvdk::Status before
  // perform further operations!
  assert(v == value2);

  printf("Successfully performed BatchWrite on anonymous global collection.\n");
  return;
}

static void test_customer_sorted_func() {
  std::string collection = "collection0";
  struct number_kv {
    std::string number_key;
    std::string value;
  };

  std::vector<number_kv> array = {
      {"100", "a"}, {"50", "c"}, {"40", "d"}, {"30", "b"}, {"90", "f"}};

  std::vector<number_kv> expected_array = {
      {"100", "a"}, {"90", "f"}, {"50", "c"}, {"40", "d"}, {"30", "b"}};

  // regitser compare function
  std::string comp_name = "double_comp";
  auto score_cmp = [](const StringView& a, const StringView& b) -> int {
    std::string str_a(a.data(), a.size());
    std::string str_b(b.data(), b.size());
    double scorea = std::stod(str_a);
    double scoreb = std::stod(str_b);
    if (scorea == scoreb)
      return 0;
    else if (scorea < scoreb)
      return 1;
    else
      return -1;
  };
  engine->registerComparator(comp_name, score_cmp);
  // create sorted collection
  kvdk::SortedCollectionConfigs s_configs;
  s_configs.comparator_name = comp_name;
  kvdk::Status s = engine->SortedCreate(collection, s_configs);
  assert(s == Ok);
  for (int i = 0; i < 5; ++i) {
    s = engine->SortedPut(collection, array[i].number_key, array[i].value);
    assert(s == Ok);
  }
  auto iter = engine->SortedIteratorCreate(collection);

  assert(iter != nullptr);

  int i = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string key = iter->Key();
    std::string value = iter->Value();
    if (key != expected_array[i].number_key) {
      printf("sort key error, current key: %s , but expected key: %s\n",
             key.c_str(), expected_array[i].number_key.c_str());
    }
    if (value != expected_array[i].value) {
      printf("sort value error, current value: %s , but expected value: %s\n",
             value.c_str(), expected_array[i].value.c_str());
    }
    ++i;
  }
  printf("Successfully collections sorted by number.\n");
  engine->SortedIteratorRelease(iter);
}

static void test_expire() {
  int64_t ttl_time;
  std::string got_val;
  kvdk::Status s;
  // For string
  {
    std::string key = "stringkey";
    std::string val = "stringval";
    // case: set expire time
    s = engine->Put(key, val, kvdk::WriteOptions{100});
    assert(s == kvdk::Status::Ok);
    s = engine->Get(key, &got_val);
    assert(s == kvdk::Status::Ok);
    assert(got_val == val);
    s = engine->GetTTL(key, &ttl_time);
    assert(s == kvdk::Status::Ok);
    // case: reset expire time
    s = engine->Expire(key, INT32_MAX);
    assert(s == kvdk::Status::Ok);
    // case: change to persist key
    s = engine->Expire(key, kvdk::kPersistTime);
    assert(s == kvdk::Status::Ok);
    s = engine->GetTTL(key, &ttl_time);
    assert(s == kvdk::Status::Ok);
    assert(ttl_time == kvdk::kPersistTime);
    // case: key is expired.
    s = engine->Expire(key, 1);
    assert(s == kvdk::Status::Ok);
    sleep(1);
    s = engine->Get(key, &got_val);
    assert(s == kvdk::Status::NotFound);
    // case: ttl time is negative.
    s = engine->Put(key, "Updatedval");
    assert(s == kvdk::Status::Ok);
    s = engine->Expire(key, -1);
    assert(s == kvdk::Status::Ok);
    s = engine->GetTTL(key, &ttl_time);
    assert(s == kvdk::Status::NotFound);
    printf("Successfully expire string\n");
  }

  {
    std::string sorted_collection = "sorted_collection";
    std::string key = "sortedkey";
    std::string val = "sortedval";

    s = engine->SortedCreate(sorted_collection);
    // case: default persist key.
    s = engine->GetTTL(sorted_collection, &ttl_time);
    assert(s == kvdk::Status::Ok);
    assert(ttl_time == kvdk::kPersistTime);
    s = engine->SortedPut(sorted_collection, key, val);
    assert(s == kvdk::Status::Ok);
    // case: set expire_time
    s = engine->Expire(sorted_collection, INT32_MAX);
    assert(s == kvdk::Status::Ok);
    // case: change to persist key
    s = engine->Expire(sorted_collection, kvdk::kPersistTime);
    s = engine->GetTTL(sorted_collection, &ttl_time);
    assert(s == kvdk::Status::Ok);
    assert(ttl_time == kvdk::kPersistTime);
    // case: key is expired.
    s = engine->Expire(sorted_collection, 1);
    assert(s == kvdk::Status::Ok);
    sleep(1);
    s = engine->SortedGet(sorted_collection, key, &got_val);
    assert(s == kvdk::Status::NotFound);
    printf("Successfully expire sorted\n");
  }

  {
    std::string hash_collection = "hash_collection";
    std::string key = "hashkey";
    std::string val = "hashval";

    // case: default persist key
    s = engine->HashCreate(hash_collection);
    assert(s == kvdk::Status::Ok);
    s = engine->HashPut(hash_collection, key, val);
    assert(s == kvdk::Status::Ok);
    s = engine->GetTTL(hash_collection, &ttl_time);
    assert(s == kvdk::Status::Ok);
    assert(ttl_time == kvdk::kPersistTime);

    // case: set expire_time
    s = engine->Expire(hash_collection, 1);
    assert(s == kvdk::Status::Ok);
    // case: change to persist key
    s = engine->Expire(hash_collection, kvdk::kPersistTime);
    s = engine->GetTTL(hash_collection, &ttl_time);
    assert(s == kvdk::Status::Ok);
    assert(ttl_time == kvdk::kPersistTime);
    // case: key is expired.
    s = engine->Expire(hash_collection, 1);
    assert(s == kvdk::Status::Ok);
    sleep(1);
    s = engine->HashGet(hash_collection, key, &got_val);
    assert(s == kvdk::Status::NotFound);

    s = engine->HashDestroy(hash_collection);
    assert(s == kvdk::Status::NotFound);
    printf("Successfully expire hash\n");
  }

  {
    // TODO: add expire list, but now list api has changed.
  }
  return;
}

int main() {
  // Initialize a KVDK instance.
  kvdk::Configs engine_configs;
  {
    // Configure for a tiny KVDK instance.
    // Please refer to "Configuration" section in user documentation for
    // details.
    engine_configs.hash_bucket_num = (1ull << 10);
  }
  std::string engine_path{pmem_path};

  // Purge old KVDK instance
  [[gnu::unused]] int sink =
      system(std::string{"rm -rf " + engine_path + "\n"}.c_str());

  status = kvdk::Engine::Open(engine_path, &engine, engine_configs, stdout);
  assert(status == kvdk::Status::Ok);
  printf("Successfully opened a KVDK instance.\n");

  // Reads and Writes on Anonymous Global Collection
  test_anon_coll();

  // Reads and Writes on Named Collection
  test_named_coll();

  // Iterating a Sorted Named Collection
  test_iterator();

  // Sorted Collection with Customer Sorted Function
  test_customer_sorted_func();

  // BatchWrite on Anonymous Global Collection
  test_batch_write();

  // Expire
  test_expire();

  // Close KVDK instance.
  delete engine;

  // Remove persisted contents on PMem
  return system(std::string{"rm -rf " + engine_path + "\n"}.c_str());
}
