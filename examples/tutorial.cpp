/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include <algorithm>
#include <cassert>
#include <random>
#include <string>
#include <thread>
#include <vector>

#define DEBUG // For assert

int main() {
  kvdk::Status status;

  // Initialize a KVDK instance.
  kvdk::Engine *db = nullptr;
  kvdk::Configs db_configs;
  {
    // Configure for a tiny KVDK instance. Approximately 10MB /mnt/pmem0/ space
    // is needed. Please refer to "Configuration" section in user documentation
    // for details.
    db_configs.pmem_file_size = (1ull << 20);
    db_configs.pmem_segment_blocks = (1ull << 8);
    db_configs.num_hash_buckets = (1ull << 10);
  }
  // The KVDK instance is mounted as DB under /mnt/pmem0/
  // Modify this path if necessary.
  std::string db_path{"/mnt/pmem0/tutorial_kvdk_example"};

  // Purge old KVDK instance
  int sink = system(std::string{"rm -rf " + db_path + "\n"}.c_str());

  status = kvdk::Engine::Open(db_path, &db, db_configs, stdout);
  assert(status == kvdk::Status::Ok);
  printf("Successfully opened a KVDK instance.\n");

  // Reads and Writes on Anonymous Global Collection
  {
    std::string key1{"key1"};
    std::string key2{"key2"};
    std::string value1{"value1"};
    std::string value2{"value2"};
    std::string v;

    // Insert key1-value1
    status = db->Set(key1, value1);
    assert(status == kvdk::Status::Ok);

    // Get value1 by key1
    status = db->Get(key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value1);

    // Update key1-value1 to key1-value2
    status = db->Set(key1, value2);
    assert(status == kvdk::Status::Ok);

    // Get value2 by key1
    status = db->Get(key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value2);

    // Insert key2-value2
    status = db->Set(key2, value2);
    assert(status == kvdk::Status::Ok);

    // Delete key1-value2
    status = db->Delete(key1);
    assert(status == kvdk::Status::Ok);

    // Delete key2-value2
    status = db->Delete(key2);
    assert(status == kvdk::Status::Ok);

    printf("Successfully performed Get, Set, Delete operations on anonymous "
           "global collection.\n");
  }

  // Reads and Writes on Named Collection
  {
    std::string collection1{"my_collection_1"};
    std::string collection2{"my_collection_2"};
    std::string key1{"key1"};
    std::string key2{"key2"};
    std::string value1{"value1"};
    std::string value2{"value2"};
    std::string v;

    // Insert key1-value1 into "my_collection_1".
    // Implicitly create a collection named "my_collection_1" in which
    // key1-value1 is stored.
    status = db->SSet(collection1, key1, value1);
    assert(status == kvdk::Status::Ok);

    // Get value1 by key1 in collection "my_collection_1"
    status = db->SGet(collection1, key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value1);

    // Insert key1-value2 into "my_collection_2".
    // Implicitly create a collection named "my_collection_2" in which
    // key1-value2 is stored.
    status = db->SSet(collection2, key1, value2);
    assert(status == kvdk::Status::Ok);

    // Get value2 by key1 in collection "my_collection_2"
    status = db->SGet(collection2, key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value2);

    // Get value1 by key1 in collection "my_collection_1"
    // key1-value2 is stored in "my_collection_2"
    // Thus key1-value1 stored in "my_collection_1" is unaffected by operation
    // db->SSet(collection2, key1, value2).
    status = db->SGet(collection1, key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value1);

    // Insert key2-value2 into collection "my_collection_2"
    // Collection "my_collection_2" already exists and no implicit collection
    // creation occurs.
    status = db->SSet(collection2, key2, value2);
    assert(status == kvdk::Status::Ok);

    // Delete key1-value1 in collection "my_collection_1"
    // Although "my_collection_1" has no elements now, the collection itself is
    // not deleted though.
    status = db->SDelete(collection1, key1);
    assert(status == kvdk::Status::Ok);

    printf("Successfully performed SGet, SSet, SDelete operations on named "
           "collections.\n");
  }

  // Iterating a Sorted Named Collection
  {
    std::string sorted_collection{"my_sorted_collection"};
    // Create toy keys and values.
    std::vector<std::pair<std::string, std::string>> kv_pairs;
    for (int i = 0; i < 10; ++i) {
      kv_pairs.emplace_back(std::make_pair("key" + std::to_string(i),
                                           "value" + std::to_string(i)));
    }
    std::shuffle(kv_pairs.begin(), kv_pairs.end(), std::mt19937{42});
    // Print out kv_pairs to check if they are really shuffled.
    printf("The shuffled kv-pairs are:\n");
    for (const auto &kv : kv_pairs)
      printf("%s\t%s\n", kv.first.c_str(), kv.second.c_str());

    // Populate collection "my_sorted_collection" with keys and values.
    // kv_pairs are not necessarily sorted, but kv-pairs in collection
    // "my_sorted_collection" are sorted.
    for (int i = 0; i < 10; ++i) {
      // Collection "my_sorted_collection" is implicitly created in first
      // iteration
      status =
          db->SSet(sorted_collection, kv_pairs[i].first, kv_pairs[i].second);
      assert(status == kvdk::Status::Ok);
    }
    // Sort kv_pairs for checking the order of "my_sorted_collection".
    std::sort(kv_pairs.begin(), kv_pairs.end());

    // Iterate through collection "my_sorted_collection"
    auto iter = db->NewSortedIterator(sorted_collection);
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
  }

  // BatchWrite on Anonymous Global Collection
  {
    std::string key1{"key1"};
    std::string key2{"key2"};
    std::string value1{"value1"};
    std::string value2{"value2"};
    std::string v;

    kvdk::WriteBatch batch;
    batch.Put(key1, value1);
    batch.Put(key1, value2);
    batch.Put(key2, value2);
    batch.Delete(key2);

    // If the batch is successfully written, there should be only key1-value2 in
    // anonymous global collection.
    status = db->BatchWrite(batch);
    assert(status == kvdk::Status::Ok);

    // Get value2 by key1
    status = db->Get(key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value2);

    // Get value2 by key1
    status = db->Get(key2, &v);
    assert(status == kvdk::Status::NotFound);
    // v is unchanged, but it is invalid. Always Check kvdk::Status before
    // perform further operations!
    assert(v == value2);

    printf(
        "Successfully performed BatchWrite on anonymous global collection.\n");
  }

  // Close KVDK instance.
  delete db;

  // Remove persisted contents on PMem
  return system(std::string{"rm -rf " + db_path + "\n"}.c_str());
}
