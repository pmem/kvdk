KVDK
=======

KVDK(Key-Value Development Kit) is a Key-Value store for Persistent memory(PMem). 

KVDK supports both sorted and unsorted KV-Pairs.

Code snippets in this user documents are from `./examples/tutorial.cpp`, which is built as `./build/dbtutorials`.

## Open a KVDK instance

A KVDK instance is associated with a PMem directory mounted under linux system. Keys and values
are stored in PMem under this path and are indexed by HashTable which is stored in DRAM.
HashTable is not persisted on PMem and is reconstructed with information on PMem.

The following code shows how to create a KVDK instance if none exist
or how to reopen an existing KVDK instance at the path supplied.
(In the following example, PMem is mounted as /mnt/pmem0/ and the KVDK instance is named tutorial_kvdk_example.)

```c++
#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include <algorithm>
#include <cassert>
#include <random>
#include <string>
#include <thread>
#include <vector>

#define DEBUG // For assert

int main()
{
  kvdk::Status status;
  kvdk::Engine *engine = nullptr;

  // Initialize a KVDK instance.
  {
    kvdk::Configs engine_configs;
    {
      // Configure for a tiny KVDK instance.
      // Approximately 10MB /mnt/pmem0/ space is needed.
      // Please refer to "Configuration" section in user documentation for
      // details.
      engine_configs.pmem_file_size = (1ull << 20);
      engine_configs.pmem_segment_blocks = (1ull << 8);
      engine_configs.num_hash_buckets = (1ull << 10);
    }
    // The KVDK instance is mounted as a directory
    // /mnt/pmem0/tutorial_kvdk_example.
    // Modify this path if necessary.
    std::string engine_path{"/mnt/pmem0/tutorial_kvdk_example"};

    // Purge old KVDK instance
    int sink = system(std::string{"rm -rf " + engine_path + "\n"}.c_str());

    status = kvdk::Engine::Open(engine_path, &engine, engine_configs, stdout);
    assert(status == kvdk::Status::Ok);
    printf("Successfully opened a KVDK instance.\n");
  }

    ... Do something with KVDK instance ...

    ... Close KVDK instance and exit ...
}
```

## Status

`kvdk::Status` indicates status of KVDK function calls. 
Functions return `kvdk::Status::Ok` if such a function call is a success.
If exceptions are raised during function calls, other `kvdk::Status` is returned,
such as `kvdk::Status::MemoryOverflow`.

## Close a KVDK instance

To close a KVDK instance, just delete the instance.

When a KVDK instance is closed, Key-Value pairs are still persisted on PMem.
KV-Pair indexing stored on DRAM are purged.
Follow "Open a KVDK instance" to reopen the KVDK instance will reconstruct indexing on DRAM with information persisted on PMem.

To purge contents on PMem, just delete its directory under mounted PMem.

```c++
int main()
{
  ... Open a KVDK instance as described in "Open a KVDK instance" ...
  ... Do something with KVDK instance ...

  // Close KVDK instance.
  delete engine;

  // Remove persisted contents on PMem
  return system(std::string{"rm -rf " + engine_path + "\n"}.c_str());
}
```

## Data types
KVDK currently supports string type for both keys and values.
### Strings
All keys and values in a KVDK instance are strings.

Keys are limited to have a maximum size of 64KB.

A string value can be at max 64MB in length by default. The maximum length can be configured when initializing a KVDK instance.

## Collections
All Key-Value pairs(KV-Pairs) are organized into collections.

There is an anonymous global collection with KV-Pairs directly accessible via Get, Set, Delete operations. The anonymous global collection is unsorted.

Users can also create named collections.

KVDK currently supports sorted named collections. Users can iterate forward or backward starting from an arbitrary point(at a key or between two keys) by an iterator. Elements can also be directly accessed via SGet, SSet, SDelete operations.

## Reads and Writes in Anonymous Global Collection

A KVDK instance provides Get, Set, Delete methods to query/modify/delete entries. 

The following code performs a series of Get, Set and Delete operations.

```c++
int main()
{
  ... Open a KVDK instance as described in "Open a KVDK instance" ...

  // Reads and Writes on Anonymous Global Collection
  {
    std::string key1{"key1"};
    std::string key2{"key2"};
    std::string value1{"value1"};
    std::string value2{"value2"};
    std::string v;

    // Insert key1-value1
    status = engine->Set(key1, value1);
    assert(status == kvdk::Status::Ok);

    // Get value1 by key1
    status = engine->Get(key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value1);

    // Update key1-value1 to key1-value2
    status = engine->Set(key1, value2);
    assert(status == kvdk::Status::Ok);

    // Get value2 by key1
    status = engine->Get(key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value2);

    // Insert key2-value2
    status = engine->Set(key2, value2);
    assert(status == kvdk::Status::Ok);

    // Delete key1-value2
    status = engine->Delete(key1);
    assert(status == kvdk::Status::Ok);

    // Delete key2-value2
    status = engine->Delete(key2);
    assert(status == kvdk::Status::Ok);

    printf("Successfully performed Get, Set, Delete operations on anonymous "
           "global collection.\n");
  }

  ... Do something else with KVDK instance ...

  ... Close KVDK instance and exit ...
}
```

## Reads and Writes in a Named Collection

A KVDK instance provides SGet, SSet, SDelete methods to query/modify/delete sorted entries. 

The following code performs a series of SGet, SSet and SDelete operations, which also initialize a named collection implicitly.

```c++
int main()
{
  ... Open a KVDK instance as described in "Open a KVDK instance" ...

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
    status = engine->SSet(collection1, key1, value1);
    assert(status == kvdk::Status::Ok);

    // Get value1 by key1 in collection "my_collection_1"
    status = engine->SGet(collection1, key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value1);

    // Insert key1-value2 into "my_collection_2".
    // Implicitly create a collection named "my_collection_2" in which
    // key1-value2 is stored.
    status = engine->SSet(collection2, key1, value2);
    assert(status == kvdk::Status::Ok);

    // Get value2 by key1 in collection "my_collection_2"
    status = engine->SGet(collection2, key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value2);

    // Get value1 by key1 in collection "my_collection_1"
    // key1-value2 is stored in "my_collection_2"
    // Thus key1-value1 stored in "my_collection_1" is unaffected by operation
    // engine->SSet(collection2, key1, value2).
    status = engine->SGet(collection1, key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value1);

    // Insert key2-value2 into collection "my_collection_2"
    // Collection "my_collection_2" already exists and no implicit collection
    // creation occurs.
    status = engine->SSet(collection2, key2, value2);
    assert(status == kvdk::Status::Ok);

    // Delete key1-value1 in collection "my_collection_1"
    // Although "my_collection_1" has no elements now, the collection itself is
    // not deleted though.
    status = engine->SDelete(collection1, key1);
    assert(status == kvdk::Status::Ok);

    printf("Successfully performed SGet, SSet, SDelete operations on named "
           "collections.\n");
  }

  ... Do something else with KVDK instance ...

  ... Close KVDK instance and exit ...
}
```

## Iterating a Named Collection
The following example demonstrates how to iterate through a named collection. It also demonstrates how to iterate through a range defined by Key.

```c++
int main()
{
  ... Open a KVDK instance as described in "Open a KVDK instance" ...

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
      status = engine->SSet(sorted_collection, kv_pairs[i].first,
                            kv_pairs[i].second);
      assert(status == kvdk::Status::Ok);
    }
    // Sort kv_pairs for checking the order of "my_sorted_collection".
    std::sort(kv_pairs.begin(), kv_pairs.end());

    // Iterate through collection "my_sorted_collection"
    auto iter = engine->NewSortedIterator(sorted_collection);
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

  ... Do something else with KVDK instance ...

  ... Close KVDK instance and exit ...
}
```

## Atomic Updates
KVDK supports organizing a series of Set, Delete operations into a `kvdk::WriteBatch` object as an atomic operation. If KVDK fail to apply the `kvdk::WriteBatch` object as a whole, i.e. the system shuts down during applying the batch, it will roll back to the status right before applying the `kvdk::WriteBatch`.

```c++
int main()
{
  ... Open a KVDK instance as described in "Open a KVDK instance" ...

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

    printf(
        "Successfully performed BatchWrite on anonymous global collection.\n");
  }

  ... Do something else with KVDK instance ...

  ... Close KVDK instance and exit ...
}
```

## Concurrency
A KVDK instance can be accessed by multiple read and write threads safely. Synchronization is handled by KVDK implementation.

## Configuration

Users can configure KVDK to adapt to their system environment by setting up a `kvdk::Configs` object and passing it to 'kvdk::Engine::Open' when initializing a KVDK instance.

### Max Write Threads
Maximum number of write threads is specified by `kvdk::Configs::max_write_threads`. Defaulted to 48. It's recommended to set this number to the number of threads provided by CPU. 

### PMem File Size
`kvdk::Configs::pmem_file_size` specifies the space allocated to a KVDK instance. Defaulted to 2^38Bytes = 256GB.

### Populate PMem Space
Specified by `kvdk::Configs::populate_pmem_space`. When set to true to populate pmem space while creating a new instance, KVDK will take extra time to set up. This will improve runtime performance.

### Block Size
Specified by `kvdk::Configs::pmem_block_size`. Defaulted to 64(Bytes) to align with cache-line. 

**This parameter is immutable after initialization of the KVDK instance.**

### Blocks per Segment
Specified by `kvdk::Configs::pmem_segment_blocks`. Defaulted to 2^21. Segment size determines the maximum size for a KV-Pair. 

Default Blocks per Segment and Block Size parameters will limit the size for a KV-Pair to 64MB(Actually slightly smaller than 64MB, for checksum and other information associated with the KV-Pair).

User is suggested to adjust this parameter instead of `kvdk::Configs::pmem_block_size`.

**This parameter is immutable after initialization of the KVDK instance.**

### HashBucket Size
Specified by `kvdk::Configs::hash_bucket_size`. Defaulted to 128(Bytes).
Larger HashBucket Size will slightly improve performance but will occupy larger space. Please read Architecture Documentation for details before tuning this parameter.

### Number of HashBucket Chains
Specified by `kvdk::Configs::num_hash_buckets`. Greater number will improve performance by reducing hash conflicts at the cost of greater DRAM space. Please read Architecture Documentation for details before tuning this parameter.

### Buckets per Slot
Specified by `kvdk::Configs::num_buckets_per_slot`. Smaller number will improve performance by reducing lock contentions and improving caching at the cost of greater DRAM space. Please read Architecture Documentation for details before tuning this parameter.
