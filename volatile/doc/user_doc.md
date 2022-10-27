KVDK
=======

KVDK(Key-Value Development Kit) is a Key-Value store for Persistent Memory (PMem).

KVDK supports basic read and write operations on both sorted and unsorted KV-Pairs, it also support some advanced features, such as **backup**, **checkpoint**, **expire key**, **atomic batch write** and **transactions**.

Code snippets in this user documents are from `./examples/tutorial/cpp_api_tutorial.cpp`, which is built as `./build/examples/tutorial/cpp_api_tutorial`.

## Open a KVDK instance

A KVDK instance is associated with a PMem directory mounted under linux system. Keys and values
are stored in PMem under this path and are indexed by HashTable which is stored in DRAM.
HashTable is not persisted on PMem and is reconstructed with information on PMem.

The following code shows how to create a KVDK instance if none exist
or how to reopen an existing KVDK instance at the path supplied.
(In the following example, PMem is mounted as /mnt/pmem0/ and the KVDK instance is named tutorial_kvdk_example.)

```c++
#include "kvdk/volatile/engine.hpp"
#include "kvdk/volatile/namespace.hpp"
#include <algorithm>
#include <cassert>
#include <random>
#include <string>
#include <thread>
#include <vector>

#define Debug // For assert

int main()
{
  kvdk::Status status;
  kvdk::Engine *engine = nullptr;

  // Initialize a KVDK instance.
  {
    kvdk::Configs engine_configs;
    {
      // Configure for a tiny KVDK instance.
      // Please refer to "Configuration" section in user documentation for
      // details.
      engine_configs.hash_bucket_num = (1ull << 10);
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
such as `kvdk::Status::MemoryOverflow` while no enough memory to allocate.

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
KVDK currently supports raw string, sorted collection, hash collection and list data type.

### Raw String

All keys and values in a KVDK instance are strings. You can directly store or read key-value pairs in global namespace, which is accessible via Get, Put, Delete and Modify operations, we call them string type data in kvdk. 

Keys are limited to have a maximum size of 64KB.

A value can be at max 64MB in length by default. The maximum length can be configured when initializing a KVDK instance.

### Collections

Instead of raw string, you can organize key-value pairs to a collection, each collection has its own namespace.

Currently we have three types of collection:

#### Sorted Collection

KV pairs are stored with some kind of order (lexicographical order by default) in Sorted Collection, they can be iterated forward or backward starting from an arbitrary point(at a key or between two keys) by an iterator. They can also be directly accessed via SortedGet, SortedPut, SortedDelete operations.

#### Hash Collection

Hash Collection is like Raw String with a name space, you can access KV pairs via HashGet, HashPut, HashDelete and HashModify operations.

In current version, performance of operations on hash collection is similar to sorted collection, which much slower than raw-string, so we recomend use raw-string or sorted collection as high priority.

#### List

List is a list of string elements, you can access elems at the front or back via ListPushFront, ListPushBack, ListPopFron, ListPopBack, or operation elems with index via ListInsertAt, ListInsertBefore, ListInsertAfter and ListErase. Notice that operation with index take O(n) time, while operation on front and back only takes O(1).

### Namespace

Each collection has its own namespace, so you can store same key in every collection. Howevery, collection name and raw string key are in a same namespace, so you can't assign same name for a collection and a string key, otherwise a error status (Status::WrongType) will be returned.

## API Examples

### Reads and Writes with String type

A KVDK instance provides Get, Put, Delete methods to query/modify/delete raw string kvs.

The following code performs a series of Get, Put and Delete operations.

```c++
int main()
{
  ... Open a KVDK instance as described in "Open a KVDK instance" ...

  // Reads and Writes String KV
  {
    std::string key1{"key1"};
    std::string key2{"key2"};
    std::string value1{"value1"};
    std::string value2{"value2"};
    std::string v;

    // Insert key1-value1
    status = engine->Put(key1, value1);
    assert(status == kvdk::Status::Ok);

    // Get value1 by key1
    status = engine->Get(key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value1);

    // Update key1-value1 to key1-value2
    status = engine->Put(key1, value2);
    assert(status == kvdk::Status::Ok);

    // Get value2 by key1
    status = engine->Get(key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value2);

    // Insert key2-value2
    status = engine->Put(key2, value2);
    assert(status == kvdk::Status::Ok);

    // Delete key1-value2
    status = engine->Delete(key1);
    assert(status == kvdk::Status::Ok);

    // Delete key2-value2
    status = engine->Delete(key2);
    assert(status == kvdk::Status::Ok);

    printf("Successfully performed Get, Put, Delete operations on anonymous "
           "global collection.\n");
  }

  ... Do something else with KVDK instance ...

  ... Close KVDK instance and exit ...
}
```

### Reads and Writes in a Sorted Collection

A KVDK instance provides SortedGet, SortedPut, SortedDelete methods to query/modify/delete sorted entries.

The following code performs a series of SortedGet, SortedPut and SortedDelete operations on a sorted collection.

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

    // You must create sorted collections before you do any operations on them
    status = engine->SortedCreate(collection1);
    assert(status == kvdk::Status::Ok);
    status = engine->SortedCreate(collection2);
    assert(status == kvdk::Status::Ok);

    // Insert key1-value1 into "my_collection_1".
    status = engine->SortedPut(collection1, key1, value1);
    assert(status == kvdk::Status::Ok);

    // Get value1 by key1 in collection "my_collection_1"
    status = engine->SortedGet(collection1, key1, &v);
    assert(status == kvdk::Status::Ok);
    assert(v == value1);

    // Insert key1-value2 into "my_collection_2".
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

    // Destroy sorted collections
    status = engine->SortedDestroy(collection1);
    assert(status == kvdk::Status::Ok);
    status = engine->SrotedDestroy(collection2);
    assert(status == kvdk::Status::Ok);

    printf("Successfully performed SortedGet, SortedPut, SortedDelete operations.\n");
  }

  ... Do something else with KVDK instance ...

  ... Close KVDK instance and exit ...
}
```

### Iterating a Sorted Collection
The following example demonstrates how to iterate through a sorted collection at a consistent view of data. It also demonstrates how to iterate through a range defined by Key.

```c++
int main()
{
  ... Open a KVDK instance as described in "Open a KVDK instance" ...

  // Iterating a Sorted Sorted Collection
  {
    std::string sorted_collection{"my_sorted_collection"};
    engine->SortedCreate(sorted_collection);
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
      status = engine->SortedPut(sorted_collection, kv_pairs[i].first,
                            kv_pairs[i].second);
      assert(status == kvdk::Status::Ok);
    }
    // Sort kv_pairs for checking the order of "my_sorted_collection".
    std::sort(kv_pairs.begin(), kv_pairs.end());

    // Iterate through collection "my_sorted_collection", the iter is
    // created on a consistent view while you create it, e.g. all    
    // modifications after you create the iter won't be observed
    auto iter = engine->SortedIteratorCreate(sorted_collection);
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

    printf("Successfully iterated through a sorted collections.\n");
    engine->SortedIteratorRelease(iter);
  }

  ... Do something else with KVDK instance ...

  ... Close KVDK instance and exit ...
}
```

### Atomic Updates
KVDK supports organizing a series of Put, Delete operations into a `kvdk::WriteBatch` object as an atomic operation. If KVDK fail to apply the `kvdk::WriteBatch` object as a whole, i.e. the system shuts down during applying the batch, it will roll back to the status right before applying the `kvdk::WriteBatch`.

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

### Max Access Threads
Maximum number of internal access threads in kvdk is specified by `kvdk::Configs::max_access_threads`. Defaulted to 64. It's recommended to set this number to the number of threads provided by CPU.

You can call KVDK API with any number of threads, but if your parallel threads more than max_access_threads, the performance will be degraded due to synchronization cost

### Clean Threads
KVDK reclaim space of updated/deleted data in background with dynamic number of clean threads, you can specify max clean thread number with `kvdk::Configs::clean_threads`. Defaulted to 8, you can config more clean threads in delete intensive workloads to avoid space be exhausted.

**This parameter is immutable after initialization of the KVDK instance.**

### HashBucket Size
Specified by `kvdk::Configs::hash_bucket_size`. Defaulted to 128(Bytes).
Larger HashBucket Size will slightly improve performance but will occupy larger space. Please read Architecture Documentation for details before tuning this parameter.

### Number of HashBucket Chains
Specified by `kvdk::Configs::hash_bucket_num`. Greater number will improve performance by reducing hash conflicts at the cost of greater DRAM space. Please read Architecture Documentation for details before tuning this parameter.

### Buckets per Slot
Specified by `kvdk::Configs::num_buckets_per_slot`. Smaller number will improve performance by reducing lock contentions and improving caching at the cost of greater DRAM space. Please read Architecture Documentation for details before tuning this parameter.

## Advanced features and more API

Please read examples/tutorial for more API and advanced features in KVDK.
