/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include <algorithm>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"

#include "../engine/alias.hpp"
#include "test_util.h"

using kvdk::StringView;

// Contains functions to iterate through a collection and check its contents
// It's up to user to maintain an unordered_multimap between keys and values
// to keep track of the kv-pairs in a certain collection in the engine instance
namespace kvdk_testing {
// Check value got by XGet(key) by looking up possible_kv_pairs
static void CheckKVPair(
    StringView key, StringView value,
    std::unordered_multimap<StringView, StringView> const &possible_kv_pairs) {
  bool match = false;
  auto range_found = possible_kv_pairs.equal_range(key);
  ASSERT_NE(range_found.first, range_found.second)
      << "No key in possible_kv_pairs matching with iterated key:\n"
      << "Iterated key: " << key;

  for (auto iter = range_found.first; iter != range_found.second; ++iter) {
    ASSERT_EQ(key, iter->first)
        << "Iterated key and key in possible_kv_pairs does not match: \n"
        << "Iterated key: " << key << "\n"
        << "Key in possible_kv_pairs: " << iter->first;
    match = match || (value == iter->second);
  }
  ASSERT_TRUE(match)
      << "No kv-pair in possible_kv_pairs matching with iterated kv-pair:\n"
      << "Key: " << key << "\n"
      << "Value: " << value << "\n";
}

// possible_kv_pairs is searched to try to find a match with iterated records
// possible_kv_pairs is copied because HashesIterateThrough erase entries to
// keep track of records
static void HashesIterateThrough(
    kvdk::Engine *engine, std::string collection_name,
    std::unordered_multimap<StringView, StringView> possible_kv_pairs,
    bool report_progress) {
  kvdk::Status status;

  std::unordered_multimap<StringView, StringView> possible_kv_pairs_copy{
      possible_kv_pairs};

  auto u_iter = engine->NewUnorderedIterator(collection_name);

  // Iterating forward then backward.
  for (size_t i = 0; i < 2; i++) {
    size_t n_total_possible_kv_pairs = possible_kv_pairs.size();
    size_t n_removed_possible_kv_pairs = 0;
    size_t old_progress = 0;

    ASSERT_TRUE(u_iter != nullptr) << "Fail to create UnorderedIterator";
    if (i == 0) {
      u_iter->SeekToFirst();
      std::cout << "[Testing] Iterating forward through Hashes." << std::endl;
    } else {
      u_iter->SeekToLast();
      std::cout << "[Testing] Iterating backward through Hashes." << std::endl;
    }

    ProgressBar progress_iterating{std::cout, "", n_total_possible_kv_pairs,
                                   report_progress};
    while (u_iter->Valid()) {
      std::string value_got;
      auto key = u_iter->Key();
      auto value = u_iter->Value();
      status = engine->HGet(collection_name, key, &value_got);
      ASSERT_EQ(status, kvdk::Status::Ok)
          << "Iteration met kv-pair cannot be got with HGet\n";
      ASSERT_EQ(value, value_got)
          << "Iterated value does not match with HGet value\n";

      CheckKVPair(key, value_got, possible_kv_pairs);

      possible_kv_pairs.erase(key);
      n_removed_possible_kv_pairs =
          n_total_possible_kv_pairs - possible_kv_pairs.size();
      if ((n_removed_possible_kv_pairs > old_progress + 1000) ||
          possible_kv_pairs.empty()) {
        progress_iterating.Update(n_removed_possible_kv_pairs);
        old_progress = n_removed_possible_kv_pairs;
      }

      if (i == 0) // i == 0 for forward
      {
        u_iter->Next();
      } else // i == 1 for backward
      {
        u_iter->Prev();
      }
    }
    // Remaining kv-pairs in possible_kv_pairs are deleted kv-pairs
    // Here we use a dirty trick to check for their deletion.
    // HGet set the return string to empty string when the kv-pair is deleted,
    // else it keeps the string unchanged.
    {
      for (auto iter = possible_kv_pairs.begin();
           iter != possible_kv_pairs.end();) {
        std::string value_got{"Dummy"};
        status = engine->HGet(collection_name, iter->first, &value_got);
        ASSERT_EQ(status, kvdk::Status::NotFound)
            << "Should not have found a key of a entry that cannot be "
               "iterated.\n";

        iter = possible_kv_pairs.erase(iter);
        n_removed_possible_kv_pairs =
            n_total_possible_kv_pairs - possible_kv_pairs.size();

        if ((n_removed_possible_kv_pairs > old_progress + 1000) ||
            possible_kv_pairs.empty()) {
          progress_iterating.Update(n_removed_possible_kv_pairs);
          old_progress = n_removed_possible_kv_pairs;
        }
      }
      ASSERT_TRUE(possible_kv_pairs.empty())
          << "There should be no key left in possible_kv_pairs, "
          << "as they all should have been erased.\n";
    }
    // Reset possible_kv_pairs for iterating backwards.
    possible_kv_pairs = std::move(possible_kv_pairs_copy);
  }
}

// possible_kv_pairs is searched to try to find a match with iterated records
// possible_kv_pairs is copied because SortedSetsIterateThrough erase entries to
// keep track of records
static void SortedSetsIterateThrough(
    kvdk::Engine *engine, std::string collection_name,
    std::unordered_multimap<StringView, StringView> possible_kv_pairs,
    bool report_progress) {
  kvdk::Status status;

  size_t n_total_possible_kv_pairs = possible_kv_pairs.size();
  size_t n_removed_possible_kv_pairs = 0;
  size_t old_progress = n_removed_possible_kv_pairs;

  kvdk::Snapshot *snapshot = engine->GetSnapshot(false);
  auto s_iter = engine->NewSortedIterator(collection_name, snapshot);
  ASSERT_TRUE(s_iter != nullptr) << "Fail to create UnorderedIterator";

  std::string old_key;
  // Set to false after first read, then we can check key ordering
  bool first_read = true;
  ProgressBar progress_iterating{std::cout, "", n_total_possible_kv_pairs,
                                 report_progress};
  for (s_iter->SeekToFirst(); s_iter->Valid(); s_iter->Next()) {
    std::string value_got;
    auto key = s_iter->Key();
    auto value = s_iter->Value();
    status = engine->SGet(collection_name, key, &value_got);
    ASSERT_EQ(status, kvdk::Status::Ok)
        << "Iteration met kv-pair cannot be got with HGet\n";
    ASSERT_EQ(value, value_got)
        << "Iterated value does not match with SGet value\n";

    if (!first_read) {
      ASSERT_LE(old_key, key) << "Keys in sorted sets should be ordered!\n";
      old_key = key;
    } else {
      first_read = false;
    }

    CheckKVPair(key, value_got, possible_kv_pairs);

    possible_kv_pairs.erase(key);
    n_removed_possible_kv_pairs =
        n_total_possible_kv_pairs - possible_kv_pairs.size();
    if ((n_removed_possible_kv_pairs > old_progress + 1000) ||
        possible_kv_pairs.empty()) {
      progress_iterating.Update(n_removed_possible_kv_pairs);
      old_progress = n_removed_possible_kv_pairs;
    }
  }
  // Remaining kv-pairs in possible_kv_pairs are deleted kv-pairs
  // We just cannot keep track of them
  {
    for (auto iter = possible_kv_pairs.begin();
         iter != possible_kv_pairs.end();) {
      std::string value_got{"Dummy"};
      status = engine->SGet(collection_name, iter->first, &value_got);
      ASSERT_EQ(status, kvdk::Status::NotFound)
          << "Should not have found a key of a entry that cannot be "
             "iterated.\n";

      iter = possible_kv_pairs.erase(iter);
      n_removed_possible_kv_pairs =
          n_total_possible_kv_pairs - possible_kv_pairs.size();

      if ((n_removed_possible_kv_pairs > old_progress + 1000) ||
          possible_kv_pairs.empty()) {
        progress_iterating.Update(n_removed_possible_kv_pairs);
        old_progress = n_removed_possible_kv_pairs;
      }
    }
    ASSERT_TRUE(possible_kv_pairs.empty())
        << "There should be no key left in possible_kv_pairs, "
        << "as they all should have been erased.\n";
  }
  engine->ReleaseSnapshot(snapshot);
}

}; // namespace kvdk_testing

/// Contains functions for putting batches of keys and values into a collection
/// in an engine instance.
namespace kvdk_testing {
namespace // nested anonymous namespace to hide implementation
{
void allXSet(
    std::function<kvdk::Status(StringView, StringView, StringView)> setter,
    std::string collection_name, std::vector<StringView> const &keys,
    std::vector<StringView> const &values, bool report_progress) {
  ASSERT_EQ(keys.size(), values.size())
      << "Must have same amount of keys and values to form kv-pairs!";
  kvdk::Status status;

  {
    ProgressBar progress_xsetting{std::cout, "", keys.size(), report_progress};
    for (size_t j = 0; j < keys.size(); j++) {
      status = setter(collection_name, keys[j], values[j]);
      ASSERT_EQ(status, kvdk::Status::Ok)
          << "Fail to Set a key " << keys[j] << " in collection "
          << collection_name;

      if ((j + 1) % 100 == 0 || j + 1 == keys.size()) {
        progress_xsetting.Update(j + 1);
      }
    }
  }
}

void evenXSetOddXDelete(
    std::function<kvdk::Status(StringView, StringView, StringView)> setter,
    std::function<kvdk::Status(StringView, StringView)> getter,
    std::string collection_name, std::vector<StringView> const &keys,
    std::vector<StringView> const &values, bool report_progress) {
  ASSERT_EQ(keys.size(), values.size())
      << "Must have same amount of keys and values to form kv-pairs!";
  kvdk::Status status;

  {
    ProgressBar progress_xupdating{std::cout, "", keys.size(), report_progress};
    for (size_t j = 0; j < keys.size(); j++) {
      if (j % 2 == 0) {
        // Even HSet
        status = setter(collection_name, keys[j], values[j]);
        ASSERT_EQ(status, kvdk::Status::Ok)
            << "Fail to Set a key " << keys[j] << " in collection "
            << collection_name;
      } else {
        // Odd HDelete
        status = getter(collection_name, keys[j]);
        ASSERT_EQ(status, kvdk::Status::Ok)
            << "Fail to Delete a key " << keys[j] << " in collection "
            << collection_name;
      }

      if ((j + 1) % 100 == 0 || j + 1 == keys.size()) {
        progress_xupdating.Update(j + 1);
      }
    }
  }
}
} // namespace

// Calling engine->HSet to put keys and values into collection named after
// collection_name.
static void AllHSet(kvdk::Engine *engine, std::string collection_name,
                    std::vector<StringView> const &keys,
                    std::vector<StringView> const &values,
                    bool report_progress) {
  auto setter = [&](StringView coll_name, StringView key, StringView value) {
    return engine->HSet(coll_name, key, value);
  };
  allXSet(setter, collection_name, keys, values, report_progress);
}

// Calling engine->HSet to put keys and values into collection named after
// collection_name.
static void AllSSetOnly(kvdk::Engine *engine, std::string collection_name,
                        std::vector<StringView> const &keys,
                        std::vector<StringView> const &values,
                        bool report_progress) {
  auto setter = [&](StringView coll_name, StringView key, StringView value) {
    return engine->SSet(coll_name, key, value);
  };
  allXSet(setter, collection_name, keys, values, report_progress);
}

// Calling engine->HSet to put evenly indexed keys and values into collection
// named after collection_name. Calling engine->HDelete to delete oddly indexed
// keys from collection named after collection_name.
static void EvenHSetOddHDelete(kvdk::Engine *engine,
                               std::string collection_name,
                               std::vector<StringView> const &keys,
                               std::vector<StringView> const &values,
                               bool report_progress) {
  auto setter = [&](StringView coll_name, StringView key, StringView value) {
    return engine->HSet(coll_name, key, value);
  };
  auto deleter = [&](StringView coll_name, StringView key) {
    return engine->HDelete(coll_name, key);
  };
  evenXSetOddXDelete(setter, deleter, collection_name, keys, values,
                     report_progress);
}

// Calling engine->SSet to put evenly indexed keys and values into collection
// named after collection_name. Calling engine->SDelete to delete oddly indexed
// keys from collection named after collection_name.
static void EvenSSetOddSDelete(kvdk::Engine *engine,
                               std::string collection_name,
                               std::vector<StringView> const &keys,
                               std::vector<StringView> const &values,
                               bool report_progress) {
  auto setter = [&](StringView coll_name, StringView key, StringView value) {
    return engine->SSet(coll_name, key, value);
  };
  auto deleter = [&](StringView coll_name, StringView key) {
    return engine->SDelete(coll_name, key);
  };
  evenXSetOddXDelete(setter, deleter, collection_name, keys, values,
                     report_progress);
}

} // namespace kvdk_testing

class EngineTestBase : public testing::Test {
protected:
  kvdk::Engine *engine = nullptr;
  kvdk::Configs configs;
  kvdk::Status status;

  const std::string path_db{"/mnt/pmem0/kvdk_test_extensive"};

  /// The following parameters are used to configure the test.
  /// Override SetUpParameters to provide different parameters
  /// Default configure parameters
  bool do_populate_when_initialize;
  size_t sz_pmem_file;
  size_t n_hash_bucket;
  size_t sz_hash_bucket;
  size_t n_blocks_per_segment;
  size_t t_background_work_interval;

  /// Test specific parameters
  size_t n_thread;
  size_t n_kv_per_thread;
  // These parameters set the range of sizes of keys and values
  size_t sz_key_min;
  size_t sz_key_max;
  size_t sz_value_min;
  size_t sz_value_max;

  // Actual keys an values used by thread for insertion
  std::vector<std::vector<StringView>> grouped_keys;
  std::vector<std::vector<StringView>> grouped_values;

  // unordered_map[collection_name, unordered_multimap[key, value]]
  std::unordered_map<std::string,
                     std::unordered_multimap<StringView, StringView>>
      hashes_possible_kv_pairs;
  std::unordered_map<std::string,
                     std::unordered_multimap<StringView, StringView>>
      sorted_sets_possible_kv_pairs;

private:
  std::vector<std::string> key_pool;
  std::vector<std::string> value_pool;
  std::default_random_engine rand{42};

protected:
  /// Other tests should overload this function to setup parameters
  virtual void SetUpParameters() = 0;

  virtual void SetUp() override {
    purgeDB();

    SetUpParameters();

    configs.populate_pmem_space = do_populate_when_initialize;
    configs.pmem_file_size = sz_pmem_file;
    configs.hash_bucket_num = n_hash_bucket;
    configs.hash_bucket_size = sz_hash_bucket;
    configs.pmem_segment_blocks = n_blocks_per_segment;
    configs.background_work_interval = t_background_work_interval;

    prepareKVPairs();

    status = kvdk::Engine::Open(path_db, &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";
  }

  virtual void TearDown() {
    delete engine;
    purgeDB();
  }

  void RebootDB() {
    delete engine;

    status = kvdk::Engine::Open(path_db, &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";
  }

  void ShuffleAllKeysValuesWithinThread() {
    for (size_t tid = 0; tid < n_thread; tid++) {
      shuffleKeys(tid);
      shuffleValues(tid);
    }
  }

  void HashesAllHSet(std::string const &collection_name) {
    updateHashesPossibleKVPairs(collection_name, false);

    auto ModifyEngine = [&](int tid) {
      if (tid == 0) {
        kvdk_testing::AllHSet(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], true);

      } else {
        kvdk_testing::AllHSet(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], false);
      }
    };

    std::cout << "[Testing] Execute HSet in " << collection_name << "."
              << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
  }

  void HashesEvenHSetOddHDelete(std::string const &collection_name) {
    updateHashesPossibleKVPairs(collection_name, true);

    auto ModifyEngine = [&](int tid) {
      if (tid == 0) {
        kvdk_testing::EvenHSetOddHDelete(engine, collection_name,
                                         grouped_keys[tid], grouped_values[tid],
                                         true);
      } else {
        kvdk_testing::EvenHSetOddHDelete(engine, collection_name,
                                         grouped_keys[tid], grouped_values[tid],
                                         false);
      }
    };
    std::cout << "[Testing] Execute HSet and HDelete in " << collection_name
              << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
  }

  void SortedSetsAllSSet(std::string const &collection_name) {
    updateSortedSetsPossibleKVPairs(collection_name, false);

    auto ModifyEngine = [&](int tid) {
      if (tid == 0) {
        kvdk_testing::AllSSetOnly(engine, collection_name, grouped_keys[tid],
                                  grouped_values[tid], true);
      } else {
        kvdk_testing::AllSSetOnly(engine, collection_name, grouped_keys[tid],
                                  grouped_values[tid], false);
      }
    };
    std::cout << "[Testing] Execute SSet in " << collection_name << "."
              << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
  }

  void SortedSetsEvenSSetOddSDelete(std::string const &collection_name) {
    updateSortedSetsPossibleKVPairs(collection_name, true);

    auto ModifyEngine = [&](int tid) {
      if (tid == 0) {
        kvdk_testing::EvenSSetOddSDelete(engine, collection_name,
                                         grouped_keys[tid], grouped_values[tid],
                                         true);
      } else {
        kvdk_testing::EvenSSetOddSDelete(engine, collection_name,
                                         grouped_keys[tid], grouped_values[tid],
                                         false);
      }
    };
    std::cout << "[Testing] Execute SSet and SDelete in " << collection_name
              << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
  }

  void CheckHashesCollection(std::string collection_name) {
    std::cout << "[Testing] Iterate through " << collection_name
              << " to check data." << std::endl;
    hashesIterateThrough(0, collection_name, true);
  }

  void CheckSortedSetsCollection(std::string collection_name) {
    std::cout << "[Testing] Iterate through " << collection_name
              << " to check data." << std::endl;
    sortedSetsIterateThrough(0, collection_name, true);
  }

private:
  void purgeDB() {
    std::string cmd = "rm -rf " + path_db + "\n";
    [[gnu::unused]] int _sink = system(cmd.data());
  }

  void shuffleKeys(size_t tid) {
    std::shuffle(grouped_keys[tid].begin(), grouped_keys[tid].end(), rand);
  }

  void shuffleValues(size_t tid) {
    std::shuffle(grouped_values[tid].begin(), grouped_values[tid].end(), rand);
  }

  void hashesIterateThrough(uint32_t tid, std::string collection_name,
                            bool report_progress) {
    if (report_progress) {
      std::cout << "[Testing] HashesIterateThrough " << collection_name
                << " with thread " << tid << ". "
                << "It may take a few seconds to copy possible_kv_pairs."
                << std::endl;
    }

    // possible_kv_pairs is copied here
    kvdk_testing::HashesIterateThrough(
        engine, collection_name, hashes_possible_kv_pairs[collection_name],
        report_progress);
  }

  void sortedSetsIterateThrough(uint32_t tid, std::string collection_name,
                                bool report_progress) {
    if (report_progress) {
      std::cout << "[Testing] SortedSetsIterateThrough " << collection_name
                << " with thread " << tid << ". "
                << "It may take a few seconds to copy possible_kv_pairs."
                << std::endl;
    }

    // possible_kv_pairs is copied here
    kvdk_testing::SortedSetsIterateThrough(
        engine, collection_name, sorted_sets_possible_kv_pairs[collection_name],
        report_progress);
  }

  void updateHashesPossibleKVPairs(std::string const &collection_name,
                                   bool odd_indexed_is_deleted) {
    std::cout << "[Testing] Updating hashes_possible_kv_pairs." << std::endl;

    auto &possible_kvs = hashes_possible_kv_pairs[collection_name];
    updatePossibleKVPairs(possible_kvs, odd_indexed_is_deleted);
  }

  void updateSortedSetsPossibleKVPairs(std::string const &collection_name,
                                       bool odd_indexed_is_deleted) {
    std::cout << "[Testing] Updating sorted_sets_possible_kv_pairs."
              << std::endl;

    auto &possible_kvs = sorted_sets_possible_kv_pairs[collection_name];
    updatePossibleKVPairs(possible_kvs, odd_indexed_is_deleted);
  }

  void updatePossibleKVPairs(
      std::unordered_multimap<StringView, StringView> &possible_kvs,
      bool odd_indexed_is_deleted) {
    {
      // Erase keys that will be overwritten
      ProgressBar progress_erasing{std::cout, "", grouped_keys.size(), true};
      for (size_t tid = 0; tid < grouped_keys.size(); tid++) {
        for (size_t i = 0; i < grouped_keys[tid].size(); i++) {
          possible_kvs.erase(grouped_keys[tid][i]);
        }
        progress_erasing.Update(tid + 1);
      }
    }

    ASSERT_EQ(grouped_keys.size(), grouped_values.size())
        << "Must have same amount of groups of keys and values!";

    {
      ProgressBar progress_updating{std::cout, "", grouped_keys.size(), true};
      for (size_t tid = 0; tid < grouped_keys.size(); tid++) {
        ASSERT_EQ(grouped_keys[tid].size(), grouped_values[tid].size())
            << "Must have same amount of keys and values to form kv-pairs!";

        // For every thread, every key has only one possible value or state
        // We use kvs to track that and then put those into possible_kvs
        std::unordered_map<StringView, StringView> kvs;
        for (size_t i = 0; i < grouped_keys[tid].size(); i++) {
          if ((i % 2 == 0) || !odd_indexed_is_deleted) {
            kvs[grouped_keys[tid][i]] = grouped_values[tid][i];
          } else {
            kvs.erase(grouped_keys[tid][i]);
          }
        }

        for (auto iter = kvs.begin(); iter != kvs.end(); ++iter) {
          possible_kvs.emplace(iter->first, iter->second);
        }

        progress_updating.Update(tid + 1);
      }
    }
  }

  void prepareKVPairs() {
    key_pool.reserve(n_thread * n_kv_per_thread);
    value_pool.reserve(n_kv_per_thread);
    grouped_keys.resize(n_thread);
    grouped_values.resize(n_thread);
    hashes_possible_kv_pairs.reserve(n_thread * n_kv_per_thread * 2);
    sorted_sets_possible_kv_pairs.reserve(n_thread * n_kv_per_thread * 2);

    for (size_t tid = 0; tid < n_thread; tid++) {
      grouped_keys[tid].reserve(n_kv_per_thread);
      grouped_values[tid].reserve(n_kv_per_thread);
    }

    std::cout << "[Testing] Generating string for keys and values" << std::endl;
    {
      ProgressBar progress_gen_kv{std::cout, "", n_kv_per_thread, true};
      for (size_t i = 0; i < n_kv_per_thread; i++) {
        value_pool.push_back(GetRandomString(sz_value_min, sz_value_max));
        for (size_t tid = 0; tid < n_thread; tid++) {
          key_pool.push_back(GetRandomString(sz_key_min, sz_key_max));
        }

        if ((i + 1) % 1000 == 0 || (i + 1) == n_kv_per_thread) {
          progress_gen_kv.Update(i + 1);
        }
      }
    }
    std::cout << "[Testing] Generating string_view for keys and values"
              << std::endl;
    {
      ProgressBar progress_gen_kv_view{std::cout, "", n_thread, true};
      for (size_t tid = 0; tid < n_thread; tid++) {
        for (size_t i = 0; i < n_kv_per_thread; i++) {
          grouped_keys[tid].emplace_back(key_pool[i * n_thread + tid]);
          grouped_values[tid].emplace_back(value_pool[i]);
        }
        progress_gen_kv_view.Update(tid + 1);
      }
    }
  }
};

class EngineStressTest : public EngineTestBase {
protected:
  virtual void SetUpParameters() override final {
    /// Default configure parameters
    do_populate_when_initialize = false;
    // 256GB PMem
    sz_pmem_file = (256ULL << 30);
    // Less buckets to increase hash collisions
    n_hash_bucket = (1ULL << 20);
    // Smaller buckets to increase hash collisions
    sz_hash_bucket = (3 + 1) * 16;
    n_blocks_per_segment = (1ULL << 10);
    t_background_work_interval = 1;

    /// Test specific parameters
    n_thread = 48;
    // 2M keys per thread, totaling about 100M records
    n_kv_per_thread = (2ULL << 20);
    // These parameters set the range of sizes of keys and values
    sz_key_min = 2;
    sz_key_max = 16;
    sz_value_min = 0;
    sz_value_max = 1024;
  }
  // Shared among EngineStressTest
  const size_t n_reboot = 3;
};

TEST_F(EngineStressTest, HashesHSetOnly) {
  std::string global_collection_name{"GlobalCollection"};

  HashesAllHSet(global_collection_name);
  CheckHashesCollection(global_collection_name);

  std::cout << "[Testing] Close, reopen, iterate through engine for "
            << n_reboot << " times to test recovery." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;
    RebootDB();
    CheckHashesCollection(global_collection_name);
    HashesAllHSet(global_collection_name);
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, HashesHSetAndHDelete) {
  std::string global_collection_name{"GlobalCollection"};

  HashesEvenHSetOddHDelete(global_collection_name);

  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckHashesCollection(global_collection_name);

  std::cout
      << "[Testing] Close, reopen, iterate through, update, iterate through "
         "engine for "
      << n_reboot << " times to test recovery and updating" << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckHashesCollection(global_collection_name);

    ShuffleAllKeysValuesWithinThread();

    HashesEvenHSetOddHDelete(global_collection_name);
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, SortedSetsSSetOnly) {
  std::string global_collection_name{"GlobalCollection"};
  kvdk::Collection *global_collection_ptr;
  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name,
                                           &global_collection_ptr),
            kvdk::Status::Ok);
  SortedSetsAllSSet(global_collection_name);
  CheckSortedSetsCollection(global_collection_name);

  std::cout << "[Testing] Close, reopen, iterate through engine for "
            << n_reboot << " times to test recovery." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckSortedSetsCollection(global_collection_name);
    SortedSetsAllSSet(global_collection_name);
    CheckSortedSetsCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, SortedSetsSSetAndSDelete) {
  std::string global_collection_name{"GlobalCollection"};
  kvdk::Collection *global_collection_ptr;
  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name,
                                           &global_collection_ptr),
            kvdk::Status::Ok);

  SortedSetsEvenSSetOddSDelete(global_collection_name);

  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckSortedSetsCollection(global_collection_name);

  std::cout
      << "[Testing] Close, reopen, iterate through, update, iterate through "
         "engine for "
      << n_reboot << " times to test recovery and updating" << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckSortedSetsCollection(global_collection_name);

    ShuffleAllKeysValuesWithinThread();

    SortedSetsEvenSSetOddSDelete(global_collection_name);
    CheckSortedSetsCollection(global_collection_name);
  }
}

class EngineHotspotTest : public EngineTestBase {
private:
  virtual void SetUpParameters() override final {
    /// Default configure parameters
    do_populate_when_initialize = false;
    // 16GB PMem
    sz_pmem_file = (16ULL << 30);
    // Less buckets to increase hash collisions
    n_hash_bucket = (1ULL << 20);
    // Small buckets to increase hash collisions
    sz_hash_bucket = (3 + 1) * 16;
    n_blocks_per_segment = (1ULL << 20);
    t_background_work_interval = 1;

    /// Test specific parameters
    // Too many threads will make this test too slow
    n_thread = 4;
    // 1M keys per thread, totaling about 50M writes
    n_kv_per_thread = (1ULL << 20);
    // 0-sized key "" is a hotspot, which may reveal many defects
    // These parameters set the range of sizes of keys and values
    sz_key_min = 0;
    sz_key_max = 1;
    sz_value_min = 0;
    sz_value_max = 1024;
  }
};

TEST_F(EngineHotspotTest, HashesMultipleHotspot) {
  std::string global_collection_name{"GlobalHashesCollection"};

  HashesEvenHSetOddHDelete(global_collection_name);
  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckHashesCollection(global_collection_name);

  RebootDB();

  HashesEvenHSetOddHDelete(global_collection_name);
  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckHashesCollection(global_collection_name);
}

TEST_F(EngineHotspotTest, SortedSetsMultipleHotspot) {
  std::string global_collection_name{"GlobalHashesCollection"};

  kvdk::Collection *global_collection_ptr;
  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name,
                                           &global_collection_ptr),
            kvdk::Status::Ok);
  SortedSetsEvenSSetOddSDelete(global_collection_name);
  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckSortedSetsCollection(global_collection_name);

  RebootDB();

  SortedSetsEvenSSetOddSDelete(global_collection_name);
  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckSortedSetsCollection(global_collection_name);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
