/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include <functional>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>

#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include "test_util.h"
#include "gtest/gtest.h"

// Provides functions to iterate through a collection and check its contents
class IteratingFacility
{
protected:
  // possible_kv_pairs is searched to try to find a match with iterated records
  // possible_kv_pairs is copied because IterateThroughHashes erase entries to keep track of records
  // TODO: Also Iterate backwards
  void IterateThroughHashes(kvdk::Engine* engine, std::string collection_name, 
                            std::unordered_multimap<std::string_view, std::string_view> possible_kv_pairs, 
                            bool report_progress) 
  {
    kvdk::Status status;
    
    int n_total_possible_kv_pairs = possible_kv_pairs.size();
    int n_removed_possible_kv_pairs = 0;
    int old_progress = n_removed_possible_kv_pairs;

    auto u_iter = engine->NewUnorderedIterator(collection_name);
    ASSERT_TRUE(u_iter != nullptr) << "Fail to create UnorderedIterator";
    for (u_iter->SeekToFirst(); u_iter->Valid(); u_iter->Next())
    {
      std::string value_got;
      auto key = u_iter->Key();
      auto value = u_iter->Value();
      status = engine->HGet(collection_name, key, &value_got);
      EXPECT_EQ(status, kvdk::Status::Ok)
        << "Iteration met kv-pair cannot be got with HGet\n";
      EXPECT_EQ(value, value_got)
        << "Iterated value does not match with HGet value\n";

      CheckXGetResult(key, value_got, possible_kv_pairs);

      possible_kv_pairs.erase(key);
      n_removed_possible_kv_pairs = n_total_possible_kv_pairs - possible_kv_pairs.size();
      if (report_progress && (n_removed_possible_kv_pairs > old_progress + 1000 || possible_kv_pairs.empty()))
      {
        ShowProgress(std::cout, n_removed_possible_kv_pairs, n_total_possible_kv_pairs);
        old_progress = n_removed_possible_kv_pairs;
      }
    }
    // Remaining kv-pairs in possible_kv_pairs are deleted kv-pairs
    // Here we use a dirty trick to check for their deletion.
    // HGet set the return string to empty string when the kv-pair is deleted,
    // else it keeps the string unchanged.
    {
      for (auto iter = possible_kv_pairs.begin(); iter != possible_kv_pairs.end(); )
      {
        std::string value_got{"Dummy"};
        status = engine->HGet(collection_name, iter->first, &value_got);
        EXPECT_EQ(status, kvdk::Status::NotFound)
          << "Should not have found a key of a entry that cannot be iterated.\n";
        EXPECT_EQ(value_got, "")
          << "HGet a DlistDeleteRecord should have set value_got as empty string\n"; 

        iter = possible_kv_pairs.erase(iter);
        n_removed_possible_kv_pairs = n_total_possible_kv_pairs - possible_kv_pairs.size();

        if (report_progress && (n_removed_possible_kv_pairs > old_progress + 1000 || possible_kv_pairs.empty()))
        {
          ShowProgress(std::cout, n_removed_possible_kv_pairs, n_total_possible_kv_pairs);
          old_progress = n_removed_possible_kv_pairs;
        }

      }
      EXPECT_TRUE(possible_kv_pairs.empty())
        << "There should be no key left in possible_kv_pairs, "
        << "as they all should have been erased.\n";
    }
  }

  // possible_kv_pairs is searched to try to find a match with iterated records
  // possible_kv_pairs is copied because IterateThroughSortedSets erase entries to keep track of records
  void IterateThroughSortedSets(kvdk::Engine* engine, std::string collection_name, 
                        std::unordered_multimap<std::string_view, std::string_view> possible_kv_pairs, 
                        bool report_progress) 
  {
    kvdk::Status status;
    
    int n_total_possible_kv_pairs = possible_kv_pairs.size();
    int n_removed_possible_kv_pairs = 0;
    int old_progress = n_removed_possible_kv_pairs;

    auto s_iter = engine->NewSortedIterator(collection_name);
    ASSERT_TRUE(s_iter != nullptr) << "Fail to create UnorderedIterator";

    std::string old_key;
    bool first_read = true;
    for (s_iter->SeekToFirst(); s_iter->Valid(); s_iter->Next())
    {
      std::string value_got;
      auto key = s_iter->Key();
      auto value = s_iter->Value();
      status = engine->SGet(collection_name, key, &value_got);
      EXPECT_EQ(status, kvdk::Status::Ok)
        << "Iteration met kv-pair cannot be got with HGet\n";
      EXPECT_EQ(value, value_got)
        << "Iterated value does not match with SGet value\n";

      if (!first_read)
      {
        ASSERT_LE(old_key, key)
          << "Keys in sorted sets should be ordered!\n";
        old_key = key;
      }
      else
      {
        first_read = false;
      }

      CheckXGetResult(key, value_got, possible_kv_pairs);
      
      possible_kv_pairs.erase(key);
      n_removed_possible_kv_pairs = n_total_possible_kv_pairs - possible_kv_pairs.size();
      if (report_progress && (n_removed_possible_kv_pairs > old_progress + 1000 || possible_kv_pairs.empty()))
      {
        ShowProgress(std::cout, n_removed_possible_kv_pairs, n_total_possible_kv_pairs);
        old_progress = n_removed_possible_kv_pairs;
      }
    }
    // Remaining kv-pairs in possible_kv_pairs are deleted kv-pairs
    // We just cannot keep track of them
    {
      for (auto iter = possible_kv_pairs.begin(); iter != possible_kv_pairs.end(); )
      {
        std::string value_got{"Dummy"};
        status = engine->SGet(collection_name, iter->first, &value_got);
        EXPECT_EQ(status, kvdk::Status::NotFound)
          << "Should not have found a key of a entry that cannot be iterated.\n";

        iter = possible_kv_pairs.erase(iter);
        n_removed_possible_kv_pairs = n_total_possible_kv_pairs - possible_kv_pairs.size();

        if (report_progress && (n_removed_possible_kv_pairs > old_progress + 1000 || possible_kv_pairs.empty()))
        {
          ShowProgress(std::cout, n_removed_possible_kv_pairs, n_total_possible_kv_pairs);
          old_progress = n_removed_possible_kv_pairs;
        }
      }
      EXPECT_TRUE(possible_kv_pairs.empty())
        << "There should be no key left in possible_kv_pairs, "
        << "as they all should have been erased.\n";
    }
  }

  // Check value got by XGet(key) by looking up possible_kv_pairs
  void CheckXGetResult(pmem::obj::string_view key, pmem::obj::string_view value, std::unordered_multimap<std::string_view, std::string_view> const& possible_kv_pairs)
  {
      bool match = false;
      auto range_found = possible_kv_pairs.equal_range(key);
      
      for (auto iter = range_found.first; iter != range_found.second; ++iter)
      {
        EXPECT_EQ(key, iter->first)
          << "Iterated key and key in possible_kv_pairs does not match: \n"
          << "Iterated key: " << key << "\n"
          << "Key in possible_kv_pairs: " << iter->first;
        match = match || (value == iter->second);
      }
      EXPECT_TRUE(match) 
        << "No kv-pair in possible_kv_pairs matching with iterated kv-pair:\n"
        << "Key: " << key << "\n"
        << "Value: " << value << "\n";
  }
};

class SetDeleteFacility
{
private:
  static constexpr std::size_t _unordered_map_load_factor_inverse_ = 2;
protected:
  // Calling engine->HSet to put keys and values into collection named after collection_name.
  void HSetOnly(kvdk::Engine* engine, 
                std::string collection_name, 
                std::vector<pmem::obj::string_view> const& keys, 
                std::vector<pmem::obj::string_view> const& values, 
                bool report_progress) 
  {
    auto setter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key, pmem::obj::string_view value)
    {
      return engine->HSet(coll_name, key, value);
    };
    xSetOnly(setter, collection_name, keys, values, report_progress);
  }

  // Calling engine->HSet to put keys and values into collection named after collection_name.
  void SSetOnly(kvdk::Engine* engine, 
                std::string collection_name, 
                std::vector<pmem::obj::string_view> const& keys, 
                std::vector<pmem::obj::string_view> const& values, 
                bool report_progress) 
  {
    auto setter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key, pmem::obj::string_view value)
    {
      return engine->SSet(coll_name, key, value);
    };
    xSetOnly(setter, collection_name, keys, values, report_progress);
  }

  // Calling engine->HSet to put evenly indexed keys and values into collection named after collection_name.
  // Calling engine->HDelete to delete oddly indexed keys from collection named after collection_name.
  void EvenHSetOddHDelete(kvdk::Engine* engine, 
                          std::string collection_name, 
                          std::vector<pmem::obj::string_view> const& keys, 
                          std::vector<pmem::obj::string_view> const& values, 
                          bool report_progress)
  {
    auto setter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key, pmem::obj::string_view value)
    {
      return engine->HSet(coll_name, key, value);
    };
    auto deleter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key)
    {
      return engine->HDelete(coll_name, key);
    };
    evenXSetOddXDelete(setter, deleter, collection_name, keys, values, report_progress);
  }

  // Calling engine->SSet to put evenly indexed keys and values into collection named after collection_name.
  // Calling engine->SDelete to delete oddly indexed keys from collection named after collection_name.
  void EvenSSetOddSDelete(kvdk::Engine* engine, 
                          std::string collection_name, 
                          std::vector<pmem::obj::string_view> const& keys, 
                          std::vector<pmem::obj::string_view> const& values, 
                          bool report_progress)
  {
    auto setter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key, pmem::obj::string_view value)
    {
      return engine->SSet(coll_name, key, value);
    };
    auto deleter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key)
    {
      return engine->SDelete(coll_name, key);
    };
    evenXSetOddXDelete(setter, deleter, collection_name, keys, values, report_progress);
  }

private:
  void xSetOnly(std::function<kvdk::Status(pmem::obj::string_view, pmem::obj::string_view, pmem::obj::string_view)> setter, 
                std::string collection_name,
                std::vector<pmem::obj::string_view> const& keys, 
                std::vector<pmem::obj::string_view> const& values, 
                bool report_progress) 
  {
    ASSERT_EQ(keys.size(), values.size()) << "Must have same amount of keys and values to form kv-pairs!";
    kvdk::Status status;
    
    for (size_t j = 0; j < keys.size(); j++)
    {
      status = setter(collection_name, keys[j], values[j]);
      EXPECT_EQ(status, kvdk::Status::Ok) 
        << "Fail to Set a key " 
        << keys[j] 
        << " in collection "
        << collection_name;
      
      if (report_progress && ((j + 1) % 1000 == 0 || j + 1 == keys.size()))
        ShowProgress(std::cout, j + 1, keys.size());
    }
  }

  void evenXSetOddXDelete(std::function<kvdk::Status(pmem::obj::string_view, pmem::obj::string_view, pmem::obj::string_view)> setter, 
                          std::function<kvdk::Status(pmem::obj::string_view, pmem::obj::string_view)> getter,
                          std::string collection_name,
                          std::vector<pmem::obj::string_view> const& keys, 
                          std::vector<pmem::obj::string_view> const& values, 
                          bool report_progress) 
  {
    ASSERT_EQ(keys.size(), values.size()) << "Must have same amount of keys and values to form kv-pairs!";
    kvdk::Status status;
    
    for (size_t j = 0; j < keys.size(); j++)
    {
      if (j % 2 == 0)
      {
        // Even HSet
        status = setter(collection_name, keys[j], values[j]);
        EXPECT_EQ(status, kvdk::Status::Ok) 
          << "Fail to Set a key " 
          << keys[j] 
          << " in collection "
          << collection_name;
      }
      else
      {
        // Odd HDelete
        status = getter(collection_name, keys[j]);
        EXPECT_EQ(status, kvdk::Status::Ok) 
          << "Fail to Delete a key " 
          << keys[j] 
          << " in collection "
          << collection_name;
      }
        
      if (report_progress && ((j + 1) % 1000 == 0 || j + 1 == keys.size()))
        ShowProgress(std::cout, j + 1, keys.size());
    }
  }
};

class EngineExtensiveTest : public testing::Test, protected IteratingFacility, protected SetDeleteFacility {
protected:
  kvdk::Engine *engine = nullptr;
  kvdk::Configs configs;
  kvdk::Status status;

  const std::string path_db{ "/mnt/pmem0/kvdk_test_extensive" };

  // Default configure parameters
  const bool do_populate_when_initialize = false;
  const size_t sz_pmem_file{ 256ULL << 30 };    // 256GB PMem
  const size_t n_hash_bucket{ 1ULL << 20 };     // Less buckets to increase hash collisions
  const size_t sz_hash_bucket{ (3 + 1) * 16 };  // Smaller buckets to increase hash collisions
  const size_t n_blocks_per_segment{ 1ULL << 20 };
  const size_t t_background_work_interval = 1;

  const size_t n_thread{ 48 };
  const size_t n_kv_per_thread{ 2ULL << 20 };   // 2M keys per thread, totaling about 100M records

  const size_t sz_key_min{ 2 };
  const size_t sz_key_max{ 8 };
  const size_t sz_value_min{ 16 };
  const size_t sz_value_max{ 1024 };

  std::vector<std::vector<std::string_view>> grouped_keys;
  std::vector<std::vector<std::string_view>> grouped_values;

  // unordered_map[collection_name, unordered_multimap[key, value]]
  std::unordered_map<std::string, std::unordered_multimap<std::string_view, std::string_view>> possible_kv_pairs;

private:
  std::vector<std::string> _values_;
  std::vector<std::string> _keys_;
  std::default_random_engine rand{ 42 };

protected:
  void PurgeDB()
  {
    std::string cmd = "rm -rf " + path_db + "\n";
    int _sink = system(cmd.data());
  }

  void RebootDB()
  {
    delete engine;

    status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";
  }

  virtual void SetUp() override 
  {
    PurgeDB();

    configs.populate_pmem_space = do_populate_when_initialize;
    configs.pmem_file_size = sz_pmem_file;
    configs.hash_bucket_num = n_hash_bucket;
    configs.hash_bucket_size = sz_hash_bucket;
    configs.pmem_segment_blocks = n_blocks_per_segment;
    configs.background_work_interval = t_background_work_interval;

    _keys_.reserve(n_thread * n_kv_per_thread);
    _values_.reserve(n_kv_per_thread);
    grouped_keys.resize(n_thread);
    grouped_values.resize(n_thread);
    possible_kv_pairs.reserve(n_thread * n_kv_per_thread * 2);
    for (size_t tid = 0; tid < n_thread; tid++)
    {
      grouped_keys[tid].reserve(n_kv_per_thread);
      grouped_values[tid].reserve(n_kv_per_thread);
    }
    
    std::cout << "[INFO] Generating string for keys and values" << std::endl; 
    for (size_t i = 0; i < n_kv_per_thread; i++)
    {
      _values_.push_back(GetRandomString(sz_value_min, sz_value_max));
      for (size_t tid = 0; tid < n_thread; tid++)
          _keys_.push_back(GetRandomString(sz_key_min, sz_key_max));
      if ((i + 1) % 1000 == 0 || (i + 1) == n_kv_per_thread)
        ShowProgress(std::cout, (i + 1), n_kv_per_thread);
    }
    std::cout << "[INFO] Generating string_view for keys and values" << std::endl; 
    for (size_t tid = 0; tid < n_thread; tid++)
    {
      for (size_t i = 0; i < n_kv_per_thread; i++)
      {
          grouped_keys[tid].emplace_back(_keys_[i*n_thread+tid]);
          grouped_values[tid].emplace_back(_values_[i]);
      }
      ShowProgress(std::cout, tid + 1, n_thread);
    }
  
    status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";
  }

  virtual void TearDown() 
  {
    delete engine;
    PurgeDB();
  }

  void ShuffleAllKeysValues()
  {
    for (size_t tid = 0; tid < n_thread; tid++)
    {
      shuffleKeys(tid);
      shuffleValues(tid);
    }
  }

  void LaunchHSetOnly(std::string const& collection_name)
  {
    updatePossibleKVPairs(collection_name, false);
    
    auto ModifyEngine = [&](int tid){ executeHSetOnly(collection_name, tid); };
    std::cout << "[INFO] Execute HSet in " << collection_name << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
  }

  void LaunchEvenHSetOddHDelete(std::string const& collection_name)
  {
    updatePossibleKVPairs(collection_name, true);

    auto ModifyEngine = [&](int tid){ executeEvenHSetOddHDelete(collection_name, tid); };
    std::cout << "[INFO] Execute HSet and HDelete in " << collection_name << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
  }

  void LaunchSSetOnly(std::string const& collection_name)
  {
    updatePossibleKVPairs(collection_name, false);

    auto ModifyEngine = [&](int tid){ executeSSetOnly(collection_name, tid); };
    std::cout << "[INFO] Execute SSet in " << collection_name << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
  }

  void LaunchEvenSSetOddSDelete(std::string const& collection_name)
  {
    updatePossibleKVPairs(collection_name, true);

    auto ModifyEngine = [&](int tid){ executeEvenSSetOddSDelete(collection_name, tid); };
    std::cout << "[INFO] Execute SSet and SDelete in " << collection_name << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
  }

  void CheckHashesCollection(std::string collection_name)
  {
    std::cout << "[INFO] Iterate through " << collection_name << " to check data." << std::endl;
    iterateThroughHashes(0, collection_name);
  }

  void CheckSortedSetsCollection(std::string collection_name)
  {
    std::cout << "[INFO] Iterate through " << collection_name << " to check data." << std::endl;
    iterateThroughSortedSets(0, collection_name);
  }

private:
  void shuffleKeys(size_t tid)
  {
    std::shuffle(grouped_keys[tid].begin(), grouped_keys[tid].end(), rand);
  }
  
  void shuffleValues(size_t tid)
  {
    std::shuffle(grouped_values[tid].begin(), grouped_values[tid].end(), rand);
  }

  void iterateThroughHashes(uint32_t tid, std::string collection_name)
  {
    bool report_progress = (tid == 0);
    if (report_progress)
      std::cout 
        << "[INFO] IterateThroughHashes " << collection_name 
        << " with thread " << tid << ". "
        << "It may take a few seconds to copy possible_kv_pairs."
        << std::endl;
    
    // possible_kv_pairs is copied here
    IteratingFacility::IterateThroughHashes(engine, collection_name, possible_kv_pairs[collection_name], report_progress);
  }

  void iterateThroughSortedSets(uint32_t tid, std::string collection_name)
  {
    bool report_progress = (tid == 0);
    if (report_progress)
      std::cout 
        << "[INFO] IterateThroughSortedSets " << collection_name 
        << " with thread " << tid << ". "
        << "It may take a few seconds to copy possible_kv_pairs."
        << std::endl;
    
    // possible_kv_pairs is copied here
    IteratingFacility::IterateThroughSortedSets(engine, collection_name, possible_kv_pairs[collection_name], report_progress);
  }

  void executeHSetOnly(std::string const& collection_name, std::uint64_t tid)
  {
    if (tid == 0)
      SetDeleteFacility::HSetOnly(engine, collection_name, grouped_keys[tid], grouped_values[tid], true);
    else
      SetDeleteFacility::HSetOnly(engine, collection_name, grouped_keys[tid], grouped_values[tid], false);
  }

  void executeSSetOnly(std::string const& collection_name, std::uint64_t tid)
  {
    if (tid == 0)
      SetDeleteFacility::SSetOnly(engine, collection_name, grouped_keys[tid], grouped_values[tid], true);
    else
      SetDeleteFacility::SSetOnly(engine, collection_name, grouped_keys[tid], grouped_values[tid], false);
  }

  void executeEvenHSetOddHDelete(std::string const& collection_name, std::uint64_t tid)
  {
    if (tid == 0)
      SetDeleteFacility::EvenHSetOddHDelete(engine, collection_name, grouped_keys[tid], grouped_values[tid], true);
    else
      SetDeleteFacility::EvenHSetOddHDelete(engine, collection_name, grouped_keys[tid], grouped_values[tid], false);
  }

  void executeEvenSSetOddSDelete(std::string const& collection_name, std::uint64_t tid)
  {
    if (tid == 0)
      SetDeleteFacility::EvenSSetOddSDelete(engine, collection_name, grouped_keys[tid], grouped_values[tid], true);
    else
      SetDeleteFacility::EvenSSetOddSDelete(engine, collection_name, grouped_keys[tid], grouped_values[tid], false);
  }

  void updatePossibleKVPairs(std::string const& collection_name, bool odd_indexed_is_deleted)
  {
    std::cout << "[INFO] Updating possible_kv_pairs." << std::endl;

    auto& possible_kvs = possible_kv_pairs[collection_name];
    // Erase keys that will be overwritten
    for (size_t tid = 0; tid < grouped_keys.size(); tid++)
    {
        for (size_t i = 0; i < grouped_keys[tid].size(); i++)
            possible_kvs.erase(grouped_keys[tid][i]);    

        ShowProgress(std::cout, tid + 1, grouped_keys.size());
    }

    ASSERT_EQ(grouped_keys.size(), grouped_values.size()) 
      << "Must have same amount of groups of keys and values!";

    for (size_t tid = 0; tid < grouped_keys.size(); tid++)
    {
      ASSERT_EQ(grouped_keys[tid].size(), grouped_values[tid].size()) 
        << "Must have same amount of keys and values to form kv-pairs!";

      for (size_t i = 0; i < grouped_keys[tid].size(); i++)
      {
          if (odd_indexed_is_deleted && (i % 2 == 1))
            continue;
          
          possible_kvs.emplace(grouped_keys[tid][i], grouped_values[tid][i]);
      }
      ShowProgress(std::cout, tid + 1, grouped_keys.size());
    }
  }
};

TEST_F(EngineExtensiveTest, HashCollectionHSetOnly) 
{
  std::string global_collection_name{"GlobalCollection"};

  LaunchHSetOnly(global_collection_name);
  CheckHashesCollection(global_collection_name);

  size_t n_repeat = 3;
  std::cout 
    << "[INFO] Close, reopen, iterate through engine for " 
    << n_repeat << " times to test recovery." << std::endl;
  for (size_t i = 0; i < n_repeat; i++)
  {
    std::cout << "[INFO] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineExtensiveTest, HashCollectionHSetAndHDelete) 
{
  std::string global_collection_name{"GlobalCollection"};

  LaunchEvenHSetOddHDelete(global_collection_name);

  std::cout << "[INFO] Iterate through collection to check data." << std::endl;
  CheckHashesCollection(global_collection_name);

  size_t n_repeat = 3;
  std::cout 
    << "[INFO] Close, reopen, iterate through, update, iterate through engine for " 
    << n_repeat << " times to test recovery and updating" << std::endl;
  for (size_t i = 0; i < n_repeat; i++)
  {
    std::cout << "[INFO] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckHashesCollection(global_collection_name);

    ShuffleAllKeysValues();

    LaunchEvenHSetOddHDelete(global_collection_name);
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineExtensiveTest, SortedCollectionSSetOnly) 
{
  std::string global_collection_name{"GlobalCollection"};

  LaunchSSetOnly(global_collection_name);
  CheckSortedSetsCollection(global_collection_name);

  size_t n_repeat = 3;
  std::cout 
    << "[INFO] Close, reopen, iterate through engine for " 
    << n_repeat << " times to test recovery." << std::endl;
  for (size_t i = 0; i < n_repeat; i++)
  {
    std::cout << "[INFO] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckSortedSetsCollection(global_collection_name);
  }
}

TEST_F(EngineExtensiveTest, SortedCollectionSSetAndSDelete) 
{
  std::string global_collection_name{"GlobalCollection"};

  LaunchEvenSSetOddSDelete(global_collection_name);

  std::cout << "[INFO] Iterate through collection to check data." << std::endl;
  CheckSortedSetsCollection(global_collection_name);

  size_t n_repeat = 3;
  std::cout 
    << "[INFO] Close, reopen, iterate through, update, iterate through engine for " 
    << n_repeat << " times to test recovery and updating" << std::endl;
  for (size_t i = 0; i < n_repeat; i++)
  {
    std::cout << "[INFO] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckSortedSetsCollection(global_collection_name);

    ShuffleAllKeysValues();

    LaunchEvenSSetOddSDelete(global_collection_name);
    CheckSortedSetsCollection(global_collection_name);
  }
}

class EngineHotspotTest : public testing::Test, protected IteratingFacility, protected SetDeleteFacility {
protected:
  kvdk::Engine *engine = nullptr;
  kvdk::Configs configs;
  kvdk::Status status;

  const std::string path_db{ "/mnt/pmem0/kvdk_test_hotspot" };

  // Default configure parameters
  const bool do_populate_when_initialize = false;
  const size_t sz_pmem_file{ 1ULL << 30 };      // 1GB PMem
  const size_t n_hash_bucket{ 1ULL << 10 };     // Less buckets to increase hash collisions
  const size_t sz_hash_bucket{ (3 + 1) * 16 };  // Smaller buckets to increase hash collisions
  const size_t n_blocks_per_segment{ 1ULL << 10 };
  const size_t t_background_work_interval = 1;

  const size_t n_thread{ 48 };
  const size_t n_kv_per_thread{ 2ULL << 10 };   // 2K keys per thread, most of which are duplicate
                                                // Actually will be less than 26^2+26+1=703 keys

  const size_t sz_key_min{ 0 };  // Small keys will raise many hotspots, empty string "" will happen about 1/3 times.
  const size_t sz_key_max{ 2 };
  const size_t sz_value_min{ 16 };
  const size_t sz_value_max{ 1024 };

  std::vector<std::vector<std::string_view>> keys;
  std::vector<std::vector<std::string_view>> values;

private:
  std::vector<std::string> _values_;
  std::vector<std::string> _keys_;
  std::default_random_engine rand{ 42 };

protected:
  void PurgeDB()
  {
    std::string cmd = "rm -rf " + path_db + "\n";
    int _sink = system(cmd.data());
  }

  virtual void SetUp() override 
  {
    PurgeDB();

    configs.populate_pmem_space = do_populate_when_initialize;
    configs.pmem_file_size = sz_pmem_file;
    configs.hash_bucket_num = n_hash_bucket;
    configs.hash_bucket_size = sz_hash_bucket;
    configs.pmem_segment_blocks = n_blocks_per_segment;
    configs.background_work_interval = t_background_work_interval;

    _keys_.reserve(n_thread * n_kv_per_thread);
    _values_.reserve(n_kv_per_thread);
    keys.resize(n_thread);
    values.resize(n_thread);
    for (size_t tid = 0; tid < n_thread; tid++)
    {
      keys[tid].reserve(n_kv_per_thread);
      values[tid].reserve(n_kv_per_thread);
    }
    
    std::cout << "[INFO] Generating string for keys and values" << std::endl; 
    for (size_t i = 0; i < n_kv_per_thread; i++)
    {
      _values_.push_back(GetRandomString(sz_value_min, sz_value_max));
      for (size_t tid = 0; tid < n_thread; tid++)
          _keys_.push_back(GetRandomString(sz_key_min, sz_key_max));
    }
    std::cout << "[INFO] Generating string_view for keys and values" << std::endl; 
    for (size_t i = 0; i < n_kv_per_thread; i++)
    {
      for (size_t tid = 0; tid < n_thread; tid++)
      {
          keys[tid].emplace_back(_keys_[i*n_thread+tid]);
          values[tid].emplace_back(_values_[i]);
      }
    }
  
    status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";
  }

  virtual void TearDown() 
  {
    delete engine;
    PurgeDB();
  }
};

TEST_F(EngineHotspotTest, HashesMultipleHotspot) 
{
  int n_repeat = 1000;
  std::string global_collection_name{ "GlobalHashesCollection" };
  // EvenWriteOddRead is Similar to EvenSetOddDelete - only evenly indexed keys may appear

  // Evenly indexed keys are write and oddly indexed keys are skipped
  auto EvenWriteOddRead = [&](uint32_t tid) 
  {
    std::string value_got;
    for (size_t j = 0; j < keys[tid].size(); j++)
    {
      if (j % 2 == 0)
      {
        status = engine->HSet(global_collection_name, keys[tid][j], values[tid][j]);
        EXPECT_TRUE(status == kvdk::Status::Ok);
      }
      else
      {
        status = engine->HGet(global_collection_name, keys[tid][j], &value_got);
        EXPECT_TRUE((status == kvdk::Status::NotFound) || (status == kvdk::Status::Ok));
        if (status == kvdk::Status::Ok)
        {
          // IteratingFacility::CheckXGetResult(keys[tid][j], value_got, possible_kv_pairs);
        }
      }
    }
  };

  std::cout << "[INFO] Writing and Reading ..." << std::endl;
  for (size_t i = 0; i < n_repeat; i++)
  {
    LaunchNThreads(n_thread, EvenWriteOddRead);
    ShowProgress(std::cout, i + 1, n_repeat);
  }
}

int main(int argc, char **argv) 
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
