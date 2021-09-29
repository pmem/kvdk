/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <future>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>

#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include "test_util.h"
#include "gtest/gtest.h"

class EngineExtensiveTest : public testing::Test {
protected:
  kvdk::Engine *engine = nullptr;
  kvdk::Configs configs;
  kvdk::Status status;

  const std::string path_db{ "/mnt/pmem0/kvdk_test_hashes" };

  // Default configure parameters
  const bool do_populate_when_initialize = false;
  const size_t sz_pmem_file{ 256ULL << 30 };
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
    
    std::cout << "Generating string for keys and values" << std::endl; 
    for (size_t i = 0; i < n_kv_per_thread; i++)
    {
      _values_.push_back(GetRandomString(sz_value_min, sz_value_max));
      for (size_t tid = 0; tid < n_thread; tid++)
          _keys_.push_back(GetRandomString(sz_key_min, sz_key_max));
      if (i % 10000 == 0)
        ShowProgress(std::cout, i, n_kv_per_thread);
    }
    std::cout << "Generating string_view for keys and values" << std::endl; 
    for (size_t i = 0; i < n_kv_per_thread; i++)
    {
      for (size_t tid = 0; tid < n_thread; tid++)
      {
          keys[tid].emplace_back(_keys_[i*n_thread+tid]);
          values[tid].emplace_back(_values_[i]);
      }
      if (i % 10000 == 0)
        ShowProgress(std::cout, i, n_kv_per_thread);
    }
  
    status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";
  }

  virtual void TearDown() 
  {
    delete engine;
    PurgeDB();
  }

  void ShuffleKeys(size_t tid)
  {
    std::shuffle(keys[tid].begin(), keys[tid].end(), rand);
  }
  
  void ShuffleValues(size_t tid)
  {
    std::shuffle(values[tid].begin(), values[tid].end(), rand);
  }

  void ShuffleAllKeysValues()
  {
    for (size_t tid = 0; tid < n_thread; tid++)
    {
      ShuffleKeys(tid);
      ShuffleValues(tid);
    }
  }

  void XSetOnly(std::function<kvdk::Status(pmem::obj::string_view, pmem::obj::string_view, pmem::obj::string_view)> xset, uint32_t tid, std::string collection_name, bool report_progress) 
  {
    kvdk::Status status;
    if (report_progress)
      std::cout << "Executing XSetOnly with thread " << tid << std::endl;
    
    for (size_t j = 0; j < n_kv_per_thread; j++)
    {
      status = xset(collection_name, keys[tid][j], values[tid][j]);
      EXPECT_EQ(status, kvdk::Status::Ok) 
        << "Fail to Set a key " 
        << keys[tid][j] 
        << " in collection "
        << collection_name;
      
      if (report_progress && j % 10000 == 0)
        ShowProgress(std::cout, j, n_kv_per_thread);
    }
  }

  void HSetOnly(uint32_t tid, std::string collection_name, bool report_progress = false) 
  {
    auto setter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key, pmem::obj::string_view value)
    {
      return engine->HSet(coll_name, key, value);
    };
    XSetOnly(setter, tid, collection_name, report_progress);
  }

  void SSetOnly(uint32_t tid, std::string collection_name, bool report_progress = false) 
  {
    auto setter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key, pmem::obj::string_view value)
    {
      return engine->SSet(coll_name, key, value);
    };
    XSetOnly(setter, tid, collection_name, report_progress);
  }

  void EvenXSetOddXDelete(std::function<kvdk::Status(pmem::obj::string_view, pmem::obj::string_view, pmem::obj::string_view)> xset, 
                          std::function<kvdk::Status(pmem::obj::string_view, pmem::obj::string_view)> xdelete,
                          uint32_t tid, std::string collection_name, bool report_progress) 
  {
    kvdk::Status status;
    if (report_progress)
      std::cout << "Executing EvenXSetOddXDelete with thread " << tid << std::endl;
    
    for (size_t j = 0; j < n_kv_per_thread; j++)
    {
      if (j % 2 == 0)
      {
        // Even HSet
        status = xset(collection_name, keys[tid][j], values[tid][j]);
        EXPECT_EQ(status, kvdk::Status::Ok) 
          << "Fail to Set a key " 
          << keys[tid][j] 
          << " in collection "
          << collection_name;
      }
      else
      {
        // Odd HDelete
        status = xdelete(collection_name, keys[tid][j]);
        EXPECT_EQ(status, kvdk::Status::Ok) 
          << "Fail to Delete a key " 
          << keys[tid][j] 
          << " in collection "
          << collection_name;
      }
        
      if (report_progress && j % 10000 == 0)
        ShowProgress(std::cout, j, n_kv_per_thread);
    }
  }

  void EvenHSetOddHDelete(uint32_t tid, std::string collection_name, bool report_progress = false) 
  {
    auto setter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key, pmem::obj::string_view value)
    {
      return engine->HSet(coll_name, key, value);
    };
    auto deleter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key)
    {
      return engine->HDelete(coll_name, key);
    };
    EvenXSetOddXDelete(setter, deleter, tid, collection_name, report_progress);
  }

  void EvenSSetOddSDelete(uint32_t tid, std::string collection_name, bool report_progress = false) 
  {
    auto setter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key, pmem::obj::string_view value)
    {
      return engine->SSet(coll_name, key, value);
    };
    auto deleter = [&](pmem::obj::string_view coll_name, pmem::obj::string_view key)
    {
      return engine->SDelete(coll_name, key);
    };
    EvenXSetOddXDelete(setter, deleter, tid, collection_name, report_progress);
  }

  // possible_kvs is searched to try to find a match with iterated records
  // possible_kvs is copied because IterateThroughHashes erase entries to keep track of records
  // TODO: Also Iterate backwards
  void IterateThroughHashes(uint32_t tid, std::string collection_name, 
                        std::unordered_multimap<std::string_view, std::string_view> possible_kvs, 
                        bool report_progress = false) 
  {
    kvdk::Status status;

    if (report_progress)
      std::cout << "IterateThroughHashes " << collection_name << " with thread " << tid << std::endl;
    
    int n_total_possible_kvs = possible_kvs.size();
    int n_removed_possible_kvs = 0;
    int old_progress = n_removed_possible_kvs;

    auto u_iter = engine->NewUnorderedIterator(collection_name);
    ASSERT_TRUE(u_iter != nullptr) << "Fail to create UnorderedIterator";
    // TODO: also HGet to check the iterator
    for (u_iter->SeekToFirst(); u_iter->Valid(); u_iter->Next())
    {
      std::string value_got;
      auto key = u_iter->Key();
      auto value = u_iter->Value();
      status = engine->HGet(collection_name, key, &value_got);
      EXPECT_EQ(value, value_got)
        << "Iterated value does not match with HGet value\n";

      bool match = false;
      auto range_found = possible_kvs.equal_range(key);
      
      for (auto iter = range_found.first; iter != range_found.second; ++iter)
      {
        EXPECT_EQ(key, iter->first)
          << "Iterated key and key in possible_kvs does not match: \n"
          << "Iterated key: " << key << "\n"
          << "Key in possible_kvs: " << iter->first;
        match = match || (value == iter->second);
      }
      EXPECT_TRUE(match) 
        << "No kv-pair in possible_kvs matching with iterated kv-pair:\n"
        << "Key: " << key << "\n"
        << "Value: " << value << "\n";

      possible_kvs.erase(key);
      n_removed_possible_kvs = n_total_possible_kvs - possible_kvs.size();
      if (report_progress && n_removed_possible_kvs > old_progress + 10000)
      {
        ShowProgress(std::cout, n_removed_possible_kvs, n_total_possible_kvs);
        old_progress = n_removed_possible_kvs;
      }
    }
    // Remaining kv-pairs in possible_kvs are deleted kv-pairs
    // Here we use a dirty trick to check for their deletion.
    // HGet set the return string to empty string when the kv-pair is deleted,
    // else it keeps the string unchanged.
    {
      for (auto iter = possible_kvs.begin(); iter != possible_kvs.end(); iter = possible_kvs.erase(iter))
      {
        std::string value_got{"Dummy"};
        status = engine->HGet(collection_name, iter->first, &value_got);
        EXPECT_EQ(status, kvdk::Status::NotFound)
          << "Should not have found a key of a entry that cannot be iterated.\n";
        EXPECT_EQ(value_got, "")
          << "HGet a DlistDeleteRecord should have set value_got as empty string\n"; 

        n_removed_possible_kvs = n_total_possible_kvs - possible_kvs.size();
        if (report_progress && n_removed_possible_kvs > old_progress + 10000)
        {
          ShowProgress(std::cout, n_removed_possible_kvs, n_total_possible_kvs);
          old_progress = n_removed_possible_kvs;
        }
      }
      EXPECT_TRUE(possible_kvs.empty())
        << "There should be no key left in possible_kvs, "
        << "as they all should have been erased.\n";
    }
  }

  // possible_kvs is searched to try to find a match with iterated records
  // possible_kvs is copied because IterateThroughHashes erase entries to keep track of records
  void IterateThroughSortedSets(uint32_t tid, std::string collection_name, 
                        std::unordered_multimap<std::string_view, std::string_view> possible_kvs, 
                        bool report_progress = false) 
  {
    kvdk::Status status;

    if (report_progress)
      std::cout << "IterateThroughSortedSets " << collection_name << " with thread " << tid << std::endl;
    
    int n_total_possible_kvs = possible_kvs.size();
    int n_removed_possible_kvs = 0;
    int old_progress = n_removed_possible_kvs;

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
      
      bool match = false;
      auto range_found = possible_kvs.equal_range(key);
      
      for (auto iter = range_found.first; iter != range_found.second; ++iter)
      {
        EXPECT_EQ(key, iter->first)
          << "Iterated key and key in possible_kvs does not match: \n"
          << "Iterated key: " << key << "\n"
          << "Key in possible_kvs: " << iter->first;
        match = match || (value == iter->second);
      }
      EXPECT_TRUE(match) 
        << "No kv-pair in possible_kvs matching with iterated kv-pair:\n"
        << "Key: " << key << "\n"
        << "Value: " << value << "\n";

      possible_kvs.erase(key);
      n_removed_possible_kvs = n_total_possible_kvs - possible_kvs.size();
      if (report_progress && n_removed_possible_kvs > old_progress + 10000)
      {
        ShowProgress(std::cout, n_removed_possible_kvs, n_total_possible_kvs);
        old_progress = n_removed_possible_kvs;
      }
    }
    // Remaining kv-pairs in possible_kvs are deleted kv-pairs
    // We just cannot keep track of them
    {
      for (auto iter = possible_kvs.begin(); iter != possible_kvs.end(); iter = possible_kvs.erase(iter))
      {
        std::string value_got{"Dummy"};
        status = engine->HGet(collection_name, iter->first, &value_got);
        EXPECT_EQ(status, kvdk::Status::NotFound)
          << "Should not have found a key of a entry that cannot be iterated.\n";

        n_removed_possible_kvs = n_total_possible_kvs - possible_kvs.size();
        if (report_progress && n_removed_possible_kvs > old_progress + 10000)
        {
          ShowProgress(std::cout, n_removed_possible_kvs, n_total_possible_kvs);
          old_progress = n_removed_possible_kvs;
        }
      }
      EXPECT_TRUE(possible_kvs.empty())
        << "There should be no key left in possible_kvs, "
        << "as they all should have been erased.\n";
    }
  }

  std::unordered_multimap<pmem::obj::string_view, pmem::obj::string_view> GetPossibleKVsForXSetOnly()
  {
    std::cout << "Preparing possible_kvs to check contents of engine from keys and values" << std::endl;
    std::unordered_multimap<std::string_view, std::string_view> possible_kvs;
    possible_kvs.reserve(n_thread * n_kv_per_thread * 2);
    for (size_t tid = 0; tid < n_thread; tid++)
    {
        for (size_t i = 0; i < n_kv_per_thread; i++)
            possible_kvs.emplace(keys[tid][i], values[tid][i]);
        ShowProgress(std::cout, tid + 1, n_thread);
    }
    return possible_kvs;
  }

  // Since we are shuffling all keys, the possible_kvs should have been dumped?
  void UpdatePossibleKVsForXSetOnly(std::unordered_multimap<std::string_view, std::string_view>& possible_kvs)
  {
    std::cout << "Updating possible_kvs to check contents of engine from keys and values" << std::endl;
    for (size_t tid = 0; tid < n_thread; tid++)
    {
        for (size_t i = 0; i < n_kv_per_thread; i++)
            possible_kvs.emplace(keys[tid][i], values[tid][i]);
        ShowProgress(std::cout, tid + 1, n_thread);
    }
  }

  std::unordered_multimap<pmem::obj::string_view, pmem::obj::string_view> GetPossibleKVsForEvenXSetOddXDelete()
  {
    std::cout << "Preparing possible_kvs to check contents of engine from keys and values" << std::endl;
    std::unordered_multimap<std::string_view, std::string_view> possible_kvs;
    possible_kvs.reserve(n_thread * n_kv_per_thread * 2);
    for (size_t tid = 0; tid < n_thread; tid++)
    {
        for (size_t i = 0; i < n_kv_per_thread; i++)
          if (i % 2 == 0)
            possible_kvs.emplace(keys[tid][i], values[tid][i]);
        ShowProgress(std::cout, tid + 1, n_thread);
    }
    return possible_kvs;
  }

  void UpdatePossibleKVsForEvenXSetOddXDelete(std::unordered_multimap<std::string_view, std::string_view>& possible_kvs)
  {
    std::cout << "Updating possible_kvs to check contents of engine from keys and values" << std::endl;
    for (size_t tid = 0; tid < n_thread; tid++)
    {
        for (size_t i = 0; i < n_kv_per_thread; i++)
          if (i % 2 == 0)
            possible_kvs.emplace(keys[tid][i], values[tid][i]);
        ShowProgress(std::cout, tid + 1, n_thread);
    }
  }

};

TEST_F(EngineExtensiveTest, HashCollectionHSetOnly) 
{
  std::string global_collection_name{"GlobalCollection"};

  auto DoHSet = [&](std::uint64_t tid)
  {
    if (tid == 0)
      HSetOnly(tid, global_collection_name, true);
    else
      HSetOnly(tid, global_collection_name, false);
  };

  LaunchNThreads(n_thread, DoHSet);

  std::unordered_multimap<std::string_view, std::string_view> possible_kvs{GetPossibleKVsForXSetOnly()};

  auto DoIterate = [&](std::uint64_t tid)
  {
    if (tid == 0)
      IterateThroughHashes(tid, global_collection_name, possible_kvs, true);
    else
      IterateThroughHashes(tid, global_collection_name, possible_kvs, false);
  };

  LaunchNThreads(1, DoIterate);  

  for (size_t i = 0; i < 3; i++)
  {
    // Repeatedly close and open engine to test recovery
    delete engine;
    status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";

    LaunchNThreads(1, DoIterate);  
  }
}

TEST_F(EngineExtensiveTest, HashCollectionHSetAndHDelete) 
{
  std::string global_collection_name{"GlobalCollection"};

  auto DoHSetHDelete = [&](std::uint64_t tid)
  {
    if (tid == 0)
      EvenHSetOddHDelete(tid, global_collection_name, true);
    else
      EvenHSetOddHDelete(tid, global_collection_name, false);
  };

  LaunchNThreads(n_thread, DoHSetHDelete);

  std::unordered_multimap<std::string_view, std::string_view> possible_kvs{GetPossibleKVsForEvenXSetOddXDelete()};

  auto DoIterate = [&](std::uint64_t tid)
  {
    if (tid == 0)
      IterateThroughHashes(tid, global_collection_name, possible_kvs, true);
    else
      IterateThroughHashes(tid, global_collection_name, possible_kvs, false);
  };

  LaunchNThreads(1, DoIterate);  

  for (size_t i = 0; i < 3; i++)
  {
    // Repeatedly close and open engine, excecute DoHSetHDelete and then check
    delete engine;

    status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";

    LaunchNThreads(1, DoIterate);

    ShuffleAllKeysValues();

    LaunchNThreads(n_thread, DoHSetHDelete);

    UpdatePossibleKVsForEvenXSetOddXDelete(possible_kvs);

    LaunchNThreads(1, DoIterate);
  }
}

TEST_F(EngineExtensiveTest, SortedCollectionSSetOnly) 
{
  std::string global_collection_name{"GlobalCollection"};

  auto DoSSet = [&](std::uint64_t tid)
  {
    if (tid == 0)
      SSetOnly(tid, global_collection_name, true);
    else
      SSetOnly(tid, global_collection_name, false);
  };

  LaunchNThreads(n_thread, DoSSet);

  std::unordered_multimap<std::string_view, std::string_view> possible_kvs{GetPossibleKVsForXSetOnly()};

  auto DoIterate = [&](std::uint64_t tid)
  {
    if (tid == 0)
      IterateThroughSortedSets(tid, global_collection_name, possible_kvs, true);
    else
      IterateThroughSortedSets(tid, global_collection_name, possible_kvs, false);
  };

  LaunchNThreads(1, DoIterate);  

  for (size_t i = 0; i < 3; i++)
  {
    // Repeatedly close and open engine to test recovery
    delete engine;

    status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";

    LaunchNThreads(1, DoIterate);  
  }
}

TEST_F(EngineExtensiveTest, SortedCollectionSSetAndSDelete) 
{
  std::string global_collection_name{"GlobalCollection"};

  auto DoSSetSDelete = [&](std::uint64_t tid)
  {
    if (tid == 0)
      EvenSSetOddSDelete(tid, global_collection_name, true);
    else
      EvenSSetOddSDelete(tid, global_collection_name, false);
  };

  LaunchNThreads(n_thread, DoSSetSDelete);

  std::unordered_multimap<std::string_view, std::string_view> possible_kvs{GetPossibleKVsForEvenXSetOddXDelete()};

  auto DoIterate = [&](std::uint64_t tid)
  {
    if (tid == 0)
      IterateThroughSortedSets(tid, global_collection_name, possible_kvs, true);
    else
      IterateThroughSortedSets(tid, global_collection_name, possible_kvs, false);
  };

  LaunchNThreads(1, DoIterate);  

  for (size_t i = 0; i < 3; i++)
  {
    // Repeatedly close and open engine, excecute DoSSetSDelete and then check
    delete engine;

    status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";

    LaunchNThreads(1, DoIterate);

    ShuffleAllKeysValues();
    
    LaunchNThreads(n_thread, DoSSetSDelete);

    UpdatePossibleKVsForEvenXSetOddXDelete(possible_kvs);

    LaunchNThreads(1, DoIterate);
  }
}

int main(int argc, char **argv) 
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
