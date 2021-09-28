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

class HashesTest : public testing::Test {
protected:
  kvdk::Engine *engine = nullptr;
  kvdk::Configs configs;

  const std::string path_db{ "/mnt/pmem0/kvdk_test_hashes" };

  // Default configure parameters
  const bool do_populate_when_initialize = false;
  const size_t sz_pmem_file{ 256ULL << 30 };
  const size_t n_hash_bucket{ 1ULL << 20 };     // Less buckets to increase hash collisions
  const size_t sz_hash_bucket{ (3 + 1) * 16 };  // Smaller buckets to increase hash collisions
  const size_t n_blocks_per_segment{ 1ULL << 20 };

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

    keys.resize(n_thread);
    values.resize(n_thread);
    for (size_t i = 0; i < n_kv_per_thread; i++)
    {
        _values_.push_back(GetRandomString(sz_value_min, sz_value_max));
        for (size_t tid = 0; tid < n_thread; tid++)
        {
            _keys_.push_back(GetRandomString(sz_key_min, sz_key_max));
        }
        if (i % 100000 == 0)
        {
          std::cout 
            << "[Info] "
            << "Generated "
            << i
            << " strings for values and "
            << i * n_thread
            << " strings for keys."
            << std::endl;
        }
    }
    for (size_t i = 0; i < n_kv_per_thread; i++)
    {
        for (size_t tid = 0; tid < n_thread; tid++)
        {
            keys[tid].emplace_back(_keys_[i*n_thread+tid]);
            values[tid].emplace_back(_values_[i]);
        }
        if (i % 100000 == 0)
        {
          std::cout 
            << "[Info] "
            << "Generated "
            << i
            << " string_views for values and "
            << i * n_thread
            << " string_views for keys."
            << std::endl;
        }
    }
    for (size_t tid = 0; tid < n_thread; tid++)
    {
        std::shuffle(keys[tid].begin(), keys[tid].end(), rand);
        std::shuffle(values[tid].begin(), values[tid].end(), rand);
        std::cout
          << "[Info] "
          << "Shuffled keys and values for thread "
          << tid
          << "."
          << std::endl;
    }
  }

  virtual void TearDown() 
  {
      PurgeDB();
  }
};

TEST_F(HashesTest, TestSetOnly) 
{
  std::mutex mu_rw;
  kvdk::Status status;
  std::unordered_multimap<std::string_view, std::string_view> possible_kvs;
  std::unordered_set<std::string_view> key_counter;
  for (size_t tid = 0; tid < n_thread; tid++)
  {
      for (size_t i = 0; i < n_kv_per_thread; i++)
      {
        possible_kvs.emplace(keys[tid][i], values[tid][i]);
        key_counter.insert(keys[tid][i]);
      }
      std::cout
        << "[Info] "
        << "Preparing unordered_multimap and unordered_set for thread "
        << tid
        << " done."
        << std::endl;
  }

  status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
  ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";

  std::string global_collection_name{"GlobalCollection"};

  auto HSetHGetHDelete = [&](uint32_t tid) 
  {
    std::string value_got;
    for (size_t j = 0; j < n_kv_per_thread; j++)
    {
      status = engine->HSet(global_collection_name, keys[tid][j], values[tid][j]);
      EXPECT_EQ(status, kvdk::Status::Ok) 
        << "Fail to set a key " 
        << keys[tid][j] 
        << " in collection "
        << global_collection_name;

      if (tid == 0 && j % 100000 == 0)
      {
        std::unique_lock<std::mutex> lock_write{mu_rw};
        std::cout 
          << "[Info] "
          << "Thread "
          << tid
          << " have HSet "
          << j
          << " records."
          << std::endl;
      }
    }
  };
  
  auto IteratingThrough = [&](uint32_t tid) 
  {
    int n_entry = 0;

    auto u_iter = engine->NewUnorderedIterator(global_collection_name);
    ASSERT_TRUE(u_iter != nullptr) << "Fail to create UnorderedIterator";
    for (u_iter->SeekToFirst(); u_iter->Valid(); u_iter->Next())
    {
      ++n_entry;
      auto key = u_iter->Key();
      auto value = u_iter->Value();
      bool match = false;
      auto range_found = possible_kvs.equal_range(key);
      for (auto iter = range_found.first; iter != range_found.second; ++iter)
      {
        EXPECT_EQ(key, iter->first)
          << "Iterated key and key in unordered_multimap does not match: \n"
          << "Iterated key: " << key << "\n"
          << "Key in unordered_multimap: " << iter->first;
        match = match || (value == iter->second);
      }
      EXPECT_TRUE(match) 
        << "No kv-pair in unordered_multimap matching with iterated kv-pair:\n"
        << "Key: " << key << "\n"
        << "Value: " << value << "\n";
      if (tid == 0 && n_entry % 1000000 == 0)
      {
        std::unique_lock<std::mutex> lock_write{mu_rw};
        std::cout 
          << "[Info] "
          << "Thread "
          << tid
          << " have iterated "
          << n_entry
          << " records."
          << std::endl;
      }
    }
    ASSERT_EQ(key_counter.size(), n_entry) << "Total entries in collection is incorrect!";
  };

  LaunchNThreads(n_thread, HSetHGetHDelete);
  LaunchNThreads(1, IteratingThrough);  

  delete engine;

  status = kvdk::Engine::Open(path_db.data(), &engine, configs, stderr);
  ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";

  LaunchNThreads(1, IteratingThrough);
}

int main(int argc, char **argv) 
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
