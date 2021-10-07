/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <future>
#include <string>
#include <thread>
#include <vector>

#include "../engine/kv_engine.hpp"
#include "../engine/pmem_allocator/pmem_allocator.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include "test_util.h"
#include "gtest/gtest.h"

using namespace KVDK_NAMESPACE;
static const uint64_t str_pool_length = 1024000;

class EngineBasicTest : public testing::Test {
protected:
  Engine *engine = nullptr;
  Configs configs;
  std::string db_path;
  std::string str_pool;

  virtual void SetUp() override {
    str_pool.resize(str_pool_length);
    random_str(&str_pool[0], str_pool_length);
    configs.pmem_file_size = (16ULL << 30);
    configs.populate_pmem_space = false;
    configs.hash_bucket_num = (1 << 10);
    configs.hash_bucket_size = 64;
    configs.pmem_segment_blocks = 8 * 1024;
    // For faster test, no interval so it would not block engine closing
    configs.background_work_interval = 0.1;
    db_path = "/mnt/pmem0/data";
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", db_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }

  virtual void TearDown() { Destroy(); }

  void AssignData(std::string &data, int len) {
    data.assign(str_pool.data() + (rand() % (str_pool_length - len)), len);
  }

  void Destroy() {
    // delete db_path
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", db_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }
};

TEST_F(EngineBasicTest, TestThreadManager) {
  int max_write_threads = 1;
  configs.max_write_threads = max_write_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string key("k");
  std::string val("value");

  ASSERT_EQ(engine->Set(key, val), Status::Ok);

  // Reach max write threads
  auto s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::TooManyWriteThreads);
  // Manually release write thread
  engine->ReleaseWriteThread();
  s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::Ok);
  // Release write thread on thread exits
  s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::Ok);
  delete engine;
}

TEST_F(EngineBasicTest, TestBasicStringOperations) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  // Test empty key
  std::string key{""}, val{"val"}, got_val;
  ASSERT_EQ(engine->Set(key, val), Status::Ok);
  ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
  ASSERT_EQ(val, got_val);
  ASSERT_EQ(engine->Delete(key), Status::Ok);
  ASSERT_EQ(engine->Get(key, &got_val), Status::NotFound);
  engine->ReleaseWriteThread();

  auto SetGetDelete = [&](uint32_t id) {
    std::string val1, val2, got_val1, got_val2;
    int cnt = 100;
    while (cnt--) {
      std::string key1(std::string(id + 1, 'a') + std::to_string(cnt));
      std::string key2(std::string(id + 1, 'b') + std::to_string(cnt));
      AssignData(val1, fast_random_64() % 1024);
      AssignData(val2, fast_random_64() % 1024);

      ASSERT_EQ(engine->Set(key1, val1), Status::Ok);
      ASSERT_EQ(engine->Set(key2, val2), Status::Ok);

      // Get
      ASSERT_EQ(engine->Get(key1, &got_val1), Status::Ok);
      ASSERT_EQ(val1, got_val1);
      ASSERT_EQ(engine->Get(key2, &got_val2), Status::Ok);
      ASSERT_EQ(val2, got_val2);

      // Delete
      ASSERT_EQ(engine->Delete(key1), Status::Ok);
      ASSERT_EQ(engine->Get(key1, &got_val1), Status::NotFound);

      // Update
      AssignData(val1, fast_random_64() % 1024);
      ASSERT_EQ(engine->Set(key1, val1), Status::Ok);
      ASSERT_EQ(engine->Get(key1, &got_val1), Status::Ok);
      ASSERT_EQ(got_val1, val1);
    }
  };

  LaunchNThreads(num_threads, SetGetDelete);
  delete engine;
}

TEST_F(EngineBasicTest, TestBatchWrite) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  int batch_num = 10;
  int count = 500;
  auto BatchSetDelete = [&](uint32_t id) {
    std::string key_prefix(std::string(id, 'a'));
    std::string got_val;
    WriteBatch batch;
    int cnt = count;
    while (cnt--) {
      for (size_t i = 0; i < batch_num; i++) {
        auto key = key_prefix + std::to_string(i) + std::to_string(cnt);
        auto val = std::to_string(i * id);
        batch.Put(key, val);
      }
      ASSERT_EQ(engine->BatchWrite(batch), Status::Ok);
      batch.Clear();
      for (size_t i = 0; i < batch_num; i++) {
        if ((i * cnt) % 2 == 1) {
          auto key = key_prefix + std::to_string(i) + std::to_string(cnt);
          auto val = std::to_string(i * id);
          ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, val);
          batch.Delete(key);
        }
      }
      engine->BatchWrite(batch);
      batch.Clear();
    }
  };

  LaunchNThreads(num_threads, BatchSetDelete);

  delete engine;

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  for (uint32_t id = 0; id < num_threads; id++) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    int cnt = count;
    while (cnt--) {
      for (size_t i = 0; i < batch_num; i++) {
        auto key = key_prefix + std::to_string(i) + std::to_string(cnt);
        if ((i * cnt) % 2 == 1) {
          ASSERT_EQ(engine->Get(key, &got_val), Status::NotFound);
        } else {
          auto val = std::to_string(i * id);
          ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, val);
        }
      }
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, DISABLED_TestFreeList) {
  // TODO: Add more cases
  configs.pmem_segment_blocks = 4 * kMinPaddingBlockSize;
  configs.max_write_threads = 1;
  configs.pmem_block_size = 64;
  configs.pmem_file_size =
      configs.pmem_segment_blocks * configs.pmem_block_size;
  configs.background_work_interval = 0.5;

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::string key1("a1");
  std::string key2("a2");
  std::string key3("a3");
  std::string key4("a4");
  std::string small_value(64 * (kMinPaddingBlockSize - 1) + 1, 'a');
  std::string large_value(64 * (kMinPaddingBlockSize * 2 - 1) + 1, 'a');
  // We have 4 kMinimalPaddingBlockSize size chunk of blocks, this will take
  // up 2 of them

  ASSERT_EQ(engine->Set(key1, large_value), Status::Ok);

  // update large value, new value will be stored in 3th chunk
  ASSERT_EQ(engine->Set(key1, small_value), Status::Ok);

  // key2 will be stored in 4th chunk
  ASSERT_EQ(engine->Set(key2, small_value), Status::Ok);

  // new key 1 and new key 2 will be stored in updated 1st and 2nd chunks
  ASSERT_EQ(engine->Set(key1, small_value), Status::Ok);

  ASSERT_EQ(engine->Set(key2, small_value), Status::Ok);

  // No more space to store large_value
  ASSERT_EQ(engine->Set(key3, large_value), Status::PmemOverflow);

  // Wait bg thread finish merging space of 3th and 4th chunks
  sleep(2);

  // large key3 will be stored in merged 3th and 4th chunks
  ASSERT_EQ(engine->Set(key3, large_value), Status::Ok);

  // No more space
  ASSERT_EQ(engine->Set(key4, small_value), Status::PmemOverflow);

  delete engine;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // Still no more space after re-open
  ASSERT_EQ(engine->Set(key4, small_value), Status::PmemOverflow);
}

TEST_F(EngineBasicTest, TestLocalSortedCollection) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::vector<int> n_local_entries(num_threads, 0);

  auto SSetSGetSDelete = [&](uint32_t id) {
    std::string thread_local_skiplist("t_skiplist" + std::to_string(id));
    std::string key1, key2, val1, val2;
    std::string got_val1, got_val2;

    AssignData(val1, 10);

    // Test Empty Key
    {
      std::string k0{""};
      ASSERT_EQ(engine->SSet(thread_local_skiplist, k0, val1), Status::Ok);
      ++n_local_entries[id];
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k0, &got_val1), Status::Ok);
      ASSERT_EQ(val1, got_val1);
      ASSERT_EQ(engine->SDelete(thread_local_skiplist, k0), Status::Ok);
      --n_local_entries[id];
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k0, &got_val1),
                Status::NotFound);
    }

    key1 = std::to_string(id);
    key2 = std::to_string(id);

    int cnt = 100;
    while (cnt--) {
      key1.append("k1");
      key2.append("k2");

      // insert
      AssignData(val1, fast_random_64() % 1024);
      AssignData(val2, fast_random_64() % 1024);
      ASSERT_EQ(engine->SSet(thread_local_skiplist, key1, val1), Status::Ok);
      ++n_local_entries[id];
      ASSERT_EQ(engine->SSet(thread_local_skiplist, key2, val2), Status::Ok);
      ++n_local_entries[id];
      ASSERT_EQ(engine->SGet(thread_local_skiplist, key1, &got_val1),
                Status::Ok);
      ASSERT_EQ(engine->SGet(thread_local_skiplist, key2, &got_val2),
                Status::Ok);
      ASSERT_EQ(val1, got_val1);
      ASSERT_EQ(val2, got_val2);

      // update
      AssignData(val1, fast_random_64() % 1024);
      ASSERT_EQ(engine->SSet(thread_local_skiplist, key1, val1), Status::Ok);
      ASSERT_EQ(engine->SGet(thread_local_skiplist, key1, &got_val1),
                Status::Ok);
      ASSERT_EQ(got_val1, val1);
      AssignData(val2, fast_random_64() % 1024);
      ASSERT_EQ(engine->SSet(thread_local_skiplist, key2, val2), Status::Ok);
      ASSERT_EQ(engine->SGet(thread_local_skiplist, key2, &got_val2),
                Status::Ok);
      ASSERT_EQ(got_val2, val2);

      // delete
      ASSERT_EQ(engine->SDelete(thread_local_skiplist, key1), Status::Ok);
      --n_local_entries[id];
      ASSERT_EQ(engine->SGet(thread_local_skiplist, key1, &got_val1),
                Status::NotFound);
    }
  };

  auto IteratingThrough = [&](uint32_t id) {
    std::string thread_local_skiplist("t_skiplist" + std::to_string(id));
    std::vector<int> n_entries(num_threads, 0);

    auto t_iter = engine->NewSortedIterator(thread_local_skiplist);
    ASSERT_TRUE(t_iter != nullptr);
    t_iter->SeekToFirst();
    if (t_iter->Valid()) {
      ++n_entries[id];
      std::string prev = t_iter->Key();
      t_iter->Next();
      while (t_iter->Valid()) {
        ++n_entries[id];
        std::string k = t_iter->Key();
        t_iter->Next();
        ASSERT_EQ(true, k.compare(prev) > 0);
        prev = k;
      }
    }
    ASSERT_EQ(n_local_entries[id], n_entries[id]);
    n_entries[id] = 0;
  };

  LaunchNThreads(num_threads, SSetSGetSDelete);
  LaunchNThreads(num_threads, IteratingThrough);

  delete engine;
}

TEST_F(EngineBasicTest, TestGlobalSortedCollection) {
  const std::string global_skiplist = "skiplist";
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::atomic<int> n_global_entries{0};

  // Test empty key
  std::string key{""}, val{"val"}, got_val;
  ASSERT_EQ(engine->SSet(global_skiplist, key, val), Status::Ok);
  ++n_global_entries;
  ASSERT_EQ(engine->SGet(global_skiplist, key, &got_val), Status::Ok);
  ASSERT_EQ(val, got_val);
  ASSERT_EQ(engine->SDelete(global_skiplist, key), Status::Ok);
  --n_global_entries;
  ASSERT_EQ(engine->SGet(global_skiplist, key, &got_val), Status::NotFound);
  engine->ReleaseWriteThread();

  auto SSetSGetSDelete = [&](uint32_t id) {
    std::string k1, k2, v1, v2;
    std::string got_v1, got_v2;

    AssignData(v1, 10);

    k1 = std::to_string(id);
    k2 = std::to_string(id);

    int cnt = 100;
    while (cnt--) {
      int v1_len = rand() % 1024;
      int v2_len = rand() % 1024;
      k1.append("k1");
      k2.append("k2");

      // insert
      AssignData(v1, v1_len);
      AssignData(v2, v2_len);
      ASSERT_EQ(engine->SSet(global_skiplist, k1, v1), Status::Ok);
      ++n_global_entries;
      ASSERT_EQ(engine->SSet(global_skiplist, k2, v2), Status::Ok);
      ++n_global_entries;
      ASSERT_EQ(engine->SGet(global_skiplist, k1, &got_v1), Status::Ok);
      ASSERT_EQ(engine->SGet(global_skiplist, k2, &got_v2), Status::Ok);
      ASSERT_EQ(v1, got_v1);
      ASSERT_EQ(v2, got_v2);

      // update
      AssignData(v1, v1_len);
      ASSERT_EQ(engine->SSet(global_skiplist, k1, v1), Status::Ok);
      ASSERT_EQ(engine->SGet(global_skiplist, k1, &got_v1), Status::Ok);
      ASSERT_EQ(got_v1, v1);
      AssignData(v2, v2_len);
      ASSERT_EQ(engine->SSet(global_skiplist, k2, v2), Status::Ok);
      ASSERT_EQ(engine->SGet(global_skiplist, k2, &got_v2), Status::Ok);
      ASSERT_EQ(got_v2, v2);

      // delete
      ASSERT_EQ(engine->SDelete(global_skiplist, k1), Status::Ok);
      --n_global_entries;
      ASSERT_EQ(engine->SGet(global_skiplist, k1, &got_v1), Status::NotFound);
    }
  };

  auto IteratingThrough = [&](uint32_t id) {
    std::vector<int> n_entries(num_threads, 0);

    auto iter = engine->NewSortedIterator(global_skiplist);
    ASSERT_TRUE(iter != nullptr);
    iter->SeekToFirst();
    if (iter->Valid()) {
      ++n_entries[id];
      std::string prev = iter->Key();
      iter->Next();
      while (iter->Valid()) {
        ++n_entries[id];
        std::string k = iter->Key();
        iter->Next();
        ASSERT_EQ(true, k.compare(prev) > 0);
        prev = k;
      }
    }
    ASSERT_EQ(n_global_entries, n_entries[id]);
    n_entries[id] = 0;
  };

  auto SeekToDeleted = [&](uint32_t id) {
    auto t_iter2 = engine->NewSortedIterator(global_skiplist);
    ASSERT_TRUE(t_iter2 != nullptr);
    // First deleted key
    t_iter2->Seek(std::to_string(id) + "k1");
    ASSERT_TRUE(t_iter2->Valid());
    // First valid key
    t_iter2->Seek(std::to_string(id) + "k2");
    ASSERT_TRUE(t_iter2->Valid());
    ASSERT_EQ(t_iter2->Key(), std::to_string(id) + "k2");
  };
  LaunchNThreads(num_threads, SSetSGetSDelete);
  LaunchNThreads(num_threads, IteratingThrough);
  LaunchNThreads(num_threads, SeekToDeleted);

  delete engine;
}

TEST_F(EngineBasicTest, TestSeek) {
  std::string val;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  // Test Seek
  std::string collection = "col1";
  uint64_t z = 0;
  auto zero_filled_str = uint64_to_string(z);
  ASSERT_EQ(engine->SSet(collection, zero_filled_str, zero_filled_str),
            Status::Ok);
  ASSERT_EQ(engine->SGet(collection, zero_filled_str, &val), Status::Ok);
  auto iter = engine->NewSortedIterator(collection);
  ASSERT_NE(iter, nullptr);
  iter->Seek(zero_filled_str);
  ASSERT_TRUE(iter->Valid());

  // Test SeekToFirst
  collection.assign("col2");
  ASSERT_EQ(engine->SSet(collection, "foo", "bar"), Status::Ok);
  ASSERT_EQ(engine->SGet(collection, "foo", &val), Status::Ok);
  ASSERT_EQ(engine->SDelete(collection, "foo"), Status::Ok);
  ASSERT_EQ(engine->SGet(collection, "foo", &val), Status::NotFound);
  ASSERT_EQ(engine->SSet(collection, "foo2", "bar2"), Status::Ok);
  iter = engine->NewSortedIterator(collection);
  ASSERT_NE(iter, nullptr);
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->Value(), "bar2");
}

TEST_F(EngineBasicTest, TestStringRestore) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  int count = 1000;
  auto SetupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (int i = 1; i <= count; i++) {
      std::string key(key_prefix + std::to_string(i));
      std::string val(std::to_string(i));
      std::string update_val(std::to_string(i * 2));
      ASSERT_EQ(engine->Set(key, val), Status::Ok);
      if ((i * id) % 2 == 1) {
        ASSERT_EQ(engine->Delete(key), Status::Ok);
        if ((i * id) % 3 == 0) {
          // Update after delete
          ASSERT_EQ(engine->Set(key, update_val), Status::Ok);
          ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, update_val);
        } else {
          ASSERT_EQ(engine->Get(key, &got_val), Status::NotFound);
        }
      } else {
        ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
        ASSERT_EQ(got_val, val);
      }
    }
  };

  LaunchNThreads(num_threads, SetupEngine);

  delete engine;

  // reopen and restore engine and try gets
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  for (uint32_t id = 0; id < num_threads; id++) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (int i = 1; i <= count; i++) {
      std::string key(key_prefix + std::to_string(i));
      std::string val(std::to_string(i));
      std::string updated_val(std::to_string(i * 2));
      Status s = engine->Get(key, &got_val);
      if ((i * id) % 3 == 0 && (id * i) % 2 == 1) { // deleted then updated ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(got_val, updated_val);
      } else if ((i * id) % 2 == 0) { // not deleted ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(got_val, val);
      } else { // deleted ones
        ASSERT_EQ(s, Status::NotFound);
      }
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestSortedRestore) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  int count = 100;
  std::string overall_skiplist = "skiplist";
  std::string thread_skiplist = "t_skiplist";
  auto SetupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    std::string t_skiplist(thread_skiplist + std::to_string(id));
    for (int i = 1; i <= count; i++) {
      auto key = key_prefix + std::to_string(i);
      auto overall_val = std::to_string(i);
      auto t_val = std::to_string(i * 2);
      ASSERT_EQ(engine->SSet(overall_skiplist, key, overall_val), Status::Ok);
      ASSERT_EQ(engine->SSet(t_skiplist, key, t_val), Status::Ok);
      ASSERT_EQ(engine->SGet(overall_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, overall_val);
      ASSERT_EQ(engine->SGet(t_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, t_val);
      if (i % 2 == 1) {
        ASSERT_EQ(engine->SDelete(overall_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SDelete(t_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SGet(overall_skiplist, key, &got_val),
                  Status::NotFound);
        ASSERT_EQ(engine->SGet(t_skiplist, key, &got_val), Status::NotFound);
      }
    }
  };

  LaunchNThreads(num_threads, SetupEngine);

  delete engine;
  std::vector<int> opt_restore_skiplists{0, 1};
  for (auto is_opt : opt_restore_skiplists) {
    configs.max_write_threads = num_threads;
    configs.opt_large_sorted_collection_restore = is_opt;
    // reopen and restore engine and try gets
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);
    for (uint32_t id = 0; id < num_threads; id++) {
      std::string t_skiplist(thread_skiplist + std::to_string(id));
      std::string key_prefix(id, 'a');
      std::string got_val;
      for (int i = 1; i <= count; i++) {
        std::string key(key_prefix + std::to_string(i));
        std::string overall_val(std::to_string(i));
        std::string t_val(std::to_string(i * 2));
        Status s = engine->SGet(overall_skiplist, key, &got_val);
        if (i % 2 == 1) {
          ASSERT_EQ(s, Status::NotFound);
        } else {
          ASSERT_EQ(s, Status::Ok);
          ASSERT_EQ(got_val, overall_val);
        }
        s = engine->SGet(t_skiplist, key, &got_val);
        if (i % 2 == 1) {
          ASSERT_EQ(s, Status::NotFound);
        } else {
          ASSERT_EQ(s, Status::Ok);
          ASSERT_EQ(got_val, t_val);
        }
      }

      auto iter = engine->NewSortedIterator(t_skiplist);
      ASSERT_TRUE(iter != nullptr);
      iter->SeekToFirst();
      std::string prev = "";
      int cnt = 0;
      while (iter->Valid()) {
        cnt++;
        std::string k = iter->Key();
        iter->Next();
        ASSERT_TRUE(k.compare(prev) > 0);
        prev = k;
      }
      ASSERT_EQ(cnt, count / 2);
    }

    auto iter = engine->NewSortedIterator(overall_skiplist);
    ASSERT_TRUE(iter != nullptr);
    iter->SeekToFirst();
    std::string prev = "";
    int cnt = 0;
    while (iter->Valid()) {
      cnt++;
      std::string k = iter->Key();
      iter->Next();
      ASSERT_TRUE(k.compare(prev) > 0);
      prev = k;
    }
    ASSERT_EQ(cnt, (count / 2) * num_threads);

    delete engine;
  }
}

TEST_F(EngineBasicTest, TestMultiThreadSortedRestore) {
  int num_threads = 48;
  int num_collections = 16;
  configs.max_write_threads = num_threads;
  configs.opt_large_sorted_collection_restore = true;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  uint64_t count = 1024;

  auto SetupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (uint64_t i = 1; i <= count; i++) {
      auto key = key_prefix + std::to_string(i);

      std::string average_skiplist("a_skiplist" +
                                   std::to_string(i % num_collections));
      auto average_val = std::to_string(i);

      std::string r_skiplist("r_skiplist" +
                             std::to_string(rand() % num_threads));
      auto r_val = std::to_string(i * 2);
      ASSERT_EQ(engine->SSet(average_skiplist, key, average_val), Status::Ok);
      ASSERT_EQ(engine->SSet(r_skiplist, key, r_val), Status::Ok);
      ASSERT_EQ(engine->SGet(average_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, average_val);
      ASSERT_EQ(engine->SGet(r_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, r_val);
      if ((rand() % i) == 0) {
        ASSERT_EQ(engine->SDelete(average_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SDelete(r_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SGet(average_skiplist, key, &got_val),
                  Status::NotFound);
        ASSERT_EQ(engine->SGet(r_skiplist, key, &got_val), Status::NotFound);
      }
    }
  };

  LaunchNThreads(num_threads, SetupEngine);

  delete engine;
  // reopen and restore engine and try gets
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  auto skiplists = (dynamic_cast<KVEngine *>(engine))->GetSkiplists();
  for (int h = 1; h <= 32; ++h) {
    for (auto s : skiplists) {
      ASSERT_EQ(s->CheckConnection(h), Status::Ok);
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestLocalUnorderedCollection) {
  int num_threads = 16;
  int count = 100;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys

  std::vector<std::vector<std::string>> local_keys(num_threads);
  std::vector<std::vector<std::string>> local_values(num_threads);
  std::vector<std::string> local_collection_names(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    local_collection_names[i] = "local_uncoll_t" + std::to_string(i);
    for (size_t j = 0; j < count * 2; j++) {
      local_keys[i].push_back(std::string{"local_key_"} + std::to_string(j));
      local_values[i].push_back(GetRandomString(1024));
    }
  }

  auto HSetHGetHDelete = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      // insert
      ASSERT_EQ(engine->HSet(local_collection_names[tid], local_keys[tid][j],
                             local_values[tid][j]),
                Status::Ok);
      ASSERT_EQ(engine->HGet(local_collection_names[tid], local_keys[tid][j],
                             &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);

      // insert another
      ASSERT_EQ(engine->HSet(local_collection_names[tid],
                             local_keys[tid][j + count],
                             local_values[tid][j + count]),
                Status::Ok);
      ASSERT_EQ(engine->HGet(local_collection_names[tid],
                             local_keys[tid][j + count], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j + count], value_got);

      // update
      ASSERT_EQ(engine->HSet(local_collection_names[tid], local_keys[tid][j],
                             local_values[tid][j] + "_new"),
                Status::Ok);
      ASSERT_EQ(engine->HGet(local_collection_names[tid], local_keys[tid][j],
                             &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j] + "_new", value_got);

      // delete the other
      ASSERT_EQ(engine->HDelete(local_collection_names[tid],
                                local_keys[tid][j + count]),
                Status::Ok);
      ASSERT_EQ(engine->HGet(local_collection_names[tid],
                             local_keys[tid][j + count], &value_got),
                Status::NotFound);
    }
  };

  auto IteratingThrough = [&](uint32_t tid) {
    int n_entry = 0;

    auto t_iter = engine->NewUnorderedIterator(local_collection_names[tid]);
    ASSERT_TRUE(t_iter != nullptr);
    for (t_iter->SeekToFirst(); t_iter->Valid(); t_iter->Next()) {
      ++n_entry;
    }
    ASSERT_EQ(count, n_entry);
  };

  LaunchNThreads(num_threads, HSetHGetHDelete);
  LaunchNThreads(num_threads, IteratingThrough);

  delete engine;
}

TEST_F(EngineBasicTest, TestGlobalUnorderedCollection) {
  int num_threads = 16;
  int count = 100;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::vector<std::vector<std::string>> global_keys(num_threads);
  std::vector<std::vector<std::string>> global_values(num_threads);
  std::string global_collection_name{"global_uncoll"};
  for (size_t i = 0; i < num_threads; i++) {
    for (size_t j = 0; j < count * 2; j++) {
      global_keys[i].push_back(std::string{"global_key_t"} + std::to_string(i) +
                               std::string{"_k_"} + std::to_string(j));
      global_values[i].push_back(GetRandomString(1024));
    }
  }

  auto HSetHGetHDelete = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      // insert
      ASSERT_EQ(engine->HSet(global_collection_name, global_keys[tid][j],
                             global_values[tid][j]),
                Status::Ok);
      ASSERT_EQ(
          engine->HGet(global_collection_name, global_keys[tid][j], &value_got),
          Status::Ok);
      ASSERT_EQ(global_values[tid][j], value_got);

      // insert another
      ASSERT_EQ(engine->HSet(global_collection_name,
                             global_keys[tid][j + count],
                             global_values[tid][j + count]),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_keys[tid][j + count], &value_got),
                Status::Ok);
      ASSERT_EQ(global_values[tid][j + count], value_got);

      // update
      ASSERT_EQ(engine->HSet(global_collection_name, global_keys[tid][j],
                             global_values[tid][j] + "_new"),
                Status::Ok);
      ASSERT_EQ(
          engine->HGet(global_collection_name, global_keys[tid][j], &value_got),
          Status::Ok);
      ASSERT_EQ(global_values[tid][j] + "_new", value_got);

      // delete the other
      ASSERT_EQ(
          engine->HDelete(global_collection_name, global_keys[tid][j + count]),
          Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_keys[tid][j + count], &value_got),
                Status::NotFound);
    }
  };

  auto IteratingThrough = [&](uint32_t tid) {
    int n_entry = 0;

    auto t_iter = engine->NewUnorderedIterator(global_collection_name);
    ASSERT_TRUE(t_iter != nullptr);
    for (t_iter->SeekToFirst(); t_iter->Valid(); t_iter->Next()) {
      ++n_entry;
    }
    ASSERT_EQ(count * num_threads, n_entry);
  };

  LaunchNThreads(num_threads, HSetHGetHDelete);
  LaunchNThreads(num_threads, IteratingThrough);

  delete engine;
}

TEST_F(EngineBasicTest, TestUnorderedCollectionRestore) {
  int count = 100;
  int num_threads = 16;

  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  // kv-pairs for insertion.
  // Remaining kvs in kv engine are also stored in global/tlocal_kvs_remaining
  std::vector<std::vector<std::pair<std::string, std::string>>>
      global_kvs_inserting(num_threads);
  std::vector<std::vector<std::pair<std::string, std::string>>>
      tlocal_kvs_inserting(num_threads);
  std::mutex lock_global_kvs_remaining;
  std::map<std::string, std::string> global_kvs_remaining;
  std::vector<std::map<std::string, std::string>> tlocal_kvs_remaining(
      num_threads);

  std::string global_collection_name = "global_uncoll";
  std::vector<std::string> tlocal_collection_names(num_threads);
  std::string updated_value_suffix = "_new";
  for (size_t tid = 0; tid < num_threads; tid++) {
    tlocal_collection_names[tid] = "local_uncoll_t" + std::to_string(tid);
    for (size_t j = 0; j < count * 2; j++) {
      std::string global_key = std::string{"global_key_t"} +
                               std::to_string(tid) + "_key_" +
                               std::to_string(j);
      global_kvs_inserting[tid].emplace_back(global_key, GetRandomString(1024));

      std::string thread_local_key =
          std::string{"local_key_"} + std::to_string(j);
      tlocal_kvs_inserting[tid].emplace_back(thread_local_key,
                                             GetRandomString(1024));
    }
  }

  auto HSetHGetHDeleteGlobal = [&](uint32_t tid) {
    std::string value_got;
    for (int j = 0; j < count; j++) {
      // Insert first kv-pair in global collection
      ASSERT_EQ(engine->HSet(global_collection_name,
                             global_kvs_inserting[tid][j].first,
                             global_kvs_inserting[tid][j].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j].second);

      // Update first kv-pair in global collection
      global_kvs_inserting[tid][j].second += updated_value_suffix;
      ASSERT_EQ(engine->HSet(global_collection_name,
                             global_kvs_inserting[tid][j].first,
                             global_kvs_inserting[tid][j].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j].second);
      {
        std::lock_guard<std::mutex> lg{lock_global_kvs_remaining};
        global_kvs_remaining.emplace(global_kvs_inserting[tid][j].first,
                                     global_kvs_inserting[tid][j].second);
      }

      // Insert second kv-pair in global collection
      ASSERT_EQ(engine->HSet(global_collection_name,
                             global_kvs_inserting[tid][j + count].first,
                             global_kvs_inserting[tid][j + count].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j + count].second);

      // Delete second kv-pair in global collection
      ASSERT_EQ(engine->HDelete(global_collection_name,
                                global_kvs_inserting[tid][j + count].first),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::NotFound);
    }
  };

  auto HSetHGetHDeleteThreadLocal = [&](uint32_t tid) {
    std::string value_got;
    for (int j = 0; j < count; j++) {
      // Insert first kv-pair in global collection
      ASSERT_EQ(engine->HSet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first,
                             tlocal_kvs_inserting[tid][j].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j].second);

      // Update first kv-pair in global collection
      tlocal_kvs_inserting[tid][j].second += updated_value_suffix;
      ASSERT_EQ(engine->HSet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first,
                             tlocal_kvs_inserting[tid][j].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j].second);
      tlocal_kvs_remaining[tid].emplace(tlocal_kvs_inserting[tid][j].first,
                                        tlocal_kvs_inserting[tid][j].second);

      // Insert second kv-pair in global collection
      ASSERT_EQ(engine->HSet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j + count].first,
                             tlocal_kvs_inserting[tid][j + count].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j + count].second);

      // Delete second kv-pair in global collection
      ASSERT_EQ(engine->HDelete(tlocal_collection_names[tid],
                                tlocal_kvs_inserting[tid][j + count].first),
                Status::Ok);
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::NotFound);
    }
  };

  // Setup engine
  LaunchNThreads(num_threads, HSetHGetHDeleteGlobal);
  LaunchNThreads(num_threads, HSetHGetHDeleteThreadLocal);

  auto IteratingThroughGlobal = [&](uint32_t tid) {
    int n_entry = 0;
    auto global_kvs_remaining_copy{global_kvs_remaining};

    auto iter_global_collection =
        engine->NewUnorderedIterator(global_collection_name);
    ASSERT_TRUE(iter_global_collection != nullptr);
    for (iter_global_collection->SeekToFirst(); iter_global_collection->Valid();
         iter_global_collection->Next()) {
      ++n_entry;
      auto key = iter_global_collection->Key();
      auto value = iter_global_collection->Value();
      auto iter_found = global_kvs_remaining_copy.find(key);
      ASSERT_NE(iter_found, global_kvs_remaining_copy.end());
      ASSERT_EQ(value, iter_found->second);
      global_kvs_remaining_copy.erase(key);
    }
    ASSERT_EQ(n_entry, num_threads * count);
    ASSERT_TRUE(global_kvs_remaining_copy.empty());
  };

  auto IteratingThroughThreadLocal = [&](uint32_t tid) {
    int n_entry = 0;
    auto tlocal_kvs_remaining_copy{tlocal_kvs_remaining[tid]};

    auto iter_tlocal_collection =
        engine->NewUnorderedIterator(tlocal_collection_names[tid]);
    ASSERT_TRUE(iter_tlocal_collection != nullptr);
    for (iter_tlocal_collection->SeekToFirst(); iter_tlocal_collection->Valid();
         iter_tlocal_collection->Next()) {
      ++n_entry;
      auto key = iter_tlocal_collection->Key();
      auto value = iter_tlocal_collection->Value();
      auto iter_found = tlocal_kvs_remaining_copy.find(key);
      ASSERT_NE(iter_found, tlocal_kvs_remaining_copy.end());
      ASSERT_EQ(value, iter_found->second);
      tlocal_kvs_remaining_copy.erase(key);
    }
    ASSERT_EQ(n_entry, count);
    ASSERT_TRUE(tlocal_kvs_remaining_copy.empty());
  };

  auto HGetGlobal = [&](uint32_t tid) {
    std::string value_got;
    for (int j = 0; j < count; j++) {
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j].second);

      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::NotFound);
    }
  };

  auto HGetThreadLocal = [&](uint32_t tid) {
    std::string value_got;
    for (int j = 0; j < count; j++) {
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j].second);

      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::NotFound);
    }
  };

  LaunchNThreads(num_threads, IteratingThroughGlobal);
  LaunchNThreads(num_threads, IteratingThroughThreadLocal);
  LaunchNThreads(num_threads, HGetGlobal);
  LaunchNThreads(num_threads, HGetThreadLocal);

  delete engine;

  // reopen and restore engine
  configs.max_write_threads = 1;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  LaunchNThreads(num_threads, IteratingThroughGlobal);
  LaunchNThreads(num_threads, IteratingThroughThreadLocal);
  LaunchNThreads(num_threads, HGetGlobal);
  LaunchNThreads(num_threads, HGetThreadLocal);

  delete engine;
}

TEST_F(EngineBasicTest, DISABLED_TestStringHotspot) {
  int n_thread_reading = 16;
  int n_thread_writing = 16;
  configs.max_write_threads = n_thread_writing;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  int count = 100000;
  std::string key{"SuperHotspot"};
  std::string val1(1024, 'a');
  std::string val2(1023, 'b');

  ASSERT_EQ(engine->Set(key, val1), Status::Ok);
  engine->ReleaseWriteThread();

  auto EvenWriteOddRead = [&](uint32_t id) {
    for (size_t i = 0; i < count; i++) {
      if (id % 2 == 0) {
        // Even Write
        if (id % 4 == 0) {
          ASSERT_EQ(engine->Set(key, val1), Status::Ok);
        } else {
          ASSERT_EQ(engine->Set(key, val2), Status::Ok);
        }
      } else {
        // Odd Read
        std::string got_val;
        ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
        bool match = false;
        match = match || (got_val == val1);
        match = match || (got_val == val2);
        if (!match) {
          std::string msg;
          msg.append("Wrong value!\n");
          msg.append("The value should be 1024 of a's or 1023 of b's.\n");
          msg.append("Actual result is:\n");
          msg.append(got_val);
          msg.append("\n");
          msg.append("Length: ");
          msg.append(std::to_string(got_val.size()));
          msg.append("\n");
          GlobalLogger.Error(msg.data());
        }
        ASSERT_TRUE(match);
      }
    }
  };

  LaunchNThreads(n_thread_reading + n_thread_writing, EvenWriteOddRead);
  delete engine;
}

TEST_F(EngineBasicTest, DISABLED_TestSortedHotspot) {
  int n_thread_reading = 16;
  int n_thread_writing = 16;
  configs.max_write_threads = n_thread_writing;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  int count = 100000;
  std::string collection{"collection"};
  std::vector<std::string> keys{"SuperHotSpot0", "SuperHotSpot2",
                                "SuperHotSpot1"};
  std::string val1(1024, 'a');
  std::string val2(1024, 'b');

  for (const std::string &key : keys) {
    ASSERT_EQ(engine->SSet(collection, key, val1), Status::Ok);
    engine->ReleaseWriteThread();

    auto EvenWriteOddRead = [&](uint32_t id) {
      for (size_t i = 0; i < count; i++) {
        if (id % 2 == 0) {
          // Even Write
          if (id % 4 == 0) {
            ASSERT_EQ(engine->SSet(collection, key, val1), Status::Ok);
          } else {
            ASSERT_EQ(engine->SSet(collection, key, val2), Status::Ok);
          }
        } else {
          // Odd Read
          std::string got_val;
          ASSERT_EQ(engine->SGet(collection, key, &got_val), Status::Ok);
          bool match = false;
          match = match || (got_val == val1);
          match = match || (got_val == val2);
          if (!match) {
            std::string msg;
            msg.append("Wrong value!\n");
            msg.append("The value should be 1024 of a's or 1023 of b's.\n");
            msg.append("Actual result is:\n");
            msg.append(got_val);
            msg.append("\n");
            msg.append("Length: ");
            msg.append(std::to_string(got_val.size()));
            msg.append("\n");
            GlobalLogger.Error(msg.data());
          }
          ASSERT_TRUE(match);
        }
      }
    };

    LaunchNThreads(n_thread_reading + n_thread_writing, EvenWriteOddRead);
  }
  delete engine;
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
