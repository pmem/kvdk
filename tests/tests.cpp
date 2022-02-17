/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <future>
#include <string>
#include <thread>
#include <vector>

#include "../engine/kv_engine.hpp"
#include "../engine/pmem_allocator/pmem_allocator.hpp"
#include "../engine/utils/sync_point.hpp"
#include "gtest/gtest.h"
#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include "test_util.h"

using namespace KVDK_NAMESPACE;
static const uint64_t str_pool_length = 1024000;

using SetOpsFunc =
    std::function<Status(const std::string& collection, const std::string& key,
                         const std::string& value)>;
using DeleteOpsFunc = std::function<Status(const std::string& collection,
                                           const std::string& key)>;

using GetOpsFunc = std::function<Status(
    const std::string& collection, const std::string& key, std::string* value)>;

enum Types { String, Sorted, Hash, Queue };

class EngineBasicTest : public testing::Test {
 protected:
  Engine* engine = nullptr;
  Configs configs;
  std::string db_path;
  std::string backup_path;
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
    configs.log_level = LogLevel::All;
    configs.max_access_threads = 1;
    db_path = "/mnt/pmem0/kvdk-test";
    backup_path = "/mnt/pmem0/kvdk-test-backup";
    char cmd[1024];
    sprintf(cmd, "rm -rf %s && rm -rf %s\n", db_path.c_str(),
            backup_path.c_str());
    int res __attribute__((unused)) = system(cmd);
    config_option = OptionConfig::Default;
    cnt = 500;
  }

  virtual void TearDown() { Destroy(); }

  void AssignData(std::string& data, int len) {
    data.assign(str_pool.data() + (rand() % (str_pool_length - len)), len);
  }

  void Destroy() {
    // delete db_path
    char cmd[1024];
    sprintf(cmd, "rm -rf %s && rm -rf %s\n", db_path.c_str(),
            backup_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }

  bool ChangedConfig() {
    config_option++;
    if (config_option >= End) {
      return false;
    } else {
      ReopenEngine();
      return true;
    }
  }

  void ReopenEngine() {
    delete engine;
    engine = nullptr;
    configs = CurrentConfigs();
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);
  }

  // Return the current configuration.
  Configs CurrentConfigs() {
    switch (config_option) {
      case MultiThread:
        configs.max_access_threads = 16;
        break;
      case OptRestore:
        configs.opt_large_sorted_collection_restore = true;
        break;
      default:
        break;
    }
    return configs;
  }

  // Set/Get/Delete
  void TestGlobalCollection(const std::string& collection, SetOpsFunc SetFunc,
                            GetOpsFunc GetFunc, DeleteOpsFunc DeleteFunc,
                            Types type) {
    // Maybe having create collection for all collection types.
    if (type == Types::Sorted) {
      Collection* collection_ptr;
      ASSERT_EQ(engine->CreateSortedCollection(collection, &collection_ptr),
                Status::Ok);
    }
    TestEmptyKey(collection, SetFunc, GetFunc, DeleteFunc);
    auto global_func = [=](uint64_t id) {
      this->CreateBasicOperationTest(collection, SetFunc, GetFunc, DeleteFunc,
                                     id);
    };
    LaunchNThreads(configs.max_access_threads, global_func);
  }

  void TestLocalCollection(const std::string& collection, SetOpsFunc SetFunc,
                           GetOpsFunc GetFunc, DeleteOpsFunc DeleteFunc) {
    auto Local_XSetXGetXDelete = [&](uint64_t id) {
      std::string thread_local_collection = collection + std::to_string(id);
      Collection* local_collection_ptr;
      ASSERT_EQ(engine->CreateSortedCollection(thread_local_collection,
                                               &local_collection_ptr),
                Status::Ok);

      TestEmptyKey(thread_local_collection, SetFunc, GetFunc, DeleteFunc);

      CreateBasicOperationTest(thread_local_collection, SetFunc, GetFunc,
                               DeleteFunc, id);
    };
    LaunchNThreads(configs.max_access_threads, Local_XSetXGetXDelete);
  }

  void TestSortedIterator(const std::string& collection,
                          bool is_local = false) {
    auto IteratingThrough = [&](uint32_t id) {
      int entries = 0;
      std::string new_collection = collection;
      if (is_local) {
        new_collection += std::to_string(id);
      }

      auto iter = engine->NewSortedIterator(new_collection);
      ASSERT_TRUE(iter != nullptr);
      // forward iterator
      iter->SeekToFirst();
      if (iter->Valid()) {
        ++entries;
        std::string prev = iter->Key();
        iter->Next();
        while (iter->Valid()) {
          ++entries;
          std::string k = iter->Key();
          iter->Next();
          ASSERT_EQ(true, k.compare(prev) > 0);
          prev = k;
        }
      }
      if (is_local) {
        ASSERT_EQ(cnt, entries);
      } else {
        ASSERT_EQ(cnt * configs.max_access_threads, entries);
      }

      // backward iterator
      iter->SeekToLast();
      if (iter->Valid()) {
        --entries;
        std::string next = iter->Key();
        iter->Prev();
        while (iter->Valid()) {
          --entries;
          std::string k = iter->Key();
          iter->Prev();
          ASSERT_EQ(true, k.compare(next) < 0);
          next = k;
        }
      }
      ASSERT_EQ(entries, 0);
      engine->ReleaseSortedIterator(iter);
    };
    LaunchNThreads(configs.max_access_threads, IteratingThrough);
  }

  void TestUnorderedIterator(const std::string& collection,
                             bool is_local = false) {
    auto IteratingThrough = [&](uint32_t id) {
      int entries = 0;
      std::string new_collection = collection;
      if (is_local) {
        new_collection += std::to_string(id);
      }

      auto iter = engine->NewUnorderedIterator(new_collection);
      ASSERT_TRUE(iter != nullptr);
      // forward iterator
      iter->SeekToFirst();
      if (iter->Valid()) {
        ++entries;
        std::string prev = iter->Key();
        iter->Next();
        while (iter->Valid()) {
          ++entries;
          std::string k = iter->Key();
          iter->Next();
          prev = k;
        }
      }
      if (is_local) {
        ASSERT_EQ(cnt, entries);
      } else {
        ASSERT_EQ(cnt * configs.max_access_threads, entries);
      }

      // backward iterator
      iter->SeekToLast();
      if (iter->Valid()) {
        --entries;
        std::string next = iter->Key();
        iter->Prev();
        while (iter->Valid()) {
          --entries;
          std::string k = iter->Key();
          iter->Prev();
          next = k;
        }
      }
      ASSERT_EQ(entries, 0);
    };
    LaunchNThreads(configs.max_access_threads, IteratingThrough);
  }

 private:
  void TestEmptyKey(const std::string& collection, SetOpsFunc SetFunc,
                    GetOpsFunc GetFunc, DeleteOpsFunc DeleteFunc) {
    std::string key, val, got_val;
    key = "", val = "val";
    ASSERT_EQ(SetFunc(collection, key, val), Status::Ok);
    ASSERT_EQ(GetFunc(collection, key, &got_val), Status::Ok);
    ASSERT_EQ(val, got_val);
    ASSERT_EQ(DeleteFunc(collection, key), Status::Ok);
    ASSERT_EQ(GetFunc(collection, key, &got_val), Status::NotFound);
    engine->ReleaseAccessThread();
  }

  void CreateBasicOperationTest(const std::string& collection,
                                SetOpsFunc SetFunc, GetOpsFunc GetFunc,
                                DeleteOpsFunc DeleteFunc, uint32_t id) {
    std::string val1, val2, got_val1, got_val2;
    int t_cnt = cnt;
    while (t_cnt--) {
      std::string key1(std::string(id + 1, 'a') + std::to_string(t_cnt));
      std::string key2(std::string(id + 1, 'b') + std::to_string(t_cnt));
      AssignData(val1, fast_random_64() % 1024);
      AssignData(val2, fast_random_64() % 1024);

      // Set
      ASSERT_EQ(SetFunc(collection, key1, val1), Status::Ok);
      ASSERT_EQ(SetFunc(collection, key2, val2), Status::Ok);

      // Get
      ASSERT_EQ(GetFunc(collection, key1, &got_val1), Status::Ok);
      ASSERT_EQ(val1, got_val1);
      ASSERT_EQ(GetFunc(collection, key2, &got_val2), Status::Ok);
      ASSERT_EQ(val2, got_val2);

      // Delete
      ASSERT_EQ(DeleteFunc(collection, key1), Status::Ok);
      ASSERT_EQ(GetFunc(collection, key1, &got_val1), Status::NotFound);

      // Update
      AssignData(val2, fast_random_64() % 1024);
      ASSERT_EQ(SetFunc(collection, key2, val2), Status::Ok);
      ASSERT_EQ(GetFunc(collection, key2, &got_val2), Status::Ok);
      ASSERT_EQ(got_val2, val2);
    }
  }

 private:
  // Sequence of option configurations to try
  enum OptionConfig { Default, MultiThread, OptRestore, End };
  int config_option;
  int cnt;
};

TEST_F(EngineBasicTest, TestThreadManager) {
  int max_access_threads = 1;
  configs.max_access_threads = max_access_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string key("k");
  std::string val("value");

  ASSERT_EQ(engine->Set(key, val), Status::Ok);

  // Reach max access threads
  auto s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::TooManyAccessThreads);
  // Manually release access thread
  engine->ReleaseAccessThread();
  s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::Ok);
  // Release access thread on thread exits
  s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::Ok);
  delete engine;
}

// Test iterator/backup/checkpoint on a snapshot
TEST_F(EngineBasicTest, TestBasicSnapshot) {
  uint32_t num_threads = 16;
  int count = 100;
  configs.max_access_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::string sorted_collection("sorted_collection");
  Collection* sorted_collection_ptr;
  ASSERT_EQ(
      engine->CreateSortedCollection(sorted_collection, &sorted_collection_ptr),
      Status::Ok);

  bool snapshot_done(false);
  std::atomic<int> set_finish_threads(0);
  SpinMutex spin;
  std::condition_variable_any cv;

  // Insert kv, then update/delete them and insert new kv after snapshot
  auto WriteThread = [&](uint32_t id) {
    int cnt = count;
    // Insert
    while (cnt--) {
      // Insert
      std::string key1(std::string(id + 1, 'a') + std::to_string(cnt));
      std::string key2(std::string(id + 1, 'b') + std::to_string(cnt));
      ASSERT_EQ(engine->Set(key1, key1), Status::Ok);
      ASSERT_EQ(engine->Set(key2, key2), Status::Ok);
      ASSERT_EQ(engine->SSet(sorted_collection, key1, key1), Status::Ok);
      ASSERT_EQ(engine->SSet(sorted_collection, key2, key2), Status::Ok);
    }
    // Wait snapshot done
    set_finish_threads.fetch_add(1);
    {
      std::unique_lock<SpinMutex> ul(spin);
      while (!snapshot_done) {
        cv.wait(ul);
      }
    }

    cnt = count * 10;
    // Update / Delete, and insert new
    while (cnt--) {
      std::string key1(std::string(id + 1, 'a') + std::to_string(cnt));
      std::string key2(std::string(id + 1, 'b') + std::to_string(cnt));
      std::string key3(std::string(id + 1, 'c') + std::to_string(cnt));
      ASSERT_EQ(engine->Set(key1, "updated " + key1), Status::Ok);
      ASSERT_EQ(engine->Delete(key1), Status::Ok);
      ASSERT_EQ(engine->Set(key3, key3), Status::Ok);
      ASSERT_EQ(engine->SSet(sorted_collection, key1, "updated " + key1),
                Status::Ok);
      ASSERT_EQ(engine->SDelete(sorted_collection, key1), Status::Ok);
      ASSERT_EQ(engine->SSet(sorted_collection, key3, key3), Status::Ok);
    }
  };

  std::vector<std::thread> ths;
  for (int i = 0; i < num_threads; i++) {
    ths.emplace_back(std::thread(WriteThread, i));
  }
  // wait until all threads insert done
  while (set_finish_threads.load() != num_threads) {
    asm volatile("pause");
  }
  Snapshot* snapshot = engine->GetSnapshot(true);
  {
    std::lock_guard<SpinMutex> ul(spin);
    snapshot_done = true;
    cv.notify_all();
  }
  engine->Backup(backup_path, snapshot);
  for (auto& t : ths) {
    t.join();
  }
  Iterator* snapshot_iter =
      engine->NewSortedIterator(sorted_collection, snapshot);
  uint64_t snapshot_iter_cnt = 0;
  snapshot_iter->SeekToFirst();
  while (snapshot_iter->Valid()) {
    ASSERT_TRUE(snapshot_iter->Valid());
    snapshot_iter_cnt++;
    ASSERT_EQ(snapshot_iter->Key(), snapshot_iter->Value());
    snapshot_iter->Next();
  }
  ASSERT_EQ(snapshot_iter_cnt, num_threads * count * 2);
  engine->ReleaseSortedIterator(snapshot_iter);
  delete engine;

  std::vector<int> opt_restore_skiplists{0, 1};
  for (auto is_opt : opt_restore_skiplists) {
    configs.opt_large_sorted_collection_restore = is_opt;
    Engine* backup_engine;
    ASSERT_EQ(
        Engine::Open(backup_path.c_str(), &backup_engine, configs, stdout),
        Status::Ok);
    configs.recover_to_checkpoint = true;
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);

    // Test backup and checkpoint instance
    // All changes after snapshot should not be seen in backup and checkpoint
    // Writes on backup should work well
    for (uint32_t id = 0; id < num_threads; id++) {
      int cnt = count;
      std::string got_v1, got_v2, got_v3;
      while (cnt--) {
        std::string key1(std::string(id + 1, 'a') + std::to_string(cnt));
        std::string key2(std::string(id + 1, 'b') + std::to_string(cnt));
        std::string key3(std::string(id + 1, 'c') + std::to_string(cnt));

        ASSERT_EQ(backup_engine->Get(key1, &got_v1), Status::Ok);
        ASSERT_EQ(backup_engine->Get(key2, &got_v2), Status::Ok);
        ASSERT_EQ(backup_engine->Get(key3, &got_v3), Status::NotFound);
        ASSERT_EQ(got_v1, key1);
        ASSERT_EQ(got_v2, key2);
        ASSERT_EQ(engine->Get(key1, &got_v1), Status::Ok);
        ASSERT_EQ(engine->Get(key2, &got_v2), Status::Ok);
        ASSERT_EQ(engine->Get(key3, &got_v3), Status::NotFound);
        ASSERT_EQ(got_v1, key1);
        ASSERT_EQ(got_v2, key2);

        ASSERT_EQ(backup_engine->SGet(sorted_collection, key1, &got_v1),
                  Status::Ok);
        ASSERT_EQ(backup_engine->SGet(sorted_collection, key2, &got_v2),
                  Status::Ok);
        ASSERT_EQ(backup_engine->SGet(sorted_collection, key3, &got_v3),
                  Status::NotFound);
        ASSERT_EQ(got_v1, key1);
        ASSERT_EQ(got_v2, key2);
        ASSERT_EQ(engine->SGet(sorted_collection, key1, &got_v1), Status::Ok);
        ASSERT_EQ(engine->SGet(sorted_collection, key2, &got_v2), Status::Ok);
        ASSERT_EQ(engine->SGet(sorted_collection, key3, &got_v3),
                  Status::NotFound);
        ASSERT_EQ(got_v1, key1);
        ASSERT_EQ(got_v2, key2);
      }
    }

    uint64_t backup_iter_cnt = 0;
    uint64_t checkpoint_iter_cnt = 0;
    auto backup_iter = backup_engine->NewSortedIterator(sorted_collection);
    auto checkpoint_iter = engine->NewSortedIterator(sorted_collection);
    backup_iter->SeekToFirst();
    checkpoint_iter->SeekToFirst();
    while (backup_iter->Valid()) {
      ASSERT_TRUE(checkpoint_iter->Valid());
      backup_iter_cnt++;
      checkpoint_iter_cnt++;
      ASSERT_EQ(backup_iter->Key(), backup_iter->Value());
      ASSERT_EQ(checkpoint_iter->Key(), checkpoint_iter->Value());
      ASSERT_EQ(checkpoint_iter->Key(), backup_iter->Key());
      backup_iter->Next();
      checkpoint_iter->Next();
    }
    ASSERT_EQ(backup_iter_cnt, num_threads * count * 2);
    ASSERT_EQ(checkpoint_iter_cnt, num_threads * count * 2);
    backup_engine->ReleaseSortedIterator(backup_iter);
    engine->ReleaseSortedIterator(checkpoint_iter);

    delete engine;
    delete backup_engine;
  }
}

TEST_F(EngineBasicTest, TestBasicStringOperations) {
  auto StringSetFunc = [&](const std::string& collection,
                           const std::string& key,
                           const std::string& value) -> Status {
    return engine->Set(key, value);
  };

  auto StringGetFunc =
      [&](const std::string& collection, const std::string& key,
          std::string* value) -> Status { return engine->Get(key, value); };

  auto StringDeleteFunc = [&](const std::string& collection,
                              const std::string& key) -> Status {
    return engine->Delete(key);
  };

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  do {
    TestGlobalCollection("global_string", StringSetFunc, StringGetFunc,
                         StringDeleteFunc, Types::String);
  } while (ChangedConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestBatchWrite) {
  int num_threads = 16;
  configs.max_access_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  int batch_size = 10;
  int count = 500;
  auto BatchSetDelete = [&](uint32_t id) {
    std::string key_prefix(std::string(id, 'a'));
    std::string got_val;
    WriteBatch batch;
    int cnt = count;
    while (cnt--) {
      for (size_t i = 0; i < batch_size; i++) {
        auto key = key_prefix + std::to_string(i) + std::to_string(cnt);
        auto val = std::to_string(i * id);
        batch.Put(key, val);
      }
      ASSERT_EQ(engine->BatchWrite(batch), Status::Ok);
      batch.Clear();
      for (size_t i = 0; i < batch_size; i++) {
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
      for (size_t i = 0; i < batch_size; i++) {
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

TEST_F(EngineBasicTest, TestLocalSortedCollection) {
  auto SortedSetFunc = [&](const std::string& collection,
                           const std::string& key,
                           const std::string& value) -> Status {
    return engine->SSet(collection, key, value);
  };

  auto SortedGetFunc = [&](const std::string& collection,
                           const std::string& key,
                           std::string* value) -> Status {
    return engine->SGet(collection, key, value);
  };

  auto SortedDeleteFunc = [&](const std::string& collection,
                              const std::string& key) -> Status {
    return engine->SDelete(collection, key);
  };

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  do {
    TestLocalCollection("thread_skiplist", SortedSetFunc, SortedGetFunc,
                        SortedDeleteFunc);
    TestSortedIterator("thread_skiplist", true);
  } while (ChangedConfig());

  delete engine;
}

TEST_F(EngineBasicTest, TestGlobalSortedCollection) {
  std::atomic<int> n_global_entries{0};

  auto SortedSetFunc = [&](const std::string& collection,
                           const std::string& key,
                           const std::string& value) -> Status {
    return engine->SSet(collection, key, value);
  };

  auto SortedGetFunc = [&](const std::string& collection,
                           const std::string& key,
                           std::string* value) -> Status {
    return engine->SGet(collection, key, value);
  };

  auto SortedDeleteFunc = [&](const std::string& collection,
                              const std::string& key) -> Status {
    return engine->SDelete(collection, key);
  };

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::string collection = "global_skiplist";
  do {
    TestGlobalCollection(collection, SortedSetFunc, SortedGetFunc,
                         SortedDeleteFunc, Types::Sorted);
    TestSortedIterator(collection, false);
  } while (ChangedConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestSeek) {
  std::string val;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  // Test Seek
  std::string collection = "col1";
  Collection* collection_ptr;
  ASSERT_EQ(engine->CreateSortedCollection(collection, &collection_ptr),
            Status::Ok);
  uint64_t z = 0;
  auto zero_filled_str = uint64_to_string(z);
  printf("%s\n", zero_filled_str.c_str());
  ASSERT_EQ(engine->SSet(collection, zero_filled_str, zero_filled_str),
            Status::Ok);
  ASSERT_EQ(engine->SGet(collection, zero_filled_str, &val), Status::Ok);
  auto iter = engine->NewSortedIterator(collection);
  ASSERT_NE(iter, nullptr);
  iter->Seek(zero_filled_str);
  ASSERT_TRUE(iter->Valid());

  // Test SeekToFirst
  collection.assign("col2");
  ASSERT_EQ(engine->CreateSortedCollection(collection, &collection_ptr),
            Status::Ok);
  ASSERT_EQ(engine->SSet(collection, "foo", "bar"), Status::Ok);
  ASSERT_EQ(engine->SGet(collection, "foo", &val), Status::Ok);
  ASSERT_EQ(engine->SDelete(collection, "foo"), Status::Ok);
  ASSERT_EQ(engine->SGet(collection, "foo", &val), Status::NotFound);
  ASSERT_EQ(engine->SSet(collection, "foo2", "bar2"), Status::Ok);
  engine->ReleaseSortedIterator(iter);
  iter = engine->NewSortedIterator(collection);
  ASSERT_NE(iter, nullptr);
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->Value(), "bar2");
  engine->ReleaseSortedIterator(iter);
  delete engine;
}

TEST_F(EngineBasicTest, TestStringRestore) {
  int num_threads = 16;
  configs.max_access_threads = num_threads;
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
      if ((i * id) % 3 == 0 &&
          (id * i) % 2 == 1) {  // deleted then updated ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(got_val, updated_val);
      } else if ((i * id) % 2 == 0) {  // not deleted ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(got_val, val);
      } else {  // deleted ones
        ASSERT_EQ(s, Status::NotFound);
      }
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestSortedRestore) {
  int num_threads = 16;
  configs.max_access_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  int count = 100;
  std::string overall_skiplist = "skiplist";
  Collection* overall_collection_ptr;
  ASSERT_EQ(
      engine->CreateSortedCollection(overall_skiplist, &overall_collection_ptr),
      Status::Ok);
  std::string thread_skiplist = "t_skiplist";
  auto SetupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    std::string t_skiplist(thread_skiplist + std::to_string(id));
    Collection* thread_collection_ptr;
    ASSERT_EQ(
        engine->CreateSortedCollection(t_skiplist, &thread_collection_ptr),
        Status::Ok);
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
    GlobalLogger.Info("Restore with opt_large_sorted_collection_restore: %d\n",
                      is_opt);
    configs.max_access_threads = num_threads;
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
      int data_entries_scan = 0;
      iter->SeekToFirst();
      if (iter->Valid()) {
        data_entries_scan++;
        std::string prev = iter->Key();
        iter->Next();
        while (iter->Valid()) {
          data_entries_scan++;
          std::string k = iter->Key();
          iter->Next();
          ASSERT_TRUE(k.compare(prev) > 0);
          prev = k;
        }
      }
      ASSERT_EQ(data_entries_scan, count / 2);

      iter->SeekToLast();
      if (iter->Valid()) {
        data_entries_scan--;
        std::string next = iter->Key();
        iter->Prev();
        while (iter->Valid()) {
          data_entries_scan--;
          std::string k = iter->Key();
          iter->Prev();
          ASSERT_TRUE(k.compare(next) < 0);
          next = k;
        }
      }
      ASSERT_EQ(data_entries_scan, 0);
      engine->ReleaseSortedIterator(iter);
    }

    int data_entries_scan = 0;
    auto iter = engine->NewSortedIterator(overall_skiplist);
    ASSERT_TRUE(iter != nullptr);
    iter->SeekToFirst();
    if (iter->Valid()) {
      std::string prev = iter->Key();
      data_entries_scan++;
      iter->Next();
      while (iter->Valid()) {
        data_entries_scan++;
        std::string k = iter->Key();
        iter->Next();
        ASSERT_TRUE(k.compare(prev) > 0);
        prev = k;
      }
    }
    ASSERT_EQ(data_entries_scan, (count / 2) * num_threads);

    iter->SeekToLast();
    if (iter->Valid()) {
      std::string next = iter->Key();
      data_entries_scan--;
      iter->Prev();
      while (iter->Valid()) {
        data_entries_scan--;
        std::string k = iter->Key();
        iter->Prev();
        ASSERT_TRUE(k.compare(next) < 0);
        next = k;
      }
    }
    ASSERT_EQ(data_entries_scan, 0);
    engine->ReleaseSortedIterator(iter);
    delete engine;
  }
}

TEST_F(EngineBasicTest, TestMultiThreadSortedRestore) {
  int num_threads = 48;
  int num_collections = 16;
  configs.max_access_threads = num_threads;
  configs.opt_large_sorted_collection_restore = true;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  uint64_t count = 1024;

  std::set<std::string> avg_nums, random_nums;
  for (uint64_t i = 1; i <= count; ++i) {
    std::string average_skiplist("a_skiplist" +
                                 std::to_string(i % num_collections));
    Collection* avg_collection_ptr;
    ASSERT_EQ(
        engine->CreateSortedCollection(average_skiplist, &avg_collection_ptr),
        Status::Ok);
  }
  for (uint32_t i = 0; i < num_threads; ++i) {
    std::string r_skiplist("r_skiplist" + std::to_string(i));
    Collection* r_collection_ptr;
    ASSERT_EQ(engine->CreateSortedCollection(r_skiplist, &r_collection_ptr),
              Status::Ok);
  }
  auto SetupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (uint64_t i = 1; i <= count; ++i) {
      std::string average_skiplist("a_skiplist" +
                                   std::to_string(i % num_collections));

      std::string r_skiplist("r_skiplist" +
                             std::to_string(rand() % num_threads));

      auto key = key_prefix + std::to_string(i);
      auto average_val = std::to_string(i);
      ASSERT_EQ(engine->SSet(average_skiplist, key, average_val), Status::Ok);
      ASSERT_EQ(engine->SGet(average_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, average_val);
      auto r_val = std::to_string(i * 2);
      ASSERT_EQ(engine->SSet(r_skiplist, key, r_val), Status::Ok);
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
  auto skiplists = (dynamic_cast<KVEngine*>(engine))->GetSkiplists();
  for (int h = 1; h <= 32; ++h) {
    for (auto s : skiplists) {
      ASSERT_EQ(s->CheckConnection(h), Status::Ok);
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestLocalUnorderedCollection) {
  auto UnorderedSetFunc = [&](const std::string& collection,
                              const std::string& key,
                              const std::string& value) -> Status {
    return engine->HSet(collection, key, value);
  };

  auto UnorderedGetFunc = [&](const std::string& collection,
                              const std::string& key,
                              std::string* value) -> Status {
    return engine->HGet(collection, key, value);
  };

  auto UnorderedDeleteFunc = [&](const std::string& collection,
                                 const std::string& key) -> Status {
    return engine->HDelete(collection, key);
  };

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  do {
    TestLocalCollection("thread_unordered", UnorderedSetFunc, UnorderedGetFunc,
                        UnorderedDeleteFunc);
    TestUnorderedIterator("thread_unordered", true);
  } while (ChangedConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestGlobalUnorderedCollection) {
  auto UnorderedSetFunc = [&](const std::string& collection,
                              const std::string& key,
                              const std::string& value) -> Status {
    return engine->HSet(collection, key, value);
  };

  auto UnorderedGetFunc = [&](const std::string& collection,
                              const std::string& key,
                              std::string* value) -> Status {
    return engine->HGet(collection, key, value);
  };

  auto UnorderedDeleteFunc = [&](const std::string& collection,
                                 const std::string& key) -> Status {
    return engine->HDelete(collection, key);
  };
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  do {
    TestGlobalCollection("global_unordered", UnorderedSetFunc, UnorderedGetFunc,
                         UnorderedDeleteFunc, Types::Hash);
    TestUnorderedIterator("global_unordered", false);
  } while (ChangedConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestUnorderedCollectionRestore) {
  int count = 100;
  int num_threads = 16;

  configs.max_access_threads = num_threads;
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
      ASSERT_EQ(
          engine->HGet(global_collection_name,
                       global_kvs_inserting[tid][j + count].first, &value_got),
          Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j + count].second);

      // Delete second kv-pair in global collection
      ASSERT_EQ(engine->HDelete(global_collection_name,
                                global_kvs_inserting[tid][j + count].first),
                Status::Ok);
      ASSERT_EQ(
          engine->HGet(global_collection_name,
                       global_kvs_inserting[tid][j + count].first, &value_got),
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
      ASSERT_EQ(
          engine->HGet(tlocal_collection_names[tid],
                       tlocal_kvs_inserting[tid][j + count].first, &value_got),
          Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j + count].second);

      // Delete second kv-pair in global collection
      ASSERT_EQ(engine->HDelete(tlocal_collection_names[tid],
                                tlocal_kvs_inserting[tid][j + count].first),
                Status::Ok);
      ASSERT_EQ(
          engine->HGet(tlocal_collection_names[tid],
                       tlocal_kvs_inserting[tid][j + count].first, &value_got),
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

      ASSERT_EQ(
          engine->HGet(global_collection_name,
                       global_kvs_inserting[tid][j + count].first, &value_got),
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

      ASSERT_EQ(
          engine->HGet(tlocal_collection_names[tid],
                       tlocal_kvs_inserting[tid][j + count].first, &value_got),
          Status::NotFound);
    }
  };

  LaunchNThreads(num_threads, IteratingThroughGlobal);
  LaunchNThreads(num_threads, IteratingThroughThreadLocal);
  LaunchNThreads(num_threads, HGetGlobal);
  LaunchNThreads(num_threads, HGetThreadLocal);

  delete engine;

  // reopen and restore engine
  configs.max_access_threads = 1;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  LaunchNThreads(1, IteratingThroughGlobal);
  LaunchNThreads(1, IteratingThroughThreadLocal);
  LaunchNThreads(1, HGetGlobal);
  LaunchNThreads(1, HGetThreadLocal);

  delete engine;
}

TEST_F(EngineBasicTest, TestLocalQueue) {
  int num_threads = 16;
  int count = 100;
  configs.max_access_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys

  std::vector<std::vector<std::string>> local_values(num_threads);
  std::vector<std::string> local_collection_names(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    local_collection_names[i] = "local_uncoll_t" + std::to_string(i);
    for (size_t j = 0; j < count * 3; j++) {
      local_values[i].push_back(GetRandomString(1024));
    }
  }

  auto LPushRPop = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(
          engine->LPush(local_collection_names[tid], local_values[tid][j]),
          Status::Ok);
    }
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->RPop(local_collection_names[tid], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);
    }
    ASSERT_EQ(engine->RPop(local_collection_names[tid], &value_got),
              Status::NotFound);
  };

  auto RPushLPop = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(
          engine->RPush(local_collection_names[tid], local_values[tid][j]),
          Status::Ok);
    }
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->LPop(local_collection_names[tid], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);
    }
    ASSERT_EQ(engine->LPop(local_collection_names[tid], &value_got),
              Status::NotFound);
  };

  LaunchNThreads(num_threads, LPushRPop);
  LaunchNThreads(num_threads, RPushLPop);

  delete engine;
}

TEST_F(EngineBasicTest, TestQueueRestoration) {
  int num_threads = 16;
  int count = 100;
  configs.max_access_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys

  std::vector<std::vector<std::string>> local_values(num_threads);
  std::vector<std::string> local_collection_names(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    local_collection_names[i] = "local_uncoll_t" + std::to_string(i);
    for (size_t j = 0; j < count; j++) {
      local_values[i].push_back(GetRandomString(1024));
    }
  }

  auto LPush = [&](uint32_t tid) {
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(
          engine->LPush(local_collection_names[tid], local_values[tid][j]),
          Status::Ok);
    }
  };

  auto RPop = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->RPop(local_collection_names[tid], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);
    }
    ASSERT_EQ(engine->RPop(local_collection_names[tid], &value_got),
              Status::NotFound);
  };

  auto RPush = [&](uint32_t tid) {
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(
          engine->RPush(local_collection_names[tid], local_values[tid][j]),
          Status::Ok);
    }
  };

  auto LPop = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->LPop(local_collection_names[tid], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);
    }
    ASSERT_EQ(engine->LPop(local_collection_names[tid], &value_got),
              Status::NotFound);
  };

  LaunchNThreads(num_threads, LPush);
  delete engine;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  LaunchNThreads(num_threads, RPop);

  LaunchNThreads(num_threads, RPush);
  delete engine;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  LaunchNThreads(num_threads, LPop);

  delete engine;
}

TEST_F(EngineBasicTest, TestStringHotspot) {
  int n_thread_reading = 16;
  int n_thread_writing = 16;
  configs.max_access_threads = n_thread_writing + n_thread_reading;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  int count = 100000;
  std::string key{"SuperHotspot"};
  std::string val1(1024, 'a');
  std::string val2(1023, 'b');

  ASSERT_EQ(engine->Set(key, val1), Status::Ok);
  engine->ReleaseAccessThread();

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

TEST_F(EngineBasicTest, TestSortedHotspot) {
  int n_thread_reading = 16;
  int n_thread_writing = 16;
  configs.max_access_threads = n_thread_writing + n_thread_reading;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  int count = 100000;
  std::string collection_name{"collection"};
  std::vector<std::string> keys{"SuperHotSpot0", "SuperHotSpot2",
                                "SuperHotSpot1"};
  std::string val1(1024, 'a');
  std::string val2(1024, 'b');
  Collection* collection_ptr;
  ASSERT_EQ(engine->CreateSortedCollection(collection_name, &collection_ptr),
            Status::Ok);

  for (const std::string& key : keys) {
    ASSERT_EQ(engine->SSet(collection_name, key, val1), Status::Ok);
    engine->ReleaseAccessThread();

    auto EvenWriteOddRead = [&](uint32_t id) {
      for (size_t i = 0; i < count; i++) {
        if (id % 2 == 0) {
          // Even Write
          if (id % 4 == 0) {
            ASSERT_EQ(engine->SSet(collection_name, key, val1), Status::Ok);
          } else {
            ASSERT_EQ(engine->SSet(collection_name, key, val2), Status::Ok);
          }
        } else {
          // Odd Read
          std::string got_val;
          ASSERT_EQ(engine->SGet(collection_name, key, &got_val), Status::Ok);
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

TEST_F(EngineBasicTest, TestSortedCustomCompareFunction) {
  using kvpair = std::pair<std::string, std::string>;
  int threads = 16;
  configs.max_access_threads = threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::vector<std::string> collections{"collection0", "collection1",
                                       "collection2"};

  auto cmp0 = [](const StringView& a, const StringView& b) -> int {
    double scorea = std::stod(string_view_2_string(a));
    double scoreb = std::stod(string_view_2_string(b));
    if (scorea == scoreb)
      return 0;
    else if (scorea < scoreb)
      return 1;
    else
      return -1;
  };

  auto cmp1 = [](const StringView& a, const StringView& b) -> int {
    double scorea = std::stod(string_view_2_string(a));
    double scoreb = std::stod(string_view_2_string(b));
    if (scorea == scoreb)
      return 0;
    else if (scorea > scoreb)
      return 1;
    else
      return -1;
  };

  int count = 10;
  std::vector<kvpair> key_values(count);
  std::map<std::string, std::string> dedup_kvs;
  std::generate(key_values.begin(), key_values.end(), [&]() {
    const char v = rand() % (90 - 65 + 1) + 65;
    std::string k = std::to_string(rand() % 100);
    dedup_kvs[k] = v;
    return std::make_pair(k, std::string(1, v));
  });

  // register compare function
  engine->RegisterComparator("collection0_cmp", cmp0);
  engine->RegisterComparator("collection1_cmp", cmp1);
  for (size_t i = 0; i < collections.size(); ++i) {
    Collection* collection_ptr;
    Status s;
    if (i < 2) {
      std::string comp_name = "collection" + std::to_string(i) + "_cmp";
      SortedCollectionConfigs s_configs;
      s_configs.comparator_name = comp_name;
      s = engine->CreateSortedCollection(collections[i], &collection_ptr,
                                         s_configs);
    } else {
      s = engine->CreateSortedCollection(collections[i], &collection_ptr);
    }
    ASSERT_EQ(s, Status::Ok);
  }
  for (size_t i = 0; i < collections.size(); ++i) {
    auto Write = [&](uint32_t id) {
      for (size_t j = 0; j < count; j++) {
        ASSERT_EQ(engine->SSet(collections[i], key_values[j].first,
                               key_values[j].second),
                  Status::Ok);
      }
    };
    LaunchNThreads(threads, Write);
  }

  delete engine;
  // Reopen engine error as the comparator is not registered in configs
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Abort);
  configs.comparator.RegisterComparator("collection0_cmp", cmp0);
  configs.comparator.RegisterComparator("collection1_cmp", cmp1);
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  for (size_t i = 0; i < collections.size(); ++i) {
    std::vector<kvpair> expected_res(dedup_kvs.begin(), dedup_kvs.end());
    if (i == 0) {
      std::sort(expected_res.begin(), expected_res.end(),
                [&](const kvpair& a, const kvpair& b) -> bool {
                  return cmp0(a.first, b.first) <= 0;
                });

    } else if (i == 1) {
      std::sort(expected_res.begin(), expected_res.end(),
                [&](const kvpair& a, const kvpair& b) -> bool {
                  return cmp1(a.first, b.first) <= 0;
                });
    }
    auto iter = engine->NewSortedIterator(collections[i]);
    ASSERT_TRUE(iter != nullptr);
    iter->SeekToFirst();
    int cnt = 0;
    while (iter->Valid()) {
      std::string key = iter->Key();
      std::string val = iter->Value();
      ASSERT_EQ(key, expected_res[cnt].first);
      ASSERT_EQ(val, expected_res[cnt].second);
      iter->Next();
      cnt++;
    }
    engine->ReleaseSortedIterator(iter);
  }
  ASSERT_EQ(engine->SDelete("collection0", "a"), Status::Ok);
  delete engine;
}

// ========================= Sync Point ======================================

#if DEBUG_LEVEL > 0
TEST_F(EngineBasicTest, TestBatchWriteSyncPoint) {
  // SyncPoint
  // The dependency is, T1->T2->T3/T4
  // T1: insert {"key8", "val15"}
  // T2: batch write: {"key0", "val0"}, ....{"key20",""val20}
  // T3: insert {"key10", "val17"}
  // T4: delete {"key0", "val0"} {"key3", "val3"} ... {"key18", "val18"}
  {
    Configs test_config = configs;
    test_config.max_access_threads = 16;
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
              Status::Ok);

    int batch_size = 20;

    SyncPoint::GetInstance()->LoadDependency(
        {{"Test::Set::Finish::1",
          "KVEngine::BatchWrite::AllocateRecord::After"},
         {"KVEngine::BatchWrite::StringBatchWriteImpl::Pesistent::Before",
          "Test::Set::Start::0"},
         {"KVEngine::BatchWrite::StringBatchWriteImpl::Pesistent::Before",
          "Test::Delete::Start::0"}});
    SyncPoint::GetInstance()->SetCallBack(
        "KVEngine::BatchWrite::AllocateRecord::After", [&](void* size) {
          size_t bsize = *((size_t*)size);
          ASSERT_EQ(batch_size, bsize);
        });

    SyncPoint::GetInstance()->EnableProcessing();

    std::vector<std::thread> ts;

    // thread 1
    ts.emplace_back([&]() {
      std::string key = "key8";
      std::string val = "val15";
      ASSERT_EQ(engine->Set(key, val), Status::Ok);
      TEST_SYNC_POINT("Test::Set::Finish::1");
    });

    // thread 2
    ts.emplace_back([&]() {
      WriteBatch wb;
      for (int i = 0; i < batch_size; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        wb.Put(key, val);
      }
      ASSERT_EQ(engine->BatchWrite(wb), Status::Ok);
    });

    // thread 3
    ts.emplace_back([&]() {
      TEST_SYNC_POINT("Test::Set::Start::0");
      std::string key = "key10";
      std::string val = "val17";
      ASSERT_EQ(engine->Set(key, val), Status::Ok);
    });

    // thread 4
    ts.emplace_back([&]() {
      TEST_SYNC_POINT("Test::Delete::Start::0");
      for (int i = 0; i < batch_size; i += 3) {
        std::string key = "key" + std::to_string(i);
        ASSERT_EQ(engine->Delete(key), Status::Ok);
      }
    });

    for (auto& t : ts) {
      t.join();
    }

    for (auto i = 0; i < batch_size; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string val;
      auto s = engine->Get(key, &val);
      if (i % 3 == 0) {
        ASSERT_EQ(s, Status::NotFound);
      } else {
        if (i == 10) {
          ASSERT_EQ(val, "val17");
        } else {
          ASSERT_EQ(val, "val" + std::to_string(i));
        }
      }
    }
  }

  delete engine;
}

TEST_F(EngineBasicTest, TestBatchWriteRecovrySyncPoint) {
  Configs test_config = configs;
  test_config.max_access_threads = 16;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
            Status::Ok);

  int batch_size = 10;
  {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->Reset();
    SyncPoint::GetInstance()->SetCallBack(
        "KVEnigne::BatchWrite::BatchWriteRecord", [&](void* index) {
          size_t idx = *(size_t*)(index);
          if (idx == (batch_size / 2)) {
            throw 1;
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    engine->Set("key2", "val2*2");
    engine->Set("key6", "val6*2");
    WriteBatch wb;
    for (int i = 0; i < batch_size; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string val = "val" + std::to_string(i);
      wb.Put(key, val);
    }
    try {
      ASSERT_EQ(engine->BatchWrite(wb), Status::Ok);
    } catch (...) {
      delete engine;
      // reopen engine
      ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
                Status::Ok);
      for (int i = 0; i < batch_size; ++i) {
        std::string got_val;
        std::string key = "key" + std::to_string(i);
        if (key == "key2") {
          ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, "val2*2");
        } else if (key == "key6") {
          ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, "val6*2");
        } else {
          ASSERT_EQ(engine->Get(key, &got_val), Status::NotFound);
        }
      }
    }
  }

  // Again write batch (the same key, the different value). Crash before
  // purgeAndFree
  {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->Reset();
    SyncPoint::GetInstance()->SetCallBack(
        "KVEngine::BatchWrite::purgeAndFree::Before", [&](void* index) {
          size_t idx = *(size_t*)(index);
          if (idx == 6) {
            throw 1;
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteBatch wb;
    for (int i = 0; i < batch_size; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string val = "val*" + std::to_string(i);

      if (i % 4 == 0) {
        ASSERT_EQ(engine->Set(key, val), Status::Ok);
        wb.Delete(key);
      } else {
        wb.Put(key, val);
      }
    }
    try {
      ASSERT_EQ(engine->BatchWrite(wb), Status::Ok);
    } catch (...) {
      delete engine;  // reopen engine;
      ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
                Status::Ok);
      for (int i = 0; i < batch_size; ++i) {
        std::string got_val;
        std::string key = "key" + std::to_string(i);
        Status s = engine->Get(key, &got_val);
        if (i % 4 == 0) {
          ASSERT_EQ(s, Status::NotFound);
        } else {
          ASSERT_EQ(s, Status::Ok);
          ASSERT_EQ(got_val, "val*" + std::to_string(i));
        }
      }
    }
  }
}

// Example Case One:
//         A <-> C <-> D
// Insert B, but crashes half way, Now the state is:
//         A <------ C <-> D
//         A <-> B ->C
// Then Repair
TEST_F(EngineBasicTest, TestSortedRecoverySyncPointCaseOne) {
  Configs test_config = configs;
  test_config.max_access_threads = 16;

  std::atomic<int> update_num(1);
  int cnt = 20;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
            Status::Ok);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  SyncPoint::GetInstance()->SetCallBack(
      "KVEngine::Skiplist::InsertDLRecord::UpdatePrev", [&](void*) {
        if (update_num % 8 == 0) {
          throw 1;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "Test::SSet::Update::Finish", [&](void*) { update_num.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  Collection* collection_ptr;
  std::string collection_name = "SortedRecoverySyncPoint";
  Status s = engine->CreateSortedCollection(collection_name, &collection_ptr);
  ASSERT_EQ(s, Status::Ok);

  try {
    for (int i = 0; i < cnt; ++i) {
      if (i % 2 == 0) {
        engine->SSet(collection_name, "key" + std::to_string(i),
                     "val" + std::to_string(i));
      } else {
        std::string new_val = "val*" + std::to_string(i);
        engine->SSet(collection_name, "key" + std::to_string(i - 1), new_val);
        TEST_SYNC_POINT("Test::SSet::Update::Finish");
      }
    }
  } catch (...) {
    delete engine;
    // reopen engine
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
              Status::Ok);
    for (int i = 0; i < cnt; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string got_val;
      Status s = engine->SGet(collection_name, key, &got_val);
      if (i % 2 != 0) {
        ASSERT_EQ(s, Status::NotFound);
      } else {
        if (i <= 14) {
          ASSERT_EQ(s, Status::Ok);
          if (i != 14) {
            ASSERT_EQ(got_val, "val*" + std::to_string(i + 1));
          } else
            ASSERT_EQ(got_val, "val" + std::to_string(i));
        } else {
          ASSERT_EQ(s, Status::NotFound);
        }
      }
    }
  }
}

// Example Case Two:
//         A <-> C <-> D
// Insert order: A, D, C
// Delete C, crash half way, now the state is:
//           A ----------> D
//           A<==>C-->D
// Then Repair

TEST_F(EngineBasicTest, TestSortedRecoverySyncPointCaseTwo) {
  Configs test_config = configs;
  test_config.max_access_threads = 16;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
            Status::Ok);

  std::atomic<bool> first_visited{false};
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  // only throw when the first call `SDelete`
  SyncPoint::GetInstance()->SetCallBack(
      "KVEngine::Skiplist::Delete::PersistNext'sPrev::After", [&](void*) {
        if (!first_visited.load()) {
          first_visited.store(true);
          throw 1;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Collection* collection_ptr;
  std::string collection_name = "SortedDeleteRecoverySyncPoint";
  Status s = engine->CreateSortedCollection(collection_name, &collection_ptr);
  ASSERT_EQ(s, Status::Ok);

  std::vector<std::string> keylists{"C", "A", "D"};
  try {
    engine->SSet(collection_name, keylists[0], "val" + keylists[0]);
    engine->SSet(collection_name, keylists[1], "val" + keylists[1]);
    engine->SSet(collection_name, keylists[2], "val" + keylists[2]);
    engine->SDelete(collection_name, keylists[0]);
  } catch (...) {
    delete engine;
    // reopen engine
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
              Status::Ok);
    // Check skiplist integrity.
    int forward_num = 0, backward_num = 0;
    auto sorted_iter = engine->NewSortedIterator(collection_name);
    // Forward traversal.
    sorted_iter->SeekToFirst();
    while (sorted_iter->Valid()) {
      forward_num++;
      sorted_iter->Next();
    }
    // Backward traversal.
    sorted_iter->SeekToLast();
    while (sorted_iter->Valid()) {
      backward_num++;
      sorted_iter->Prev();
    }
    engine->ReleaseSortedIterator(sorted_iter);
    ASSERT_EQ(forward_num, backward_num);

    std::string got_val;
    ASSERT_EQ(engine->SGet(collection_name, keylists[0], &got_val), Status::Ok);
    ASSERT_EQ(engine->SGet(collection_name, keylists[2], &got_val), Status::Ok);
    ASSERT_EQ(got_val, "val" + keylists[2]);
    ASSERT_EQ(engine->SGet(collection_name, keylists[1], &got_val), Status::Ok);
    ASSERT_EQ(got_val, "val" + keylists[1]);

    // Again delete "C".
    ASSERT_EQ(engine->SDelete(collection_name, keylists[0]), Status::Ok);
  }
}

// Example:
//   {key0, val0} <-> {key2, val2}
//   thread1 insert : {key0, val0} <-> {key1, val1} <-> {key2, val2}
//   thread2: iter
TEST_F(EngineBasicTest, TestSortedSyncPoint) {
  Configs test_config = configs;
  test_config.max_access_threads = 16;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
            Status::Ok);
  std::vector<std::thread> ths;
  std::string collection_name = "skiplist";
  Collection* collection_ptr;
  ASSERT_EQ(engine->CreateSortedCollection(collection_name, &collection_ptr),
            Status::Ok);

  engine->SSet(collection_name, "key0", "val0");
  engine->SSet(collection_name, "key2", "val2");

  std::atomic<bool> first_record(false);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  SyncPoint::GetInstance()->LoadDependency(
      {{"KVEngine::Skiplist::InsertDLRecord::UpdatePrev", "Test::Iter::key0"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // insert
  ths.emplace_back(std::thread([&]() {
    engine->SSet(collection_name, "key1", "val1");
    std::string got_val;
    ASSERT_EQ(engine->SGet(collection_name, "key1", &got_val), Status::Ok);
  }));

  // Iter
  ths.emplace_back(std::thread([&]() {
    auto sorted_iter = engine->NewSortedIterator(collection_name);
    sorted_iter->SeekToLast();
    int iter_num = 0;
    if (sorted_iter->Valid()) {
      std::string next = sorted_iter->Key();
      ASSERT_EQ(next, "key2");
      sorted_iter->Prev();
      while (sorted_iter->Valid()) {
        std::string k = sorted_iter->Key();
        TEST_SYNC_POINT("Test::Iter::" + k);
        if (k == "key0") {
          sorted_iter->Next();
          ASSERT_EQ(sorted_iter->Key(), "key1");
          sorted_iter->Prev();
        }
        sorted_iter->Prev();
        ASSERT_EQ(true, k.compare(next) < 0);
        next = k;
      }
    }
    engine->ReleaseSortedIterator(sorted_iter);
  }));
  for (auto& thread : ths) {
    thread.join();
  }
}

#endif

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
