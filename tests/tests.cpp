/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../engine/kv_engine.hpp"
#include "../engine/pmem_allocator/pmem_allocator.hpp"
#include "../engine/utils/sync_point.hpp"
#include "kvdk/engine.hpp"
#include "test_util.h"

DEFINE_string(path, "/mnt/pmem0/kvdk_unit_test",
              "Path of KVDK instance on PMem.");

using namespace KVDK_NAMESPACE;
static const uint64_t str_pool_length = 1024000;

using PutOpsFunc =
    std::function<Status(const std::string& collection, const std::string& key,
                         const std::string& value)>;
using DeleteOpsFunc = std::function<Status(const std::string& collection,
                                           const std::string& key)>;

using DestroyFunc = std::function<Status(const std::string& collection)>;

using GetOpsFunc = std::function<Status(
    const std::string& collection, const std::string& key, std::string* value)>;

enum class Types { String, Sorted, Hash };

class EngineBasicTest : public testing::Test {
 protected:
  Engine* engine = nullptr;
  Configs configs;
  std::string db_path;
  std::string backup_path;
  std::string backup_log;
  std::string str_pool;

  virtual void SetUp() override {
    str_pool.resize(str_pool_length);
    random_str(&str_pool[0], str_pool_length);
    // No logs by default, for debug, set it to All
    configs.log_level = LogLevel::Debug;
    configs.pmem_file_size = (16ULL << 30);
    configs.populate_pmem_space = false;
    configs.hash_bucket_num = (1 << 10);
    configs.pmem_segment_blocks = 8 * 1024;
    // For faster test, no interval so it would not block engine closing
    configs.background_work_interval = 0.1;
    configs.max_access_threads = 1;
    db_path = FLAGS_path;
    backup_path = FLAGS_path + "_backup";
    backup_log = FLAGS_path + ".backup";
    char cmd[1024];
    sprintf(cmd, "rm -rf %s && rm -rf %s && rm -rf %s\n", db_path.c_str(),
            backup_path.c_str(), backup_log.c_str());
    int res __attribute__((unused)) = system(cmd);
    config_option = OptionConfig::Default;
    cnt = 500;
  }

  virtual void TearDown() {
#if KVDK_DEBUG_LEVEL > 0
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->Reset();
#endif
    Destroy();
  }

  void AssignData(std::string& data, int len) {
    data.assign(str_pool.data() + (rand() % (str_pool_length - len)), len);
  }

  void Destroy() {
    // delete db_path
    char cmd[1024];
    sprintf(cmd, "rm -rf %s && rm -rf %s && rm -rf %s\n", db_path.c_str(),
            backup_path.c_str(), backup_log.c_str());
    int res __attribute__((unused)) = system(cmd);
  }

  bool ChangeConfig() {
    config_option++;
    if (config_option >= End) {
      return false;
    } else {
      ReCreateEngine();
      return engine != nullptr;
    }
  }

  void ReCreateEngine() {
    delete engine;
    engine = nullptr;
    Destroy();
    configs = CurrentConfigs();
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);
  }

  void Reboot() {
    delete engine;
    engine = nullptr;
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);
    GlobalLogger.Debug("Reboot\n");
  }

  // Return the current configuration.
  Configs CurrentConfigs() {
    switch (config_option) {
      case MultiThread:
        configs.max_access_threads = 16;
        break;
      case OptRestore:
        configs.opt_large_sorted_collection_recovery = true;
        break;
      default:
        break;
    }
    return configs;
  }

  // Put/Get/Delete
  void TestString(uint64_t n_threads) {
    auto StringPutFunc = [&](const std::string&, const std::string& key,
                             const std::string& value) -> Status {
      return engine->Put(key, value);
    };

    auto StringGetFunc = [&](const std::string&, const std::string& key,
                             std::string* value) -> Status {
      return engine->Get(key, value);
    };

    auto StringDeleteFunc = [&](const std::string&,
                                const std::string& key) -> Status {
      return engine->Delete(key);
    };

    TestEmptyKey("", StringPutFunc, StringGetFunc, StringDeleteFunc);
    auto global_func = [=](uint64_t id) {
      this->CreateBasicOperationTest("", StringPutFunc, StringGetFunc,
                                     StringDeleteFunc, id);
    };
    LaunchNThreads(n_threads, global_func);
  }

  void TestGlobalSortedCollection(const std::string& collection,
                                  const SortedCollectionConfigs& s_configs) {
    auto SortedPutFunc = [&](const std::string& collection,
                             const std::string& key,
                             const std::string& value) -> Status {
      return engine->SortedPut(collection, key, value);
    };

    auto SortedGetFunc = [&](const std::string& collection,
                             const std::string& key,
                             std::string* value) -> Status {
      return engine->SortedGet(collection, key, value);
    };

    auto SortedDeleteFunc = [&](const std::string& collection,
                                const std::string& key) -> Status {
      return engine->SortedDelete(collection, key);
    };

    auto SortedDestroyFunc = [&](const std::string& collection) {
      return engine->SortedDestroy(collection);
    };

    ASSERT_EQ(engine->SortedCreate(collection, s_configs), Status::Ok);

    TestDestroy(collection, SortedDestroyFunc, SortedPutFunc, SortedGetFunc,
                SortedDeleteFunc);

    ASSERT_EQ(engine->SortedCreate(collection, s_configs), Status::Ok);

    auto global_func = [=](uint64_t id) {
      this->CreateBasicOperationTest(collection, SortedPutFunc, SortedGetFunc,
                                     SortedDeleteFunc, id);
    };
    LaunchNThreads(configs.max_access_threads, global_func);
  }

  void TestLocalSortedCollection(Engine* engine, const std::string& collection,
                                 const SortedCollectionConfigs& s_configs) {
    auto SortedPutFunc = [&](const std::string& collection,
                             const std::string& key,
                             const std::string& value) -> Status {
      return engine->SortedPut(collection, key, value);
    };

    auto SortedGetFunc = [&](const std::string& collection,
                             const std::string& key,
                             std::string* value) -> Status {
      return engine->SortedGet(collection, key, value);
    };

    auto SortedDeleteFunc = [&](const std::string& collection,
                                const std::string& key) -> Status {
      return engine->SortedDelete(collection, key);
    };

    auto SortedDestroyFunc = [&](const std::string& collection) {
      return engine->SortedDestroy(collection);
    };

    auto AccessTest = [&](uint64_t id) {
      std::string thread_local_collection = collection + std::to_string(id);
      ASSERT_EQ(engine->SortedCreate(thread_local_collection, s_configs),
                Status::Ok);

      TestEmptyKey(thread_local_collection, SortedPutFunc, SortedGetFunc,
                   SortedDeleteFunc);
      TestDestroy(thread_local_collection, SortedDestroyFunc, SortedPutFunc,
                  SortedGetFunc, SortedDeleteFunc);

      ASSERT_EQ(engine->SortedCreate(thread_local_collection, s_configs),
                Status::Ok);
      CreateBasicOperationTest(thread_local_collection, SortedPutFunc,
                               SortedGetFunc, SortedDeleteFunc, id);
    };
    LaunchNThreads(configs.max_access_threads, AccessTest);
  }

  void TestSortedIterator(const std::string& collection,
                          bool is_local = false) {
    auto IteratingThrough = [&](uint32_t id) {
      int entries = 0;
      std::string new_collection = collection;
      if (is_local) {
        new_collection += std::to_string(id);
      }

      size_t collection_size;
      ASSERT_EQ(engine->SortedSize(new_collection, &collection_size),
                Status::Ok);

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
      ASSERT_EQ(collection_size, entries);
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

 private:
  void TestEmptyKey(const std::string& collection, PutOpsFunc PutFunc,
                    GetOpsFunc GetFunc, DeleteOpsFunc DeleteFunc) {
    std::string key, val, got_val;
    key = "", val = "val";
    ASSERT_EQ(PutFunc(collection, key, val), Status::Ok);
    ASSERT_EQ(GetFunc(collection, key, &got_val), Status::Ok);
    ASSERT_EQ(val, got_val);
    ASSERT_EQ(DeleteFunc(collection, key), Status::Ok);
    ASSERT_EQ(GetFunc(collection, key, &got_val), Status::NotFound);
    engine->ReleaseAccessThread();
  }

  void TestDestroy(const std::string& collection, DestroyFunc DestroyFunc,
                   PutOpsFunc PutFunc, GetOpsFunc GetFunc,
                   DeleteOpsFunc DeleteFunc) {
    std::string key{"test_key"};
    std::string val{"test_val"};
    std::string got_val;
    ASSERT_EQ(PutFunc(collection, key, val), Status::Ok);
    ASSERT_EQ(GetFunc(collection, key, &got_val), Status::Ok);
    ASSERT_EQ(val, got_val);
    ASSERT_EQ(DestroyFunc(collection), Status::Ok);
    ASSERT_EQ(PutFunc(collection, key, val), Status::NotFound);
    ASSERT_EQ(GetFunc(collection, key, &got_val), Status::NotFound);
    ASSERT_EQ(DeleteFunc(collection, key), Status::Ok);
  }

  void CreateBasicOperationTest(const std::string& collection,
                                PutOpsFunc PutFunc, GetOpsFunc GetFunc,
                                DeleteOpsFunc DeleteFunc, uint32_t id) {
    std::string val1, val2, got_val1, got_val2;
    int t_cnt = cnt;
    while (t_cnt--) {
      std::string key1(std::string(id + 1, 'a') + std::to_string(t_cnt));
      std::string key2(std::string(id + 1, 'b') + std::to_string(t_cnt));
      AssignData(val1, fast_random_64() % 1024);
      AssignData(val2, fast_random_64() % 1024);

      // Put
      ASSERT_EQ(PutFunc(collection, key1, val1), Status::Ok);
      ASSERT_EQ(PutFunc(collection, key2, val2), Status::Ok);

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
      ASSERT_EQ(PutFunc(collection, key2, val2), Status::Ok);
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

class BatchWriteTest : public EngineBasicTest {};

TEST_F(EngineBasicTest, TestUniqueKey) {
  std::string sorted_collection("sorted_collection");
  std::string unordered_collection("unordered_collection");
  std::string list("list");
  std::string str("str");
  std::string elem_key("elem");
  std::string val("val");

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  ASSERT_EQ(engine->Put(str, val), Status::Ok);

  ASSERT_EQ(engine->SortedCreate(sorted_collection), Status::Ok);

  ASSERT_EQ(engine->HashCreate(unordered_collection), Status::Ok);
  ASSERT_EQ(engine->HashPut(unordered_collection, elem_key, val), Status::Ok);

  ASSERT_EQ(engine->ListCreate(list), Status::Ok);
  ASSERT_EQ(engine->ListPushBack(list, elem_key), Status::Ok);

  std::string got_val;
  // Test string
  for (const std::string& string_key :
       {sorted_collection, unordered_collection, list, str}) {
    Status ret_s = string_key == str ? Status::Ok : Status::WrongType;
    std::string new_val("new_str_val");
    // Put
    ASSERT_EQ(engine->Put(string_key, new_val), ret_s);
    // Get
    ASSERT_EQ(engine->Get(string_key, &got_val), ret_s);
    if (ret_s == Status::Ok) {
      ASSERT_EQ(got_val, new_val);
    }
  }

  // Test sorted
  for (const std::string& collection_name :
       {sorted_collection, unordered_collection, list, str}) {
    Status ret_s =
        collection_name == sorted_collection ? Status::Ok : Status::WrongType;
    std::string new_val("new_sorted_val");
    // Create
    ASSERT_EQ(engine->SortedCreate(collection_name), ret_s);
    // Put
    ASSERT_EQ(engine->SortedPut(collection_name, elem_key, new_val), ret_s);
    // Get
    ASSERT_EQ(engine->SortedGet(collection_name, elem_key, &got_val), ret_s);
    if (ret_s == Status::Ok) {
      ASSERT_EQ(got_val, new_val);
    }
    // Delete elem
    ASSERT_EQ(engine->SortedDelete(collection_name, elem_key), ret_s);
  }

  // Test unordered
  for (const std::string& collection_name :
       {sorted_collection, unordered_collection, list, str}) {
    Status ret_s = collection_name == unordered_collection ? Status::Ok
                                                           : Status::WrongType;
    std::string new_val("new_unordered_val");
    // Put
    ASSERT_EQ(engine->HashPut(collection_name, elem_key, new_val), ret_s);
    // Get
    ASSERT_EQ(engine->HashGet(collection_name, elem_key, &got_val), ret_s);
    if (ret_s == Status::Ok) {
      ASSERT_EQ(got_val, new_val);
    }
    // Delete
    ASSERT_EQ(engine->HashDelete(collection_name, elem_key), ret_s);
  }

  // Test list
  for (const std::string& collection_name :
       {sorted_collection, unordered_collection, list, str}) {
    Status ret_s = collection_name == list ? Status::Ok : Status ::WrongType;
    std::string new_val_back("new_back_val");
    std::string new_val_front("new_front_val");
    size_t length;
    std::string got_val_back;
    std::string got_val_front;

    // Push
    ASSERT_EQ(engine->ListPushBack(collection_name, new_val_back), ret_s);
    ASSERT_EQ(engine->ListPushFront(collection_name, new_val_front), ret_s);
    // Pop
    ASSERT_EQ(engine->ListPopBack(collection_name, &got_val_back), ret_s);
    ASSERT_EQ(engine->ListPopFront(collection_name, &got_val_front), ret_s);
    // Length
    ASSERT_EQ(engine->ListLength(collection_name, &length), ret_s);

    if (ret_s == Status::Ok) {
      ASSERT_EQ(got_val_back, new_val_back);
      ASSERT_EQ(got_val_front, new_val_front);
      ASSERT_EQ(length, 1);
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TypeOfKey) {
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::unordered_map<std::string, ValueType> key_types;
  for (auto type : {ValueType::String, ValueType::HashSet, ValueType::List,
                    ValueType::SortedSet}) {
    std::string key = KVDKValueTypeString[type];
    key_types[key] = type;
    ValueType type_resp;
    switch (type) {
      case ValueType::String: {
        ASSERT_EQ(engine->Put(key, ""), Status::Ok);
        break;
      }
      case ValueType::HashSet: {
        ASSERT_EQ(engine->HashCreate(key), Status::Ok);
        break;
      }
      case ValueType::List: {
        ASSERT_EQ(engine->ListCreate(key), Status::Ok);
        break;
      }
      case ValueType::SortedSet: {
        ASSERT_EQ(engine->SortedCreate(key), Status::Ok);
        break;
      }
    }
    ASSERT_EQ(engine->TypeOf(key, &type_resp), Status::Ok);
    ASSERT_EQ(type_resp, type);
    ASSERT_EQ(engine->TypeOf("non-exist", &type_resp), Status::NotFound);
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestThreadManager) {
  int max_access_threads = 1;
  configs.max_access_threads = max_access_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string key("k");
  std::string val("value");
  ASSERT_EQ(engine->Put(key, val, WriteOptions()), Status::Ok);

  // Reach max access threads
  auto s = std::async(&Engine::Put, engine, key, val, WriteOptions());
  ASSERT_EQ(s.get(), Status::TooManyAccessThreads);
  // Manually release access thread
  engine->ReleaseAccessThread();
  s = std::async(&Engine::Put, engine, key, val, WriteOptions());
  ASSERT_EQ(s.get(), Status::Ok);
  // Release access thread on thread exits
  s = std::async(&Engine::Put, engine, key, val, WriteOptions());
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
  std::string sorted_collection_after_snapshot(
      "sorted_collection_after_snapshot");
  ASSERT_EQ(engine->SortedCreate(sorted_collection), Status::Ok);

  bool snapshot_done(false);
  std::atomic_uint64_t set_finished_threads(0);
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
      WriteOptions write_options;
      ASSERT_EQ(engine->Put(key1, key1, write_options), Status::Ok);
      ASSERT_EQ(engine->Put(key2, key2, write_options), Status::Ok);
      ASSERT_EQ(engine->SortedPut(sorted_collection, key1, key1), Status::Ok);
      ASSERT_EQ(engine->SortedPut(sorted_collection, key2, key2), Status::Ok);
    }
    // Wait snapshot done
    set_finished_threads.fetch_add(1);
    engine->ReleaseAccessThread();
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
      ASSERT_EQ(engine->Put(key1, "updated " + key1), Status::Ok);
      ASSERT_EQ(engine->Delete(key1), Status::Ok);
      ASSERT_EQ(engine->Put(key3, key3), Status::Ok);
      ASSERT_EQ(engine->SortedPut(sorted_collection, key1, "updated " + key1),
                Status::Ok);
      ASSERT_EQ(engine->SortedDelete(sorted_collection, key2), Status::Ok);
      ASSERT_EQ(engine->SortedPut(sorted_collection, key3, key3), Status::Ok);
      ASSERT_EQ(engine->SortedPut(sorted_collection_after_snapshot, key1, key1),
                Ok);
    }
  };

  std::vector<std::thread> ths;
  for (size_t i = 0; i < num_threads; i++) {
    ths.emplace_back(std::thread(WriteThread, i));
  }
  // wait until all threads insert done
  while (set_finished_threads.load() != num_threads) {
    asm volatile("pause");
  }
  Snapshot* snapshot = engine->GetSnapshot(true);
  // Insert a new collection after snapshot
  ASSERT_EQ(engine->SortedCreate(sorted_collection_after_snapshot), Status::Ok);
  {
    std::lock_guard<SpinMutex> ul(spin);
    snapshot_done = true;
    cv.notify_all();
  }
  engine->Backup(backup_log, snapshot);
  for (auto& t : ths) {
    t.join();
  }

  Iterator* snapshot_iter =
      engine->NewSortedIterator(sorted_collection, snapshot);
  // Destroyed collection still should be accessable by snapshot_iter
  engine->SortedDestroy(sorted_collection);

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

  snapshot_iter =
      engine->NewSortedIterator(sorted_collection_after_snapshot, snapshot);
  snapshot_iter->SeekToFirst();
  ASSERT_FALSE(snapshot_iter->Valid());
  engine->ReleaseSortedIterator(snapshot_iter);

  delete engine;

  auto Validation = [&]() {
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

        ASSERT_EQ(engine->Get(key1, &got_v1), Status::Ok);
        ASSERT_EQ(engine->Get(key2, &got_v2), Status::Ok);
        ASSERT_EQ(engine->Get(key3, &got_v3), Status::NotFound);
        ASSERT_EQ(got_v1, key1);
        ASSERT_EQ(got_v2, key2);

        ASSERT_EQ(engine->SortedGet(sorted_collection, key1, &got_v1),
                  Status::Ok);
        ASSERT_EQ(engine->SortedGet(sorted_collection, key2, &got_v2),
                  Status::Ok);
        ASSERT_EQ(engine->SortedGet(sorted_collection, key3, &got_v3),
                  Status::NotFound);
        ASSERT_EQ(got_v1, key1);
        ASSERT_EQ(got_v2, key2);
        ASSERT_EQ(
            engine->SortedGet(sorted_collection_after_snapshot, key1, &got_v1),
            Status::NotFound);
      }
    }

    uint64_t sorted_iter_cnt = 0;
    auto sorted_iter = engine->NewSortedIterator(sorted_collection);
    ASSERT_TRUE(sorted_iter != nullptr);
    sorted_iter->SeekToFirst();
    while (sorted_iter->Valid()) {
      ASSERT_TRUE(sorted_iter->Valid());
      sorted_iter_cnt++;
      ASSERT_EQ(sorted_iter->Key(), sorted_iter->Value());
      sorted_iter->Next();
    }
    ASSERT_EQ(sorted_iter_cnt, num_threads * count * 2);
    engine->ReleaseSortedIterator(sorted_iter);
    ASSERT_EQ(engine->NewSortedIterator(sorted_collection_after_snapshot),
              nullptr);
  };

  std::vector<int> opt_restore_skiplists{0, 1};
  for (auto is_opt : opt_restore_skiplists) {
    configs.opt_large_sorted_collection_recovery = is_opt;

    ASSERT_EQ(
        Engine::Restore(backup_path, backup_log, &engine, configs, stdout),
        Status::Ok);
    Validation();
    delete engine;

    configs.recover_to_checkpoint = true;
    ASSERT_EQ(Engine::Open(db_path, &engine, configs, stdout), Status::Ok);
    Validation();
    delete engine;
  }
}

TEST_F(EngineBasicTest, TestBasicStringOperations) {
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  do {
    TestString(configs.max_access_threads);
  } while (ChangeConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestStringModify) {
  struct IncNArgs {
    size_t incr_by;
    size_t result;
  };
  auto IncN = [](const std::string* old_val, std::string* new_value,
                 void* modify_args) {
    assert(modify_args);
    IncNArgs* args = static_cast<IncNArgs*>(modify_args);
    size_t old_num;
    if (old_val == nullptr) {
      // if key not exist, start from 0
      old_num = 0;
    } else {
      if (old_val->size() != sizeof(size_t)) {
        return ModifyOperation::Abort;
      } else {
        memcpy(&old_num, old_val->data(), sizeof(size_t));
      }
    }
    args->result = old_num + args->incr_by;

    new_value->assign((char*)&args->result, sizeof(size_t));
    return ModifyOperation::Write;
  };

  int num_threads = 16;
  int ops_per_thread = 1000;
  uint64_t incr_by = 5;
  configs.max_access_threads = num_threads;

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string incr_key = "plus";

  std::string wrong_value_key = "wrong_value";
  ASSERT_EQ(engine->Put(wrong_value_key, std::string(10, 'a')), Status::Ok);
  engine->ReleaseAccessThread();

  auto TestModify = [&](int) {
    IncNArgs args{5, 0};
    ASSERT_EQ(engine->Modify(wrong_value_key, IncN, &args), Status::Abort);
    for (int i = 0; i < ops_per_thread; i++) {
      size_t prev_num = args.result;
      ASSERT_EQ(engine->Modify(incr_key, IncN, &args), Status::Ok);
      ASSERT_TRUE(args.result > prev_num);
    }
  };

  LaunchNThreads(num_threads, TestModify);
  std::string val;
  size_t val_num;
  ASSERT_EQ(engine->Get(incr_key, &val), Status::Ok);
  ASSERT_EQ(val.size(), sizeof(size_t));
  memcpy(&val_num, val.data(), sizeof(size_t));
  ASSERT_EQ(val_num, ops_per_thread * num_threads * incr_by);
  delete engine;
}

TEST_F(EngineBasicTest, BatchWriteSorted) {
  size_t num_threads = 1;
  configs.max_access_threads = num_threads + 1;
  for (int index_with_hashtable : {0, 1}) {
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);
    size_t batch_size = 100;
    size_t count = 100 * batch_size;

    std::string collection_name{"sorted" +
                                std::to_string(index_with_hashtable)};
    SortedCollectionConfigs s_configs;
    s_configs.index_with_hashtable = index_with_hashtable;
    ASSERT_EQ(engine->SortedCreate(collection_name), Status::Ok);

    std::vector<std::vector<std::string>> elems(num_threads);
    std::vector<std::vector<std::string>> values(num_threads);
    for (size_t tid = 0; tid < num_threads; tid++) {
      for (size_t i = 0; i < count; i++) {
        elems[tid].push_back(std::to_string(tid) + "_" + std::to_string(i));
        values[tid].emplace_back();
      }
    }

    auto Put = [&](size_t tid) {
      for (size_t i = 0; i < count; i++) {
        values[tid][i] = GetRandomString(120);
        ASSERT_EQ(
            engine->SortedPut(collection_name, elems[tid][i], values[tid][i]),
            Status::Ok);
      }
    };

    auto BatchWrite = [&](size_t tid) {
      auto batch = engine->WriteBatchCreate();
      for (size_t i = 0; i < count; i++) {
        if (i % 2 == 0) {
          values[tid][i] = GetRandomString(120);
          batch->SortedPut(collection_name, elems[tid][i], values[tid][i]);
        } else {
          values[tid][i].clear();
          batch->SortedDelete(collection_name, elems[tid][i]);
        }
        if ((i + 1) % batch_size == 0) {
          // Delete a non-existing elem
          batch->SortedDelete(collection_name, "non-existing");
          ASSERT_EQ(engine->BatchWrite(batch), Status::Ok);
          batch->Clear();
        }
      }
    };

    auto Check = [&](size_t tid) {
      for (size_t i = 0; i < count; i++) {
        std::string val_resp;
        if (values[tid][i].empty()) {
          ASSERT_EQ(
              engine->SortedGet(collection_name, elems[tid][i], &val_resp),
              Status::NotFound);
        } else {
          ASSERT_EQ(
              engine->SortedGet(collection_name, elems[tid][i], &val_resp),
              Status::Ok);
          ASSERT_EQ(values[tid][i], val_resp);
        }
      }
    };

    LaunchNThreads(num_threads, Put);
    LaunchNThreads(num_threads, Check);
    LaunchNThreads(num_threads, BatchWrite);
    LaunchNThreads(num_threads, Check);

    Reboot();
    LaunchNThreads(num_threads, Check);
    LaunchNThreads(num_threads, Put);
    LaunchNThreads(num_threads, Check);
    LaunchNThreads(num_threads, BatchWrite);
    LaunchNThreads(num_threads, Check);

    delete engine;
  }
}

TEST_F(BatchWriteTest, BatchWriteString) {
  size_t num_threads = 16;
  configs.max_access_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  size_t batch_size = 100;
  size_t count = 100 * batch_size;
  std::vector<std::vector<std::string>> keys(num_threads);
  std::vector<std::vector<std::string>> values(num_threads);
  for (size_t tid = 0; tid < num_threads; tid++) {
    for (size_t i = 0; i < count; i++) {
      keys[tid].push_back(std::to_string(tid) + "_" + std::to_string(i));
      values[tid].emplace_back();
    }
  }

  auto Put = [&](size_t tid) {
    for (size_t i = 0; i < count; i++) {
      values[tid][i] = GetRandomString(120);
      ASSERT_EQ(engine->Put(keys[tid][i], values[tid][i]), Status::Ok);
    }
  };

  auto BatchWrite = [&](size_t tid) {
    auto batch = engine->WriteBatchCreate();
    for (size_t i = 0; i < count; i++) {
      if (i % 2 == 0) {
        values[tid][i] = GetRandomString(120);
        // The first Put is overwritten by the second Put.
        batch->StringPut(keys[tid][i], GetRandomString(120));
        batch->StringPut(keys[tid][i], values[tid][i]);
      } else {
        values[tid][i].clear();
        batch->StringDelete(keys[tid][i]);
        batch->StringDelete(keys[tid][i]);
      }
      if ((i + 1) % batch_size == 0) {
        // Delete a non-existing key
        batch->StringDelete("non-existing");
        ASSERT_EQ(batch->Size(), batch_size + 1);
        ASSERT_EQ(engine->BatchWrite(batch), Status::Ok);
        batch->Clear();
      }
    }
  };

  auto Check = [&](size_t tid) {
    for (size_t i = 0; i < count; i++) {
      std::string val_resp;
      if (values[tid][i].empty()) {
        ASSERT_EQ(engine->Get(keys[tid][i], &val_resp), Status::NotFound);
      } else {
        ASSERT_EQ(engine->Get(keys[tid][i], &val_resp), Status::Ok);
        ASSERT_EQ(values[tid][i], val_resp);
      }
    }
  };

  LaunchNThreads(num_threads, Put);
  LaunchNThreads(num_threads, Check);
  LaunchNThreads(num_threads, BatchWrite);
  LaunchNThreads(num_threads, Check);

  Reboot();

  LaunchNThreads(num_threads, Check);
  LaunchNThreads(num_threads, Put);
  LaunchNThreads(num_threads, Check);
  LaunchNThreads(num_threads, BatchWrite);
  LaunchNThreads(num_threads, Check);

  delete engine;
}

TEST_F(BatchWriteTest, BatchWriteHash) {
  size_t num_threads = 16;
  configs.max_access_threads = num_threads + 1;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  size_t batch_size = 100;
  size_t count = 100 * batch_size;

  std::string key{"hash"};
  ASSERT_EQ(engine->HashCreate(key), Status::Ok);

  std::vector<std::vector<std::string>> fields(num_threads);
  std::vector<std::vector<std::string>> values(num_threads);
  for (size_t tid = 0; tid < num_threads; tid++) {
    for (size_t i = 0; i < count; i++) {
      fields[tid].push_back(std::to_string(tid) + "_" + std::to_string(i));
      values[tid].emplace_back();
    }
  }

  auto Put = [&](size_t tid) {
    for (size_t i = 0; i < count; i++) {
      values[tid][i] = GetRandomString(120);
      ASSERT_EQ(engine->HashPut(key, fields[tid][i], values[tid][i]),
                Status::Ok);
    }
  };

  auto BatchWrite = [&](size_t tid) {
    auto batch = engine->WriteBatchCreate();
    for (size_t i = 0; i < count; i++) {
      if (i % 2 == 0) {
        values[tid][i] = GetRandomString(120);
        batch->HashPut(key, fields[tid][i], values[tid][i]);
      } else {
        values[tid][i].clear();
        batch->HashDelete(key, fields[tid][i]);
      }
      if ((i + 1) % batch_size == 0) {
        // Delete a non-existing key
        batch->HashDelete(key, "non-existing");
        ASSERT_EQ(engine->BatchWrite(batch), Status::Ok);
        batch->Clear();
      }
    }
  };

  auto Check = [&](size_t tid) {
    for (size_t i = 0; i < count; i++) {
      std::string val_resp;
      if (values[tid][i].empty()) {
        ASSERT_EQ(engine->HashGet(key, fields[tid][i], &val_resp),
                  Status::NotFound);
      } else {
        ASSERT_EQ(engine->HashGet(key, fields[tid][i], &val_resp), Status::Ok);
        ASSERT_EQ(values[tid][i], val_resp);
      }
    }
  };

  LaunchNThreads(num_threads, Put);
  LaunchNThreads(num_threads, Check);
  LaunchNThreads(num_threads, BatchWrite);
  LaunchNThreads(num_threads, Check);

  Reboot();

  LaunchNThreads(num_threads, Check);
  LaunchNThreads(num_threads, Put);
  LaunchNThreads(num_threads, Check);
  LaunchNThreads(num_threads, BatchWrite);
  LaunchNThreads(num_threads, Check);

  delete engine;
}

TEST_F(EngineBasicTest, TestLocalSortedCollection) {
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  do {
    for (int index_with_hashtable : {0, 1}) {
      SortedCollectionConfigs s_configs;
      s_configs.index_with_hashtable = index_with_hashtable;
      TestLocalSortedCollection(engine,
                                "hash_index" +
                                    std::to_string(index_with_hashtable) +
                                    "thread_skiplist",
                                s_configs);
      TestSortedIterator("hash_index" + std::to_string(index_with_hashtable) +
                             "thread_skiplist",
                         true);
    }
  } while (ChangeConfig());

  delete engine;
}

TEST_F(EngineBasicTest, TestGlobalSortedCollection) {
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  do {
    for (int index_with_hashtable : {0, 1}) {
      SortedCollectionConfigs s_configs;
      s_configs.index_with_hashtable = index_with_hashtable;
      std::string collection =
          std::to_string(index_with_hashtable) + "global_skiplist";
      TestGlobalSortedCollection(collection, s_configs);
      TestSortedIterator(collection, false);
    }
  } while (ChangeConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestSeek) {
  std::string val;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  // Test Seek
  std::string collection = "col1";
  ASSERT_EQ(engine->SortedCreate(collection), Status::Ok);
  uint64_t z = 0;
  auto zero_filled_str = uint64_to_string(z);
  ASSERT_EQ(engine->SortedPut(collection, zero_filled_str, zero_filled_str),
            Status::Ok);
  ASSERT_EQ(engine->SortedGet(collection, zero_filled_str, &val), Status::Ok);
  auto iter = engine->NewSortedIterator(collection);
  ASSERT_NE(iter, nullptr);
  iter->Seek(zero_filled_str);
  ASSERT_TRUE(iter->Valid());

  // Test SeekToFirst
  collection.assign("col2");
  ASSERT_EQ(engine->SortedCreate(collection), Status::Ok);
  ASSERT_EQ(engine->SortedPut(collection, "foo", "bar"), Status::Ok);
  ASSERT_EQ(engine->SortedGet(collection, "foo", &val), Status::Ok);
  ASSERT_EQ(engine->SortedDelete(collection, "foo"), Status::Ok);
  ASSERT_EQ(engine->SortedGet(collection, "foo", &val), Status::NotFound);
  ASSERT_EQ(engine->SortedPut(collection, "foo2", "bar2"), Status::Ok);
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
  size_t num_threads = 16;
  configs.max_access_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  size_t count = 1000;
  auto PutupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (size_t i = 1; i <= count; i++) {
      std::string key(key_prefix + std::to_string(i));
      std::string val(std::to_string(i));
      std::string update_val(std::to_string(i * 2));
      ASSERT_EQ(engine->Put(key, val), Status::Ok);
      if ((i * id) % 2 == 1) {
        ASSERT_EQ(engine->Delete(key), Status::Ok);
        if ((i * id) % 3 == 0) {
          // Update after delete
          ASSERT_EQ(engine->Put(key, update_val), Status::Ok);
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

  LaunchNThreads(num_threads, PutupEngine);

  delete engine;

  // reopen and restore engine and try gets
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  for (size_t id = 0; id < num_threads; id++) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (size_t i = 1; i <= count; i++) {
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

TEST_F(EngineBasicTest, TestStringLargeValue) {
  configs.pmem_block_size = (1UL << 6);
  configs.pmem_segment_blocks = (1UL << 24);
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  for (size_t sz = 1024; sz < (1UL << 30); sz *= 2) {
    std::string key{"large"};
    std::string value(sz, 'a');
    std::string sink;

    ASSERT_EQ(engine->Put(key, value), Status::Ok);
    ASSERT_EQ(engine->Get(key, &sink), Status::Ok);
    ASSERT_EQ(value, sink);
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestSortedRestore) {
  size_t num_threads = 16;
  configs.max_access_threads = num_threads;
  for (int opt_large_sorted_collection_recovery : {0, 1}) {
    for (int index_with_hashtable : {0, 1}) {
      SortedCollectionConfigs s_configs;
      s_configs.index_with_hashtable = index_with_hashtable;
      ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
                Status::Ok);
      // Test destroy a collction
      std::string empty_skiplist("empty_skiplist");
      size_t empty_skiplist_size;
      ASSERT_EQ(engine->SortedCreate(empty_skiplist, s_configs), Status::Ok);
      ASSERT_EQ(engine->SortedSize(empty_skiplist, &empty_skiplist_size),
                Status::Ok);
      ASSERT_EQ(empty_skiplist_size, 0);
      ASSERT_EQ(engine->SortedDestroy(empty_skiplist), Status::Ok);
      ASSERT_EQ(engine->SortedSize(empty_skiplist, &empty_skiplist_size),
                Status::NotFound);
      // insert and delete some keys, then re-insert some deleted keys
      int count = 100;
      std::string global_skiplist =
          std::to_string(index_with_hashtable) + "skiplist";
      ASSERT_EQ(engine->SortedCreate(global_skiplist, s_configs), Status::Ok);
      std::string thread_skiplist =
          std::to_string(index_with_hashtable) + "t_skiplist";
      auto PutupEngine = [&](uint32_t id) {
        std::string key_prefix(id, 'a');
        std::string got_val;
        std::string t_skiplist(thread_skiplist + std::to_string(id));
        ASSERT_EQ(engine->SortedCreate(t_skiplist, s_configs), Status::Ok);
        for (int i = 1; i <= count; i++) {
          auto key = key_prefix + std::to_string(i);
          auto overall_val = std::to_string(i);
          auto t_val = std::to_string(i * 2);
          ASSERT_EQ(engine->SortedPut(global_skiplist, key, overall_val),
                    Status::Ok);
          ASSERT_EQ(engine->SortedPut(t_skiplist, key, t_val), Status::Ok);
          ASSERT_EQ(engine->SortedGet(global_skiplist, key, &got_val),
                    Status::Ok);
          ASSERT_EQ(got_val, overall_val);
          ASSERT_EQ(engine->SortedGet(t_skiplist, key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, t_val);
          if (i % 2 == 1) {
            ASSERT_EQ(engine->SortedDelete(global_skiplist, key), Status::Ok);
            ASSERT_EQ(engine->SortedDelete(t_skiplist, key), Status::Ok);
            ASSERT_EQ(engine->SortedGet(global_skiplist, key, &got_val),
                      Status::NotFound);
            ASSERT_EQ(engine->SortedGet(t_skiplist, key, &got_val),
                      Status::NotFound);
          }
        }
      };

      LaunchNThreads(num_threads, PutupEngine);

      delete engine;

      GlobalLogger.Debug(
          "Restore with opt_large_sorted_collection_restore: %d\n",
          opt_large_sorted_collection_recovery);
      configs.max_access_threads = num_threads;
      configs.opt_large_sorted_collection_recovery =
          opt_large_sorted_collection_recovery;
      // reopen and restore engine and try gets
      ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
                Status::Ok);
      ASSERT_EQ(engine->SortedSize(empty_skiplist, &empty_skiplist_size),
                Status::NotFound);
      for (size_t id = 0; id < num_threads; id++) {
        std::string t_skiplist(thread_skiplist + std::to_string(id));
        std::string key_prefix(id, 'a');
        std::string got_val;
        for (int i = 1; i <= count; i++) {
          std::string key(key_prefix + std::to_string(i));
          std::string overall_val(std::to_string(i));
          std::string t_val(std::to_string(i * 2));
          Status s = engine->SortedGet(global_skiplist, key, &got_val);
          if (i % 2 == 1) {
            ASSERT_EQ(s, Status::NotFound);
          } else {
            ASSERT_EQ(s, Status::Ok);
            ASSERT_EQ(got_val, overall_val);
          }
          s = engine->SortedGet(t_skiplist, key, &got_val);
          if (i % 2 == 1) {
            ASSERT_EQ(s, Status::NotFound);
          } else {
            ASSERT_EQ(s, Status::Ok);
            ASSERT_EQ(got_val, t_val);
          }
        }

        size_t t_skiplist_size;
        ASSERT_EQ(engine->SortedSize(t_skiplist, &t_skiplist_size), Status::Ok);

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
        ASSERT_EQ(data_entries_scan, t_skiplist_size);

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

      size_t global_skiplist_size;
      ASSERT_EQ(engine->SortedSize(global_skiplist, &global_skiplist_size),
                Status::Ok);
      int data_entries_scan = 0;
      auto iter = engine->NewSortedIterator(global_skiplist);
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
      ASSERT_EQ(global_skiplist_size, data_entries_scan);

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
}

TEST_F(EngineBasicTest, TestMultiThreadSortedRestore) {
  size_t num_threads = 16;
  size_t num_collections = 16;
  configs.max_access_threads = num_threads;
  configs.opt_large_sorted_collection_recovery = true;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  size_t count = 1024;

  std::set<std::string> avg_nums, random_nums;
  for (size_t i = 1; i <= count; ++i) {
    std::string average_skiplist("a_skiplist" +
                                 std::to_string(i % num_collections));
    ASSERT_EQ(engine->SortedCreate(average_skiplist), Status::Ok);
  }
  for (size_t i = 0; i < num_threads; ++i) {
    std::string r_skiplist("r_skiplist" + std::to_string(i));
    ASSERT_EQ(engine->SortedCreate(r_skiplist), Status::Ok);
  }
  auto PutupEngine = [&](size_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (size_t i = 1; i <= count; ++i) {
      std::string average_skiplist("a_skiplist" +
                                   std::to_string(i % num_collections));

      std::string r_skiplist("r_skiplist" +
                             std::to_string(rand() % num_threads));

      auto key = key_prefix + std::to_string(i);
      auto average_val = std::to_string(i);
      ASSERT_EQ(engine->SortedPut(average_skiplist, key, average_val),
                Status::Ok);
      ASSERT_EQ(engine->SortedGet(average_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, average_val);
      auto r_val = std::to_string(i * 2);
      ASSERT_EQ(engine->SortedPut(r_skiplist, key, r_val), Status::Ok);
      ASSERT_EQ(engine->SortedGet(r_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, r_val);
      if ((rand() % i) == 0) {
        ASSERT_EQ(engine->SortedDelete(average_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SortedDelete(r_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SortedGet(average_skiplist, key, &got_val),
                  Status::NotFound);
        ASSERT_EQ(engine->SortedGet(r_skiplist, key, &got_val),
                  Status::NotFound);
      }
    }
  };

  LaunchNThreads(num_threads, PutupEngine);

  delete engine;
  // reopen and restore engine and try gets
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  auto skiplists = (dynamic_cast<KVEngine*>(engine))->GetSkiplists();
  for (auto s : skiplists) {
    if (s.second->IndexWithHashtable()) {
      ASSERT_EQ(s.second->CheckIndex(), Status::Ok);
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestList) {
  size_t num_threads = 16;
  size_t count = 1000;
  configs.max_access_threads = num_threads + 1;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::vector<std::vector<std::string>> elems_vec(num_threads);
  std::vector<std::string> key_vec(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    key_vec[i] = "List_" + std::to_string(i);
    ASSERT_EQ(engine->ListCreate(key_vec[i]), Status::Ok);
    ASSERT_EQ(engine->ListDestroy(key_vec[i]), Status::Ok);
    ASSERT_EQ(engine->ListCreate(key_vec[i]), Status::Ok);
    for (size_t j = 0; j < count; j++) {
      elems_vec[i].push_back(std::to_string(i) + "_" + std::to_string(j));
    }
  }
  std::vector<std::list<std::string>> list_copy_vec(num_threads);

  auto LPush = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto const& elems = elems_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t sz;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->ListPushFront(key, elems[j]), Status::Ok);
      list_copy.push_front(elems[j]);
      ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
      ASSERT_EQ(sz, list_copy.size());
    }
  };

  auto RPush = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto const& elems = elems_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t sz;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->ListPushBack(key, elems[j]), Status::Ok);
      list_copy_vec[tid].push_back(elems[j]);
      ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
      ASSERT_EQ(sz, list_copy.size());
    }
  };

  auto LPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    std::string value_got;
    size_t sz;
    for (size_t j = 0; j < count; j++) {
      if (list_copy.empty()) {
        ASSERT_EQ(engine->ListPopFront(key, &value_got), Status::NotFound);
        break;
      }
      ASSERT_EQ(engine->ListPopFront(key, &value_got), Status::Ok);
      ASSERT_EQ(list_copy.front(), value_got);
      list_copy.pop_front();
      ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
      ASSERT_EQ(sz, list_copy.size());
    }
  };

  auto RPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    std::string value_got;
    size_t sz;
    for (size_t j = 0; j < count; j++) {
      if (list_copy.empty()) {
        ASSERT_EQ(engine->ListPopFront(key, &value_got), Status::NotFound);
        break;
      }
      ASSERT_EQ(engine->ListPopBack(key, &value_got), Status::Ok);
      ASSERT_EQ(list_copy.back(), value_got);
      list_copy.pop_back();
      ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
      ASSERT_EQ(sz, list_copy.size());
    }
  };

  auto LBatchPush = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto const& elems = elems_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    for (size_t j = 0; j < count; j++) {
      list_copy.push_front(elems[j]);
    }
    ASSERT_EQ(engine->ListBatchPushFront(key, elems), Status::Ok);
    size_t sz;
    ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto RBatchPush = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto const& elems = elems_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    for (size_t j = 0; j < count; j++) {
      list_copy.push_back(elems[j]);
    }
    ASSERT_EQ(engine->ListBatchPushBack(key, elems), Status::Ok);
    size_t sz;
    ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto LBatchPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    std::vector<std::string> elems_resp;
    ASSERT_EQ(engine->ListBatchPopFront(key, count, &elems_resp), Status::Ok);
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(list_copy.front(), elems_resp[j]);
      list_copy.pop_front();
    }
    size_t sz;
    ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto RBatchPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    std::vector<std::string> elems_resp;
    ASSERT_EQ(engine->ListBatchPopBack(key, count, &elems_resp), Status::Ok);
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(list_copy.back(), elems_resp[j]);
      list_copy.pop_back();
    }
    size_t sz;
    ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto RPushLPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];

    auto elem_copy = list_copy.front();
    list_copy.push_back(elem_copy);
    list_copy.pop_front();

    std::string elem;
    ASSERT_EQ(engine->ListMove(key, 0, key, -1, &elem), Status::Ok);
    ASSERT_EQ(elem, elem_copy);

    size_t sz;
    ASSERT_EQ(engine->ListLength(key, &sz), Status::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto ListIterate = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];

    auto iter = engine->ListCreateIterator(key);
    ASSERT_TRUE((list_copy.empty() && iter == nullptr) || (iter != nullptr));
    if (iter != nullptr) {
      iter->Seek(0);
      for (auto iter2 = list_copy.begin(); iter2 != list_copy.end(); iter2++) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->Value(), *iter2);
        iter->Next();
      }

      iter->Seek(-1);
      for (auto iter2 = list_copy.rbegin(); iter2 != list_copy.rend();
           iter2++) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->Value(), *iter2);
        iter->Prev();
      }
    }
  };

  auto ListInsertPutRemove = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t len;
    size_t const insert_pos = 5;
    std::string elem;

    ASSERT_EQ(engine->ListLength(key, &len), Status::Ok);
    ASSERT_GT(len, insert_pos);

    auto iter = engine->ListCreateIterator(key);
    ASSERT_NE(iter, nullptr);

    iter->Seek(insert_pos);
    auto iter2 = std::next(list_copy.begin(), insert_pos);
    ASSERT_EQ(iter->Value(), *iter2);

    elem = *iter2 + "_before";
    ASSERT_EQ(engine->ListInsertBefore(iter, elem), Status::Ok);
    iter2 = list_copy.insert(iter2, elem);
    ASSERT_EQ(iter->Value(), *iter2);

    iter->Prev();
    iter->Prev();
    --iter2;
    --iter2;
    ASSERT_EQ(iter->Value(), *iter2);
    elem = *iter2 + "_new";
    ASSERT_EQ(engine->ListReplace(iter, elem), Status::Ok);
    *iter2 = elem;
    ASSERT_EQ(iter->Value(), *iter2);

    iter->Prev();
    iter->Prev();
    --iter2;
    --iter2;
    ASSERT_EQ(iter->Value(), *iter2);
    ASSERT_EQ(engine->ListErase(iter), Status::Ok);
    iter2 = list_copy.erase(iter2);
    ASSERT_EQ(iter->Value(), *iter2);
  };

  for (size_t i = 0; i < 3; i++) {
    LaunchNThreads(num_threads, LPop);
    LaunchNThreads(num_threads, RPop);
    LaunchNThreads(num_threads, LPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LBatchPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RBatchPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LBatchPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RBatchPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LPush);
    LaunchNThreads(num_threads, ListIterate);
    for (size_t j = 0; j < 100; j++) {
      LaunchNThreads(num_threads, ListInsertPutRemove);
      LaunchNThreads(num_threads, ListIterate);
      LaunchNThreads(num_threads, RPushLPop);
      LaunchNThreads(num_threads, ListIterate);
    }
    Reboot();
  }

  delete engine;
}

TEST_F(EngineBasicTest, TestHash) {
  size_t num_threads = 16;
  size_t count = 1000;
  configs.max_access_threads = num_threads + 1;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string key{"Hash"};
  ASSERT_EQ(engine->HashCreate(key), Status::Ok);
  ASSERT_EQ(engine->HashDestroy(key), Status::Ok);
  ASSERT_EQ(engine->HashCreate(key), Status::Ok);
  using umap = std::unordered_map<std::string, std::string>;
  std::vector<umap> local_copies(num_threads);
  std::mutex mu;

  auto HPut = [&](size_t tid) {
    umap& local_copy = local_copies[tid];
    for (size_t j = 0; j < count; j++) {
      std::string field{std::to_string(tid) + "_" + GetRandomString(10)};
      std::string value{GetRandomString(120)};
      ASSERT_EQ(engine->HashPut(key, field, value), Status::Ok);
      local_copy[field] = value;
    }
  };

  auto HGet = [&](size_t tid) {
    umap const& local_copy = local_copies[tid];
    for (auto const& kv : local_copy) {
      std::string resp;
      ASSERT_EQ(engine->HashGet(key, kv.first, &resp), Status::Ok);
      ASSERT_EQ(resp, kv.second) << "Field:\t" << kv.first << "\n";
    }
  };

  auto HDelete = [&](size_t tid) {
    umap& local_copy = local_copies[tid];
    std::string sink;
    for (size_t i = 0; i < count / 2; i++) {
      auto iter = local_copy.begin();
      ASSERT_EQ(engine->HashDelete(key, iter->first), Status::Ok);
      ASSERT_EQ(engine->HashGet(key, iter->first, &sink), Status::NotFound);
      local_copy.erase(iter);
    }
  };

  auto HashLength = [&](size_t) {
    size_t len = 0;
    ASSERT_EQ(engine->HashLength(key, &len), Status::Ok);
    size_t cnt = 0;
    for (size_t tid = 0; tid < num_threads; tid++) {
      cnt += local_copies[tid].size();
    }
    ASSERT_EQ(len, cnt);
  };

  auto HashIterate = [&](size_t tid) {
    umap combined;
    for (size_t tid = 0; tid < num_threads; tid++) {
      umap const& local_copy = local_copies[tid];
      for (auto const& kv : local_copy) {
        combined[kv.first] = kv.second;
      }
    }

    auto iter = engine->HashCreateIterator(key);

    ASSERT_NE(iter, nullptr);
    size_t cnt = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ++cnt;
      ASSERT_EQ(combined[iter->Key()], iter->Value());
    }
    ASSERT_EQ(cnt, combined.size());

    cnt = 0;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ++cnt;
      ASSERT_EQ(combined[iter->Key()], iter->Value());
    }
    ASSERT_EQ(cnt, combined.size());

    std::regex re1{".*"};
    std::regex re2{std::to_string(tid) + "_.*"};
    size_t match_cnt1 = 0;
    size_t match_cnt2 = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      match_cnt1 += iter->MatchKey(re1) ? 1 : 0;
      match_cnt2 += iter->MatchKey(re2) ? 1 : 0;
    }
    ASSERT_EQ(match_cnt1, combined.size());
    ASSERT_EQ(match_cnt2, local_copies[tid].size());
  };

  std::string counter{"counter"};
  auto HashModify = [&](size_t) {
    struct FetchAddArgs {
      size_t old;
      size_t n;
    };
    auto FetchAdd = [](std::string const* old_val, std::string* new_value,
                       void* args) {
      FetchAddArgs* fa_args = static_cast<FetchAddArgs*>(args);
      if (old_val != nullptr) {
        try {
          fa_args->old = std::stoul(*old_val);
        } catch (std::invalid_argument const&) {
          return ModifyOperation::Abort;
        } catch (std::out_of_range const&) {
          return ModifyOperation::Abort;
        }
      } else {
        fa_args->old = 0;
      }
      new_value->assign(std::to_string(fa_args->old + fa_args->n));
      return ModifyOperation::Write;
    };

    FetchAddArgs args;
    args.n = 1;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->HashModify(key, counter, FetchAdd, &args), Status::Ok);
    }
  };

  for (size_t i = 0; i < 3; i++) {
    Reboot();
    LaunchNThreads(num_threads, HPut);
    LaunchNThreads(num_threads, HGet);
    LaunchNThreads(num_threads, HDelete);
    LaunchNThreads(num_threads, HashIterate);
    LaunchNThreads(num_threads, HashLength);
    LaunchNThreads(num_threads, HPut);
    LaunchNThreads(num_threads, HGet);
    LaunchNThreads(num_threads, HDelete);
    LaunchNThreads(num_threads, HashIterate);
    LaunchNThreads(num_threads, HashLength);
  }
  LaunchNThreads(num_threads, HashModify);
  std::string resp;
  ASSERT_EQ(engine->HashGet(key, counter, &resp), Status::Ok);
  ASSERT_EQ(resp, std::to_string(num_threads * count));

  delete engine;
}

TEST_F(EngineBasicTest, TestStringHotspot) {
  size_t n_thread_reading = 16;
  size_t n_thread_writing = 16;
  configs.max_access_threads = n_thread_writing + n_thread_reading;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  size_t count = 100000;
  std::string key{"SuperHotspot"};
  std::string val1(1024, 'a');
  std::string val2(1023, 'b');

  ASSERT_EQ(engine->Put(key, val1), Status::Ok);
  engine->ReleaseAccessThread();

  auto EvenWriteOddRead = [&](uint32_t id) {
    for (size_t i = 0; i < count; i++) {
      if (id % 2 == 0) {
        // Even Write
        if (id % 4 == 0) {
          ASSERT_EQ(engine->Put(key, val1), Status::Ok);
        } else {
          ASSERT_EQ(engine->Put(key, val2), Status::Ok);
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
  size_t n_thread_reading = 16;
  size_t n_thread_writing = 16;
  configs.max_access_threads = n_thread_writing + n_thread_reading;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  size_t count = 100000;
  std::string collection_name{"collection"};
  std::vector<std::string> keys{"SuperHotSpot0", "SuperHotSpot2",
                                "SuperHotSpot1"};
  std::string val1(1024, 'a');
  std::string val2(1024, 'b');
  ASSERT_EQ(engine->SortedCreate(collection_name), Status::Ok);

  for (const std::string& key : keys) {
    ASSERT_EQ(engine->SortedPut(collection_name, key, val1), Status::Ok);
    engine->ReleaseAccessThread();

    auto EvenWriteOddRead = [&](uint32_t id) {
      for (size_t i = 0; i < count; i++) {
        if (id % 2 == 0) {
          // Even Write
          if (id % 4 == 0) {
            ASSERT_EQ(engine->SortedPut(collection_name, key, val1),
                      Status::Ok);
          } else {
            ASSERT_EQ(engine->SortedPut(collection_name, key, val2),
                      Status::Ok);
          }
        } else {
          // Odd Read
          std::string got_val;
          ASSERT_EQ(engine->SortedGet(collection_name, key, &got_val),
                    Status::Ok);
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
  size_t num_threads = 16;
  configs.max_access_threads = num_threads;
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

  size_t count = 10;
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
    Status s;
    if (i < 2) {
      std::string comp_name = "collection" + std::to_string(i) + "_cmp";
      SortedCollectionConfigs s_configs;
      s_configs.comparator_name = comp_name;
      s = engine->SortedCreate(collections[i], s_configs);
    } else {
      s = engine->SortedCreate(collections[i]);
    }
    ASSERT_EQ(s, Status::Ok);
  }
  for (size_t i = 0; i < collections.size(); ++i) {
    auto Write = [&](size_t) {
      for (size_t j = 0; j < count; j++) {
        ASSERT_EQ(engine->SortedPut(collections[i], key_values[j].first,
                                    key_values[j].second),
                  Status::Ok);
      }
    };
    LaunchNThreads(num_threads, Write);
  }

  delete engine;
  // Reopen engine error as the comparator is not registered in configs
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Abort);
  ASSERT_TRUE(configs.comparator.RegisterComparator("collection0_cmp", cmp0));
  ASSERT_TRUE(configs.comparator.RegisterComparator("collection1_cmp", cmp1));
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
    size_t cnt = 0;
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
  delete engine;
}

TEST_F(EngineBasicTest, TestHashTableIterator) {
  size_t threads = 32;
  configs.max_access_threads = threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string collection_name = "sortedcollection";
  engine->SortedCreate(collection_name);
  auto MixedPut = [&](size_t id) {
    if (id % 2 == 0) {
      ASSERT_EQ(engine->Put("stringkey" + std::to_string(id), "stringval"),
                Status::Ok);
    } else {
      ASSERT_EQ(
          engine->SortedPut(collection_name, "sortedkey" + std::to_string(id),
                            "sortedval"),
          Status::Ok);
    }
  };
  LaunchNThreads(threads, MixedPut);

  auto test_kvengine = static_cast<KVEngine*>(engine);
  auto hash_table = test_kvengine->GetHashTable();
  size_t total_entry_num = 0;
  // Hash Table Iterator
  // scan hash table with locked slot.
  {
    auto hashtable_iter = hash_table->GetIterator(0, hash_table->GetSlotsNum());
    while (hashtable_iter.Valid()) {
      auto slot_iter = hashtable_iter.Slot();
      while (slot_iter.Valid()) {
        switch (slot_iter->GetIndexType()) {
          case PointerType::StringRecord: {
            total_entry_num++;
            ASSERT_EQ(string_view_2_string(
                          slot_iter->GetIndex().string_record->Value()),
                      "stringval");
            break;
          }
          case PointerType::Skiplist: {
            total_entry_num++;
            ASSERT_EQ(
                string_view_2_string(slot_iter->GetIndex().skiplist->Name()),
                collection_name);
            break;
          }
          case PointerType::SkiplistNode: {
            total_entry_num++;
            ASSERT_EQ(string_view_2_string(
                          slot_iter->GetIndex().skiplist_node->record->Value()),
                      "sortedval");
            break;
          }
          case PointerType::DLRecord: {
            total_entry_num++;
            ASSERT_EQ(
                string_view_2_string(slot_iter->GetIndex().dl_record->Value()),
                "sortedval");
            break;
          }
          default:
            ASSERT_EQ((slot_iter->GetIndexType() == PointerType::Invalid) ||
                          (slot_iter->GetIndexType() == PointerType::Empty),
                      true);
            break;
        }
        slot_iter++;
      }
      hashtable_iter.Next();
    }
    ASSERT_EQ(total_entry_num, threads + 1);
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestExpireAPI) {
  size_t n_thread_reading = 1;
  size_t n_thread_writing = 1;
  configs.max_access_threads = n_thread_writing + n_thread_reading;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::string got_val;
  int64_t ttl_time;
  WriteOptions write_options1{1};
  WriteOptions write_options2{INT64_MAX / 1000};
  std::string key = "expired_key";
  std::string val(10, 'a');
  std::string val2(10, 'b');
  std::string list_collection = "ListCollection";
  std::string sorted_collection = "SortedCollection";
  std::string hashes_collection = "HashesCollection";
  int64_t normal_ttl_time = 10000; /* 10s */
  int64_t max_ttl_time = INT64_MAX - 1;

  // For string
  {
    // key is expired. Check expired time when reading.
    ASSERT_EQ(engine->Put(key, val, write_options1), Status::Ok);
    sleep(1);
    ASSERT_EQ(engine->Get(key, &got_val), Status::NotFound);

    // update kv pair with new expired time.
    ASSERT_EQ(engine->Put(key, val2, write_options2), Status::Ok);
    ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
    ASSERT_EQ(got_val, val2);

    // Get expired time.
    ASSERT_EQ(engine->GetTTL(key, &ttl_time), Status::Ok);

    // reset expired time for string record.
    ASSERT_EQ(engine->Expire(key, normal_ttl_time), Status::Ok);

    // set negative ttl time.
    std::string expire_key = "expired_key1";
    std::string expire_val = "expired_val1";
    ASSERT_EQ(engine->Put(expire_key, expire_val, WriteOptions{}), Status::Ok);
    ASSERT_EQ(engine->GetTTL(expire_key, &ttl_time), Status::Ok);
    ASSERT_EQ(ttl_time, kPersistTime);
    ASSERT_EQ(engine->Expire(expire_key, -30), Status::Ok);
    ASSERT_EQ(engine->GetTTL(expire_key, &ttl_time), Status::NotFound);
  }

  // For sorte collection
  {
    Status s = engine->SortedCreate(sorted_collection);
    ASSERT_EQ(s, Status::Ok);
    ASSERT_EQ(
        engine->SortedPut(sorted_collection, "sorted" + key, "sorted" + val),
        Status::Ok);
    // Set expired time for collection
    ASSERT_EQ(engine->Expire(sorted_collection, max_ttl_time),
              Status::InvalidArgument);
    ASSERT_EQ(
        engine->SortedPut(sorted_collection, "sorted2" + key, "sorted2" + val),
        Status::Ok);
    ASSERT_EQ(engine->GetTTL(sorted_collection, &ttl_time), Status::Ok);
    // check sorted_collection is persist;
    ASSERT_EQ(ttl_time, kPersistTTL);
    // reset expired time for collection
    ASSERT_EQ(engine->Expire(sorted_collection, 2), Status::Ok);
    sleep(2);
    ASSERT_EQ(engine->SortedGet(sorted_collection, "sorted" + key, &got_val),
              Status::NotFound);
    ASSERT_EQ(engine->GetTTL(sorted_collection, &ttl_time), Status::NotFound);
    ASSERT_EQ(ttl_time, kInvalidTTL);

    // set negative or 0 ttl time.
    ASSERT_EQ(engine->SortedCreate(sorted_collection), Status::Ok);
    ASSERT_EQ(engine->GetTTL(sorted_collection, &ttl_time), Status::Ok);
    ASSERT_EQ(ttl_time, kPersistTime);
    ASSERT_EQ(engine->Expire(sorted_collection, 0), Status::Ok);
    ASSERT_EQ(engine->GetTTL(sorted_collection, &ttl_time), Status::NotFound);
    ASSERT_EQ(ttl_time, kInvalidTTL);
  }

  // For hashes collection
  {
    ASSERT_EQ(engine->HashCreate(hashes_collection), Status::Ok);
    ASSERT_EQ(
        engine->HashPut(hashes_collection, "hashes" + key, "hashes" + val),
        Status::Ok);
    // Set expired time for collection, max_ttl_time is overflow.
    ASSERT_EQ(engine->Expire(hashes_collection, max_ttl_time),
              Status::InvalidArgument);
    ASSERT_EQ(
        engine->HashPut(hashes_collection, "hashes2" + key, "hashes2" + val),
        Status::Ok);

    // reset expired time for collection
    ASSERT_EQ(engine->Expire(hashes_collection, normal_ttl_time), Status::Ok);
    ASSERT_EQ(engine->HashGet(hashes_collection, "hashes" + key, &got_val),
              Status::Ok);
    ASSERT_EQ(got_val, "hashes" + val);
    // get collection ttl time
    sleep(2);
    ASSERT_EQ(engine->GetTTL(hashes_collection, &ttl_time), Status::Ok);
  }

  // For list
  {
    ASSERT_EQ(engine->ListCreate(list_collection), Status::Ok);
    ASSERT_EQ(engine->ListPushFront(list_collection, "list" + val), Status::Ok);
    // Set expired time for collection
    ASSERT_EQ(engine->Expire(list_collection, max_ttl_time),
              Status::InvalidArgument);
    ASSERT_EQ(engine->GetTTL(list_collection, &ttl_time), Status::Ok);
    // check list is persist
    ASSERT_EQ(ttl_time, kPersistTime);
    // reset expired time for collection
    ASSERT_EQ(engine->Expire(list_collection, normal_ttl_time), Status::Ok);
    ASSERT_EQ(engine->GetTTL(list_collection, &ttl_time), Status::Ok);
  }

  // Close engine and Recovery
  delete engine;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // Get string record expired time
  ASSERT_EQ(engine->GetTTL(key, &ttl_time), Status::Ok);

  // Get sorted record expired time
  ASSERT_EQ(engine->GetTTL(sorted_collection, &ttl_time), Status::NotFound);

  // Get hashes record expired time
  ASSERT_EQ(engine->GetTTL(hashes_collection, &ttl_time), Status::Ok);

  // Get list record expired time
  ASSERT_EQ(engine->GetTTL(list_collection, &ttl_time), Status::Ok);
  delete engine;
}

TEST_F(EngineBasicTest, TestbackgroundDestroyCollections) {
  size_t n_thread_writing = 16;
  configs.max_access_threads = n_thread_writing;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  TTLType ttl = 1000;  // 1s
  int cnt = 100;
  size_t num_thread = 4;

  auto list0_push = [&](size_t id) {
    std::string list_key0 = "listkey0" + std::to_string(id);
    ASSERT_EQ(engine->ListCreate(list_key0), Status::Ok);
    for (int i = 0; i < cnt; ++i) {
      ASSERT_EQ(
          engine->ListPushFront(list_key0, "list_elem" + std::to_string(i)),
          Status::Ok);
    }
    ASSERT_EQ(engine->Expire(list_key0, ttl), Status::Ok);
  };
  auto list1_push = [&](size_t id) {
    std::string list_key1 = "listkey1" + std::to_string(id);
    ASSERT_EQ(engine->ListCreate(list_key1), Status::Ok);
    for (int i = 0; i < cnt; ++i) {
      ASSERT_EQ(
          engine->ListPushFront(list_key1, "list_elem" + std::to_string(i)),
          Status::Ok);
    }
  };
  auto hash0_push = [&](size_t id) {
    std::string hash_key0 = "hashkey0" + std::to_string(id);
    ASSERT_EQ(engine->HashCreate(hash_key0), Status::Ok);
    for (int i = 0; i < cnt; ++i) {
      std::string str = std::to_string(i);
      ASSERT_EQ(
          engine->HashPut(hash_key0, "hash_elem" + str, "hash_value" + str),
          Status::Ok);
    }
    ASSERT_EQ(engine->Expire(hash_key0, ttl), Status::Ok);
  };

  LaunchNThreads(num_thread, list0_push);
  LaunchNThreads(num_thread, list1_push);
  LaunchNThreads(num_thread, hash0_push);

  sleep(2);
  for (size_t i = 0; i < num_thread; ++i) {
    std::string str = std::to_string(i);
    TTLType got_ttl;
    ASSERT_EQ(engine->GetTTL("hashkey0" + str, &got_ttl), Status::NotFound);
    ASSERT_EQ(engine->GetTTL("listkey0" + str, &got_ttl), Status::NotFound);
    ASSERT_EQ(engine->GetTTL("listkey1" + str, &got_ttl), Status::Ok);
  }

  delete engine;
}
// ========================= Sync Point ======================================

#if KVDK_DEBUG_LEVEL > 0

TEST_F(EngineBasicTest, BatchWriteSortedRollback) {
  size_t num_threads = 1;
  configs.max_access_threads = num_threads + 1;
  for (int index_with_hashtable : {0, 1}) {
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);
    size_t batch_size = 100;
    size_t count = batch_size;

    std::string key{"sorted"};
    SortedCollectionConfigs s_configs;
    s_configs.index_with_hashtable = index_with_hashtable;
    ASSERT_EQ(engine->SortedCreate(key), Status::Ok);

    std::vector<std::vector<std::string>> elems(num_threads);
    std::vector<std::vector<std::string>> values(num_threads);
    // Two new field that will be inserted but rolled back
    std::string rolled_back{"rolled-back"};
    std::string rolled_back2{"rolled-back2"};
    for (size_t tid = 0; tid < num_threads; tid++) {
      for (size_t i = 0; i < count; i++) {
        elems[tid].push_back(std::to_string(tid) + "_" + std::to_string(i));
        values[tid].emplace_back();
      }
    }

    auto Put = [&](size_t tid) {
      for (size_t i = 0; i < count; i++) {
        values[tid][i] = GetRandomString(120);
        ASSERT_EQ(engine->SortedPut(key, elems[tid][i], values[tid][i]),
                  Status::Ok);
      }
    };

    auto BatchWrite = [&](size_t tid) {
      auto batch = engine->WriteBatchCreate();
      batch->SortedPut(key, rolled_back, GetRandomString(120));
      for (size_t i = 0; i < batch_size; i++) {
        if (i % 2 == 0) {
          batch->SortedPut(key, elems[tid][i], GetRandomString(120));
        } else {
          batch->SortedDelete(key, elems[tid][i]);
        }
      }
      batch->SortedPut(key, rolled_back2, GetRandomString(120));
      // Notice: catch exception here will prevent engine from crash, which
      // makes background threads access failed batch write result
      // TODO: fix this test
      ASSERT_THROW(engine->BatchWrite(batch), SyncPoint::CrashPoint);
    };

    auto Check = [&](size_t tid) {
      std::string val_resp;
      ASSERT_EQ(engine->SortedGet(key, rolled_back, &val_resp),
                Status::NotFound);
      ASSERT_EQ(engine->SortedGet(key, rolled_back2, &val_resp),
                Status::NotFound);
      for (size_t i = 0; i < count; i++) {
        if (values[tid][i].empty()) {
          ASSERT_EQ(engine->SortedGet(key, elems[tid][i], &val_resp),
                    Status::NotFound);
        } else {
          ASSERT_EQ(engine->SortedGet(key, elems[tid][i], &val_resp),
                    Status::Ok);
          ASSERT_EQ(values[tid][i], val_resp);
        }
      }
    };

    // Test crash before commit
    SyncPoint::GetInstance()->EnableCrashPoint(
        "KVEngine::batchWriteImpl::BeforeCommit");
    SyncPoint::GetInstance()->EnableProcessing();
    // Put some KVs
    LaunchNThreads(num_threads, Put);
    // Check KVs in engine
    LaunchNThreads(num_threads, Check);
    // Try BatchWrite, crashed by crash point before commitment
    // the BatchWrite will not be visible after recovery
    LaunchNThreads(num_threads, BatchWrite);

    Reboot();

    // Check KVs in engine, the batch is indeed rolled back.
    LaunchNThreads(num_threads, Check);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->Reset();

    // Test crash with half linked record
    size_t num_write = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "KVEngine::Skiplist::LinkDLRecord::HalfLink", [&](void*) {
          if (++num_write == batch_size / 2) {
            throw SyncPoint::CrashPoint{"Crash with half linkage"};
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    // Try BatchWrite, crashed by sync point while the last write is half linked
    LaunchNThreads(num_threads, BatchWrite);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->Reset();
    Reboot();

    // Check KVs in engine, the batch is indeed rolled back.
    LaunchNThreads(num_threads, Check);

    delete engine;
  }
}

TEST_F(EngineBasicTest, BatchWriteStringRollBack) {
  // This test case can only be run with single thread.
  // If multiple threads run batchwrite,
  // a thread may crash at CrashPoint and release its id,
  // another thread may reuse this id and the old batch log file
  // is overwritten.
  size_t num_threads = 1;
  configs.max_access_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  size_t batch_size = 100;
  size_t count = batch_size;
  std::vector<std::vector<std::string>> keys(num_threads);
  // Keeps track of actual values in engine
  std::vector<std::vector<std::string>> expected_values(num_threads);
  for (size_t tid = 0; tid < num_threads; tid++) {
    for (size_t i = 0; i < count; i++) {
      keys[tid].push_back(std::to_string(tid) + "_" + std::to_string(i));
      expected_values[tid].emplace_back();
    }
  }

  // Write to engine and update expected_values vector.
  auto Put = [&](size_t tid) {
    for (size_t i = 0; i < count; i++) {
      std::string value = GetRandomString(120);
      expected_values[tid][i] = value;
      ASSERT_EQ(engine->Put(keys[tid][i], value), Status::Ok);
    }
  };

  // Try BatchWrite, which will not be commited
  // expected_values vector is not updated.
  auto BatchWrite = [&](size_t tid) {
    auto batch = engine->WriteBatchCreate();
    for (size_t i = 0; i < batch_size; i++) {
      if (i % 2 == 0) {
        batch->StringPut(keys[tid][i], GetRandomString(110));
      } else {
        batch->StringDelete(keys[tid][i]);
        // Delete a non-existing key
        batch->StringDelete("non-existing");
      }
    }
    ASSERT_THROW(engine->BatchWrite(batch), SyncPoint::CrashPoint);
  };

  auto Check = [&](size_t tid) {
    for (size_t i = 0; i < count; i++) {
      std::string val_resp;
      ASSERT_EQ(engine->Get(keys[tid][i], &val_resp), Status::Ok);
      ASSERT_EQ(expected_values[tid][i], val_resp);
    }
  };

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  SyncPoint::GetInstance()->EnableCrashPoint(
      "KVEngine::batchWriteImpl::BeforeCommit");
  SyncPoint::GetInstance()->EnableProcessing();

  // Put some KVs
  LaunchNThreads(num_threads, Put);
  // Check KVs in engine
  LaunchNThreads(num_threads, Check);
  // Try BatchWrite, crashed by crash point before commitment
  // the BatchWrite will not be visible after recovery
  LaunchNThreads(num_threads, BatchWrite);

  Reboot();

  // Check KVs in engine, the batch is indeed rolled back.
  LaunchNThreads(num_threads, Check);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();

  delete engine;
}

TEST_F(BatchWriteTest, BatchWriteHashRollback) {
  size_t num_threads = 1;
  configs.max_access_threads = num_threads + 1;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  size_t batch_size = 100;
  size_t count = batch_size;

  std::string key{"hash"};
  ASSERT_EQ(engine->HashCreate(key), Status::Ok);

  std::vector<std::vector<std::string>> fields(num_threads);
  std::vector<std::vector<std::string>> values(num_threads);
  // Two new field that will be inserted but rolled back
  std::string rolled_back{"rolled-back"};
  std::string rolled_back2{"rolled-back2"};
  for (size_t tid = 0; tid < num_threads; tid++) {
    for (size_t i = 0; i < count; i++) {
      fields[tid].push_back(std::to_string(tid) + "_" + std::to_string(i));
      values[tid].emplace_back();
    }
  }

  auto Put = [&](size_t tid) {
    for (size_t i = 0; i < count; i++) {
      values[tid][i] = GetRandomString(120);
      ASSERT_EQ(engine->HashPut(key, fields[tid][i], values[tid][i]),
                Status::Ok);
    }
  };

  auto BatchWrite = [&](size_t tid) {
    auto batch = engine->WriteBatchCreate();
    batch->HashPut(key, rolled_back, GetRandomString(120));
    for (size_t i = 0; i < batch_size; i++) {
      if (i % 2 == 0) {
        batch->HashPut(key, fields[tid][i], GetRandomString(120));
      } else {
        batch->HashDelete(key, fields[tid][i]);
      }
    }
    batch->HashPut(key, rolled_back2, GetRandomString(120));
    ASSERT_THROW(engine->BatchWrite(batch), SyncPoint::CrashPoint);
  };

  auto Check = [&](size_t tid) {
    std::string val_resp;
    ASSERT_EQ(engine->HashGet(key, rolled_back, &val_resp), Status::NotFound);
    ASSERT_EQ(engine->HashGet(key, rolled_back2, &val_resp), Status::NotFound);
    for (size_t i = 0; i < count; i++) {
      if (values[tid][i].empty()) {
        ASSERT_EQ(engine->HashGet(key, fields[tid][i], &val_resp),
                  Status::NotFound);
      } else {
        ASSERT_EQ(engine->HashGet(key, fields[tid][i], &val_resp), Status::Ok);
        ASSERT_EQ(values[tid][i], val_resp);
      }
    }
  };

  SyncPoint::GetInstance()->EnableCrashPoint(
      "KVEngine::batchWriteImpl::BeforeCommit");
  SyncPoint::GetInstance()->EnableProcessing();

  // Put some KVs
  LaunchNThreads(num_threads, Put);
  // Check KVs in engine
  LaunchNThreads(num_threads, Check);
  // Try BatchWrite, crashed by crash point before commitment
  // the BatchWrite will not be visible after recovery
  LaunchNThreads(num_threads, BatchWrite);

  Reboot();

  // Check KVs in engine, the batch is indeed rolled back.
  LaunchNThreads(num_threads, Check);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();

  delete engine;
}

TEST_F(BatchWriteTest, ListBatchOperationRollback) {
  configs.max_access_threads = 1;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  size_t count = 100;

  std::string key{"hash"};
  ASSERT_EQ(engine->ListCreate(key), Status::Ok);

  std::vector<std::string> elems;
  std::list<std::string> list_copy;

  auto Fill = [&]() {
    for (size_t i = 0; i < count; i++) {
      elems.emplace_back(GetRandomString(120));
      list_copy.push_back(elems.back());
      ASSERT_EQ(engine->ListPushBack(key, elems.back()), Status::Ok);
    }
  };

  auto LBatchPush = [&]() {
    ASSERT_THROW(engine->ListBatchPushFront(key, elems), SyncPoint::CrashPoint);
  };

  auto RBatchPush = [&]() {
    ASSERT_THROW(engine->ListBatchPushFront(key, elems), SyncPoint::CrashPoint);
  };

  auto LBatchPop = [&]() {
    std::vector<std::string> elems_resp;
    ASSERT_THROW(engine->ListBatchPopFront(key, count, &elems_resp),
                 SyncPoint::CrashPoint);
  };

  auto RBatchPop = [&]() {
    std::vector<std::string> elems_resp;
    ASSERT_THROW(engine->ListBatchPopFront(key, count, &elems_resp),
                 SyncPoint::CrashPoint);
  };

  auto RPushLPop = [&]() {
    std::string elem;
    ASSERT_THROW(engine->ListMove(key, 0, key, -1, &elem),
                 SyncPoint::CrashPoint);
  };

  auto Check = [&]() {
    auto iter = engine->ListCreateIterator(key);
    ASSERT_TRUE((list_copy.empty() && iter == nullptr) || (iter != nullptr));
    if (iter != nullptr) {
      iter->Seek(0);
      for (auto iter2 = list_copy.begin(); iter2 != list_copy.end(); iter2++) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->Value(), *iter2);
        iter->Next();
      }

      iter->Seek(-1);
      for (auto iter2 = list_copy.rbegin(); iter2 != list_copy.rend();
           iter2++) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->Value(), *iter2);
        iter->Prev();
      }
    }
  };

  SyncPoint::GetInstance()->EnableCrashPoint("KVEngine::ListMove");
  SyncPoint::GetInstance()->EnableCrashPoint("KVEngine::listBatchPushImpl");
  SyncPoint::GetInstance()->EnableCrashPoint("KVEngine::listBatchPopImpl");
  SyncPoint::GetInstance()->EnableProcessing();

  Fill();
  Check();

  Reboot();
  Check();

  LBatchPush();
  Reboot();
  Check();

  LBatchPop();
  Reboot();
  Check();

  RBatchPush();
  Reboot();
  Check();

  RBatchPop();
  Reboot();
  Check();

  RPushLPop();
  Reboot();
  Check();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();

  delete engine;
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
      "KVEngine::Skiplist::LinkDLRecord::HalfLink", [&](void*) {
        if (update_num % 8 == 0) {
          throw 1;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "Test::SortedPut::Update::Finish",
      [&](void*) { update_num.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string collection_name = "SortedRecoverySyncPoint";
  Status s = engine->SortedCreate(collection_name);
  ASSERT_EQ(s, Status::Ok);

  try {
    for (int i = 0; i < cnt; ++i) {
      if (i % 2 == 0) {
        engine->SortedPut(collection_name, "key" + std::to_string(i),
                          "val" + std::to_string(i));
      } else {
        std::string new_val = "val*" + std::to_string(i);
        engine->SortedPut(collection_name, "key" + std::to_string(i - 1),
                          new_val);
        TEST_SYNC_POINT("Test::SortedPut::Update::Finish");
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
      Status s = engine->SortedGet(collection_name, key, &got_val);
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
  delete engine;
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
  std::atomic<bool> first_visited{false};
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  // abandon background cleaner thread
  SyncPoint::GetInstance()->SetCallBack(
      "KVEngine::backgroundCleaner::NothingToDo", [&](void* thread_id) {
        *((size_t*)thread_id) = test_config.clean_threads;
        return;
      });
  // only throw when the first call `SortedDelete`
  SyncPoint::GetInstance()->SetCallBack(
      "KVEngine::Skiplist::Delete::PersistNext'sPrev::After", [&](void*) {
        if (!first_visited.load()) {
          first_visited.store(true);
          throw 1;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  test_config.max_access_threads = 16;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, test_config, stdout),
            Status::Ok);

  std::string collection_name = "SortedDeleteRecoverySyncPoint";
  Status s = engine->SortedCreate(collection_name);
  ASSERT_EQ(s, Status::Ok);

  std::vector<std::string> keylists{"C", "A", "D"};
  try {
    engine->SortedPut(collection_name, keylists[0], "val" + keylists[0]);
    engine->SortedPut(collection_name, keylists[1], "val" + keylists[1]);
    engine->SortedPut(collection_name, keylists[2], "val" + keylists[2]);
    engine->SortedDelete(collection_name, keylists[0]);
    auto test_kvengine = static_cast<KVEngine*>(engine);
    test_kvengine->CleanOutDated(0,
                                 test_kvengine->GetHashTable()->GetSlotsNum());
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
    ASSERT_EQ(engine->SortedGet(collection_name, keylists[0], &got_val),
              Status::NotFound);
    ASSERT_EQ(engine->SortedGet(collection_name, keylists[2], &got_val),
              Status::Ok);
    ASSERT_EQ(got_val, "val" + keylists[2]);
    ASSERT_EQ(engine->SortedGet(collection_name, keylists[1], &got_val),
              Status::Ok);
    ASSERT_EQ(got_val, "val" + keylists[1]);

    // Again delete "C".
    ASSERT_EQ(engine->SortedDelete(collection_name, keylists[0]), Status::Ok);
  }
  delete engine;
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
  ASSERT_EQ(engine->SortedCreate(collection_name), Status::Ok);

  engine->SortedPut(collection_name, "key0", "val0");
  engine->SortedPut(collection_name, "key2", "val2");

  std::atomic<bool> first_record(false);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  SyncPoint::GetInstance()->LoadDependency(
      {{"KVEngine::Skiplist::LinkDLRecord::HalfLink", "Test::Iter::key0"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // insert
  ths.emplace_back(std::thread([&]() {
    engine->SortedPut(collection_name, "key1", "val1");
    std::string got_val;
    ASSERT_EQ(engine->SortedGet(collection_name, "key1", &got_val), Status::Ok);
  }));

  // Iter
  ths.emplace_back(std::thread([&]() {
    sleep(1);
    auto sorted_iter = engine->NewSortedIterator(collection_name);
    sorted_iter->SeekToLast();
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
  delete engine;
}

TEST_F(EngineBasicTest, TestHashTableRangeIter) {
  uint64_t threads = 16;
  configs.max_access_threads = threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string key = "stringkey";
  std::string val = "stringval";
  std::string updated_val = "stringupdatedval";

  ASSERT_EQ(engine->Put(key, val), Status::Ok);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  SyncPoint::GetInstance()->LoadDependency(
      {{"ScanHashTable", "KVEngine::StringPutImpl::BeforeLock"}});
  SyncPoint::GetInstance()->EnableProcessing();

  auto StringUpdate = [&]() {
    ASSERT_EQ(engine->Put(key, updated_val), Status::Ok);
  };

  auto HashTableScan = [&]() {
    auto test_kvengine = static_cast<KVEngine*>(engine);
    auto hash_table = test_kvengine->GetHashTable();
    auto hashtable_iter = hash_table->GetIterator(0, hash_table->GetSlotsNum());
    while (hashtable_iter.Valid()) {
      auto slot_lock = hashtable_iter.AcquireSlotLock();
      auto slot_iter = hashtable_iter.Slot();
      while (slot_iter.Valid()) {
        if (slot_iter->GetIndexType() == PointerType::StringRecord) {
          TEST_SYNC_POINT("ScanHashTable");
          sleep(2);
          ASSERT_EQ(slot_iter->GetIndex().string_record->Key(), key);
          ASSERT_EQ(slot_iter->GetIndex().string_record->Value(), val);
        }
        slot_iter++;
      }
      hashtable_iter.Next();
    }
  };

  std::vector<std::thread> ts;
  ts.emplace_back(std::thread(StringUpdate));
  ts.emplace_back(std::thread(HashTableScan));
  for (auto& t : ts) t.join();
  delete engine;
}

TEST_F(EngineBasicTest, TestBackGroundCleaner) {
  int cleaner_execute_time = 1;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  // abandon background cleaner thread
  SyncPoint::GetInstance()->SetCallBack(
      "KVEngine::backgroundCleaner::NothingToDo", [&](void* thread_id) {
        *((size_t*)thread_id) = configs.clean_threads;
        return;
      });
  SyncPoint::GetInstance()->SetCallBack("KVEngine::backgroundCleaner::Start",
                                        [&](void* terminal) {
                                          *((bool*)terminal) = false;
                                          return;
                                        });
  SyncPoint::GetInstance()->SetCallBack(
      "KVEngine::backgroundCleaner::ExecuteNTime", [&](void* terminal) {
        cleaner_execute_time--;
        if (cleaner_execute_time == 0) {
          *((bool*)terminal) = true;
        }
        sleep(1);
        return;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  configs.max_access_threads = 16;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  int cnt = 100;

  auto PutString = [&]() {
    for (int i = 0; i < cnt; ++i) {
      std::string key = std::to_string(i) + "stringk";
      std::string val = std::to_string(i) + "stringval";
      ASSERT_EQ(engine->Put(key, val, WriteOptions{INT32_MAX}), Status::Ok);
    }
  };

  auto ExpireString = [&]() {
    for (int i = 0; i < cnt; ++i) {
      // string
      std::string key = std::to_string(i) + "stringk";
      std::string got_val;
      if (engine->Get(key, &got_val) == Status::Ok) {
        ASSERT_EQ(engine->Expire(key, 1), Status::Ok);
      }
    }
  };

  auto GetString = [&]() {
    for (int i = 0; i < cnt; ++i) {
      // string
      std::string key = std::to_string(i) + "stringk";
      std::string got_val;
      int64_t ttl_time;
      Status s = engine->GetTTL(key, &ttl_time);
      if (s == Status::Ok) {
        if (ttl_time > 1) {
          ASSERT_EQ(ttl_time <= INT32_MAX, true);
        }
      } else {
        ASSERT_EQ(s, Status::NotFound);
        ASSERT_EQ(ttl_time, kInvalidTTL);
      }
    }
  };

  auto PutSorted = [&]() {
    for (int i = 0; i < cnt; ++i) {
      for (int index_with_hashtable : {0, 1}) {
        std::string sorted_collection =
            std::to_string(i) + "sorted" + std::to_string(index_with_hashtable);
        std::string key = std::to_string(i) + "sortedk";
        std::string val = std::to_string(i) + "sortedval";
        SortedCollectionConfigs s_configs;
        s_configs.index_with_hashtable = index_with_hashtable;
        ASSERT_EQ(engine->SortedCreate(sorted_collection, s_configs),
                  Status::Ok);
        ASSERT_EQ(engine->SortedPut(sorted_collection, key, val), Status::Ok);
        bool set_expire = fast_random_64() % 2 == 0;
        if (set_expire) {
          ASSERT_EQ(engine->Expire(sorted_collection, 1), Status::Ok);
        }
      }
    }
  };

  auto GetSorted = [&]() {
    for (int i = 0; i < cnt; ++i) {
      for (int index_with_hashtable : {0, 1}) {
        std::string sorted_collection =
            std::to_string(i) + "sorted" + std::to_string(index_with_hashtable);
        std::string key = std::to_string(i) + "sortedk";
        std::string val = std::to_string(i) + "sortedval";
        std::string got_val;
        int64_t ttl_time;
        Status s = engine->GetTTL(sorted_collection, &ttl_time);
        if (s == Status::Ok) {
          if (ttl_time == kPersistTime) {
            ASSERT_EQ(engine->SortedGet(sorted_collection, key, &got_val),
                      Status::Ok);
            ASSERT_EQ(got_val, val);
          } else {
            ASSERT_TRUE(ttl_time <= 1);
          }
        } else {
          ASSERT_EQ(s, Status::NotFound);
          ASSERT_EQ(ttl_time, kInvalidTTL);
          ASSERT_EQ(engine->SortedGet(sorted_collection, key, &got_val),
                    Status::NotFound);
        }
      }
    }
  };

  std::string sorted_collection = "sorted_collection";
  std::string list_collection = "list_collection";
  std::string hashlist_collection = "hashlist_collection";

  auto CreateAndDestroySorted = [&]() {
    std::string key = "sorted_key";
    ASSERT_EQ(engine->SortedCreate(sorted_collection), Status::Ok);
    for (int i = 0; i < cnt; ++i) {
      auto new_key = key + std::to_string(i);
      ASSERT_EQ(engine->SortedPut(sorted_collection, new_key, "sorted_value"),
                Status::Ok);
      ASSERT_EQ(
          engine->SortedPut(sorted_collection, new_key, "sorted_update_value"),
          Status::Ok);
      if (i % 2 != 0) {
        ASSERT_EQ(engine->SortedDelete(sorted_collection, new_key), Status::Ok);
      }
    }
    ASSERT_EQ(engine->SortedDestroy(sorted_collection), Status::Ok);
    ASSERT_EQ(engine->SortedCreate(sorted_collection), Status::Ok);
  };

  auto CreateAndDestroyList = [&]() {
    std::string key = "list_key";
    std::string got_key;
    ASSERT_EQ(engine->ListCreate(list_collection), Status::Ok);
    for (int i = 0; i < cnt; ++i) {
      auto new_key = key + std::to_string(i);
      ASSERT_EQ(engine->ListPushFront(list_collection, new_key), Status::Ok);
      ASSERT_EQ(engine->ListPopBack(list_collection, &got_key), Status::Ok);
      ASSERT_EQ(got_key, new_key);
    }
    ASSERT_EQ(engine->ListDestroy(list_collection), Status::Ok);
    ASSERT_EQ(engine->ListCreate(list_collection), Status::Ok);
  };

  auto CreateAndDestroyHashList = [&]() {
    std::string key = "hashlist_key";
    ASSERT_EQ(engine->HashCreate(hashlist_collection), Status::Ok);
    for (int i = 0; i < cnt; ++i) {
      auto new_key = key + std::to_string(i);
      ASSERT_EQ(engine->HashPut(hashlist_collection, new_key, "hashlist_value"),
                Status::Ok);
      ASSERT_EQ(engine->HashPut(hashlist_collection, new_key,
                                "hashlist_update_value"),
                Status::Ok);
      if (i % 2 != 0) {
        ASSERT_EQ(engine->HashDelete(hashlist_collection, new_key), Status::Ok);
      }
    }
    ASSERT_EQ(engine->HashDestroy(hashlist_collection), Status::Ok);
    ASSERT_EQ(engine->HashCreate(hashlist_collection), Status::Ok);
  };

  auto ExpiredClean = [&]() {
    auto test_kvengine = static_cast<KVEngine*>(engine);
    test_kvengine->CleanOutDated(0,
                                 test_kvengine->GetHashTable()->GetSlotsNum());
  };

  {
    cleaner_execute_time = 1;
    std::vector<std::thread> ts;
    ts.emplace_back(std::thread(PutString));
    ts.emplace_back(std::thread(ExpireString));
    sleep(2);
    ts.emplace_back(std::thread(ExpiredClean));
    for (auto& t : ts) t.join();

    // check
    GetString();
  }

  {
    cleaner_execute_time = 1;
    std::vector<std::thread> ts;
    ts.emplace_back(std::thread(PutString));
    ts.emplace_back(std::thread(ExpireString));
    ts.emplace_back(std::thread(ExpiredClean));
    ts.emplace_back(std::thread(GetString));
    for (auto& t : ts) t.join();
  }

  {
    cleaner_execute_time = 1;
    PutSorted();
    auto t = std::thread(ExpiredClean);
    GetSorted();
    t.join();
  }

  {
    cleaner_execute_time = 3;
    CreateAndDestroySorted();
    CreateAndDestroyHashList();
    CreateAndDestroyList();
    ExpiredClean();
    {
      size_t size;
      ASSERT_EQ(engine->SortedSize(sorted_collection, &size), Status::Ok);
      ASSERT_EQ(size, 0);
      ASSERT_EQ(engine->HashLength(hashlist_collection, &size), Status::Ok);
      ASSERT_EQ(size, 0);
      ASSERT_EQ(engine->ListLength(list_collection, &size), Status::Ok);
      ASSERT_EQ(size, 0);
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestBackGroundIterNoHashIndexSkiplist) {
  int cleaner_execute_time = 3;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->Reset();
  // abandon background cleaner thread
  SyncPoint::GetInstance()->SetCallBack(
      "KVEngine::backgroundCleaner::NothingToDo", [&](void* thread_id) {
        *((size_t*)thread_id) = configs.clean_threads;
        return;
      });
  SyncPoint::GetInstance()->SetCallBack("KVEngine::backgroundCleaner::Start",
                                        [&](void* terminal) {
                                          *((bool*)terminal) = false;
                                          return;
                                        });
  SyncPoint::GetInstance()->SetCallBack(
      "KVEngine::backgroundCleaner::ExecuteNTime", [&](void* terminal) {
        cleaner_execute_time--;
        if (cleaner_execute_time == 0) {
          *((bool*)terminal) = true;
        }
        sleep(1);
        return;
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"KVEngine::BackgroundCleaner::IterSkiplist::UnlinkDeleteRecord",
        "KVEngine::SkiplistNoHashIndex::Put"}});
  SyncPoint::GetInstance()->EnableProcessing();
  uint64_t threads = 16;
  configs.max_access_threads = threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string collection_name = "Skiplist_with_hash_index";
  SortedCollectionConfigs s_configs;
  s_configs.index_with_hashtable = false;
  ASSERT_EQ(engine->SortedCreate(collection_name, s_configs), Status::Ok);
  int cnt = 100;

  // Two case: (1) record->old_record->old_record;
  // (2)record->delete_record->old_record
  auto PutAndDeleteSorted = [&]() {
    for (int i = 0; i < cnt; ++i) {
      std::string key = "sorted_key" + std::to_string(i);
      std::string value = "sorted_value" + std::to_string(i);
      ASSERT_EQ(engine->SortedPut(collection_name, key, value), Status::Ok);
      if ((i % 2) == 0) {
        ASSERT_EQ(engine->SortedDelete(collection_name, key), Status::Ok);
      } else {
        ASSERT_EQ(engine->SortedPut(collection_name, key,
                                    "update_value" + std::to_string(i)),
                  Status::Ok);
      }

      TEST_SYNC_POINT("KVEngine::SkiplistNoHashIndex::Put");
      ASSERT_EQ(engine->SortedPut(collection_name, key,
                                  "update_value_again" + std::to_string(i)),
                Status::Ok);
    }
  };

  auto backgroundCleaner = [&]() {
    auto test_kvengine = static_cast<KVEngine*>(engine);
    test_kvengine->CleanOutDated(0,
                                 test_kvengine->GetHashTable()->GetSlotsNum());
  };
  std::vector<std::thread> ts;
  ts.emplace_back(PutAndDeleteSorted);
  ts.emplace_back(backgroundCleaner);
  for (auto& t : ts) t.join();

  int entries = 0;
  // iterating sorted collection
  auto iter = engine->NewSortedIterator(collection_name);
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
  engine->ReleaseSortedIterator(iter);
  ASSERT_EQ(entries, cnt);
  delete engine;
}
#endif

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
