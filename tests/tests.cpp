/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "../engine/pmem_allocator/pmem_allocator.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include "test_util.h"
#include "gtest/gtest.h"
#include <string>
#include <thread>
#include <vector>

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
    configs.hash_bucket_num = (1 << 5);
    configs.hash_bucket_size = 64;
    configs.pmem_segment_blocks = 8 * 1024;
    // For faster test, no interval so it would not block engine closing
    configs.background_work_interval = 0;
    db_path = "/mnt/pmem0/data";
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", db_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }

  virtual void TearDown() {
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", db_path.c_str());
    int res __attribute__((unused)) = system(cmd);
    // delete db_path
  }

  void AssignData(std::string &data, int len) {
    data.assign(str_pool.data() + (rand() % (str_pool_length - len)), len);
  }
};

TEST_F(EngineBasicTest, TestBasicHashOperations) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  auto ops = [&](int id) {
    std::string k1, k2, v1, v2;
    std::string got_v1, got_v2;
    int cnt = 1000;
    while (cnt--) {
      int v1_len = rand() % 1024;
      int v2_len = rand() % 1024;
      k1 = std::to_string(id) + "k" + k1;
      k2 = std::to_string(id) + "kk" + k2;
      AssignData(v1, v1_len);
      AssignData(v2, v2_len);

      if (id == 0) {
        std::string k0{""};
        ASSERT_EQ(engine->Set(k0, v1), Status::Ok);
        ASSERT_EQ(engine->Get(k0, &got_v1), Status::Ok);
        ASSERT_EQ(v1, got_v1);
        ASSERT_EQ(engine->Delete(k0), Status::Ok);
        ASSERT_EQ(engine->Get(k0, &got_v1), Status::NotFound);
      }

      ASSERT_EQ(engine->Set(k1, v1), Status::Ok);

      ASSERT_EQ(engine->Set(k2, v2), Status::Ok);
      ASSERT_EQ(engine->Get(k1, &got_v1), Status::Ok);

      ASSERT_EQ(engine->Get(k2, &got_v2), Status::Ok);
      ASSERT_EQ(v1, got_v1);
      ASSERT_EQ(v2, got_v2);

      ASSERT_EQ(engine->Delete(k1), Status::Ok);

      ASSERT_EQ(engine->Get(k1, &got_v1), Status::NotFound);
      AssignData(v1, v1_len);
      ASSERT_EQ(engine->Set(k1, v1), Status::Ok);

      ASSERT_EQ(engine->Get(k1, &got_v1), Status::Ok);
      ASSERT_EQ(got_v1, v1);
    }
  };

  std::vector<std::thread> ts;
  for (int i = 0; i < num_threads; i++) {
    ts.emplace_back(std::thread(ops, i));
  }
  for (auto &t : ts)
    t.join();
  delete engine;
}

TEST_F(EngineBasicTest, TestBatchWrite) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  int batch_num = 10;
  int count = 500;
  auto ops = [&](int id) {
    std::string v;
    WriteBatch batch;
    int cnt = count;
    while (cnt--) {
      for (size_t i = 0; i < batch_num; i++) {
        auto key =
            std::string(id, 'a') + std::string(i, 'b') + std::to_string(cnt);
        auto value = std::to_string(i * id);
        batch.Put(key, value);
      }
      ASSERT_EQ(engine->BatchWrite(batch), Status::Ok);
      batch.Clear();
      for (size_t i = 0; i < batch_num; i++) {
        if ((i * cnt) % 2 == 1) {
          auto key =
              std::string(id, 'a') + std::string(i, 'b') + std::to_string(cnt);
          auto value = std::to_string(i * id);
          ASSERT_EQ(engine->Get(key, &v), Status::Ok);
          ASSERT_EQ(v, value);
          batch.Delete(key);
        }
      }
      engine->BatchWrite(batch);
      batch.Clear();
    }
  };

  std::vector<std::thread> ts;
  for (int i = 1; i <= num_threads; i++) {
    ts.emplace_back(std::thread(ops, i));
  }
  for (auto &t : ts)
    t.join();
  delete engine;

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  for (int id = 1; id <= num_threads; id++) {
    std::string v;
    int cnt = count;
    while (cnt--) {
      for (size_t i = 0; i < batch_num; i++) {
        auto key =
            std::string(id, 'a') + std::string(i, 'b') + std::to_string(cnt);
        if ((i * cnt) % 2 == 1) {
          ASSERT_EQ(engine->Get(key, &v), Status::NotFound);
        } else {
          auto value = std::to_string(i * id);
          ASSERT_EQ(engine->Get(key, &v), Status::Ok);
          ASSERT_EQ(v, value);
        }
      }
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestFreeList) {
  // TODO: Add more cases
  configs.pmem_segment_blocks = 4 * kMinPaddingBlockSize;
  configs.max_write_threads = 1;
  configs.pmem_block_size = 64;
  configs.pmem_file_size =
      configs.pmem_segment_blocks * configs.pmem_block_size;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string key1("a1");
  std::string key2("a2");
  std::string key3("a3");
  std::string key4("a4");
  std::string small_value(64 * (kMinPaddingBlockSize - 1) + 1, 'a');
  std::string large_value(64 * (kMinPaddingBlockSize * 2 - 1) + 1, 'a');
  // We have 4 kMinimalPaddingBlockSize size chunk of blocks, this will take up
  // 2 of them

  ASSERT_EQ(engine->Set(key1, large_value), Status::Ok);

  // update large value, new value will be stored in 3th chunk
  ASSERT_EQ(engine->Set(key1, small_value), Status::Ok);

  // key2 will be stored in 4th chunk
  ASSERT_EQ(engine->Set(key2, small_value), Status::Ok);

  // new key 1 and new key 2 will be stored in updated 1st and 2nd chunks
  ASSERT_EQ(engine->Set(key1, small_value), Status::Ok);

  ASSERT_EQ(engine->Set(key2, small_value), Status::Ok);

  delete engine;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  // large key3 will be stored in merged 3th and 4th chunks
  ASSERT_EQ(engine->Set(key3, large_value), Status::Ok);

  // No more space
  ASSERT_EQ(engine->Set(key4, small_value), Status::PmemOverflow);
}

TEST_F(EngineBasicTest, TestLocalSortedCollection) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::vector<int> n_local_entries(num_threads, 0);

  auto ops = [&](int id) {
    std::string thread_local_skiplist("t_skiplist" + std::to_string(id));
    std::string k1, k2, v1, v2;
    std::string got_v1, got_v2;

    AssignData(v1, 10);

    // Test Empty Key
    {
      std::string k0{""};
      ASSERT_EQ(engine->SSet(thread_local_skiplist, k0, v1), Status::Ok);
      ++n_local_entries[id];
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k0, &got_v1), Status::Ok);
      ASSERT_EQ(v1, got_v1);
      ASSERT_EQ(engine->SDelete(thread_local_skiplist, k0), Status::Ok);
      --n_local_entries[id];
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k0, &got_v1),
                Status::NotFound);
    }

    k1 = std::to_string(id);
    k2 = std::to_string(id);

    int cnt = 100;
    while (cnt--) {
      int v1_len = rand() % 1024;
      int v2_len = rand() % 1024;
      k1.append("k1");
      k2.append("k2");

      // insert
      v1.append(std::to_string(id));
      v2.append(std::to_string(id));
      ASSERT_EQ(engine->SSet(thread_local_skiplist, k1, v1), Status::Ok);
      ++n_local_entries[id];
      ASSERT_EQ(engine->SSet(thread_local_skiplist, k2, v2), Status::Ok);
      ++n_local_entries[id];
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k1, &got_v1), Status::Ok);
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k2, &got_v2), Status::Ok);
      ASSERT_EQ(v1, got_v1);
      ASSERT_EQ(v2, got_v2);

      // update
      AssignData(v1, v1_len);
      ASSERT_EQ(engine->SSet(thread_local_skiplist, k1, v1), Status::Ok);
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k1, &got_v1), Status::Ok);
      ASSERT_EQ(got_v1, v1);
      AssignData(v2, v2_len);
      ASSERT_EQ(engine->SSet(thread_local_skiplist, k2, v2), Status::Ok);
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k2, &got_v2), Status::Ok);
      ASSERT_EQ(got_v2, v2);

      // delete
      ASSERT_EQ(engine->SDelete(thread_local_skiplist, k1), Status::Ok);
      --n_local_entries[id];
      ASSERT_EQ(engine->SGet(thread_local_skiplist, k1, &got_v1),
                Status::NotFound);
    }
  };
  auto ops2 = [&](int id) {
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

  {
    std::vector<std::thread> ts;
    for (int i = 0; i < num_threads; i++) {
      ts.emplace_back(std::thread(ops, i));
    }
    for (auto &t : ts)
      t.join();
  }

  {
    std::vector<std::thread> ts;
    for (int i = 0; i < num_threads; i++) {
      ts.emplace_back(std::thread(ops2, i));
    }
    for (auto &t : ts)
      t.join();
  }

  delete engine;
}

TEST_F(EngineBasicTest, TestGlobalSortedCollection) {
  const std::string global_skiplist = "skiplist";
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::atomic<int> n_global_entries{0};

  auto ops = [&](int id) {
    std::string k1, k2, v1, v2;
    std::string got_v1, got_v2;

    AssignData(v1, 10);

    if (id == 0) {
      std::string k0{""};
      ASSERT_EQ(engine->SSet(global_skiplist, k0, v1), Status::Ok);
      ++n_global_entries;
      ASSERT_EQ(engine->SGet(global_skiplist, k0, &got_v1), Status::Ok);
      ASSERT_EQ(v1, got_v1);
      ASSERT_EQ(engine->SDelete(global_skiplist, k0), Status::Ok);
      --n_global_entries;
      ASSERT_EQ(engine->SGet(global_skiplist, k0, &got_v1), Status::NotFound);
    }

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
  auto ops2 = [&](int id) {
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

  auto ops3 = [&](int id) {
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

  {
    std::vector<std::thread> ts;
    for (int i = 0; i < num_threads; i++) {
      ts.emplace_back(std::thread(ops, i));
    }
    for (auto &t : ts)
      t.join();
  }

  {
    std::vector<std::thread> ts;
    for (int i = 0; i < num_threads; i++) {
      ts.emplace_back(std::thread(ops2, i));
    }
    for (auto &t : ts)
      t.join();
  }

  {
      std::vector<std::thread> ts;
      for (int i = 0; i < num_threads; i++) {
          ts.emplace_back(std::thread(ops2, i));
      }
      for (auto& t : ts)
          t.join();
  }

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

TEST_F(EngineBasicTest, TestRestore) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  int count = 1000;
  auto ops = [&](int id) {
    std::string a(id, 'a');
    std::string v;
    for (int i = 1; i <= count; i++) {
      ASSERT_EQ(engine->Set(a + std::to_string(id * i), std::to_string(id * i)),
                Status::Ok);
      if ((i * id) % 2 == 1) {
        ASSERT_EQ(engine->Delete(a + std::to_string((id * i))), Status::Ok);
        if ((i * id) % 3 == 0) {
          ASSERT_EQ(engine->Set(a + std::to_string(id * i), std::to_string(id)),
                    Status::Ok);
          ASSERT_EQ(engine->Get(a + std::to_string(id * i), &v), Status::Ok);
          ASSERT_EQ(v, std::to_string(id));
        } else {
          ASSERT_EQ(engine->Get(a + std::to_string(id * i), &v),
                    Status::NotFound);
        }
      } else {
        ASSERT_EQ(engine->Get(a + std::to_string(id * i), &v), Status::Ok);
      }
    }
  };
  std::vector<std::thread> ts;
  for (int i = 1; i <= num_threads; i++) {
    ts.emplace_back(std::thread(ops, i));
  }
  for (auto &t : ts)
    t.join();
  delete engine;

  // reopen and restore engine and try gets
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  for (int i = 1; i <= num_threads; i++) {
    std::string a(i, 'a');
    std::string v;
    for (int j = 1; j <= count; j++) {
      Status s = engine->Get(a + std::to_string(i * j), &v);
      if ((j * i) % 3 == 0 && (i * j) % 2 == 1) { // deleted then updated ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(v, std::to_string(i));
      } else if ((j * i) % 2 == 0) { // not deleted ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(v, std::to_string(i * j));
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
  auto ops = [&](int id) {
    std::string a(id, 'a');
    std::string v;
    std::string t_skiplist(thread_skiplist + std::to_string(id));
    for (int i = 1; i <= count; i++) {
      auto key = a + std::to_string(id * i);
      auto value = std::to_string(id * i);
      ASSERT_EQ(engine->SSet(overall_skiplist, key, value), Status::Ok);
      ASSERT_EQ(engine->SSet(t_skiplist, key, value + std::to_string(id)),
                Status::Ok);
      ASSERT_EQ(engine->SGet(overall_skiplist, key, &v), Status::Ok);
      ASSERT_EQ(v, value);
      ASSERT_EQ(engine->SGet(t_skiplist, key, &v), Status::Ok);
      ASSERT_EQ(v, value + std::to_string(id));
    }
  };
  std::vector<std::thread> ts;
  for (int i = 1; i <= num_threads; i++) {
    ts.emplace_back(std::thread(ops, i));
  }
  for (auto &t : ts)
    t.join();
  delete engine;

  // reopen and restore engine and try gets
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  for (int i = 1; i <= num_threads; i++) {
    std::string t_skiplist(thread_skiplist + std::to_string(i));
    std::string a(i, 'a');
    std::string v;
    for (int j = 1; j <= count; j++) {
      auto key = a + std::to_string(i * j), value = std::to_string(i * j);
      Status s = engine->SGet(overall_skiplist, key, &v);
      ASSERT_EQ(s, Status::Ok);
      ASSERT_EQ(v, value);
      s = engine->SGet(t_skiplist, key, &v);
      ASSERT_EQ(s, Status::Ok);
      ASSERT_EQ(v, value + std::to_string(i));
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
    ASSERT_EQ(cnt, count);
  }

  auto iter = engine->NewSortedIterator(overall_skiplist);
  ASSERT_TRUE(iter != nullptr);
  iter->SeekToFirst();
  std::string prev = "";
  int i = 0;
  while (iter->Valid()) {
    i++;
    std::string k = iter->Key();
    iter->Next();
    ASSERT_TRUE(k.compare(prev) > 0);
    prev = k;
  }
  ASSERT_EQ(i, count * num_threads);

  delete engine;
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
