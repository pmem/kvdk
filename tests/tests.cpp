/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "pmemdb/db.hpp"
#include "pmemdb/namespace.hpp"
#include "test_util.h"
#include "gtest/gtest.h"
#include <thread>
#include <vector>

using namespace PMEMDB_NAMESPACE;
static const uint64_t str_pool_length = 1024000;

class EngineBasicTest : public testing::Test {
protected:
  DB *db = nullptr;
  DBOptions db_options;
  std::string db_path;
  char str_pool[str_pool_length];

  virtual void SetUp() override {
    random_str(str_pool, str_pool_length);
    db_options.pmem_file_size = (16ULL << 30);
    db_options.populate_pmem_space = false;
    db_options.hash_bucket_num = (1 << 5);
    db_options.hash_bucket_size = 64;
    db_path = "/mnt/pmem1/DB";
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", db_path.c_str());
    system(cmd);
  }

  virtual void TearDown() {
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", db_path.c_str());
    system(cmd);
    // delete db_path
  }

  void AssignData(std::string &data, int len) {
    data.assign(str_pool + (rand() % (str_pool_length - len)), len);
  }
};

TEST_F(EngineBasicTest, TestBasicSortedOperations) {
  db_options.pmem_segment_blocks = 8 * 1024;
  ASSERT_EQ(DB::Open(db_path.c_str(), &db, db_options, stdout), Status::Ok);
  int thread_num = 16;
  auto ops = [&](int id) {
    std::string k1, k2, v1, v2;
    std::string got_v1, got_v2;
    int cnt = 100;
    while (cnt--) {
      int v1_len = rand() % 1024;
      int v2_len = rand() % 1024;
      k1 = std::to_string(id) + "k" + k1;
      k2 = std::to_string(id) + "kk" + k2;
      AssignData(v1, v1_len);
      AssignData(v2, v2_len);
      ASSERT_EQ(db->SSet(k1, v1), Status::Ok);

      ASSERT_EQ(db->SSet(k2, v2), Status::Ok);

      ASSERT_EQ(db->SGet(k1, &got_v1), Status::Ok);

      ASSERT_EQ(db->SGet(k2, &got_v2), Status::Ok);
      ASSERT_EQ(v1, got_v1);
      ASSERT_EQ(v2, got_v2);

      AssignData(v1, v1_len);

      ASSERT_EQ(db->SSet(k1, v1), Status::Ok);

      ASSERT_EQ(db->SGet(k1, &got_v1), Status::Ok);
      ASSERT_EQ(got_v1, v1);
    }

    auto iter = db->NewSortedIterator();
    iter->SeekToFirst();
    std::string prev = "";
    int i = 0;
    while (iter->Valid()) {
      i++;
      std::string k = iter->Key();
      iter->Next();
      ASSERT_EQ(true, k.compare(prev) > 0);
    }
  };
  std::vector<std::thread> ts;
  for (int i = 0; i < thread_num; i++) {
    ts.emplace_back(std::thread(ops, i));
  }
  for (auto &t : ts)
    t.join();
  delete db;
}

///*
TEST_F(EngineBasicTest, TestBasicHashOperations) {
  db_options.pmem_segment_blocks = 8 * 1024;
  ASSERT_EQ(DB::Open(db_path.c_str(), &db, db_options, stdout), Status::Ok);
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

      ASSERT_EQ(db->Set(k1, v1), Status::Ok);

      ASSERT_EQ(db->Set(k2, v2), Status::Ok);
      ASSERT_EQ(db->Get(k1, &got_v1), Status::Ok);

      ASSERT_EQ(db->Get(k2, &got_v2), Status::Ok);
      ASSERT_EQ(v1, got_v1);
      ASSERT_EQ(v2, got_v2);

      ASSERT_EQ(db->Delete(k1), Status::Ok);

      ASSERT_EQ(db->Get(k1, &got_v1), Status::NotFound);
      AssignData(v1, v1_len);
      ASSERT_EQ(db->Set(k1, v1), Status::Ok);

      ASSERT_EQ(db->Get(k1, &got_v1), Status::Ok);
      ASSERT_EQ(got_v1, v1);
    }
  };
  std::vector<std::thread> ts;
  for (int i = 0; i < 48; i++) {
    ts.emplace_back(std::thread(ops, i));
  }
  for (auto &t : ts)
    t.join();
  delete db;
}

TEST_F(EngineBasicTest, TestSortedRestore) {
  int threads = 16;
  int count = 100;
  db_options.max_write_threads = threads;
  db_options.pmem_segment_blocks = 8 * 1024;
  ASSERT_EQ(DB::Open(db_path.c_str(), &db, db_options, stdout), Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  auto ops = [&](int id) {
    std::string a(id, 'a');
    std::string v;
    for (int i = 1; i <= count; i++) {
      ASSERT_EQ(db->SSet(a + std::to_string(id * i), std::to_string(id * i)),
                Status::Ok);
      ASSERT_EQ(db->SGet(a + std::to_string(id * i), &v), Status::Ok);
    }
  };
  std::vector<std::thread> ts;
  for (int i = 1; i <= threads; i++) {
    ts.emplace_back(std::thread(ops, i));
  }
  for (auto &t : ts)
    t.join();
  delete db;

  // reopen and restore db and try gets
  ASSERT_EQ(DB::Open(db_path.c_str(), &db, db_options, stdout), Status::Ok);
  for (int i = 1; i <= threads; i++) {
    std::string a(i, 'a');
    std::string v;
    for (int j = 1; j <= count; j++) {
      //      fprintf(stderr, "test %s\n", (a + std::to_string(i * j)).c_str());
      Status s = db->SGet(a + std::to_string(i * j), &v);
      ASSERT_EQ(s, Status::Ok);
    }
  }

  auto iter = db->NewSortedIterator();
  iter->SeekToFirst();
  std::string prev = "";
  int i = 0;
  while (iter->Valid()) {
    i++;
    std::string k = iter->Key();
    iter->Next();
    ASSERT_EQ(true, k.compare(prev) > 0);
  }
  ASSERT_EQ(i, count * threads);

  delete db;
}

TEST_F(EngineBasicTest, TestRestore) {
  int threads = 48;
  int count = 1000;
  db_options.max_write_threads = threads;
  db_options.pmem_segment_blocks = 8 * 1024;
  ASSERT_EQ(DB::Open(db_path.c_str(), &db, db_options, stdout), Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  auto ops = [&](int id) {
    std::string a(id, 'a');
    std::string v;
    for (int i = 1; i <= count; i++) {
      ASSERT_EQ(db->Set(a + std::to_string(id * i), std::to_string(id * i)),
                Status::Ok);
      if ((i * id) % 2 == 1) {
        ASSERT_EQ(db->Delete(a + std::to_string((id * i))), Status::Ok);
        if ((i * id) % 3 == 0) {
          ASSERT_EQ(db->Set(a + std::to_string(id * i), std::to_string(id)),
                    Status::Ok);
          ASSERT_EQ(db->Get(a + std::to_string(id * i), &v), Status::Ok);
          ASSERT_EQ(v, std::to_string(id));
        } else {
          ASSERT_EQ(db->Get(a + std::to_string(id * i), &v), Status::NotFound);
        }
      } else {
        ASSERT_EQ(db->Get(a + std::to_string(id * i), &v), Status::Ok);
      }
    }
  };
  std::vector<std::thread> ts;
  for (int i = 1; i <= threads; i++) {
    ts.emplace_back(std::thread(ops, i));
  }
  for (auto &t : ts)
    t.join();
  delete db;

  // reopen and restore db and try gets
  ASSERT_EQ(DB::Open(db_path.c_str(), &db, db_options, stdout), Status::Ok);
  for (int i = 1; i <= threads; i++) {
    std::string a(i, 'a');
    std::string v;
    for (int j = 1; j <= count; j++) {
      //      fprintf(stderr, "test %s\n", (a + std::to_string(i * j)).c_str());
      Status s = db->Get(a + std::to_string(i * j), &v);
      if ((j * i) % 3 == 0 && (i * j) % 2 == 1) { // deleted then updated ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(v, std::to_string(i));
      } else if ((j * i) % 2 == 0) { // not deleted ones
        if (s != Status::Ok) {
          fprintf(stderr, "key %s error\n",
                  (a + std::to_string(i * j)).c_str());
        }
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(v, std::to_string(i * j));
      } else { // deleted ones
        ASSERT_EQ(s, Status::NotFound);
      }
    }
  }
  delete db;
}

//*/
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
