/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "atomic"
#include "cassert"
#include "hash_table.hpp"
#include "list"
#include "logger.hpp"
#include "mempool.hpp"
#include "pmemdb/db.hpp"
#include "skiplist.hpp"
#include "structures.hpp"
#include "time.h"
#include "unordered_map"
#include "utils.hpp"
#include <memory>

namespace PMEMDB_NAMESPACE {

class EngineImpl {
public:
  EngineImpl();
  ~EngineImpl();

  void Init(const std::string &name, const DBOptions &options);

  Status Get(const Slice &key, std::string *value);
  Status Set(const Slice &key, const Slice &value);
  Status Delete(const Slice &key);
  Status SGet(const Slice &key, std::string *value);
  Status SSet(const Slice &key, const Slice &value);
  std::shared_ptr<SortedIterator> NewSortedIterator();

private:
  Status SetValueOffset(uint32_t &b_size, SpaceEntry *space_entry);

  Status HashGetImpl(const Slice &key, std::string *value, uint8_t type_mask);

  inline void InitTID();

  Status MaybeInitSkiplist();

  Status SetImpl(const Slice &key, const Slice &value, uint8_t dt);

  void Recovery(uint64_t start);

  void InitDataSize2BSize();

  uint16_t GetBlockSize(uint32_t data_size);

  inline uint64_t get_ts() {
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)lo) | (((uint64_t)hi) << 32);
  }

  inline uint64_t get_version() {
    auto res = get_ts() - ts_on_startup_ + newest_version_on_startup_;
    return res;
  }

  DBOptions options_;
  uint64_t pmem_capacity_ = 0;
  std::vector<ThreadSpace> thread_space_;

  // PMEM space
  char *pmem_value_log_ = nullptr; // start position to store values
  std::atomic<uint64_t> pmem_value_log_head_;

  // allocate thread id
  std::atomic<int> threads_{0};
  // restored kvs in reopen
  std::atomic<uint64_t> restored_{0};
  std::vector<uint16_t> data_size_2_b_size;

  uint64_t ts_on_startup_ = 0;
  uint64_t newest_version_on_startup_ = 0;
  HashTable *hash_table = nullptr;

  std::unique_ptr<Skiplist> skiplist_ = nullptr;
  std::mutex skiplist_mu_;
};

} // namespace PMEMDB_NAMESPACE
