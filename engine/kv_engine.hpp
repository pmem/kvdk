/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "atomic"
#include "cassert"
#include "data_entry.hpp"
#include "dram_allocator.hpp"
#include "hash_list.hpp"
#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "list"
#include "logger.hpp"
#include "pmem_allocator.hpp"
#include "skiplist.hpp"
#include "structures.hpp"
#include "thread_manager.hpp"
#include "time.h"
#include "unordered_map"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

class KVEngine : public Engine {
public:
  KVEngine();
  ~KVEngine();

  static Status Open(const std::string &name, Engine **engine_ptr,
                     const Configs &configs);
  Status Get(const std::string &key, std::string *value) override;
  Status Set(const std::string &key, const std::string &value) override;
  Status Delete(const std::string &key) override;
  Status SGet(const std::string &collection, const std::string &user_key,
              std::string *value) override;
  Status SSet(const std::string &collection, const std::string &user_key,
              const std::string &value) override;
  // TODO: Release delete record and deleted nodes
  Status SDelete(const std::string &collection,
                 const std::string &user_key) override;
  Status BatchWrite(const WriteBatch &write_batch) override;
  std::shared_ptr<Iterator>
  NewSortedIterator(const std::string &collection) override;

private:
  struct BatchWriteHint {
    SizedSpaceEntry sized_space_entry;
    uint64_t ts;
  };

  struct ThreadLocalRes {
    ThreadLocalRes() = default;

    alignas(64) uint64_t newest_restored_ts = 0;
    PendingBatch *persisted_pending_batch = nullptr;
  };

  bool CheckKeySize(const std::string &key) {
    return key.size() <= UINT16_MAX;
  }

  bool CheckValueSize(const std::string &value) {
    return value.size() <= UINT32_MAX;
  }

  Status Init(const std::string &name, const Configs &configs);

  Status HashGetImpl(const Slice &key, std::string *value, uint16_t type_mask);

  inline Status MaybeInitWriteThread();

  Status SearchOrInitPersistentList(const std::string &collection,
                                    PersistentList **list, bool init,
                                    uint16_t header_type);

  Status SearchOrInitHashlist(const std::string &collection,
                              HashList **hashlist, bool init) {
    if (!CheckKeySize(collection)) {
      return Status::InvalidDataSize;
    }
    return SearchOrInitPersistentList(collection, (PersistentList **)hashlist,
                                      init, HASH_LIST_HEADER_RECORD);
  };

  Status SearchOrInitSkiplist(const std::string &collection,
                              Skiplist **skiplist, bool init) {
    if (!CheckKeySize(collection)) {
      return Status::InvalidDataSize;
    }
    return SearchOrInitPersistentList(collection, (PersistentList **)skiplist,
                                      init, SORTED_HEADER_RECORD);
  }

  Status MaybeInitPendingBatchFile();

  Status HashSetImpl(const Slice &key, const Slice &value, uint16_t dt,
                     BatchWriteHint *batch_hint = nullptr);

  Status SSetImpl(Skiplist *skiplist, const std::string &user_key,
                  const std::string &value, uint16_t dt);

  Status Recovery();

  Status RestoreData(uint64_t thread_id);

  Status RestorePendingBatch();

  Status PersistOrRecoverImmutableConfigs();

  void PersistDataEntry(char *block_base, DataEntry *data_entry,
                        const Slice &key, const Slice &value, uint16_t type);

  Status CheckConfigs(const Configs &configs);

  inline uint64_t get_cpu_tsc() {
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)lo) | (((uint64_t)hi) << 32);
  }

  inline uint64_t get_timestamp() {
    auto res = get_cpu_tsc() - ts_on_startup_ + newest_version_on_startup_;
    return res;
  }

  inline std::string db_file_name() { return dir_ + "data"; }

  inline std::string persisted_pending_block_file(int thread_id) {
    return dir_ + "pending_block" + std::to_string(thread_id);
  }

  inline std::string config_file_name() { return dir_ + "configs"; }

  std::vector<ThreadLocalRes> thread_res_;

  // restored kvs in reopen
  std::atomic<uint64_t> restored_{0};
  std::atomic<uint64_t> list_id_{0};

  uint64_t ts_on_startup_ = 0;
  uint64_t newest_version_on_startup_ = 0;
  std::shared_ptr<HashTable> hash_table_;

  std::vector<std::shared_ptr<Skiplist>> skiplists_;
  std::vector<std::shared_ptr<HashList>> hashlists_;
  std::mutex list_mu_;

  std::string dir_;
  std::string db_file_;
  std::shared_ptr<ThreadManager> thread_manager_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  Configs configs_;
};

} // namespace KVDK_NAMESPACE