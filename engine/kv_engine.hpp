/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
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
#include "pmem_allocator/pmem_allocator.hpp"
#include "skiplist.hpp"
#include "structures.hpp"
#include "thread_manager.hpp"
#include "time.h"
#include "unordered_map"
#include "utils.hpp"

namespace KVDK_NAMESPACE {
class KVEngine : public Engine {
public:
  friend class ConcurrentRebuildSorted;
  KVEngine();
  ~KVEngine();

  static Status Open(const std::string &name, Engine **engine_ptr,
                     const Configs &configs);
  Status Get(const pmem::obj::string_view key, std::string *value) override;
  Status Set(const pmem::obj::string_view key,
             const pmem::obj::string_view value) override;
  Status Delete(const pmem::obj::string_view key) override;
  Status SGet(const pmem::obj::string_view collection,
              const pmem::obj::string_view user_key,
              std::string *value) override;
  Status SSet(const pmem::obj::string_view collection,
              const pmem::obj::string_view user_key,
              const pmem::obj::string_view value) override;
  // TODO: Release delete record and deleted nodes
  Status SDelete(const pmem::obj::string_view collection,
                 const pmem::obj::string_view user_key) override;
  Status BatchWrite(const WriteBatch &write_batch) override;
  std::shared_ptr<Iterator>
  NewSortedIterator(const pmem::obj::string_view collection) override;
  void ReleaseWriteThread() override { write_thread.Release(); }

private:
  struct BatchWriteHint {
    uint64_t timestamp{0};
    SizedSpaceEntry allocated_space{};
    SizedSpaceEntry free_after_finish{};
    bool delay_free{false};
  };

  struct ThreadLocalRes {
    ThreadLocalRes() = default;

    alignas(64) uint64_t newest_restored_ts = 0;
    PendingBatch *persisted_pending_batch = nullptr;
    std::unordered_map<uint64_t, int> visited_skiplist_ids_;
  };

  bool CheckKeySize(const pmem::obj::string_view &key) {
    return key.size() <= UINT16_MAX;
  }

  bool CheckValueSize(const pmem::obj::string_view &value) {
    return value.size() <= UINT32_MAX;
  }

  Status Init(const std::string &name, const Configs &configs);

  Status HashGetImpl(const pmem::obj::string_view &key, std::string *value,
                     uint16_t type_mask);

  inline Status MaybeInitWriteThread();

  Status SearchOrInitPersistentList(const pmem::obj::string_view &collection,
                                    PersistentList **list, bool init,
                                    uint16_t header_type);

  Status SearchOrInitHashlist(const pmem::obj::string_view &collection,
                              HashList **hashlist, bool init) {
    if (!CheckKeySize(collection)) {
      return Status::InvalidDataSize;
    }
    return SearchOrInitPersistentList(collection, (PersistentList **)hashlist,
                                      init, HashListHeaderRecord);
  };

  Status SearchOrInitSkiplist(const pmem::obj::string_view &collection,
                              Skiplist **skiplist, bool init) {
    if (!CheckKeySize(collection)) {
      return Status::InvalidDataSize;
    }
    return SearchOrInitPersistentList(collection, (PersistentList **)skiplist,
                                      init, SortedHeaderRecord);
  }

  Status MaybeInitPendingBatchFile();

  Status StringBatchWriteImpl(const WriteBatch::KV &kv,
                              BatchWriteHint &batch_hint);

  Status StringWriteImpl(const pmem::obj::string_view &key,
                         const pmem::obj::string_view &value, uint16_t dt);

  Status SSetImpl(Skiplist *skiplist, const pmem::obj::string_view &user_key,
                  const pmem::obj::string_view &value, uint16_t dt);

  Status Recovery();

  Status RestoreData(uint64_t thread_id);

  Status RestoreSkiplistHead(DLDataEntry *pmem_data_entry,
                             DataEntry *cached_meta);

  Status RestoreSortedRecord(DLDataEntry *pmem_data_entry,
                             DataEntry *cached_meta);

  Status RestoreStringRecord(DataEntry *pmem_data_entry,
                             DataEntry *cached_meta);

  uint32_t CalculateChecksum(DataEntry *data_entry);

  Status RestorePendingBatch();

  Status PersistOrRecoverImmutableConfigs();

  // Regularly works excecuted by background thread
  void BackgroundWork();

  void PersistDataEntry(char *block_base, DataEntry *data_entry,
                        const pmem::obj::string_view &key,
                        const pmem::obj::string_view &value, uint16_t type);

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
    return pending_batch_dir_ + std::to_string(thread_id);
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
  std::string pending_batch_dir_;
  std::string db_file_;
  std::shared_ptr<ThreadManager> thread_manager_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  Configs configs_;
  bool closing_{false};
  std::vector<std::thread> bg_threads_;
};

} // namespace KVDK_NAMESPACE
