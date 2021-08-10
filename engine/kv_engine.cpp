/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <future>
#include <libpmem.h>
#include <limits>
#include <mutex>
#include <sys/mman.h>
#include <thread>

#include "configs.hpp"
#include "dram_allocator.hpp"
#include "hash_list.hpp"
#include "kvdk/engine.hpp"
#include "skiplist.hpp"
#include "structures.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

// use buffer to acc nt-write
thread_local std::string write_buffer;
static const int buffer_size = 1024 * 1024;

void PendingBatch::PersistProcessing(
    void *target, const std::vector<uint64_t> &entry_offsets) {
  pmem_memcpy_persist((char *)target + sizeof(PendingBatch),
                      entry_offsets.data(), entry_offsets.size() * 8);
  stage = Stage::Processing;
  num_kv = entry_offsets.size();
  pmem_memcpy_persist(target, this, sizeof(PendingBatch));
}

void PendingBatch::PersistStage(Stage s) {
  stage = s;
  pmem_persist(&s, sizeof(Stage));
}

KVEngine::KVEngine() {}

KVEngine::~KVEngine() { GlobalLogger.Log("instance closed\n"); }

Status KVEngine::Open(const std::string &name, Engine **engine_ptr,
                      const Configs &configs) {
  KVEngine *engine = new KVEngine;
  Status s = engine->Init(name, configs);
  if (s == Status::Ok) {
    *engine_ptr = engine;
  }
  return s;
}

Status KVEngine::Init(const std::string &name, const Configs &configs) {
  local_thread.id = 0;
  int res = create_dir_if_missing(name);
  if (res != 0) {
    GlobalLogger.Error("Create engine dir %s error\n", name.c_str());
    return Status::IOError;
  }
  dir_ = format_dir_path(name);
  db_file_ = db_file_name();
  configs_ = configs;
  Status s = PersistOrRecoverImmutableConfigs();
  if (s != Status::Ok) {
    return s;
  }
  pmem_allocator_.reset(new PMEMAllocator(
      db_file_, configs_.pmem_file_size, configs_.pmem_segment_blocks,
      configs_.pmem_block_size, configs_.max_write_threads));

  thread_res_.resize(configs_.max_write_threads);
  thread_manager_.reset(new ThreadManager(configs_.max_write_threads));
  hash_table_.reset(new HashTable(configs_.num_hash_buckets,
                                  configs_.hash_bucket_size,
                                  configs_.num_buckets_per_slot,
                                  pmem_allocator_, configs_.max_write_threads));
  ts_on_startup_ = get_cpu_tsc();
  s = Recovery();
  local_thread.id = -1;
  return s;
}

std::shared_ptr<Iterator>
KVEngine::NewSortedIterator(const std::string &collection) {
  Skiplist *skiplist;
  Status s = SearchOrInitSkiplist(collection, &skiplist, false);

  return s == Status::Ok
             ? std::make_shared<SortedIterator>(skiplist, pmem_allocator_)
             : nullptr;
}

Status KVEngine::MaybeInitWriteThread() {
  return thread_manager_->MaybeInitThread(local_thread);
}

Status KVEngine::RestoreData(uint64_t thread_id) {
  local_thread.id = thread_id;
  char existing_data_entry_buffer[sizeof(DLDataEntry)];
  char recovering_data_entry_buffer[sizeof(DLDataEntry)];
  DataEntry *existing_data_entry = (DataEntry *)existing_data_entry_buffer;
  DataEntry *recovering_data_entry = (DataEntry *)recovering_data_entry_buffer;
  HashEntry hash_entry;

  SizedSpaceEntry segment_space;
  bool fetch = false;
  uint64_t cnt = 0;
  while (true) {
    if (segment_space.size == 0) {
      fetch = true;
    }
    if (fetch) {
      if (!pmem_allocator_->FreeAndFetchSegment(&segment_space)) {
        break;
      }
      fetch = false;
    }

    HashEntry *entry_base = nullptr;

    DataEntry *pmem_data_entry = (DataEntry *)pmem_allocator_->offset2addr(
        segment_space.space_entry.offset);
    memcpy(recovering_data_entry, pmem_data_entry,
           data_entry_size(pmem_data_entry->type));

    // reach the of of this segment
    if (recovering_data_entry->header.b_size == 0) {
      fetch = true;
      continue;
    }

    bool dl_entry = recovering_data_entry->type & (DLDataEntryType);
    uint32_t checksum = dl_entry ? ((DLDataEntry *)pmem_data_entry)->Checksum()
                                 : pmem_data_entry->Checksum();
    if (recovering_data_entry->header.checksum != checksum ||
        recovering_data_entry->type == PADDING) {
      pmem_allocator_->Free(SizedSpaceEntry(
          pmem_allocator_->addr2offset(pmem_data_entry),
          recovering_data_entry->header.b_size, nullptr, nullptr));
      if (recovering_data_entry->header.checksum != checksum) {
        pmem_data_entry->type = PADDING;
        pmem_persist(&pmem_data_entry->type, sizeof(DATA_ENTRY_TYPE));
      }

      segment_space.size -= recovering_data_entry->header.b_size;
      segment_space.space_entry.offset += recovering_data_entry->header.b_size;
      continue;
    }

    cnt++;

    if (recovering_data_entry->timestamp >
        thread_res_[thread_id].newest_restored_ts) {
      thread_res_[thread_id].newest_restored_ts =
          recovering_data_entry->timestamp;
    }

    Slice pmem_key = dl_entry ? ((DLDataEntry *)pmem_data_entry)->Key()
                              : pmem_data_entry->Key();
    std::string key(pmem_key.data(), pmem_key.size());

    if (recovering_data_entry->type == SORTED_HEADER_RECORD) {
      uint64_t id;
      memcpy_8(&id, ((DLDataEntry *)pmem_data_entry)->Value().data());
      Skiplist *skiplist;
      {
        std::lock_guard<std::mutex> lg(list_mu_);
        skiplists_.push_back(
            std::make_shared<Skiplist>((DLDataEntry *)pmem_data_entry, key, id,
                                       pmem_allocator_, hash_table_));
        skiplist = skiplists_.back().get();
      }
      compare_excange_if_larger(list_id_, id + 1);

      // Here key is the collection name
      auto hint = hash_table_->GetHint(key);
      std::lock_guard<SpinMutex> lg(*hint.spin);
      Status s =
          hash_table_->Search(hint, key, SORTED_HEADER_RECORD, &hash_entry,
                              existing_data_entry, &entry_base, true);
      if (s == Status::MemoryOverflow) {
        return s;
      }
      assert(s == Status::NotFound);
      hash_table_->Insert(hint, entry_base, SORTED_HEADER_RECORD,
                          (uint64_t)skiplist, false);
    } else {
      auto hint = hash_table_->GetHint(key);
      std::lock_guard<SpinMutex> lg(*hint.spin);
      Status s = hash_table_->Search(
          hint, key,
          dl_entry ? (SORTED_DATA_RECORD | SORTED_DELETE_RECORD)
                   : (STRING_DELETE_RECORD | STRING_DATA_RECORD),
          &hash_entry, existing_data_entry, &entry_base, true);
      if (s == Status::MemoryOverflow) {
        return s;
      }
      bool found = s == Status::Ok;
      bool should_insert = true;
      if (found) {
        if (existing_data_entry->timestamp >=
            recovering_data_entry->timestamp) {
          should_insert = false;
        }
      }

      auto new_ref = recovering_data_entry->type & DLDataEntryType
                         ? entry_base->header.reference
                         : entry_base->header.reference + 1;

      if (should_insert) {
        uint64_t offset;
        if (dl_entry) {
          SkiplistNode *node;
          if (!found) {
            node = SkiplistNode::NewNode(Skiplist::UserKey(key),
                                         (DLDataEntry *)pmem_data_entry,
                                         Skiplist::RandomHeight());
            if (node == nullptr) {
              return Status::MemoryOverflow;
            }
            offset = (uint64_t)node;
            hash_table_->Insert(hint, entry_base, recovering_data_entry->type,
                                offset, false);
          } else {
            node = (SkiplistNode *)hash_entry.offset;
            offset = pmem_allocator_->addr2offset(node->data_entry);
            node->data_entry = (DLDataEntry *)pmem_data_entry;
            pmem_allocator_->Free(
                SizedSpaceEntry(offset, existing_data_entry->header.b_size,
                                entry_base, hint.spin));
            entry_base->header.reference = new_ref;
            entry_base->header.type = recovering_data_entry->type;
          }
        } else {
          offset = pmem_allocator_->addr2offset(pmem_data_entry);
          hash_table_->Insert(hint, entry_base, recovering_data_entry->type,
                              offset, found);
          if (found) {
            pmem_allocator_->Free(SizedSpaceEntry(
                hash_entry.offset, existing_data_entry->header.b_size,
                hash_entry.header.type == STRING_DELETE_RECORD ? nullptr
                                                               : entry_base,
                hash_entry.header.type == STRING_DELETE_RECORD ? nullptr
                                                               : hint.spin));
          }
        }
      } else {
        auto *entry_reference =
            (recovering_data_entry->type == STRING_DELETE_RECORD) ? nullptr
                                                                  : entry_base;
        auto *entry_lock = (recovering_data_entry->type == STRING_DELETE_RECORD)
                               ? nullptr
                               : hint.spin;
        pmem_allocator_->Free(SizedSpaceEntry(
            pmem_allocator_->addr2offset(pmem_data_entry),
            recovering_data_entry->header.b_size, entry_reference, entry_lock));
        entry_base->header.reference = new_ref;
      }
    }
    segment_space.size -= recovering_data_entry->header.b_size;
    segment_space.space_entry.offset += recovering_data_entry->header.b_size;
  }
  local_thread.id = -1;
  restored_.fetch_add(cnt);
  return Status::Ok;
}

Status KVEngine::SearchOrInitPersistentList(const std::string &collection,
                                            PersistentList **list, bool init,
                                            uint16_t header_type) {
  auto hint = hash_table_->GetHint(collection);
  HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  Status s = hash_table_->Search(hint, collection, header_type, &hash_entry,
                                 nullptr, &entry_base, false);
  if (s == Status::NotFound) {
    if (init) {
      std::lock_guard<SpinMutex> lg(*hint.spin);
      // Since we do the first search without lock, we need to check again
      entry_base = nullptr;
      s = hash_table_->Search(hint, collection, header_type, &hash_entry,
                              nullptr, &entry_base, true);
      if (s == Status::MemoryOverflow) {
        return s;
      }
      if (s == Status::NotFound) {
        uint32_t request_size =
            sizeof(DLDataEntry) + collection.size() + 8 /* id */;
        SizedSpaceEntry sized_space_entry =
            pmem_allocator_->Allocate(request_size);
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }
        char *block_base =
            pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);
        DLDataEntry data_entry(0, sized_space_entry.size, get_timestamp(),
                               header_type, collection.size(), 8,
                               NULL_PMEM_OFFSET, NULL_PMEM_OFFSET);
        uint64_t id = list_id_.fetch_add(1);
        PersistDataEntry(block_base, &data_entry, collection,
                         Slice((char *)&id, 8), header_type);
        {
          std::lock_guard<std::mutex> lg(list_mu_);
          switch (header_type) {
          case SORTED_HEADER_RECORD:
            skiplists_.push_back(std::make_shared<Skiplist>(
                (DLDataEntry *)block_base, collection, id, pmem_allocator_,
                hash_table_));
            *list = skiplists_.back().get();
            break;
          case HASH_LIST_HEADER_RECORD:
            hashlists_.push_back(std::make_shared<HashList>());
            *list = hashlists_.back().get();
            break;
          default:
            return Status::NotSupported;
          }
        }
        hash_table_->Insert(hint, entry_base, header_type, (uint64_t)(*list),
                            false);
        return Status::Ok;
      }
    }
  }

  if (s == Status::Ok) {
    *list = (PersistentList *)hash_entry.offset;
  }

  return s;
}

Status KVEngine::PersistOrRecoverImmutableConfigs() {
  size_t mapped_len;
  int is_pmem;
  ImmutableConfigs *configs = (ImmutableConfigs *)pmem_map_file(
      config_file_name().c_str(), sizeof(ImmutableConfigs), PMEM_FILE_CREATE,
      0666, &mapped_len, &is_pmem);
  if (configs == nullptr || !is_pmem ||
      mapped_len != sizeof(ImmutableConfigs)) {
    return Status::IOError;
  }
  if (configs->Valid()) {
    configs->AssignImmutableConfigs(configs_);
  }

  Status s = CheckConfigs(configs_);
  if (s == Status::Ok) {
    configs->PersistImmutableConfigs(configs_);
  }
  return s;
}

Status KVEngine::RestorePendingBatch() {
  // TODO: iterate all pending batch files
  uint64_t id = 0;
  size_t mapped_len;
  int is_pmem;
  PendingBatch *pending_batch = nullptr;
  uint64_t persisted_pending_file_size =
      MAX_WRITE_BATCH_SIZE * 8 + sizeof(PendingBatch);
  while ((pending_batch = (PendingBatch *)pmem_map_file(
              persisted_pending_block_file(id).c_str(),
              persisted_pending_file_size, PMEM_FILE_EXCL, 0666, &mapped_len,
              &is_pmem)) != nullptr) {
    assert(is_pmem);
    assert(mapped_len = persisted_pending_file_size);
    if (id < configs_.max_write_threads) {
      thread_res_[id].persisted_pending_batch = pending_batch;
    }

    if (pending_batch->Unfinished()) {
      uint64_t *invalid_offsets = (uint64_t *)(pending_batch + 1);
      for (uint32_t i = 0; i < pending_batch->num_kv; i++) {
        DataEntry *data_entry =
            (DataEntry *)pmem_allocator_->offset2addr(invalid_offsets[i]);
        if (data_entry->timestamp == pending_batch->timestamp) {
          data_entry->type = PADDING;
          pmem_persist(&data_entry->type, 8);
        }
      }
    }
    id++;
  }
  return Status::Ok;
}

Status KVEngine::Recovery() {
  auto s = RestorePendingBatch();
  if (s != Status::Ok) {
    return s;
  }

  std::vector<std::future<Status>> fs;
  for (uint32_t i = 0; i < configs_.max_write_threads; i++) {
    fs.push_back(std::async(&KVEngine::RestoreData, this, i));
  }

  for (auto &f : fs) {
    Status s = f.get();
    if (s != Status::Ok) {
      return s;
    }
  }

  fs.clear();
  for (auto s : skiplists_) {
    fs.push_back(std::async(&Skiplist::Rebuild, s));
  }

  for (auto &f : fs) {
    Status s = f.get();
    if (s != Status::Ok) {
      return s;
    }
  }

  GlobalLogger.Log("In restoring: iterated %lu records\n", restored_.load());

  if (restored_.load() == 0) {
    if (configs_.populate_pmem_space) {
      pmem_allocator_->PopulateSpace();
    }
  } else {
    for (auto &ts : thread_res_) {
      if (ts.newest_restored_ts > newest_version_on_startup_) {
        newest_version_on_startup_ = ts.newest_restored_ts;
      }
    }
  }

  return Status::Ok;
}

Status KVEngine::HashGetImpl(const Slice &key, std::string *value,
                             uint16_t type_mask) {
  std::unique_ptr<DataEntry> data_entry(
      type_mask & DLDataEntryType ? new DLDataEntry : new DataEntry);
  HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  bool is_found = hash_table_->Search(hash_table_->GetHint(key), key, type_mask,
                                      &hash_entry, data_entry.get(),
                                      &entry_base, false) == Status::Ok;
  if (!is_found || (hash_entry.header.type & DeleteDataEntryType)) {
    return Status::NotFound;
  }

  char *block_base = nullptr;
  if (hash_entry.header.type & (STRING_DATA_RECORD | HASH_LIST_DATA_RECORD)) {
    block_base = pmem_allocator_->offset2addr(hash_entry.offset);
  } else if (hash_entry.header.type == SORTED_DATA_RECORD) {
    SkiplistNode *node = (SkiplistNode *)hash_entry.offset;
    block_base = (char *)node->data_entry;
  } else {
    return Status::NotSupported;
  }

  while (1) {
    value->assign(block_base + key.size() +
                      data_entry_size(hash_entry.header.type),
                  data_entry->v_size);

    std::atomic_thread_fence(std::memory_order_acquire);
    // Double check for lock free read
    // TODO double check for skiplist get
    if (__glibc_likely(
            (hash_entry.header.type & DLDataEntryType) ||
            (hash_entry.offset == entry_base->offset &&
             hash_entry.header.key_prefix == entry_base->header.key_prefix)))
      break;
    if (entry_base->header.key_prefix != hash_entry.header.key_prefix ||
        entry_base->header.type == DeleteDataEntryType) {
      value->clear();
      return Status::NotFound;
    }
    HashEntry::CopyOffset(&hash_entry, entry_base);
    block_base = pmem_allocator_->offset2addr(hash_entry.offset);

    memcpy(data_entry.get(), block_base, sizeof(DataEntry));
  }
  return Status::Ok;
}

Status KVEngine::Get(const std::string &key, std::string *value) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return HashGetImpl(key, value, StringDataEntryType);
}

Status KVEngine::Delete(const std::string &key) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return HashSetImpl(key, "", STRING_DELETE_RECORD);
}

inline void KVEngine::PersistDataEntry(char *block_base, DataEntry *data_entry,
                                       const Slice &key, const Slice &value,
                                       uint16_t type) {
  char *data_cpy_target;
  auto entry_size = data_entry_size(type);
  bool with_buffer = entry_size + key.size() + value.size() <= buffer_size;
  if (with_buffer) {
    if (write_buffer.empty()) {
      write_buffer.resize(buffer_size);
    }
    data_cpy_target = &write_buffer[0];
  } else {
    data_cpy_target = block_base;
  }
  memcpy(data_cpy_target, data_entry, entry_size);
  memcpy(data_cpy_target + entry_size, key.data(), key.size());
  memcpy(data_cpy_target + entry_size + key.size(), value.data(), value.size());
  if (type & DLDataEntryType) {
    DLDataEntry *entry_with_data = ((DLDataEntry *)data_cpy_target);
    entry_with_data->header.checksum = entry_with_data->Checksum();
  } else {
    DataEntry *entry_with_data = ((DataEntry *)data_cpy_target);
    entry_with_data->header.checksum = entry_with_data->Checksum();
  }
  if (with_buffer) {
    pmem_memcpy(block_base, data_cpy_target,
                entry_size + key.size() + value.size(), PMEM_F_MEM_NONTEMPORAL);
  } else {
    pmem_flush(block_base, entry_size + key.size() + value.size());
  }
  pmem_drain();
}

Status KVEngine::SSetImpl(Skiplist *skiplist, const std::string &user_key,
                          const std::string &value, uint16_t dt) {
  uint64_t id = skiplist->id();
  std::string collection_key(PersistentList::ListKey(user_key, id));
  if (!CheckKeySize(collection_key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  HashEntry hash_entry;
  DLDataEntry data_entry;
  SkiplistNode *node = nullptr;
  uint64_t old_entry_offset;
  bool is_delete = (dt & (DeleteDataEntryType));

  auto request_size =
      value.size() + collection_key.size() + sizeof(DLDataEntry);
  SizedSpaceEntry sized_space_entry;
  if (!(dt & (DeleteDataEntryType))) {
    sized_space_entry = pmem_allocator_->Allocate(request_size);
    if (sized_space_entry.size == 0) {
      return Status::PmemOverflow;
    }
  }
  while (1) {
    HashEntry *entry_base = nullptr;
    auto hint = hash_table_->GetHint(collection_key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s = hash_table_->Search(hint, collection_key,
                                   SORTED_DATA_RECORD | SORTED_DELETE_RECORD,
                                   &hash_entry, &data_entry, &entry_base, true);
    if (s == Status ::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;
    if (dt & DeleteDataEntryType) {
      if (found && entry_base->header.type != dt) {
        if (sized_space_entry.size == 0) {
          sized_space_entry = pmem_allocator_->Allocate(request_size);
        }
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }
      } else {
        return Status::Ok;
      }
    }

    char *block_base =
        pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);
    uint64_t new_ts = get_timestamp();

    if (found && new_ts < data_entry.timestamp) {
      pmem_allocator_->Free(sized_space_entry);
      return Status::Ok;
    }

    std::vector<SpinMutex *> spins;
    thread_local Skiplist::Splice splice;
    if (!skiplist->FindAndLockWritePos(&splice, user_key, hint, spins,
                                       found ? &data_entry : nullptr)) {
      continue;
    }

    uint64_t prev_offset = pmem_allocator_->addr2offset(splice.prev_data_entry);
    uint64_t next_offset = pmem_allocator_->addr2offset(splice.next_data_entry);

    DLDataEntry write_entry(0, sized_space_entry.size, new_ts, dt,
                            collection_key.size(), value.size(), prev_offset,
                            next_offset);
    PersistDataEntry(block_base, &write_entry, collection_key, value, dt);

    if (found) {
      node = (SkiplistNode *)(hash_entry.offset);
      old_entry_offset = pmem_allocator_->addr2offset(node->data_entry);
    }

    node = (SkiplistNode *)skiplist->InsertDataEntry(
        &splice, (DLDataEntry *)block_base, user_key, node);

    if (!found) {
      hash_table_->Insert(hint, entry_base, dt, (uint64_t)node, false);
    } else {
      entry_base->header.type = dt;
      pmem_allocator_->Free(SizedSpaceEntry(
          old_entry_offset, data_entry.header.b_size, entry_base, hint.spin));
      if (!is_delete) {
        entry_base->header.reference = entry_base->header.reference + 1;
      }
    }

    for (auto &m : spins) {
      m->unlock();
    }
    break;
  }
  sized_space_entry.MaybeDeref();
  return Status::Ok;
}

Status KVEngine::SSet(const std::string &collection,
                      const std::string &user_key, const std::string &value) {
  MaybeInitWriteThread();
  Skiplist *skiplist = nullptr;
  Status s = SearchOrInitSkiplist(collection, &skiplist, true);
  if (s != Status::Ok) {
    return s;
  }

  return SSetImpl(skiplist, user_key, value, SORTED_DATA_RECORD);
}

Status KVEngine::CheckConfigs(const Configs &configs) {
  auto is_2pown = [](uint64_t n) { return (n > 0) && (n & (n - 1)) == 0; };

  if (configs.pmem_block_size < 16) {
    GlobalLogger.Error("pmem block size too small\n");
    return Status::InvalidConfiguration;
  }

  if (configs.pmem_segment_blocks * configs.pmem_block_size < 1024) {
    GlobalLogger.Error("pmem segment size too small\n");
    return Status::InvalidConfiguration;
  }

  if (configs.pmem_segment_blocks * configs.pmem_block_size *
          configs.max_write_threads >
      configs.pmem_file_size) {
    GlobalLogger.Error(
        "pmem file too small, should larger than pmem_segment_blocks * "
        "pmem_block_size * max_write_threads\n");
    return Status::InvalidConfiguration;
  }

  if (configs.hash_bucket_size < sizeof(HashEntry) + 8) {
    GlobalLogger.Error("hash bucket too small\n");
    return Status::InvalidConfiguration;
  }

  if (!is_2pown(configs.num_hash_buckets) ||
      !is_2pown(configs.num_buckets_per_slot)) {
    GlobalLogger.Error(
        "num_hash_buckets and num_buckets_per_slot should be 2^n\n");
    return Status::InvalidConfiguration;
  }

  if (configs.num_hash_buckets >= ((uint64_t)1 << 32)) {
    GlobalLogger.Error("too many hash buckets\n");
    return Status::InvalidConfiguration;
  }

  if (configs.num_buckets_per_slot > configs.num_hash_buckets) {
    GlobalLogger.Error(
        "num_buckets_per_slot should less than num_hash_buckets\n");
    return Status::InvalidConfiguration;
  }

  return Status::Ok;
}

Status KVEngine::SDelete(const std::string &collection,
                         const std::string &user_key) {
  MaybeInitWriteThread();
  Skiplist *skiplist = nullptr;
  Status s = SearchOrInitSkiplist(collection, &skiplist, false);
  if (s != Status::Ok) {
    return s == Status::NotFound ? Status::Ok : s;
  }

  return SSetImpl(skiplist, user_key, "", SORTED_DELETE_RECORD);
}

Status KVEngine::MaybeInitPendingBatchFile() {
  if (thread_res_[local_thread.id].persisted_pending_batch == nullptr) {
    int is_pmem;
    size_t mapped_len;
    uint64_t persisted_pending_file_size =
        MAX_WRITE_BATCH_SIZE * 8 + sizeof(PendingBatch);
    if ((thread_res_[local_thread.id].persisted_pending_batch =
             (PendingBatch *)pmem_map_file(
                 persisted_pending_block_file(local_thread.id).c_str(),
                 persisted_pending_file_size, PMEM_FILE_CREATE, 0666,
                 &mapped_len, &is_pmem)) == nullptr ||
        !is_pmem || mapped_len != persisted_pending_file_size) {
      return Status::MapError;
    }
  }
  return Status::Ok;
}

Status KVEngine::BatchWrite(const WriteBatch &write_batch) {
  if (write_batch.Size() > MAX_WRITE_BATCH_SIZE) {
    return Status::BatchOverflow;
  }

  MaybeInitWriteThread();
  Status s = MaybeInitPendingBatchFile();
  if (s != Status::Ok) {
    return s;
  }

  uint64_t ts = get_timestamp();

  std::vector<BatchWriteHint> batch_hints(write_batch.Size());
  std::vector<uint64_t> space_entry_offsets(write_batch.Size());
  for (size_t i = 0; i < write_batch.Size(); i++) {
    auto &kv = write_batch.kvs[i];
    uint32_t requested_size =
        kv.key.size() + kv.value.size() + sizeof(DataEntry);
    batch_hints[i].sized_space_entry =
        pmem_allocator_->Allocate(requested_size);
    if (batch_hints[i].sized_space_entry.size == 0) {
      return s;
    }

    batch_hints[i].ts = ts;
    space_entry_offsets[i] =
        batch_hints[i].sized_space_entry.space_entry.offset *
        configs_.pmem_block_size;
  }

  PendingBatch pending_batch(PendingBatch::Stage::Processing,
                             write_batch.Size(), ts);
  pending_batch.PersistProcessing(
      thread_res_[local_thread.id].persisted_pending_batch,
      space_entry_offsets);

  for (size_t i = 0; i < write_batch.Size(); i++) {
    auto &kv = write_batch.kvs[i];
    s = HashSetImpl(kv.key, kv.value, kv.type, &batch_hints[i]);

    if (s != Status::Ok) {
      return s;
    }
  }

  pending_batch.PersistStage(PendingBatch::Stage::Done);
  return s;
}

Status KVEngine::SGet(const std::string &collection,
                      const std::string &user_key, std::string *value) {
  Skiplist *skiplist = nullptr;
  Status s = SearchOrInitSkiplist(collection, &skiplist, false);
  if (s != Status::Ok) {
    return s;
  }
  assert(skiplist);
  uint64_t id = skiplist->id();
  std::string skiplist_key(PersistentList::ListKey(user_key, id));
  return HashGetImpl(skiplist_key, value,
                     SORTED_DATA_RECORD | SORTED_DELETE_RECORD);
}

Status KVEngine::HashSetImpl(const Slice &key, const Slice &value, uint16_t dt,
                             BatchWriteHint *batch_hint) {
  MaybeInitWriteThread();

  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  uint32_t v_size = value.size();

  uint32_t requested_size = v_size + key.size() + sizeof(DataEntry);
  char *block_base = nullptr;
  SizedSpaceEntry sized_space_entry;

  if (batch_hint == nullptr) {
    if (dt != STRING_DELETE_RECORD) {
      sized_space_entry = pmem_allocator_->Allocate(requested_size);
      if (sized_space_entry.size == 0) {
        return Status::PmemOverflow;
      }
    }
  } else {
    sized_space_entry = batch_hint->sized_space_entry;
  }

  {
    auto hint = hash_table_->GetHint(key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s = hash_table_->Search(hint, key,
                                   STRING_DELETE_RECORD | STRING_DATA_RECORD,
                                   &hash_entry, &data_entry, &entry_base, true);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;
    if (dt == STRING_DELETE_RECORD && batch_hint == nullptr) {
      if (found && entry_base->header.type != STRING_DELETE_RECORD) {
        sized_space_entry = pmem_allocator_->Allocate(requested_size);
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }
      } else {
        return Status::Ok;
      }
    }

    block_base =
        pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);

    uint64_t new_ts = batch_hint ? batch_hint->ts : get_timestamp();
    if (found && new_ts < data_entry.timestamp) {
      if (sized_space_entry.size > 0) {
        pmem_allocator_->Free(sized_space_entry);
      }
      return Status::Ok;
    }

    DataEntry write_entry(0, sized_space_entry.size, new_ts, dt, key.size(),
                          v_size);
    PersistDataEntry(block_base, &write_entry, key, value, dt);

    hash_table_->Insert(hint, entry_base, dt,
                        sized_space_entry.space_entry.offset,
                        found ? &data_entry : nullptr);
    if (found) {
      pmem_allocator_->Free(SizedSpaceEntry(
          hash_entry.offset, data_entry.header.b_size,
          hash_entry.header.type == STRING_DELETE_RECORD ? nullptr : entry_base,
          hash_entry.header.type == STRING_DELETE_RECORD ? nullptr
                                                         : hint.spin));
    }
  }

  sized_space_entry.MaybeDeref();
  return Status::Ok;
}

Status KVEngine::Set(const std::string &key, const std::string &value) {
  if (!CheckKeySize(key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }
  return HashSetImpl(key, value, STRING_DATA_RECORD);
}

} // namespace KVDK_NAMESPACE