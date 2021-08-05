/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "engine_impl.hpp"

#include <libpmem.h>
#include <sys/mman.h>

#include "mempool.hpp"
#include "safestringlib/include/safe_lib.h"
#include <algorithm>
#include <iostream>
#include <limits>
#include <thread>

namespace PMEMDB_NAMESPACE {

// use buffer to acc nt-write
thread_local std::string write_buffer;
static const int buffer_size = 1024 * 1024;

thread_local int t_id = -1;

#ifdef DO_STATS

void Stats::Print() {
#ifdef DE_LOG
  GlobalLogger.Error(
      "@@@ set stats: set_lru %llu set_pmem %llu "
      "search_hash_in_set %llu search_free_list %llu write_value %llu "
      "set_lru_hash_table_in_set %llu\n"
      "@@@ get stats: get_lru %llu get_pmem %llu "
      "get_offset %llu get_value %llu search_hash_in_get %llu, "
      "set_lru_hash_table_in_get %llu, search_lru_hash_table_in_get %llu\n",
      set_lru, set_pmem, search_hash_in_set, search_free_list, write_value,
      set_lru_hash_table_in_set, get_lru, get_pmem, get_offset, get_value,
      search_hash_in_get, set_lru_hash_table_in_get,
      search_lru_hash_table_in_get);
#endif
}
#endif

EngineImpl::EngineImpl() {}

EngineImpl::~EngineImpl() {
  if (hash_table)
    delete hash_table;
//  if (skiplist_)
//    delete skiplist_;
  pmem_unmap(pmem_value_log_, pmem_capacity_);
#ifdef DO_LOG
  GlobalLogger.Log("DB closed\n");
#endif
}

void EngineImpl::InitDataSize2BSize() {
  data_size_2_b_size.resize(4096);
  for (size_t i = 0; i < data_size_2_b_size.size(); i++) {
    data_size_2_b_size[i] = (i / options_.pmem_block_size) +
                            (i % options_.pmem_block_size == 0 ? 0 : 1);
  }
}

uint16_t EngineImpl::GetBlockSize(uint32_t data_size) {
  if (data_size < data_size_2_b_size.size()) {
    return data_size_2_b_size[data_size];
  }
  return data_size / options_.pmem_block_size +
         (data_size % options_.pmem_block_size == 0 ? 0 : 1);
}

void EngineImpl::Init(const std::string &name, const DBOptions &options) {
  options_ = options;
  int is_pmem;
  GlobalLogger.Log("Init db path %s\n", name.c_str());
  if ((pmem_value_log_ = (char *)pmem_map_file(
           name.c_str(), options_.pmem_file_size, PMEM_FILE_CREATE, 0666,
           &pmem_capacity_, &is_pmem)) == nullptr) {
    GlobalLogger.Error("Pmem map file %s failed: %s\n", name.c_str(),
                       strerror(errno));
    exit(1);
  }
  if (!is_pmem) {
    GlobalLogger.Error("%s is not a pmem path\n", name.c_str());
    exit(1);
  }
  if (pmem_capacity_ != options_.pmem_file_size) {
    GlobalLogger.Error("Pmem map file %s size %lu\n", name.c_str(),
                       pmem_capacity_);
  }
  GlobalLogger.Log("Map db path done\n");

  pmem_value_log_head_ = 0;
  thread_space_.resize(options_.max_write_threads);

  InitDataSize2BSize();

  hash_table =
      new HashTable(options_.max_memory_usage, options_.hash_bucket_num,
                    options_.hash_bucket_size, options_.slot_grain,
                    pmem_value_log_, options_.pmem_block_size, thread_space_);
  ts_on_startup_ = get_ts();
  std::vector<std::thread> ths;
  for (uint32_t i = 0; i < options_.max_write_threads; i += 1) {
    ths.emplace_back(std::thread(&EngineImpl::Recovery, this, i));
  }

  for (uint32_t i = 0; i < options_.max_write_threads; i++)
    ths[i].join();

#ifdef DO_LOG
  GlobalLogger.Log("In restoring: iterated %lu records\n", restored_.load());
#endif

  if (restored_.load() == 0) {
    if (options_.populate_pmem_space) {
      GlobalLogger.Log("populate pmem space ...\n");
      std::vector<std::thread> ths_init;
      for (int i = 0; i < 16; i++) {
        ths_init.emplace_back([=]() {
          // init pmem
          pmem_memset(pmem_value_log_ + pmem_capacity_ * i / 16, 0,
                      pmem_capacity_ / 16, PMEM_F_MEM_NONTEMPORAL);
        });
      }
      for (auto &t : ths_init)
        t.join();
      GlobalLogger.Log("Ok\n");
    }
  } else {
    for (auto &ts : thread_space_) {
      if (ts.newest_restored_version > newest_version_on_startup_) {
        newest_version_on_startup_ = ts.newest_restored_version;
      }
    }
  }
}

std::shared_ptr<SortedIterator> EngineImpl::NewSortedIterator() {
  InitTID();
  MaybeInitSkiplist();
  return std::make_shared<SortedIterator>(skiplist_.get(), pmem_value_log_);
}

void EngineImpl::InitTID() {
  if (t_id < 0) {
    t_id = threads_.fetch_add(1, std::memory_order_relaxed);
    if (t_id >= options_.max_write_threads) {
      GlobalLogger.Error("Too many write threads, exit\n");
      exit(1);
    }
  }
}

Status EngineImpl::MaybeInitSkiplist() {
  if (!skiplist_) {
    std::lock_guard<std::mutex> lg(skiplist_mu_);
    if (!skiplist_) {
      uint32_t b_size = GetBlockSize(sizeof(SortedDataEntry));
      SpaceEntry space_entry;
      Status s = SetValueOffset(b_size, &space_entry);
      if (s != Status::Ok) {
        return s;
      }
      char *block_base =
          pmem_value_log_ + space_entry.block_offset * options_.pmem_block_size;
      SortedDataEntry data_entry(0, b_size, get_version(), SORTED_HEADER_RECORD,
                                 0, 0, NULL_PMEM_OFFSET, NULL_PMEM_OFFSET);
      data_entry.checksum = data_entry.Checksum(hash_key("", 0));
      pmem_memcpy_persist(block_base, &data_entry, sizeof(SortedDataEntry));
      skiplist_.reset(new Skiplist((SortedDataEntry *)block_base, pmem_value_log_));
    }
  }
  return Status::Ok;
}

void EngineImpl::Recovery(uint64_t start) {
  char existing_data_entry_buffer[sizeof(SortedDataEntry)];
  char recovering_data_entry_buffer[sizeof(SortedDataEntry)];
  DataEntry *existing_data_entry = (DataEntry *)existing_data_entry_buffer;
  DataEntry *recovering_data_entry = (DataEntry *)recovering_data_entry_buffer;
  HashEntry hash_entry;
  bool rebuild_skiplist = false;

  uint64_t key_hash_value;
  std::string key;
  Slice pmem_key;
  HashEntry *entry_base;
  char *block_base;

  int cnt = 0;
  uint64_t segment_head = pmem_value_log_head_.fetch_add(
      options_.pmem_segment_blocks, std::memory_order_relaxed);
  while (1) {
    if (segment_head >= pmem_capacity_ / options_.pmem_block_size)
      break;
    block_base = pmem_value_log_ + segment_head * options_.pmem_block_size;
    DataEntry *pmem_data_entry = (DataEntry *)block_base;
    memcpy_s(recovering_data_entry, sizeof(SortedDataEntry), pmem_data_entry,
             data_entry_size(pmem_data_entry->type));
    if (recovering_data_entry->checksum == 0 &&
        recovering_data_entry->b_size == 0) {
      if (segment_head % options_.pmem_segment_blocks ==
          0) { // never written segment
        break;
      } else {
        uint64_t remained = options_.pmem_segment_blocks -
                            segment_head % options_.pmem_segment_blocks;
        if (remained >= PMEM_FREE_LIST_SLOT_NUM) {
          break;
        } else {
          thread_space_[start].free_list.Push(segment_head, remained, nullptr,
                                              nullptr);
          segment_head = pmem_value_log_head_.fetch_add(
              options_.pmem_segment_blocks, std::memory_order_relaxed);
          continue;
        }
        // TODO add a tag to indicate a padding space
      }
    }

    if (recovering_data_entry->version >
        thread_space_[start].newest_restored_version) {
      thread_space_[start].newest_restored_version =
          recovering_data_entry->version;
    }

    bool is_sorted_entry =
        recovering_data_entry->type &
        (SORTED_DATA_RECORD | SORTED_DELETE_RECORD | SORTED_HEADER_RECORD);

    pmem_key = is_sorted_entry ? ((SortedDataEntry *)pmem_data_entry)->Key()
                               : pmem_data_entry->Key();
    key.assign(pmem_key.data(), pmem_key.size());
    key_hash_value = hash_key(key.data(), key.size());
    uint32_t key_hash_prefix = key_hash_value >> 32;
    uint32_t checksum =
        is_sorted_entry
            ? ((SortedDataEntry *)pmem_data_entry)->Checksum(key_hash_value)
            : pmem_data_entry->Checksum(key_hash_value);
    if (recovering_data_entry->checksum != checksum ||
        recovering_data_entry->version == 0 /* padding block */) {
      // data corrupt
      thread_space_[start].free_list.Push(
          (block_base - pmem_value_log_) / options_.pmem_block_size,
          recovering_data_entry->b_size, nullptr, nullptr);

      // assert(segment_head + pmem_b_size <= options_.pmem_segment_blocks);
      segment_head += recovering_data_entry->b_size;
      if (segment_head % options_.pmem_segment_blocks == 0) {
        segment_head = pmem_value_log_head_.fetch_add(
            options_.pmem_segment_blocks, std::memory_order_relaxed);
      }
      continue;
    }

    if (recovering_data_entry->type == SORTED_HEADER_RECORD) {
      rebuild_skiplist = true;
      skiplist_.reset(
        new Skiplist((SortedDataEntry *)pmem_data_entry, pmem_value_log_));

      segment_head += recovering_data_entry->b_size;
      if (segment_head % options_.pmem_segment_blocks == 0) {
        segment_head = pmem_value_log_head_.fetch_add(
            options_.pmem_segment_blocks, std::memory_order_relaxed);
      }
      continue;
    }

    cnt++;

    // update hashtable.
    {
      auto hint = hash_table->GetHint(key_hash_value);
      std::lock_guard<SpinMutex> lg(*hint.spin);
      bool is_found = hash_table->Search(
          start, key, key_hash_value,
          is_sorted_entry ? (SORTED_DATA_RECORD | SORTED_DELETE_RECORD)
                          : (HASH_DELETE_RECORD | HASH_DATA_RECORD),
          &hash_entry, existing_data_entry, &entry_base, true, &hint);
      bool should_insert = !is_found;
      if (is_found) {
        entry_base->reference++;
        if (existing_data_entry->version < recovering_data_entry->version) {
          should_insert = true;
        } else {
          entry_base->reference++;
        }
      }

      if (should_insert) {
        uint64_t offset;
        if (is_sorted_entry) {
          Node *node;
          if (!is_found) {
            node = (Node *)malloc(sizeof(Node) + Skiplist::RandomHeight() * 8);
            if (!node)
              exit(1);
            offset = (uint64_t)node;
            hash_table->Insert(start, entry_base, key_hash_value,
                               recovering_data_entry->type, offset, nullptr,
                               &hint);
          } else {
            node = (Node *)hash_entry.offset;
            uint64_t old_offset = ((char *)node->data_entry - pmem_value_log_) /
                                  options_.pmem_block_size;
            thread_space_[start].free_list.Push(
                old_offset, existing_data_entry->b_size, entry_base, hint.spin);
            offset = hash_entry.offset;
            entry_base->reference++;
          }
          node->data_entry = (SortedDataEntry *)pmem_data_entry;
        } else {
          offset = (block_base - pmem_value_log_) / options_.pmem_block_size;
          hash_table->Insert(start, entry_base, key_hash_value,
                             recovering_data_entry->type, offset,
                             is_found ? existing_data_entry : nullptr, &hint);
        }
      } else {
        auto *entry_reference =
            (recovering_data_entry->type == HASH_DELETE_RECORD) ? nullptr
                                                                : entry_base;
        auto *entry_lock = (recovering_data_entry->type == HASH_DELETE_RECORD)
                               ? nullptr
                               : hint.spin;

        thread_space_[start].free_list.Push(
            (block_base - pmem_value_log_) / options_.pmem_block_size,
            recovering_data_entry->b_size, entry_reference, entry_lock);
      }
    }

    // assert(segment_head + pmem_b_size <= options_.pmem_segment_blocks);
    segment_head += recovering_data_entry->b_size;
    if (segment_head % options_.pmem_segment_blocks == 0) {
      segment_head = pmem_value_log_head_.fetch_add(
          options_.pmem_segment_blocks, std::memory_order_relaxed);
    }
  }

  if (rebuild_skiplist) {
    skiplist_->Rebuild(hash_table);
  }

  thread_space_[start].pmem_blocks_offset = segment_head;
  thread_space_[start].usable_blocks =
      options_.pmem_segment_blocks -
      (segment_head % options_.pmem_segment_blocks);
  restored_.fetch_add(cnt);
}

Status EngineImpl::HashGetImpl(const Slice &key, std::string *value,
                               uint8_t type_mask) {
  std::unique_ptr<DataEntry> data_entry(
      type_mask &
              (SORTED_DATA_RECORD | SORTED_HEADER_RECORD | SORTED_DELETE_RECORD)
          ? new SortedDataEntry
          : new DataEntry);
  HashEntry hash_entry;
  HashEntry *entry_base;
  uint64_t key_hash_value = hash_key(key.data(), key.size());
  char *block_base = nullptr;
  bool is_found =
      hash_table->Search(t_id, key, key_hash_value, type_mask, &hash_entry,
                         data_entry.get(), &entry_base, false, nullptr);

  if (!is_found ||
      (hash_entry.type & (HASH_DELETE_RECORD | SORTED_DELETE_RECORD))) {
    return Status::NotFound;
  }

  if (hash_entry.type == HASH_DATA_RECORD) {
    block_base = pmem_value_log_ + hash_entry.offset * options_.pmem_block_size;
  } else {
    Node *node = (Node *)hash_entry.offset;
    block_base = (char *)node->data_entry;
  }

  while (1) {
    value->assign(block_base + key.size() + data_entry_size(hash_entry.type),
                  data_entry->v_size);
    std::atomic_thread_fence(std::memory_order_acquire);
    // TODO double check for skiplist get
    if (__glibc_likely(hash_entry.type == SORTED_DATA_RECORD ||
                       hash_entry.offset == entry_base->offset))
      break;
    // Double check, 防止读取的value被更新后，空间被复用给其他value。
    memcpy_s(&hash_entry, sizeof(HashEntry), entry_base, sizeof(HashEntry));
    block_base = pmem_value_log_ +
                 (uint64_t)hash_entry.offset * options_.pmem_block_size;
    memcpy_s(data_entry.get(), sizeof(DataEntry), block_base, sizeof(DataEntry));
  }
  return Status::Ok;
}

Status EngineImpl::Get(const Slice &key, std::string *value) {
  return HashGetImpl(key, value, HASH_DELETE_RECORD | HASH_DATA_RECORD);
}

Status EngineImpl::Delete(const Slice &key) {
  return SetImpl(key, "", HASH_DELETE_RECORD);
}

Status EngineImpl::SSet(const Slice &key, const Slice &value) {
  InitTID();
  Status s = MaybeInitSkiplist();
  if (s != Status::Ok) {
    return s;
  }
  // init skiplist_

  SortedDataEntry data_entry;
  HashEntry hash_entry;
  Node *node = nullptr;
  HashEntry *entry_base = nullptr;
  uint64_t key_hash_value = hash_key(key.data(), key.size());

  uint32_t b_size =
      GetBlockSize(value.size() + key.size() + sizeof(SortedDataEntry));
  char *block_base = nullptr;
  SpaceEntry space_entry;

  s = SetValueOffset(b_size, &space_entry);
  if (s != Status::Ok) {
    return s;
  }

  block_base =
      pmem_value_log_ + space_entry.block_offset * options_.pmem_block_size;

  {
    auto hint = hash_table->GetHint(key_hash_value);
    while (1) {
      std::lock_guard<SpinMutex> lg(*hint.spin);
      bool found = hash_table->Search(
          t_id, key, key_hash_value, SORTED_DELETE_RECORD | SORTED_DATA_RECORD,
          &hash_entry, &data_entry, &entry_base, true, &hint);

      uint64_t new_version = get_version();

      if (found) {
        node = (Node *)(hash_entry.offset);
      }

      SortedDataEntry *prev;
      SortedDataEntry *next;
      uint64_t prev_offset;
      uint64_t next_offset;
      thread_local Skiplist::Splice splice;
      if (found) {
        prev = (SortedDataEntry *)(pmem_value_log_ + data_entry.prev);
        next = data_entry.next == NULL_PMEM_OFFSET
                   ? nullptr
                   : (SortedDataEntry *)(pmem_value_log_ + data_entry.next);
        prev_offset = data_entry.prev;
        next_offset = data_entry.next;
      } else {
        skiplist_->Seek(key, &splice);
        prev = splice.prev_data_entry;
        next = splice.next_data_entry;
        prev_offset = (char *)prev - pmem_value_log_;
        next_offset = next ? (char *)next - pmem_value_log_ : NULL_PMEM_OFFSET;
      }

      // sequentially lock to prevent deadlock
      int64_t prev_slot = -1, next_slot = -1;
      std::vector<std::pair<int64_t, SpinMutex *>> spins;
      auto cmp = [](const std::pair<int64_t, SpinMutex *> &p1,
                    const std::pair<int64_t, SpinMutex *> &p2) {
        return p1.first < p2.first;
      };
      if (prev == skiplist_->header_->data_entry) {
        spins.push_back({-1, &skiplist_->spin_});
      } else {
        auto tmp =
            hash_table->GetHint(hash_key((char *)(prev + 1), prev->k_size));
        prev_slot = tmp.slot;
        if (prev_slot != hint.slot) {
          spins.push_back({prev_slot, tmp.spin});
        }
      }
      if (next != nullptr) {
        auto tmp =
            hash_table->GetHint(hash_key((char *)(next + 1), next->k_size));
        next_slot = tmp.slot;
        if (next_slot != hint.slot && next_slot != prev_slot) {
          spins.push_back({next_slot, tmp.spin});
        }
      }
      std::sort(spins.begin(), spins.end(), cmp);
      bool retry = false;
      for (int i = 0; i < spins.size(); i++) {
        if (spins[i].second->try_lock()) {
        } else {
          retry = true;
          for (int j = 0; j < i; j++) {
            spins[j].second->unlock();
          }
          break;
        }
      }
      if (retry)
        continue;

      if ((!found && (prev->next != next_offset ||
                      (next && next->prev != prev_offset))) ||
          (found &&
           (data_entry.prev != prev_offset || data_entry.next != next_offset ||
            (prev->next != (char *)node->data_entry - pmem_value_log_) ||
            (next &&
             next->prev != (char *)node->data_entry - pmem_value_log_)))) {
        for (auto &m : spins) {
          m.second->unlock();
        }
        continue;
      }

      if (sizeof(SortedDataEntry) + key.size() + value.size() <= buffer_size) {
        if (write_buffer.empty()) {
          write_buffer.resize(buffer_size);
        }
        SortedDataEntry *pmem_data_entry = (SortedDataEntry *)&write_buffer[0];
        *pmem_data_entry =
            SortedDataEntry(0, 0, new_version, SORTED_DATA_RECORD, key.size(),
                            value.size(), prev_offset, next_offset);
        memcpy_s((char *)pmem_data_entry + sizeof(SortedDataEntry),
                 buffer_size - sizeof(SortedDataEntry), key.data(), key.size());
        memcpy_s((char *)pmem_data_entry + sizeof(SortedDataEntry) + key.size(),
                 buffer_size - sizeof(SortedDataEntry) - key.size(),
                 value.data(), value.size());
        pmem_data_entry->checksum = pmem_data_entry->Checksum(key_hash_value);
        pmem_data_entry->b_size = b_size;
        pmem_memcpy(block_base, &write_buffer[0],
                    sizeof(SortedDataEntry) + key.size() + value.size(),
                    PMEM_F_MEM_NONTEMPORAL);
        pmem_drain();
        prev->next = block_base - pmem_value_log_;
        pmem_persist(&prev->next, 8);
        if (next) {
          next->prev = block_base - pmem_value_log_;
          pmem_persist(&next->prev, 8);
        }
      }

      if (!found) {
        auto height = Skiplist::RandomHeight();
        node = (Node *)malloc(sizeof(Node) + height * 8);
        if (!node)
          exit(1);
        node->data_entry = (SortedDataEntry *)block_base;
        memcpy_8(node->key, key.data());
        for (int i = 0; i < height; i++) {
          while (1) {
            node->RelaxedSetNext(i, splice.nexts[i]);
            if (splice.prevs[i]->CASNext(i, splice.nexts[i], node)) {
              break;
            }
            splice.Recompute(key, i);
          }
        }
        hash_table->Insert(t_id, entry_base, key_hash_value, SORTED_DATA_RECORD,
                           (uint64_t)node, nullptr, &hint);
      } else {
        uint64_t old_offset =
            ((char *)((Node *)entry_base->offset)->data_entry -
             pmem_value_log_) /
            options_.pmem_block_size;
        node->data_entry = (SortedDataEntry *)block_base;
        thread_space_[t_id].free_list.Push(old_offset, data_entry.b_size,
                                           entry_base, hint.spin);
        entry_base->reference++;
      }

      for (auto &m : spins) {
        m.second->unlock();
      }
      break;
    }
  }
  space_entry.MaybeDeref();
  return s;
}

Status EngineImpl::SGet(const Slice &key, std::string *value) {
  return HashGetImpl(key, value, SORTED_DATA_RECORD | SORTED_HEADER_RECORD);
}

Status EngineImpl::SetImpl(const Slice &key, const Slice &value, uint8_t dt) {
  InitTID();

  Status s;
  thread_local DataEntry data_entry;
  thread_local HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  uint64_t key_hash_value = hash_key(key.data(), key.size());

  uint32_t new_hash_b_size =
      GetBlockSize(value.size() + key.size() + sizeof(DataEntry));
  // 获取空间是线程内，不需要加锁
  char *block_base = nullptr;
  SpaceEntry space_entry;

  if (dt != HASH_DELETE_RECORD) {
    s = SetValueOffset(new_hash_b_size, &space_entry);
    if (s != Status::Ok) {
      return s;
    }
    block_base =
        pmem_value_log_ + space_entry.block_offset * options_.pmem_block_size;
  }

  {
    // 以下需要保证 setvalue 完成前不会由于另一线程更新该 key
    // 导致写的空间被释放后重新分配至另一线程的 free_list
    // 中，所以需要全部加锁
    auto hint = hash_table->GetHint(key_hash_value);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    bool is_found = hash_table->Search(
        t_id, key, key_hash_value, HASH_DELETE_RECORD | HASH_DATA_RECORD,
        &hash_entry, &data_entry, &entry_base, true, &hint);

    if (dt == HASH_DELETE_RECORD) {
      if (is_found && entry_base->type != HASH_DELETE_RECORD) {
        s = SetValueOffset(new_hash_b_size, &space_entry);
        if (s != Status::Ok) {
          return s;
        }
        block_base = pmem_value_log_ +
                     space_entry.block_offset * options_.pmem_block_size;
      } else {
        return Status::Ok;
      }
    }

    uint64_t new_hash_version = get_version();
    if (is_found && new_hash_version <= data_entry.version) {
      new_hash_version = get_version();
    }

    if (sizeof(DataEntry) + key.size() + value.size() <= buffer_size) {
      if (write_buffer.empty())
        write_buffer.resize(buffer_size);
      DataEntry *pmem_data_entry = (DataEntry *)&write_buffer[0];
      *pmem_data_entry =
          DataEntry(0, 0, new_hash_version, dt, key.size(), value.size());
      memcpy_s((char *)(pmem_data_entry + 1), buffer_size - sizeof(DataEntry),
               key.data(), key.size());
      memcpy_s((char *)(pmem_data_entry + 1) + key.size(),
               buffer_size - sizeof(DataEntry) - key.size(), value.data(),
               value.size());
      pmem_data_entry->checksum = pmem_data_entry->Checksum(key_hash_value);
      pmem_data_entry->b_size = new_hash_b_size;

      pmem_memcpy(block_base, pmem_data_entry,
                  sizeof(DataEntry) + key.size() + value.size(),
                  PMEM_F_MEM_NONTEMPORAL);
    } else {
      // TODO accelerate
      DataEntry *pmem_data_entry = (DataEntry *)&block_base[0];
      *pmem_data_entry =
          DataEntry(0, 0, new_hash_version, dt, key.size(), value.size());
      memcpy_s((char *)(pmem_data_entry + 1), key.size(), key.data(),
               key.size());
      memcpy_s((char *)(pmem_data_entry + 1) + key.size(), value.size(),
               value.data(), value.size());
      pmem_data_entry->checksum = pmem_data_entry->Checksum(key_hash_value);
      pmem_data_entry->b_size = new_hash_b_size;
      pmem_flush(block_base, sizeof(DataEntry) + key.size() + value.size());
    }
    pmem_drain();

    hash_table->Insert(t_id, entry_base, key_hash_value, dt,
                       space_entry.block_offset,
                       is_found ? &data_entry : nullptr, &hint);
  }

  space_entry.MaybeDeref();
  return Status::Ok;
}

Status EngineImpl::Set(const Slice &key, const Slice &value) {
  return SetImpl(key, value, HASH_DATA_RECORD);
}

Status EngineImpl::SetValueOffset(uint32_t &b_size, SpaceEntry *space_entry) {
  uint64_t b_off = 0;

  bool full_segment = thread_space_[t_id].usable_blocks < b_size;

  if (full_segment) {
    // full segment, search free list first
    if (thread_space_[t_id].free_list.Get(b_size, space_entry) == Status::Ok) {
      return Status::Ok;
    }

    // allocate a new segment, padding and add remainning space of the old one
    // to free list
    // TODO: insert a padding tag here
    thread_space_[t_id].free_list.Push(thread_space_[t_id].pmem_blocks_offset,
                                       thread_space_[t_id].usable_blocks,
                                       nullptr, nullptr);

    thread_space_[t_id].pmem_blocks_offset = pmem_value_log_head_.fetch_add(
        options_.pmem_segment_blocks, std::memory_order_relaxed);
    thread_space_[t_id].usable_blocks = options_.pmem_segment_blocks;
    if (thread_space_[t_id].pmem_blocks_offset >=
        (pmem_capacity_ / options_.pmem_block_size) - b_size) {
#ifdef DO_LOG
      GlobalLogger.Error("PMEM OVERFLOW! pmem file size is %lu but try to "
                         "allocate space at offset %lu.\n",
                         pmem_capacity_, b_off * options_.pmem_block_size);
#endif
      return Status::PmemOverflow;
    }
  }
  b_off = thread_space_[t_id].pmem_blocks_offset;
  thread_space_[t_id].pmem_blocks_offset += b_size;
  thread_space_[t_id].usable_blocks -= b_size;

  space_entry->block_offset = b_off;
  space_entry->hash_entry_mutex = nullptr;
  space_entry->hash_entry_reference = nullptr;
  return Status::Ok;
}
} // namespace PMEMDB_NAMESPACE
