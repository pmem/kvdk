/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"

#include <dirent.h>
#include <sys/mman.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <future>
#include <limits>
#include <mutex>
#include <thread>

#include "backup_log.hpp"
#include "dram_allocator.hpp"
#include "hash_collection/iterator.hpp"
#include "kvdk/volatile/engine.hpp"
#include "list_collection/iterator.hpp"
#include "sorted_collection/iterator.hpp"
#include "structures.hpp"
#include "utils/sync_point.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

// Set class name of memory allocator for volatile key values.
#ifndef KVDK_VOLATILE_KV_MEMORY_ALLOCATOR_CLASS
#define KVDK_VOLATILE_KV_MEMORY_ALLOCATOR_CLASS SystemMemoryAllocator
#endif
// Enable jemalloc memory allocator for volatile key values if available.
#ifdef KVDK_WITH_JEMALLOC
#undef KVDK_VOLATILE_KV_MEMORY_ALLOCATOR_CLASS
#define KVDK_VOLATILE_KV_MEMORY_ALLOCATOR_CLASS JemallocMemoryAllocator
#endif

// Set class name of memory allocator for Skiplist nodes.
#ifndef KVDK_SKIPLIST_NODE_MEMORY_ALLOCATOR_CLASS
#define KVDK_SKIPLIST_NODE_MEMORY_ALLOCATOR_CLASS SystemMemoryAllocator
#endif

// Set class name of memory allocator for Hash Table's new buckets.
// HashTable's initial buckets are allocated by `global_memory_allocator`
#ifndef KVDK_HASH_TABLE_NEW_BUCKET_MEMORY_ALLOCATOR_CLASS
#define KVDK_HASH_TABLE_NEW_BUCKET_MEMORY_ALLOCATOR_CLASS SystemMemoryAllocator
#endif

KVEngine::~KVEngine() {
  GlobalLogger.Info("Closing instance ... \n");
  GlobalLogger.Info("Waiting bg threads exit ... \n");
  closing_ = true;
  terminateBackgroundWorks();
  // deleteCollections();
  ReportMemoryUsage();

  GlobalLogger.Info("Instance closed\n");
}

Status KVEngine::Open(const StringView engine_path, Engine** engine_ptr,
                      const Configs& configs) {
  std::string engine_path_str(string_view_2_string(engine_path));
  GlobalLogger.Info("Opening kvdk instance from %s ...\n",
                    engine_path_str.c_str());
  KVEngine* engine = new KVEngine(configs);
  Status s = engine->init(engine_path_str, configs);

  if (s == Status::Ok) {
    *engine_ptr = engine;
    engine->startBackgroundWorks();
    engine->ReportMemoryUsage();
  } else {
    GlobalLogger.Error("Init kvdk instance failed: %d\n", s);
    delete engine;
  }
  return s;
}

void KVEngine::ReportMemoryUsage() {
  // Check KV allocator is initialized before use it.
  // It may not be successfully initialized due to file operation errors.
  if (kv_allocator_ == nullptr) {
    return;
  }

  auto bytes = global_memory_allocator()->BytesAllocated();
  auto total = bytes;
  GlobalLogger.Info(
      "[0] Global Memory Allocator Usage: %ld B, %ld KB, %ld MB, %ld GB\n",
      bytes, (bytes / (1LL << 10)), (bytes / (1LL << 20)),
      (bytes / (1LL << 30)));

  bytes = kv_allocator_->BytesAllocated();
  total += bytes;
  GlobalLogger.Info(
      "[1] KV Memory Allocator Usage: %ld B, %ld KB, %ld MB, %ld GB\n", bytes,
      (bytes / (1LL << 10)), (bytes / (1LL << 20)), (bytes / (1LL << 30)));

  bytes = skiplist_node_allocator_->BytesAllocated();
  total += bytes;
  GlobalLogger.Info(
      "[2] Skiplist Node Memory Allocator Usage: %ld B, %ld KB, %ld MB, %ld "
      "GB\n",
      bytes, (bytes / (1LL << 10)), (bytes / (1LL << 20)),
      (bytes / (1LL << 30)));

  bytes = hashtable_new_bucket_allocator_->BytesAllocated();
  total += bytes;
  GlobalLogger.Info(
      "[3] Hashtable New Bucket Memory Allocator Usage: %ld B, %ld KB, %ld MB, "
      "%ld "
      "GB\n",
      bytes, (bytes / (1LL << 10)), (bytes / (1LL << 20)),
      (bytes / (1LL << 30)));

  GlobalLogger.Info("[+] Total Memory Usage: %ld B, %ld KB, %ld MB, %ld GB\n",
                    total, (total / (1LL << 10)), (total / (1LL << 20)),
                    (total / (1LL << 30)));
}

void KVEngine::startBackgroundWorks() {
  std::unique_lock<SpinMutex> ul(bg_work_signals_.terminating_lock);
  bg_work_signals_.terminating = false;
  bg_threads_.emplace_back(&KVEngine::backgroundMemoryUsageReporter, this);

  bool close_reclaimer = false;
  TEST_SYNC_POINT_CALLBACK("KVEngine::backgroundCleaner::NothingToDo",
                           &close_reclaimer);
  if (!close_reclaimer) {
    cleaner_.Start();
  }
}

void KVEngine::terminateBackgroundWorks() {
  cleaner_.Close();
  {
    std::unique_lock<SpinMutex> ul(bg_work_signals_.terminating_lock);
    bg_work_signals_.terminating = true;
    bg_work_signals_.dram_cleaner_cv.notify_all();
    bg_work_signals_.memory_usage_reporter_cv.notify_all();
  }
  for (auto& t : bg_threads_) {
    t.join();
  }
}

Status KVEngine::init(const std::string& name, const Configs& configs) {
  Status s = Status::Ok;
  configs_ = configs;
  s = checkGeneralConfigs(configs);
  if (s != Status::Ok) {
    return s;
  }

  (void)name;  // To suppress compile warnings
  Allocator* kv_allocator = new KVDK_VOLATILE_KV_MEMORY_ALLOCATOR_CLASS();
  kv_allocator->SetMaxAccessThreads(configs_.max_access_threads);
  kv_allocator->SetDestMemoryNodes(configs_.dest_memory_nodes);
  kv_allocator_.reset(kv_allocator);

  Allocator* skiplist_node_allocator =
      new KVDK_SKIPLIST_NODE_MEMORY_ALLOCATOR_CLASS();
  skiplist_node_allocator->SetMaxAccessThreads(configs_.max_access_threads);
  skiplist_node_allocator_.reset(skiplist_node_allocator);

  Allocator* hashtable_new_bucket_allocator =
      new KVDK_HASH_TABLE_NEW_BUCKET_MEMORY_ALLOCATOR_CLASS();
  hashtable_new_bucket_allocator->SetMaxAccessThreads(
      configs_.max_access_threads);
  hashtable_new_bucket_allocator_.reset(hashtable_new_bucket_allocator);

  GlobalLogger.Info("Global memory allocator: %s\n",
                    global_memory_allocator()->AllocatorName().c_str());
  GlobalLogger.Info("KV memory allocator: %s\n",
                    kv_allocator_->AllocatorName().c_str());
  GlobalLogger.Info("Skiplist node memory allocator: %s\n",
                    skiplist_node_allocator_->AllocatorName().c_str());
  GlobalLogger.Info("Hashtable new bucket memory allocator: %s\n",
                    hashtable_new_bucket_allocator->AllocatorName().c_str());

  hash_table_.reset(HashTable::NewHashTable(
      configs_.hash_bucket_num, configs_.num_buckets_per_slot,
      kv_allocator_.get(), hashtable_new_bucket_allocator,
      configs_.max_access_threads));
  dllist_locks_.reset(new LockTable{1UL << 20});

  if (kv_allocator_ == nullptr || hash_table_ == nullptr ||
      dllist_locks_ == nullptr) {
    GlobalLogger.Error("Init kvdk basic components error\n");
    return Status::Abort;
  }

  registerComparator("default", compare_string_view);
  return s;
}

Status KVEngine::Backup(const pmem::obj::string_view backup_log,
                        const Snapshot* snapshot) {
  std::string backup_log_file = string_view_2_string(backup_log);
  BackupLog backup;
  Status s = backup.Init(backup_log_file);
  GlobalLogger.Info("Backup instance to %s ...\n", backup_log_file.c_str());
  if (s != Status::Ok) {
    return s;
  }
  TimestampType backup_ts =
      static_cast<const SnapshotImpl*>(snapshot)->GetTimestamp();
  auto hashtable_iterator =
      hash_table_->GetIterator(0, hash_table_->GetSlotsNum());
  while (hashtable_iterator.Valid()) {
    auto ul = hashtable_iterator.AcquireSlotLock();
    auto slot_iter = hashtable_iterator.Slot();
    while (slot_iter.Valid()) {
      switch (slot_iter->GetRecordType()) {
        case RecordType::String: {
          StringRecord* record = slot_iter->GetIndex().string_record;
          while (record != nullptr && record->GetTimestamp() > backup_ts) {
            record =
                kv_allocator_->offset2addr<StringRecord>(record->old_version);
          }
          if (record && record->GetRecordStatus() == RecordStatus::Normal &&
              !record->HasExpired()) {
            s = backup.Append(RecordType::String, record->Key(),
                              record->Value(), record->GetExpireTime());
          }
          break;
        }
        case RecordType::SortedHeader: {
          DLRecord* header = slot_iter->GetIndex().skiplist->HeaderRecord();
          while (header != nullptr && header->GetTimestamp() > backup_ts) {
            header = kv_allocator_->offset2addr<DLRecord>(header->old_version);
          }
          if (header && header->GetRecordStatus() == RecordStatus::Normal &&
              !header->HasExpired()) {
            s = backup.Append(RecordType::SortedHeader, header->Key(),
                              header->Value(), header->GetExpireTime());
            if (s == Status::Ok) {
              // Append skiplist elems following the header
              auto skiplist = getSkiplist(Skiplist::FetchID(header));
              kvdk_assert(skiplist != nullptr,
                          "Backup skiplist should exist in map");
              auto skiplist_iter = SortedIteratorImpl(
                  skiplist.get(), kv_allocator_.get(),
                  static_cast<const SnapshotImpl*>(snapshot), false);
              for (skiplist_iter.SeekToFirst(); skiplist_iter.Valid();
                   skiplist_iter.Next()) {
                s = backup.Append(RecordType::SortedElem, skiplist_iter.Key(),
                                  skiplist_iter.Value(), kPersistTime);
                if (s != Status::Ok) {
                  break;
                }
              }
            }
          }
          break;
        }
        case RecordType::HashHeader: {
          DLRecord* header = slot_iter->GetIndex().hlist->HeaderRecord();
          while (header != nullptr && header->GetTimestamp() > backup_ts) {
            header = kv_allocator_->offset2addr<DLRecord>(header->old_version);
          }
          if (header && header->GetRecordStatus() == RecordStatus::Normal &&
              !header->HasExpired()) {
            s = backup.Append(RecordType::HashHeader, header->Key(),
                              header->Value(), header->GetExpireTime());
            if (s == Status::Ok) {
              // Append hlist elems following the header
              auto hlist = getHashlist(HashList::FetchID(header));
              kvdk_assert(hlist != nullptr, "Backup hlist should exist in map");
              auto hlist_iter = HashIteratorImpl(
                  hlist.get(), static_cast<const SnapshotImpl*>(snapshot),
                  false);
              for (hlist_iter.SeekToFirst(); hlist_iter.Valid();
                   hlist_iter.Next()) {
                s = backup.Append(RecordType::HashElem, hlist_iter.Key(),
                                  hlist_iter.Value(), kPersistTime);
                if (s != Status::Ok) {
                  break;
                }
              }
            }
          }
          break;
        }
        case RecordType::ListHeader: {
          DLRecord* header = slot_iter->GetIndex().list->HeaderRecord();
          while (header != nullptr && header->GetTimestamp() > backup_ts) {
            header = kv_allocator_->offset2addr<DLRecord>(header->old_version);
          }
          if (header && header->GetRecordStatus() == RecordStatus::Normal &&
              !header->HasExpired()) {
            s = backup.Append(RecordType::ListHeader, header->Key(),
                              header->Value(), header->GetExpireTime());
            if (s == Status::Ok) {
              // Append hlist elems following the header
              auto list = getList(List::FetchID(header));
              kvdk_assert(list != nullptr, "Backup list should exist in map");
              auto list_iter = ListIteratorImpl(
                  list.get(), static_cast<const SnapshotImpl*>(snapshot),
                  false);
              for (list_iter.SeekToFirst(); list_iter.Valid();
                   list_iter.Next()) {
                s = backup.Append(RecordType::ListElem, "", list_iter.Value(),
                                  kPersistTime);
                if (s != Status::Ok) {
                  break;
                }
              }
            }
          }
          break;
        }
        default:
          // Hash and list backup is not supported yet
          break;
      }
      if (s != Status::Ok) {
        backup.Destroy();
        return s;
      }
      slot_iter++;
    }
    hashtable_iterator.Next();
  }
  backup.Finish();
  GlobalLogger.Info("Backup instance to %s Finished\n",
                    backup_log_file.c_str());
  return Status::Ok;
}

Status KVEngine::Restore(const StringView engine_path,
                         const StringView backup_log, Engine** engine_ptr,
                         const Configs& configs) {
  std::string engine_path_str(string_view_2_string(engine_path));
  std::string backup_log_str(string_view_2_string(backup_log));
  GlobalLogger.Info(
      "Restoring kvdk instance from backup log %s to engine path %s\n",
      backup_log_str.c_str(), engine_path_str.c_str());
  KVEngine* engine = new KVEngine(configs);
  Status s = engine->init(engine_path_str, configs);
  if (s == Status::Ok) {
    s = engine->restoreDataFromBackup(backup_log_str);
  }

  if (s == Status::Ok) {
    *engine_ptr = engine;
    engine->startBackgroundWorks();
    engine->ReportMemoryUsage();
  } else {
    GlobalLogger.Error("Restore kvdk instance from backup log %s failed: %d\n",
                       backup_log_str.c_str(), s);
    delete engine;
  }
  return s;
}

Status KVEngine::restoreDataFromBackup(const std::string& backup_log) {
  // TODO: make this multi-thread
  BackupLog backup;
  Status s = backup.Open(backup_log);
  if (s != Status::Ok) {
    return s;
  }
  auto wo = WriteOptions();
  auto iter = backup.GetIterator();
  if (iter == nullptr) {
    GlobalLogger.Error("Restore from a not finished backup log %s\n",
                       backup_log.c_str());
    return Status::Abort;
  }
  uint64_t cnt = 0;
  GlobalLogger.Info("Start iterating backup log\n");
  while (iter->Valid()) {
    auto record = iter->Record();
    bool expired = TimeUtils::CheckIsExpired(record.expire_time);
    wo.ttl_time = record.expire_time;
    switch (record.type) {
      case RecordType::String: {
        if (!expired) {
          cnt++;
          s = Put(record.key, record.val, wo);
        }
        iter->Next();
        break;
      }
      case RecordType::SortedHeader: {
        // Maybe reuse id?
        std::shared_ptr<Skiplist> skiplist = nullptr;
        if (!expired) {
          CollectionIDType id;
          SortedCollectionConfigs s_configs;
          s = Skiplist::DecodeSortedCollectionValue(record.val, id, s_configs);
          if (s != Status::Ok) {
            break;
          }
          s = buildSkiplist(record.key, s_configs, skiplist);
          if (s == Status::Ok && wo.ttl_time != kPersistTime) {
            skiplist->SetExpireTime(wo.ttl_time,
                                    version_controller_.GetCurrentTimestamp());
          }
          if (s != Status::Ok) {
            break;
          }
          cnt++;
        }
        iter->Next();
        // the header is followed by all its elems in backup log
        while (iter->Valid()) {
          record = iter->Record();
          if (record.type != RecordType::SortedElem) {
            break;
          }
          if (!expired) {
            auto ret = skiplist->Put(record.key, record.val,
                                     version_controller_.GetCurrentTimestamp());
            s = ret.s;
            if (s != Status::Ok) {
              break;
            }
            cnt++;
          }
          iter->Next();
        }
        break;
      }
      case RecordType::HashHeader: {
        std::shared_ptr<HashList> hlist = nullptr;
        if (!expired) {
          s = buildHashlist(record.key, hlist);
          if (s == Status::Ok && wo.ttl_time != kPersistTime) {
            hlist->SetExpireTime(wo.ttl_time,
                                 version_controller_.GetCurrentTimestamp());
          }
          if (s != Status::Ok) {
            break;
          }
          cnt++;
        }
        iter->Next();
        // the header is followed by all its elems in backup log
        while (iter->Valid()) {
          record = iter->Record();
          if (record.type != RecordType::HashElem) {
            break;
          }
          if (!expired) {
            auto ret = hlist->Put(record.key, record.val,
                                  version_controller_.GetCurrentTimestamp());
            s = ret.s;
            if (s != Status::Ok) {
              break;
            }
            cnt++;
          }
          iter->Next();
        }
        break;
      }
      case RecordType::ListHeader: {
        std::shared_ptr<List> list = nullptr;
        if (!expired) {
          s = buildList(record.key, list);
          if (s == Status::Ok && wo.ttl_time != kPersistTime) {
            list->SetExpireTime(wo.ttl_time,
                                version_controller_.GetCurrentTimestamp());
          }
          if (s != Status::Ok) {
            break;
          }
          cnt++;
        }
        iter->Next();
        // the header is followed by all its elems in backup log
        while (iter->Valid()) {
          record = iter->Record();
          if (record.type != RecordType::ListElem) {
            break;
          }
          if (!expired) {
            auto ret = list->PushBack(
                record.val, version_controller_.GetCurrentTimestamp());
            s = ret.s;
            if (s != Status::Ok) {
              break;
            }
            cnt++;
          }
          iter->Next();
        }
        break;
      }
      case RecordType::SortedElem: {
        GlobalLogger.Error("sorted elems not lead by header in backup log %s\n",
                           backup_log.c_str());
        return Status::Abort;
      }
      case RecordType::HashElem: {
        GlobalLogger.Error("hash elems not lead by header in backup log %s\n",
                           backup_log.c_str());
        return Status::Abort;
      }
      case RecordType::ListElem: {
        GlobalLogger.Error("list elems not lead by header in backup log %s\n",
                           backup_log.c_str());
        return Status::Abort;
      }
      default:
        GlobalLogger.Error("unsupported record type %u in backup log %s\n",
                           record.type, backup_log.c_str());
        return Status::Abort;
    }
    if (s != Status::Ok) {
      return Status::Abort;
    }
  }
  GlobalLogger.Info("Restore %lu records from backup log\n", cnt);
  return Status::Ok;
}

Status KVEngine::checkGeneralConfigs(const Configs& configs) {
  auto is_2pown = [](uint64_t n) { return (n > 0) && (n & (n - 1)) == 0; };

  if (!is_2pown(configs.hash_bucket_num) ||
      !is_2pown(configs.num_buckets_per_slot)) {
    GlobalLogger.Error(
        "hash_bucket_num and num_buckets_per_slot should be 2^n\n");
    return Status::InvalidConfiguration;
  }

  if (configs.hash_bucket_num >= ((uint64_t)1 << 32)) {
    GlobalLogger.Error("too many hash buckets\n");
    return Status::InvalidConfiguration;
  }

  if (configs.num_buckets_per_slot > configs.hash_bucket_num) {
    GlobalLogger.Error(
        "num_buckets_per_slot should less than hash_bucket_num\n");
    return Status::InvalidConfiguration;
  }

  return Status::Ok;
}

Status KVEngine::BatchWrite(std::unique_ptr<WriteBatch> const& batch) {
  WriteBatchImpl const* batch_impl =
      dynamic_cast<WriteBatchImpl const*>(batch.get());
  if (batch_impl == nullptr) {
    return Status::InvalidArgument;
  }

  return batchWriteImpl(*batch_impl);
}

Status KVEngine::maybeInitBatchLogFile() {
  // TODO Implement batch write without pmem
  return Status::Ok;
}

Status KVEngine::batchWriteImpl(WriteBatchImpl const& batch) {
  if (batch.Size() > BatchWriteLog::Capacity()) {
    return Status::InvalidBatchSize;
  }

  auto thread_holder = AcquireAccessThread();

  Status s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }

  // Prevent collection and nodes in double linked lists from being deleted
  auto access_token = version_controller_.GetLocalSnapshotHolder();

  std::vector<StringWriteArgs> string_args;
  string_args.reserve(batch.StringOps().size());
  std::vector<SortedWriteArgs> sorted_args;
  sorted_args.reserve(batch.SortedOps().size());
  std::vector<HashWriteArgs> hash_args;
  hash_args.reserve(batch.HashOps().size());

  for (auto const& string_op : batch.StringOps()) {
    string_args.emplace_back();
    string_args.back().Assign(string_op);
  }

  // Lookup Skiplists and Hashes for further operations
  for (auto const& sorted_op : batch.SortedOps()) {
    auto res = lookupKey<false>(sorted_op.collection, RecordType::SortedHeader);
    /// TODO: this is a temporary work-around
    /// We cannot lock both key and field, which may trigger deadlock.
    /// However, if a collection is created and a field is inserted,
    /// Delete operation in batch may skip this field,
    /// which causes inconsistency.
    if (res.s == Status::Outdated) {
      return Status::NotFound;
    }
    if (res.s != Status::Ok) {
      return res.s;
    }
    Skiplist* skiplist = res.entry.GetIndex().skiplist;
    sorted_args.emplace_back(
        skiplist->InitWriteArgs(sorted_op.key, sorted_op.value, sorted_op.op));
  }

  for (auto const& hash_op : batch.HashOps()) {
    HashList* hlist;
    Status s = hashListFind(hash_op.collection, &hlist);
    if (s != Status::Ok) {
      return s;
    }
    hash_args.emplace_back(
        hlist->InitWriteArgs(hash_op.key, hash_op.value, hash_op.op));
  }

  // Keys/internal keys to be locked on HashTable
  std::vector<std::string> keys_to_lock;
  for (auto const& string_op : batch.StringOps()) {
    keys_to_lock.push_back(string_op.key);
  }
  for (auto const& arg : sorted_args) {
    keys_to_lock.push_back(arg.skiplist->InternalKey(arg.key));
  }
  for (auto const& arg : hash_args) {
    keys_to_lock.push_back(arg.hlist->InternalKey(arg.key));
  }

  auto guard = hash_table_->RangeLock(keys_to_lock);
  keys_to_lock.clear();

  // Lookup keys, allocate space according to result.
  auto ReleaseResources = [&]() {
  // Don't Free() if we simulate a crash.
#ifndef KVDK_ENABLE_CRASHPOINT
    for (auto iter = hash_args.rbegin(); iter != hash_args.rend(); ++iter) {
      kv_allocator_->Free(iter->space);
      if (iter->lookup_result.entry_ptr->Allocated()) {
        kvdk_assert(iter->lookup_result.s == Status::NotFound, "");
        iter->lookup_result.entry_ptr->Clear();
      }
    }
    for (auto iter = sorted_args.rbegin(); iter != sorted_args.rend(); ++iter) {
      kv_allocator_->Free(iter->space);
      if (iter->lookup_result.entry_ptr->Allocated()) {
        kvdk_assert(iter->lookup_result.s == Status::NotFound, "");
        iter->lookup_result.entry_ptr->Clear();
      }
    }
    for (auto iter = string_args.rbegin(); iter != string_args.rend(); ++iter) {
      kv_allocator_->Free(iter->space);
      if (iter->res.entry_ptr->Allocated()) {
        kvdk_assert(iter->res.s == Status::NotFound, "");
        iter->res.entry_ptr->Clear();
      }
    }
#endif
  };

  defer(ReleaseResources());

  // Prevent generating snapshot newer than this WriteBatch
  auto bw_token = version_controller_.GetBatchWriteToken();

  // Prepare for Strings
  for (auto& args : string_args) {
    Status s = stringWritePrepare(args, bw_token.Timestamp());
    if (s != Status::Ok) {
      return s;
    }
  }

  // Prepare for Sorted Elements
  for (auto& args : sorted_args) {
    Status s = sortedWritePrepare(args, bw_token.Timestamp());
    if (s != Status::Ok) {
      return s;
    }
  }

  // Prepare for Hash Elements
  for (auto& args : hash_args) {
    Status s = hashWritePrepare(args, bw_token.Timestamp());
    if (s != Status::Ok) {
      return s;
    }
  }

  // Preparation done. Persist BatchLog for rollback.
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());
  auto& tc = engine_thread_cache_[ThreadManager::ThreadID() %
                                  configs_.max_access_threads];
  for (auto& args : string_args) {
    if (args.space.size == 0) {
      continue;
    }
    if (args.op == WriteOp::Put) {
      log.StringPut(args.space.offset);
    } else {
      log.StringDelete(args.space.offset);
    }
  }
  for (auto& args : sorted_args) {
    if (args.space.size == 0) {
      continue;
    }
    if (args.op == WriteOp::Put) {
      log.SortedPut(args.space.offset);
    } else {
      log.SortedDelete(args.space.offset);
    }
  }

  for (auto& args : hash_args) {
    if (args.space.size == 0) {
      continue;
    }
    if (args.op == WriteOp::Put) {
      log.HashPut(args.space.offset);
    } else {
      log.HashDelete(args.space.offset);
    }
  }

  log.EncodeTo(tc.batch_log);

  BatchWriteLog::MarkProcessing(tc.batch_log);

  // After preparation stage, no runtime error is allowed for now,
  // otherwise we have to perform runtime rollback.

  // Write Strings
  for (auto& args : string_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = stringWrite(args);
    kvdk_assert(s == Status::Ok, "");
  }

  // Write Sorted Elems
  for (auto& args : sorted_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = sortedWrite(args);
    kvdk_assert(s == Status::Ok, "");
  }

  // Write Hash Elems
  for (auto& args : hash_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = hashListWrite(args);
    kvdk_assert(s == Status::Ok, "");
  }

  TEST_CRASH_POINT("KVEngine::batchWriteImpl::BeforeCommit", "");

  BatchWriteLog::MarkCommitted(tc.batch_log);

  // Publish stages is where Strings and Collections make BatchWrite
  // visible to other threads.
  // This stage allows no failure during runtime,
  // otherwise dirty read may occur.
  // Crash is tolerated as BatchWrite will be recovered.

  // Publish Strings to HashTable
  for (auto const& args : string_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = stringWritePublish(args);
    kvdk_assert(s == Status::Ok, "");
  }

  // Publish Sorted Elements to HashTable
  for (auto const& args : sorted_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = sortedWritePublish(args);
    kvdk_assert(s == Status::Ok, "");
  }

  // Publish Hash Elements to HashTable
  for (auto& args : hash_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = hashListPublish(args);
    kvdk_assert(s == Status::Ok, "");
  }

  hash_args.clear();
  sorted_args.clear();
  string_args.clear();

  return Status::Ok;
}

Status KVEngine::GetTTL(const StringView key, TTLType* ttl_time) {
  *ttl_time = kInvalidTTL;
  auto ul = hash_table_->AcquireLock(key);
  auto res = lookupKey<false>(key, ExpirableRecordType);

  if (res.s == Status::Ok) {
    ExpireTimeType expire_time;
    switch (res.entry_ptr->GetIndexType()) {
      case PointerType::Skiplist: {
        expire_time = res.entry_ptr->GetIndex().skiplist->GetExpireTime();
        break;
      }
      case PointerType::List: {
        expire_time = res.entry_ptr->GetIndex().list->GetExpireTime();
        break;
      }
      case PointerType::HashList: {
        expire_time = res.entry_ptr->GetIndex().hlist->GetExpireTime();
        break;
      }
      case PointerType::StringRecord: {
        expire_time = res.entry_ptr->GetIndex().string_record->GetExpireTime();
        break;
      }
      default: {
        return Status::NotSupported;
      }
    }
    // return ttl time
    *ttl_time = TimeUtils::ExpireTimeToTTL(expire_time);
  }
  return res.s == Status::Outdated ? Status::NotFound : res.s;
}

Status KVEngine::TypeOf(StringView key, ValueType* type) {
  auto res = lookupKey<false>(key, ExpirableRecordType);

  if (res.s == Status::Ok) {
    switch (res.entry_ptr->GetIndexType()) {
      case PointerType::Skiplist: {
        *type = ValueType::SortedCollection;
        break;
      }
      case PointerType::List: {
        *type = ValueType::List;
        break;
      }
      case PointerType::HashList: {
        *type = ValueType::HashCollection;
        break;
      }
      case PointerType::StringRecord: {
        *type = ValueType::String;
        break;
      }
      default: {
        return Status::Abort;
      }
    }
  }
  return res.s == Status::Outdated ? Status::NotFound : res.s;
}

Status KVEngine::Expire(const StringView key, TTLType ttl_time) {
  auto thread_holder = AcquireAccessThread();

  int64_t base_time = TimeUtils::millisecond_time();
  if (!TimeUtils::CheckTTL(ttl_time, base_time)) {
    return Status::InvalidArgument;
  }

  ExpireTimeType expired_time = TimeUtils::TTLToExpireTime(ttl_time, base_time);
  auto ul = hash_table_->AcquireLock(key);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  // TODO: maybe have a wrapper function(lookupKeyAndMayClean).
  auto lookup_result = lookupKey<false>(key, ExpirableRecordType);
  if (lookup_result.s == Status::Outdated) {
    return Status::NotFound;
  }

  if (lookup_result.s == Status::Ok) {
    WriteOptions write_option{ttl_time};
    switch (lookup_result.entry_ptr->GetIndexType()) {
      case PointerType::StringRecord: {
        ul.unlock();
        version_controller_.ReleaseLocalSnapshot();
        lookup_result.s = Modify(
            key,
            [](const std::string* old_val, std::string* new_val, void*) {
              new_val->assign(*old_val);
              return ModifyOperation::Write;
            },
            nullptr, write_option);
        break;
      }
      case PointerType::Skiplist: {
        auto new_ts = snapshot_holder.Timestamp();
        Skiplist* skiplist = lookup_result.entry_ptr->GetIndex().skiplist;
        std::unique_lock<std::mutex> skiplist_lock(skiplists_mu_);
        expirable_skiplists_.erase(skiplist);
        auto ret = skiplist->SetExpireTime(expired_time, new_ts);
        expirable_skiplists_.emplace(skiplist);
        lookup_result.s = ret.s;
        break;
      }
      case PointerType::HashList: {
        auto new_ts = snapshot_holder.Timestamp();
        HashList* hlist = lookup_result.entry_ptr->GetIndex().hlist;
        std::unique_lock<std::mutex> hlist_lock(hlists_mu_);
        expirable_hlists_.erase(hlist);
        lookup_result.s = hlist->SetExpireTime(expired_time, new_ts).s;
        expirable_hlists_.emplace(hlist);
        break;
      }
      case PointerType::List: {
        auto new_ts = snapshot_holder.Timestamp();
        List* list = lookup_result.entry_ptr->GetIndex().list;
        lookup_result.s = list->SetExpireTime(expired_time, new_ts).s;
        break;
      }
      default: {
        return Status::NotSupported;
      }
    }
  }
  return lookup_result.s;
}
}  // namespace KVDK_NAMESPACE

// lookupKey
namespace KVDK_NAMESPACE {
template <bool may_insert>
HashTable::LookupResult KVEngine::lookupElem(StringView key,
                                             uint8_t type_mask) {
  kvdk_assert(type_mask & (RecordType::HashElem | RecordType::SortedElem), "");
  return hash_table_->Lookup<may_insert>(key, type_mask);
}

template HashTable::LookupResult KVEngine::lookupElem<true>(StringView,
                                                            uint8_t);
template HashTable::LookupResult KVEngine::lookupElem<false>(StringView,
                                                             uint8_t);

template <bool may_insert>
HashTable::LookupResult KVEngine::lookupKey(StringView key, uint8_t type_mask) {
  auto result = hash_table_->Lookup<may_insert>(key, PrimaryRecordType);

  if (result.s != Status::Ok) {
    kvdk_assert(
        result.s == Status::NotFound || result.s == Status::MemoryOverflow, "");
    return result;
  }

  RecordType type = result.entry.GetRecordType();
  RecordStatus record_status = result.entry.GetRecordStatus();
  bool type_match = type_mask & type;

  // TODO: fix mvcc of different type keys
  if (!type_match) {
    result.s = Status::WrongType;
  } else if (record_status == RecordStatus::Outdated) {
    result.s = Status::Outdated;
  } else {
    switch (type) {
      case RecordType::String: {
        result.s = result.entry.GetIndex().string_record->HasExpired()
                       ? Status::Outdated
                       : Status::Ok;
        break;
      }
      case RecordType::SortedHeader:
        result.s = result.entry.GetIndex().skiplist->HasExpired()
                       ? Status::Outdated
                       : Status::Ok;

        break;
      case RecordType::ListHeader:
        result.s = result.entry.GetIndex().list->HasExpired() ? Status::Outdated
                                                              : Status::Ok;
        break;
      case RecordType::HashHeader: {
        result.s = result.entry.GetIndex().hlist->HasExpired()
                       ? Status::Outdated
                       : Status::Ok;
        break;
      }
      default: {
        kvdk_assert(false, "Unreachable branch!");
        std::abort();
      }
    }
  }
  return result;
}

template HashTable::LookupResult KVEngine::lookupKey<true>(StringView, uint8_t);
template HashTable::LookupResult KVEngine::lookupKey<false>(StringView,
                                                            uint8_t);

template <typename T>
T* KVEngine::removeOutDatedVersion(T* record, TimestampType min_snapshot_ts) {
  static_assert(
      std::is_same<T, StringRecord>::value || std::is_same<T, DLRecord>::value,
      "Invalid record type, should be StringRecord or DLRecord.");
  T* ret = nullptr;
  auto old_record = record;
  while (old_record && old_record->GetTimestamp() > min_snapshot_ts) {
    old_record =
        static_cast<T*>(kv_allocator_->offset2addr(old_record->old_version));
  }

  // the snapshot should access the old record, so we need to purge and free the
  // older version of the old record
  if (old_record && old_record->old_version != kNullMemoryOffset) {
    T* remove_record =
        kv_allocator_->offset2addr_checked<T>(old_record->old_version);
    ret = remove_record;
    old_record->PersistOldVersion(kNullMemoryOffset);
    while (remove_record != nullptr) {
      if (remove_record->GetRecordStatus() == RecordStatus::Normal) {
        remove_record->PersistStatus(RecordStatus::Dirty);
      }
      remove_record = kv_allocator_->offset2addr<T>(remove_record->old_version);
    }
  }
  return ret;
}

template StringRecord* KVEngine::removeOutDatedVersion<StringRecord>(
    StringRecord*, TimestampType);
template DLRecord* KVEngine::removeOutDatedVersion<DLRecord>(DLRecord*,
                                                             TimestampType);

}  // namespace KVDK_NAMESPACE

// Snapshot, delayFree and background work
namespace KVDK_NAMESPACE {

/// TODO: move this into VersionController.
Snapshot* KVEngine::GetSnapshot(bool make_checkpoint) {
  Snapshot* ret = version_controller_.NewGlobalSnapshot();
  return ret;
}

void KVEngine::backgroundMemoryUsageReporter() {
  auto interval = std::chrono::milliseconds{
      static_cast<std::uint64_t>(configs_.report_memory_usage_interval * 1000)};
  while (!bg_work_signals_.terminating) {
    {
      std::unique_lock<SpinMutex> ul(bg_work_signals_.terminating_lock);
      if (!bg_work_signals_.terminating) {
        bg_work_signals_.memory_usage_reporter_cv.wait_for(ul, interval);
      }
    }
    ReportMemoryUsage();
    GlobalLogger.Info("Cleaner Thread Num: %ld\n", cleaner_.ActiveThreadNum());
  }
}

}  // namespace KVDK_NAMESPACE
