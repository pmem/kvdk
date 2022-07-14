#pragma once

#include "../dl_list.hpp"
#include "../generic_list.hpp"
#include "../hash_table.hpp"
#include "../version/version_controller.hpp"
#include "../write_batch_impl.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"
#include "kvdk/types.hpp"

namespace KVDK_NAMESPACE {

class HashIteratorImpl;

class HashList : public Collection {
 public:
  struct WriteResult {
    Status s = Status::Ok;
    DLRecord* existing_record = nullptr;
    DLRecord* write_record = nullptr;
    HashEntry* hash_entry_ptr = nullptr;
  };

  HashList(DLRecord* header, const StringView& name, CollectionIDType id,
           PMEMAllocator* pmem_allocator, HashTable* hash_table,
           LockTable* lock_table)
      : Collection(name, id),
        dl_list_(header, pmem_allocator, lock_table),
        size_(0),
        pmem_allocator_(pmem_allocator),
        hash_table_(hash_table) {}

  ~HashList() final = default;

  DLList* GetDLList() { return &dl_list_; }

  DLRecord* HeaderRecord() const { return dl_list_.Header(); }

  size_t Size() { return size_; }

  WriteResult Put(const StringView& key, const StringView& value,
                  TimeStampType timestamp) {
    WriteResult ret;
    HashWriteArgs args = InitWriteArgs(key, value, WriteBatchImpl::Op::Put);
    ret.s = PrepareWrite(args, timestamp);
    if (ret.s == Status::Ok) {
      ret = Write(args);
    }
    return ret;
  }

  Status Get(const StringView& key, std::string* value) {
    std::string internal_key(InternalKey(key));
    auto lookup_result =
        hash_table_->Lookup<false>(internal_key, RecordType::HashElem);
    if (lookup_result.s != Status::Ok ||
        lookup_result.entry.GetRecordStatus() == RecordStatus::Outdated) {
      return Status::NotFound;
    }

    DLRecord* pmem_record = lookup_result.entry.GetIndex().dl_record;
    kvdk_assert(pmem_record->GetRecordType() == RecordType::HashElem, "");
    // As get is lockless, skiplist node may point to a new elem delete record
    // after we get it from hashtable
    if (pmem_record->GetRecordStatus() == RecordStatus::Outdated) {
      return Status::NotFound;
    } else {
      value->assign(pmem_record->Value().data(), pmem_record->Value().size());
      return Status::Ok;
    }
  }

  WriteResult Delete(const StringView& key, TimeStampType timestamp) {
    WriteResult ret;
    HashWriteArgs args = InitWriteArgs(key, "", WriteBatchImpl::Op::Delete);
    ret.s = PrepareWrite(args, timestamp);
    if (ret.s == Status::Ok && args.space.size > 0) {
      ret = Write(args);
    }
    return ret;
  }

  WriteResult Modify(const StringView key, ModifyFunc modify_func,
                     void* modify_args, TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(key));
    auto lookup_result =
        hash_table_->Lookup<true>(internal_key, RecordType::HashElem);
    DLRecord* existing_record = nullptr;
    std::string exisiting_value;
    std::string new_value;
    bool data_existing = false;
    if (lookup_result.s == Status::Ok) {
      existing_record = lookup_result.entry.GetIndex().dl_record;
      ret.existing_record = existing_record;
      if (existing_record->GetRecordStatus() != RecordStatus::Outdated) {
        data_existing = true;
        exisiting_value.assign(existing_record->Value().data(),
                               existing_record->Value().size());
      }
    } else if (lookup_result.s == Status::NotFound) {
      // nothing todo
    } else {
      ret.s = lookup_result.s;
      return ret;
    }

    auto modify_operation = modify_func(
        data_existing ? &exisiting_value : nullptr, &new_value, modify_args);
    switch (modify_operation) {
      case ModifyOperation::Write: {
        // TODO: check new value size
        HashWriteArgs args =
            InitWriteArgs(key, new_value, WriteBatchImpl::Op::Put);
        args.ts = ts;
        args.lookup_result = lookup_result;
        args.space = pmem_allocator_->Allocate(
            DLRecord::RecordSize(internal_key, new_value));
        if (args.space.size == 0) {
          ret.s = Status::PmemOverflow;
          return ret;
        }
        return Write(args);
      }

      case ModifyOperation::Delete: {
        HashWriteArgs args = InitWriteArgs(key, "", WriteBatchImpl::Op::Delete);
        args.ts = ts;
        args.lookup_result = lookup_result;
        args.space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, ""));
        if (args.space.size == 0) {
          ret.s = Status::PmemOverflow;
          return ret;
        }
        return Write(args);
      }

      case ModifyOperation::Abort: {
        ret.s = Status::Abort;
        return ret;
      }

      default: {
        std::abort();  // non-reach area
      }
    }
  }

  HashWriteArgs InitWriteArgs(const StringView& key, const StringView& value,
                              WriteBatchImpl::Op op) {
    HashWriteArgs args;
    args.key = key;
    args.value = value;
    args.op = op;
    args.collection = Name();
    args.hlist = this;
    return args;
  }

  Status PrepareWrite(HashWriteArgs& args, TimeStampType ts) {
    kvdk_assert(args.op == WriteBatchImpl::Op::Put || args.value.size() == 0,
                "value of delete operation should be empty");
    if (args.hlist != this) {
      return Status::InvalidArgument;
    }

    args.ts = ts;
    bool op_delete = args.op == WriteBatchImpl::Op::Delete;
    std::string internal_key(InternalKey(args.key));
    bool allocate_space = true;
    if (op_delete) {
      args.lookup_result =
          hash_table_->Lookup<false>(internal_key, RecordType::HashElem);
    } else {
      args.lookup_result =
          hash_table_->Lookup<true>(internal_key, RecordType::HashElem);
    }

    switch (args.lookup_result.s) {
      case Status::Ok: {
        if (op_delete && args.lookup_result.entry.GetRecordStatus() ==
                             RecordStatus::Outdated) {
          allocate_space = false;
        }
        break;
      }
      case Status::NotFound: {
        if (op_delete) {
          allocate_space = false;
        }
        break;
      }
      case Status::MemoryOverflow: {
        return args.lookup_result.s;
      }
      default:
        std::abort();  // never should reach
    }

    if (allocate_space) {
      auto request_size = DLRecord::RecordSize(internal_key, args.value);
      args.space = pmem_allocator_->Allocate(request_size);
      if (args.space.size == 0) {
        return Status::PmemOverflow;
      }
    }

    return Status::Ok;
  }

  WriteResult Write(HashWriteArgs& args) {
    WriteResult ret;
    if (args.hlist != this) {
      ret.s = Status::InvalidArgument;
      return ret;
    }
    if (args.op == WriteBatchImpl::Op::Put) {
      ret = putPrepared(args.lookup_result, args.key, args.value, args.ts,
                        args.space);
    } else {
      ret = deletePrepared(args.lookup_result, args.key, args.ts, args.space);
    }
    return ret;
  }

  WriteResult SetExpireTime(ExpireTimeType expired_time,
                            TimeStampType timestamp) {
    WriteResult ret;
    DLRecord* header = HeaderRecord();
    SpaceEntry space = pmem_allocator_->Allocate(
        DLRecord::RecordSize(header->Key(), header->Value()));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space.offset), space.size,
        timestamp, RecordType::HashHeader, RecordStatus::Normal,
        pmem_allocator_->addr2offset_checked(header), header->prev,
        header->next, header->Key(), header->Value(), expired_time);
    bool success = dl_list_.Replace(header, pmem_record);
    kvdk_assert(success, "existing header should be linked on its list");
    ret.existing_record = header;
    ret.write_record = pmem_record;
    return ret;
  }

  bool Replace(DLRecord* old_record, DLRecord* new_record) {
    return dl_list_.Replace(old_record, new_record);
  }

  ExpireTimeType GetExpireTime() const final {
    return HeaderRecord()->GetExpireTime();
  }

  TimeStampType GetTimeStamp() const { return HeaderRecord()->GetTimestamp(); }

  bool HasExpired() const final {
    return TimeUtils::CheckIsExpired(GetExpireTime());
  }

  Status SetExpireTime(ExpireTimeType expired_time) final {
    HeaderRecord()->PersistExpireTimeCLWB(expired_time);
    return Status::Ok;
  }

  // Destroy and free the whole hash list with old version list.
  void DestroyAll() {}

  void Destroy() {}

  void UpdateSize(int64_t delta) {
    kvdk_assert(delta >= 0 || size_.load() >= static_cast<size_t>(-delta),
                "Update skiplist size to negative");
    size_.fetch_add(delta, std::memory_order_relaxed);
  }

  Status CheckIndex() {
    DLRecord* prev = HeaderRecord();
    size_t cnt = 0;
    while (true) {
      DLRecord* curr =
          pmem_allocator_->offset2addr_checked<DLRecord>(prev->next);
      if (curr == HeaderRecord()) {
        break;
      }
      StringView key = curr->Key();
      auto ret = hash_table_->Lookup<false>(key, curr->GetRecordType());
      if (ret.s != Status::Ok) {
        GlobalLogger.Error(
            "Check hash index error: record not exist in hash table\n");
        return Status::Abort;
      }
      if (ret.entry.GetIndex().dl_record != curr) {
        GlobalLogger.Error(
            "Check hash index error: Dlrecord miss-match with hash "
            "table\n");
        return Status::Abort;
      }
      if (!DLList::CheckLinkage(curr, pmem_allocator_)) {
        GlobalLogger.Error("Check hash index error: dl record linkage error\n");
        return Status::Abort;
      }
      cnt++;
      prev = curr;
    }
    return Status::Ok;
  }

  inline static CollectionIDType HashListID(const DLRecord* record) {
    assert(record != nullptr);
    switch (record->GetRecordType()) {
      case RecordType::HashElem:
        return ExtractID(record->Key());
      case RecordType::HashHeader:
        return DecodeID(record->Value());
      default:
        GlobalLogger.Error("Wrong record type %u in HashListID",
                           record->GetRecordType());
        kvdk_assert(false, "Wrong type in HashListID");
        return 0;
    }
  }

 private:
  friend HashIteratorImpl;
  DLList dl_list_;
  std::atomic<size_t> size_;
  PMEMAllocator* pmem_allocator_;
  HashTable* hash_table_;

  WriteResult putPrepared(const HashTable::LookupResult& lookup_result,
                          const StringView& key, const StringView& value,
                          TimeStampType timestamp, const SpaceEntry& space) {
    WriteResult ret;
    std::string internal_key(InternalKey(key));
    DLList::WriteArgs args(internal_key, value, RecordType::HashElem,
                           RecordStatus::Normal, timestamp, space);
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    ret.hash_entry_ptr = lookup_result.entry_ptr;
    if (lookup_result.s == Status::Ok) {
      ret.existing_record = lookup_result.entry.GetIndex().dl_record;
      kvdk_assert(timestamp > ret.existing_record->GetTimestamp(), "");
      while (dl_list_.Update(args, ret.existing_record) != Status::Ok) {
      }
    } else {
      kvdk_assert(lookup_result.s == Status::NotFound, "");
      bool push_back = fast_random_64() % 2 == 0;
      Status s = push_back ? dl_list_.PushBack(args) : dl_list_.PushFront(args);
      kvdk_assert(s == Status::Ok, "");
      UpdateSize(1);
    }
    hash_table_->Insert(lookup_result, RecordType::HashElem,
                        RecordStatus::Normal, ret.write_record,
                        PointerType::DLRecord);
    return ret;
  }

  WriteResult deletePrepared(const HashTable::LookupResult& lookup_result,
                             const StringView& key, TimeStampType timestamp,
                             const SpaceEntry& space) {
    WriteResult ret;
    std::string internal_key(InternalKey(key));
    kvdk_assert(
        lookup_result.s == Status::Ok &&
            lookup_result.entry.GetRecordType() == RecordType::HashElem &&
            lookup_result.entry.GetRecordStatus() == RecordStatus::Normal,
        "");
    assert(space.size >= DLRecord::RecordSize(internal_key, ""));
    ret.existing_record = lookup_result.entry.GetIndex().dl_record;
    kvdk_assert(timestamp > ret.existing_record->GetTimestamp(), "");
    DLList::WriteArgs args(internal_key, "", RecordType::HashElem,
                           RecordStatus::Outdated, timestamp, space);
    while (dl_list_.Update(args, ret.existing_record) != Status::Ok) {
    }
    UpdateSize(-1);
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    hash_table_->Insert(lookup_result, RecordType::HashElem,
                        RecordStatus::Outdated, ret.write_record,
                        PointerType::DLRecord);
    return ret;
  }
};

class HashIteratorImpl final : public HashIterator {
 public:
  HashIteratorImpl(Engine* engine, HashList* hlist,
                   const SnapshotImpl* snapshot, bool own_snapshot)
      : engine_(engine),
        hlist_(hlist),
        snapshot_(snapshot),
        own_snapshot_(own_snapshot),
        dl_iter_(&hlist->dl_list_, hlist->pmem_allocator_, snapshot,
                 own_snapshot) {}
  void SeekToFirst() final { dl_iter_.SeekToFirst(); }

  void SeekToLast() final { dl_iter_.SeekToLast(); }

  bool Valid() const final {
    // list->Head() == list->Tail()
    return dl_iter_.Valid();
  }

  void Next() final { dl_iter_.Next(); }

  void Prev() final { dl_iter_.Prev(); }

  std::string Key() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return std::string{};
    }
    return string_view_2_string(Collection::ExtractUserKey(dl_iter_.Key()));
  }

  std::string Value() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return std::string{};
    }
    return string_view_2_string(dl_iter_.Value());
  }

  bool MatchKey(std::regex const& re) final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return false;
    }
    return std::regex_match(Key(), re);
  }

  ~HashIteratorImpl() final {
    if (own_snapshot_ && snapshot_) {
      engine_->ReleaseSnapshot(snapshot_);
    }
  };

 private:
  using AccessToken = std::shared_ptr<VersionController::GlobalSnapshotHolder>;

 public:
  HashIteratorImpl(DLList* l, PMEMAllocator* pmem_allocator,
                   const SnapshotImpl* snapshot, bool own_snapshot)
      : dl_iter_(l, pmem_allocator, snapshot, own_snapshot) {
    kvdk_assert(l != nullptr, "");
  }

 private:
  Engine* engine_;
  HashList* hlist_;
  const SnapshotImpl* snapshot_;
  bool own_snapshot_;
  DLListAccessIterator dl_iter_;
};

}  // namespace KVDK_NAMESPACE
