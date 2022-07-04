#pragma once

#include "dl_list.hpp"
#include "generic_list.hpp"
#include "hash_table.hpp"
#include "kvdk/iterator.hpp"
#include "version/version_controller.hpp"
#include "write_batch_impl.hpp"

namespace KVDK_NAMESPACE {

class HashList2 : public Collection {
 public:
  struct WriteResult {
    Status s = Status::Ok;
    DLRecord* existing_record = nullptr;
    DLRecord* write_record = nullptr;
    HashEntry* hash_entry_ptr = nullptr;
  };

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

  HashWriteArgs InitWriteArgs(const StringView& key, const StringView& value,
                              WriteBatchImpl::Op op) {
    HashWriteArgs args;
    args.key = key;
    args.value = value;
    args.op = op;
    args.collection = Name();
    // args.hlist = this;
    return args;
  }

  Status PrepareWrite(HashWriteArgs& args, TimeStampType ts) {
    kvdk_assert(args.op == WriteBatchImpl::Op::Put || args.value.size() == 0,
                "value of delete operation should be empty");
    // if (args.hlist != this) {
    // return Status::InvalidArgument;
    // }

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
    // if (args.skiplist != this) {
    // ret.s = Status::InvalidArgument;
    // return ret;
    // }
    if (args.op == WriteBatchImpl::Op::Put) {
      ret = putPrepared(args.lookup_result, args.key, args.value, args.ts,
                        args.space);
    } else {
      ret = deletePrepared(args.lookup_result, args.key, args.ts, args.space);
    }
    return ret;
  }

 private:
  DLList dl_list_;
  HashTable* hash_table_;
  PMEMAllocator* pmem_allocator_;

  WriteResult putPrepared(const HashTable::LookupResult& lookup_result,
                          const StringView& key, const StringView& value,
                          TimeStampType timestamp, const SpaceEntry& space) {
    WriteResult ret;
    std::string internal_key(InternalKey(key));
    DLList::WriteArgs args(internal_key, value, RecordType::HashElem,
                           RecordStatus::Normal, timestamp, space);
    if (lookup_result.s == Status::Ok) {
      ret.existing_record = lookup_result.entry.GetIndex().dl_record;
      kvdk_assert(timestamp > ret.existing_record->GetTimestamp(), "");
      while (dl_list_.Update(args, ret.existing_record) != Status::Ok) {
      }

      ret.write_record =
          pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
      ret.hash_entry_ptr = lookup_result.entry_ptr;
    } else {
      kvdk_assert(lookup_result.s == Status::NotFound, "");
      bool push_back = fast_random_64() % 2 == 0;
      Status s = push_back ? dl_list_.PushBack(args) : dl_list_.PushFront(args);
      kvdk_assert(s == Status::Ok, "");
    }
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
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    hash_table_->Insert(lookup_result, RecordType::HashElem,
                        RecordStatus::Outdated, ret.write_record,
                        PointerType::DLRecord);
    return ret;
  }
};

using HashList = GenericList<RecordType::HashRecord, RecordType::HashElem>;
using HashListBuilder =
    GenericListBuilder<RecordType::HashRecord, RecordType::HashElem>;

class HashIteratorImpl final : public HashIterator {
 public:
  void SeekToFirst() final { rep = list->Front(); }

  void SeekToLast() final { rep = list->Back(); }

  bool Valid() const final {
    // list->Head() == list->Tail()
    return (rep != list->Tail());
  }

  void Next() final {
    if (!Valid()) return;
    ++rep;
  }

  void Prev() final {
    if (!Valid()) return;
    --rep;
  }

  std::string Key() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return std::string{};
    }
    auto sw = Collection::ExtractUserKey(rep->Key());
    return std::string{sw.data(), sw.size()};
  }

  std::string Value() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return std::string{};
    }
    auto sw = rep->Value();
    return std::string{sw.data(), sw.size()};
  }

  bool MatchKey(std::regex const& re) final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return false;
    }
    return std::regex_match(Key(), re);
  }

  ~HashIteratorImpl() final = default;

 private:
  using AccessToken = std::shared_ptr<VersionController::GlobalSnapshotHolder>;

 public:
  HashIteratorImpl(HashList* l, AccessToken t)
      : list{l}, rep{l->Front()}, token{t} {
    kvdk_assert(list != nullptr, "");
  }

 private:
  HashList* list;
  HashList::Iterator rep;
  AccessToken token;
};

}  // namespace KVDK_NAMESPACE
