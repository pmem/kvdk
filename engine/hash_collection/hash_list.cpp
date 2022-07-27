/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_list.hpp"

#include "../hash_table.hpp"
#include "../version/version_controller.hpp"
#include "../write_batch_impl.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"
#include "kvdk/types.hpp"

namespace KVDK_NAMESPACE {
HashList::WriteResult HashList::Put(const StringView& key,
                                    const StringView& value,
                                    TimeStampType timestamp) {
  WriteResult ret;
  HashWriteArgs args = InitWriteArgs(key, value, WriteBatchImpl::Op::Put);
  ret.s = PrepareWrite(args, timestamp);
  if (ret.s == Status::Ok) {
    ret = Write(args);
  }
  return ret;
}

Status HashList::Get(const StringView& key, std::string* value) {
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

HashList::WriteResult HashList::Delete(const StringView& key,
                                       TimeStampType timestamp) {
  WriteResult ret;
  HashWriteArgs args = InitWriteArgs(key, "", WriteBatchImpl::Op::Delete);
  ret.s = PrepareWrite(args, timestamp);
  if (ret.s == Status::Ok && args.space.size > 0) {
    ret = Write(args);
  }
  return ret;
}

HashList::WriteResult HashList::Modify(const StringView key,
                                       ModifyFunc modify_func,
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

HashWriteArgs HashList::InitWriteArgs(const StringView& key,
                                      const StringView& value,
                                      WriteBatchImpl::Op op) {
  HashWriteArgs args;
  args.key = key;
  args.value = value;
  args.op = op;
  args.collection = Name();
  args.hlist = this;
  return args;
}

Status HashList::PrepareWrite(HashWriteArgs& args, TimeStampType ts) {
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

HashList::WriteResult HashList::Write(HashWriteArgs& args) {
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

HashList::WriteResult HashList::SetExpireTime(ExpireTimeType expired_time,
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
      pmem_allocator_->offset2addr_checked(space.offset), space.size, timestamp,
      RecordType::HashHeader, RecordStatus::Normal,
      pmem_allocator_->addr2offset_checked(header), header->prev, header->next,
      header->Key(), header->Value(), expired_time);
  bool success = dl_list_.Replace(header, pmem_record);
  kvdk_assert(success, "existing header should be linked on its list");
  ret.existing_record = header;
  ret.write_record = pmem_record;
  return ret;
}

Status HashList::CheckIndex() {
  DLRecord* prev = HeaderRecord();
  size_t cnt = 0;
  while (true) {
    DLRecord* curr = pmem_allocator_->offset2addr_checked<DLRecord>(prev->next);
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

CollectionIDType HashList::HashListID(const DLRecord* record) {
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

void HashList::Destroy() {
  std::vector<SpaceEntry> to_free;
  DLRecord* header = HeaderRecord();
  if (header) {
    DLRecord* to_destroy = nullptr;
    do {
      to_destroy = pmem_allocator_->offset2addr_checked<DLRecord>(header->next);
      StringView key = to_destroy->Key();
      auto ul = hash_table_->AcquireLock(key);
      // We need to purge destroyed records one by one in case engine crashed
      // during destroy
      if (dl_list_.Remove(to_destroy)) {
        auto lookup_result =
            hash_table_->Lookup<false>(key, to_destroy->GetRecordType());

        if (lookup_result.s == Status::Ok) {
          DLRecord* hash_indexed_record = nullptr;
          auto hash_index = lookup_result.entry.GetIndex();
          switch (lookup_result.entry.GetIndexType()) {
            case PointerType::HashList:
              hash_indexed_record = hash_index.hlist->HeaderRecord();
              break;
            case PointerType::DLRecord:
              hash_indexed_record = hash_index.dl_record;
              break;
            default:
              kvdk_assert(false, "Wrong hash index type of sorted record");
          }
          if (hash_indexed_record == to_destroy) {
            hash_table_->Erase(lookup_result.entry_ptr);
          }
        }
        to_destroy->Destroy();

        to_free.emplace_back(pmem_allocator_->addr2offset_checked(to_destroy),
                             to_destroy->entry.header.record_size);
      }

    } while (to_destroy !=
             header /* header record should be the last detroyed one */);
  }
  pmem_allocator_->BatchFree(to_free);
}

HashList::WriteResult HashList::putPrepared(
    const HashTable::LookupResult& lookup_result, const StringView& key,
    const StringView& value, TimeStampType timestamp, const SpaceEntry& space) {
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
  hash_table_->Insert(lookup_result, RecordType::HashElem, RecordStatus::Normal,
                      ret.write_record, PointerType::DLRecord);
  return ret;
}

HashList::WriteResult HashList::deletePrepared(
    const HashTable::LookupResult& lookup_result, const StringView& key,
    TimeStampType timestamp, const SpaceEntry& space) {
  WriteResult ret;
  std::string internal_key(InternalKey(key));
  kvdk_assert(lookup_result.s == Status::Ok &&
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

}  // namespace KVDK_NAMESPACE