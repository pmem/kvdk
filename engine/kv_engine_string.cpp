/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

Status KVEngine::Modify(const StringView key, ModifyFunc modify_func,
                        void* modify_args, const WriteOptions& write_options) {
  int64_t base_time = TimeUtils::millisecond_time();
  if (!TimeUtils::CheckTTL(write_options.ttl_time, base_time)) {
    return Status::InvalidArgument;
  }

  ExpireTimeType expired_time = write_options.ttl_time == kPersistTime
                                    ? kPersistTime
                                    : write_options.ttl_time + base_time;

  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  auto ul = hash_table_->AcquireLock(key);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimeStampType new_ts = holder.Timestamp();
  auto lookup_result = lookupKey<true>(key, StringRecordType);

  StringRecord* existing_record = nullptr;
  std::string existing_value;
  std::string new_value;
  // push it into cleaner
  if (lookup_result.s == Status::Ok) {
    existing_record = lookup_result.entry.GetIndex().string_record;
    existing_value.assign(existing_record->Value().data(),
                          existing_record->Value().size());
  } else if (lookup_result.s == Status::Outdated) {
    existing_record = lookup_result.entry.GetIndex().string_record;
  } else if (lookup_result.s == Status::NotFound) {
    // nothing todo
  } else {
    return lookup_result.s;
  }

  auto modify_operation =
      modify_func(lookup_result.s == Status::Ok ? &existing_value : nullptr,
                  &new_value, modify_args);
  switch (modify_operation) {
    case ModifyOperation::Write: {
      if (!CheckValueSize(new_value)) {
        return Status::InvalidDataSize;
      }
      SpaceEntry space_entry =
          pmem_allocator_->Allocate(StringRecord::RecordSize(key, new_value));
      if (space_entry.size == 0) {
        return Status::PmemOverflow;
      }

      StringRecord* new_record =
          pmem_allocator_->offset2addr_checked<StringRecord>(
              space_entry.offset);
      StringRecord::PersistStringRecord(
          new_record, space_entry.size, new_ts, StringDataRecord,
          existing_record == nullptr
              ? kNullPMemOffset
              : pmem_allocator_->addr2offset_checked(existing_record),
          key, new_value, expired_time);
      insertKeyOrElem(lookup_result, StringDataRecord, new_record);
      break;
    }
    case ModifyOperation::Delete: {
      if (lookup_result.s == Status::Ok) {
        SpaceEntry space_entry =
            pmem_allocator_->Allocate(StringRecord::RecordSize(key, ""));
        if (space_entry.size == 0) {
          return Status::PmemOverflow;
        }

        void* pmem_ptr =
            pmem_allocator_->offset2addr_checked(space_entry.offset);
        StringRecord::PersistStringRecord(
            pmem_ptr, space_entry.size, new_ts, StringDeleteRecord,
            pmem_allocator_->addr2offset_checked(existing_record), key, "");
        insertKeyOrElem(lookup_result, StringDeleteRecord, pmem_ptr);
        break;
      }
      case ModifyOperation::Abort: {
        return Status::Abort;
      }
      case ModifyOperation::Noop: {
        return Status::Ok;
      }
    }
  }

  return Status::Ok;
}

Status KVEngine::Put(const StringView key, const StringView value,
                     const WriteOptions& options) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  return StringPutImpl(key, value, options);
}

Status KVEngine::Get(const StringView key, std::string* value) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  auto holder = version_controller_.GetLocalSnapshotHolder();
  auto ret = lookupKey<false>(key, StringDataRecord | StringDeleteRecord);
  if (ret.s == Status::Ok) {
    StringRecord* string_record = ret.entry.GetIndex().string_record;
    kvdk_assert(string_record->GetRecordType() == StringDataRecord,
                "Got wrong data type in string get");
    kvdk_assert(string_record->Validate(), "Corrupted data in string get");
    value->assign(string_record->Value().data(), string_record->Value().size());
    return Status::Ok;
  } else {
    return ret.s == Status::Outdated ? Status::NotFound : ret.s;
  }
}

Status KVEngine::Delete(const StringView key) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }

  return StringDeleteImpl(key);
}

Status KVEngine::StringDeleteImpl(const StringView& key) {
  auto ul = hash_table_->AcquireLock(key);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimeStampType new_ts = holder.Timestamp();

  auto lookup_result =
      lookupKey<false>(key, StringDeleteRecord | StringDataRecord);
  if (lookup_result.s == Status::Ok) {
    // We only write delete record if key exist
    auto request_size = key.size() + sizeof(StringRecord);
    SpaceEntry space_entry = pmem_allocator_->Allocate(request_size);
    if (space_entry.size == 0) {
      return Status::PmemOverflow;
    }

    void* pmem_ptr = pmem_allocator_->offset2addr_checked(space_entry.offset);
    StringRecord::PersistStringRecord(
        pmem_ptr, space_entry.size, new_ts, StringDeleteRecord,
        pmem_allocator_->addr2offset_checked(
            lookup_result.entry.GetIndex().string_record),
        key, "");
    insertKeyOrElem(lookup_result, StringDeleteRecord, pmem_ptr);
  }

  return (lookup_result.s == Status::NotFound ||
          lookup_result.s == Status::Outdated)
             ? Status::Ok
             : lookup_result.s;
}

Status KVEngine::StringPutImpl(const StringView& key, const StringView& value,
                               const WriteOptions& write_options) {
  int64_t base_time = TimeUtils::millisecond_time();
  if (!TimeUtils::CheckTTL(write_options.ttl_time, base_time)) {
    return Status::InvalidArgument;
  }

  ExpireTimeType expired_time =
      TimeUtils::TTLToExpireTime(write_options.ttl_time, base_time);

  TEST_SYNC_POINT("KVEngine::StringPutImpl::BeforeLock");
  auto ul = hash_table_->AcquireLock(key);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimeStampType new_ts = holder.Timestamp();

  // Lookup key in hashtable
  auto lookup_result =
      lookupKey<true>(key, StringDataRecord | StringDeleteRecord);
  if (lookup_result.s == Status::MemoryOverflow ||
      lookup_result.s == Status::WrongType) {
    return lookup_result.s;
  }

  kvdk_assert(lookup_result.s == Status::NotFound ||
                  lookup_result.s == Status::Ok ||
                  lookup_result.s == Status::Outdated,
              "Wrong return status in lookupKey in StringPutImpl");
  StringRecord* existing_record =
      lookup_result.s == Status::NotFound
          ? nullptr
          : lookup_result.entry.GetIndex().string_record;
  kvdk_assert(!existing_record || new_ts > existing_record->GetTimestamp(),
              "existing record has newer timestamp or wrong return status in "
              "string set");

  // Persist key-value pair to PMem
  uint32_t requested_size = value.size() + key.size() + sizeof(StringRecord);
  SpaceEntry space_entry = pmem_allocator_->Allocate(requested_size);
  if (space_entry.size == 0) {
    return Status::PmemOverflow;
  }

  StringRecord* new_record =
      pmem_allocator_->offset2addr_checked<StringRecord>(space_entry.offset);
  StringRecord::PersistStringRecord(
      new_record, space_entry.size, new_ts, StringDataRecord,
      pmem_allocator_->addr2offset(existing_record), key, value, expired_time);

  insertKeyOrElem(lookup_result, StringDataRecord, new_record);

  if (existing_record) {
    auto old_record = removeOutDatedVersion<StringRecord>(
        new_record, version_controller_.GlobalOldestSnapshotTs());
    if (old_record) {
      auto& tc = cleaner_thread_cache_[access_thread.id];
      std::unique_lock<SpinMutex> lock(tc.mtx);
      tc.old_str_records.emplace_back(old_record);
    }
  }

  return Status::Ok;
}

Status KVEngine::restoreStringRecord(StringRecord* pmem_record,
                                     const DataEntry& cached_entry) {
  assert(pmem_record->entry.meta.type & StringRecordType);
  if (RecoverToCheckpoint() &&
      cached_entry.meta.timestamp > persist_checkpoint_->CheckpointTS()) {
    purgeAndFree(pmem_record);
    return Status::Ok;
  }

  auto view = pmem_record->Key();
  std::string key{view.data(), view.size()};
  auto ul = hash_table_->AcquireLock(key);
  auto lookup_result = hash_table_->Lookup<true>(key, StringRecordType);

  if (lookup_result.s == Status::MemoryOverflow) {
    return lookup_result.s;
  }

  if (lookup_result.s == Status::Ok &&
      lookup_result.entry.GetIndex().string_record->GetTimestamp() >=
          cached_entry.meta.timestamp) {
    purgeAndFree(pmem_record);
    return Status::Ok;
  }

  insertKeyOrElem(lookup_result, cached_entry.meta.type, pmem_record);
  pmem_record->PersistOldVersion(kNullPMemOffset);

  if (lookup_result.s == Status::Ok) {
    purgeAndFree(lookup_result.entry.GetIndex().ptr);
  }

  return Status::Ok;
}

Status KVEngine::stringWritePrepare(StringWriteArgs& args) {
  args.res = lookupKey<true>(args.key, StringRecordType);
  if (args.res.s != Status::Ok && args.res.s != Status::NotFound &&
      args.res.s != Status::Outdated) {
    return args.res.s;
  }
  if (args.op == WriteBatchImpl::Op::Delete && args.res.s != Status::Ok) {
    return Status::Ok;
  }
  args.space =
      pmem_allocator_->Allocate(StringRecord::RecordSize(args.key, args.value));
  if (args.space.size == 0) {
    return Status::PmemOverflow;
  }
  return Status::Ok;
}

Status KVEngine::stringWrite(StringWriteArgs& args) {
  RecordType type = (args.op == WriteBatchImpl::Op::Put)
                        ? RecordType::StringDataRecord
                        : RecordType::StringDeleteRecord;
  void* new_addr = pmem_allocator_->offset2addr_checked(args.space.offset);
  PMemOffsetType old_off;
  if (args.res.s == Status::NotFound) {
    kvdk_assert(args.op == WriteBatchImpl::Op::Put, "");
    old_off = kNullPMemOffset;
  } else {
    kvdk_assert(args.res.s == Status::Ok || args.res.s == Status::Outdated, "");
    old_off = pmem_allocator_->addr2offset_checked(
        args.res.entry.GetIndex().string_record);
  }
  args.new_rec = StringRecord::PersistStringRecord(
      new_addr, args.space.size, args.ts, type, old_off, args.key, args.value);
  return Status::Ok;
}

Status KVEngine::stringWritePublish(StringWriteArgs const& args) {
  RecordType type = (args.op == WriteBatchImpl::Op::Put)
                        ? RecordType::StringDataRecord
                        : RecordType::StringDeleteRecord;
  insertKeyOrElem(args.res, type, const_cast<StringRecord*>(args.new_rec));
  return Status::Ok;
}

Status KVEngine::stringRollback(TimeStampType,
                                BatchWriteLog::StringLogEntry const& log) {
  static_cast<DataEntry*>(pmem_allocator_->offset2addr_checked(log.offset))
      ->Destroy();
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE
