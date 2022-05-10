/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

Status KVEngine::Modify(const StringView key, ModifyFunc modify_func,
                        void* modify_args, const WriteOptions& write_options) {
  int64_t base_time = TimeUtils::millisecond_time();
  if (write_options.ttl_time <= 0 ||
      !TimeUtils::CheckTTL(write_options.ttl_time, base_time)) {
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
      if (lookup_result.s == Status::Ok) {
        ul.unlock();
        delayFree(OldDataRecord{existing_record, new_ts});
      }
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
        ul.unlock();
        SpinMutex* hash_lock = ul.release();
        delayFree(OldDataRecord{lookup_result.entry.GetIndex().string_record,
                                new_ts});
        delayFree(OldDeleteRecord(pmem_ptr, lookup_result.entry_ptr,
                                  PointerType::HashEntry, new_ts, hash_lock));
      }
      break;
    }
    case ModifyOperation::Abort: {
      return Status::Abort;
    }
    case ModifyOperation::Noop: {
      return Status::Ok;
    }
  }

  return Status::Ok;
}

Status KVEngine::Set(const StringView key, const StringView value,
                     const WriteOptions& options) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  return StringSetImpl(key, value, options);
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
    ul.unlock();

    SpinMutex* hash_lock = ul.release();
    delayFree(
        OldDataRecord{lookup_result.entry.GetIndex().string_record, new_ts});
    // Free this delete record to recycle PMem and DRAM space
    delayFree(OldDeleteRecord(pmem_ptr, lookup_result.entry_ptr,
                              PointerType::HashEntry, new_ts, hash_lock));
  }

  return (lookup_result.s == Status::NotFound ||
          lookup_result.s == Status::Outdated)
             ? Status::Ok
             : lookup_result.s;
}

Status KVEngine::StringSetImpl(const StringView& key, const StringView& value,
                               const WriteOptions& write_options) {
  int64_t base_time = TimeUtils::millisecond_time();
  if (write_options.ttl_time <= 0 ||
      !TimeUtils::CheckTTL(write_options.ttl_time, base_time)) {
    return Status::InvalidArgument;
  }

  ExpireTimeType expired_time =
      TimeUtils::TTLToExpireTime(write_options.ttl_time, base_time);

  KeyStatus entry_status =
      expired_time != kPersistTime ? KeyStatus::Volatile : KeyStatus::Persist;

  TEST_SYNC_POINT("KVEngine::StringSetImpl::BeforeLock");
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
              "Wrong return status in lookupKey in StringSetImpl");
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

  insertKeyOrElem(lookup_result, StringDataRecord, new_record, entry_status);
  // Free existing record
  bool need_free =
      existing_record &&
      lookup_result.entry.GetRecordType() != StringDeleteRecord &&
      !lookup_result.entry.IsExpiredStatus() /*Check if expired_key already
                                       handled by background cleaner*/
      ;

  if (need_free) {
    ul.unlock();
    delayFree(
        OldDataRecord{lookup_result.entry.GetIndex().string_record, new_ts});
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
  if (lookup_result.s == Status::Ok) {
    purgeAndFree(lookup_result.entry.GetIndex().ptr);
  }

  return Status::Ok;
}

Status KVEngine::StringBatchWriteImpl(const WriteBatch::KV& kv,
                                      BatchWriteHint& batch_hint) {
  {
    // key should be alread locked, so we do not acquire lock here
    auto lookup_result = hash_table_->Lookup<true>(kv.key, StringRecordType);

    if (lookup_result.s == Status::MemoryOverflow) {
      return lookup_result.s;
    }
    batch_hint.hash_entry_ptr = lookup_result.entry_ptr;
    bool found = lookup_result.s == Status::Ok;

    // Deleting kv is not existing
    if (kv.type == StringDeleteRecord && !found) {
      batch_hint.space_not_used = true;
      return Status::Ok;
    }

    kvdk_assert(
        !found ||
            batch_hint.timestamp >=
                lookup_result.entry.GetIndex().string_record->GetTimestamp(),
        "ts of new data smaller than existing data in batch write");

    void* new_pmem_ptr =
        pmem_allocator_->offset2addr(batch_hint.allocated_space.offset);

    TEST_SYNC_POINT(
        "KVEngine::BatchWrite::StringBatchWriteImpl::Pesistent::Before");

    StringRecord::PersistStringRecord(
        new_pmem_ptr, batch_hint.allocated_space.size, batch_hint.timestamp,
        static_cast<RecordType>(kv.type),
        found ? pmem_allocator_->addr2offset_checked(
                    lookup_result.entry.GetIndex().string_record)
              : kNullPMemOffset,
        kv.key, kv.type == StringDataRecord ? kv.value : "");

    insertKeyOrElem(lookup_result, (RecordType)kv.type, new_pmem_ptr);

    if (found) {
      if (kv.type == StringDeleteRecord) {
        batch_hint.delete_record_to_free = new_pmem_ptr;
      }
      if (lookup_result.entry.GetRecordType() == StringDataRecord) {
        batch_hint.data_record_to_free =
            lookup_result.entry.GetIndex().string_record;
      }
    }
  }

  return Status::Ok;
}

Status KVEngine::stringWrite(WriteBatchImpl::StringOp const& op, LookupResult const& res, SpaceEntry space, StringRecord** new_rec) {
  *new_rec = StringRecord::PersistStringRecord(pmem_allocator_->offset2addr_checked(space.offset), space.size, )
  return Status::NotSupported;
}

Status KVEngine::stringCommit(WriteBatchImpl::StringOp const& op, LookupResult const& res, StringRecord const* new_rec) {
  return Status::NotSupported;
}

Status KVEngine::stringRollbackBatch(BatchWriteLog::StringLog const& log) {
  return Status::NotSupported;
}

}  // namespace KVDK_NAMESPACE