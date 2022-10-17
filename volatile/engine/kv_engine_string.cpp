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

  auto thread_holder = AcquireAccessThread();

  auto ul = hash_table_->AcquireLock(key);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimestampType new_ts = holder.Timestamp();
  auto lookup_result = lookupKey<true>(key, RecordType::String);

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
      if (!checkValueSize(new_value)) {
        return Status::InvalidDataSize;
      }

      ExpireTimeType expired_time =
          lookup_result.s == Status::Ok && !write_options.update_ttl
              ? existing_record->GetExpireTime()
              : TimeUtils::TTLToExpireTime(write_options.ttl_time, base_time);

      SpaceEntry space_entry =
          kv_allocator_->Allocate(StringRecord::RecordSize(key, new_value));
      if (space_entry.size == 0) {
        return Status::MemoryOverflow;
      }

      StringRecord* new_record =
          kv_allocator_->offset2addr_checked<StringRecord>(space_entry.offset);
      StringRecord::PersistStringRecord(
          new_record, space_entry.size, new_ts, RecordType::String,
          RecordStatus::Normal,
          existing_record == nullptr
              ? kNullMemoryOffset
              : kv_allocator_->addr2offset_checked(existing_record),
          key, new_value, expired_time);
      insertKeyOrElem(lookup_result, RecordType::String, RecordStatus::Normal,
                      new_record);
      break;
    }
    case ModifyOperation::Delete: {
      if (lookup_result.s == Status::Ok) {
        SpaceEntry space_entry =
            kv_allocator_->Allocate(StringRecord::RecordSize(key, ""));
        if (space_entry.size == 0) {
          return Status::MemoryOverflow;
        }

        void* data_ptr = kv_allocator_->offset2addr_checked(space_entry.offset);
        StringRecord::PersistStringRecord(
            data_ptr, space_entry.size, new_ts, RecordType::String,
            RecordStatus::Outdated,
            kv_allocator_->addr2offset_checked(existing_record), key, "");
        insertKeyOrElem(lookup_result, RecordType::String,
                        RecordStatus::Outdated, data_ptr);
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
  auto thread_holder = AcquireAccessThread();

  if (!checkKeySize(key) || !checkValueSize(value)) {
    return Status::InvalidDataSize;
  }

  return stringPutImpl(key, value, options);
}

Status KVEngine::Get(const StringView key, std::string* value) {
  auto thread_holder = AcquireAccessThread();

  if (!checkKeySize(key)) {
    return Status::InvalidDataSize;
  }
  auto holder = version_controller_.GetLocalSnapshotHolder();
  auto ret = lookupKey<false>(key, RecordType::String);
  if (ret.s == Status::Ok) {
    StringRecord* string_record = ret.entry.GetIndex().string_record;
    kvdk_assert(string_record->GetRecordType() == RecordType::String &&
                    string_record->GetRecordStatus() != RecordStatus::Outdated,
                "Got wrong data type in string get");
    kvdk_assert(string_record->ValidOrDirty(), "Corrupted data in string get");
    value->assign(string_record->Value().data(), string_record->Value().size());
    return Status::Ok;
  } else {
    return ret.s == Status::Outdated ? Status::NotFound : ret.s;
  }
}

Status KVEngine::Delete(const StringView key) {
  auto thread_holder = AcquireAccessThread();

  if (!checkKeySize(key)) {
    return Status::InvalidDataSize;
  }

  return stringDeleteImpl(key);
}

Status KVEngine::stringDeleteImpl(const StringView& key) {
  auto ul = hash_table_->AcquireLock(key);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimestampType new_ts = holder.Timestamp();

  auto lookup_result = lookupKey<false>(key, RecordType::String);
  if (lookup_result.s == Status::Ok) {
    // We only write delete record if key exist
    auto request_size = key.size() + sizeof(StringRecord);
    SpaceEntry space_entry = kv_allocator_->Allocate(request_size);
    if (space_entry.size == 0) {
      return Status::MemoryOverflow;
    }

    StringRecord* data_ptr =
        kv_allocator_->offset2addr_checked<StringRecord>(space_entry.offset);
    StringRecord::PersistStringRecord(
        data_ptr, space_entry.size, new_ts, RecordType::String,
        RecordStatus::Outdated,
        kv_allocator_->addr2offset_checked(
            lookup_result.entry.GetIndex().string_record),
        key, "");
    insertKeyOrElem(lookup_result, RecordType::String, RecordStatus::Outdated,
                    data_ptr);

    removeAndCacheOutdatedVersion(data_ptr);
  }
  tryCleanCachedOutdatedRecord();

  return (lookup_result.s == Status::NotFound ||
          lookup_result.s == Status::Outdated)
             ? Status::Ok
             : lookup_result.s;
}

Status KVEngine::stringPutImpl(const StringView& key, const StringView& value,
                               const WriteOptions& write_options) {
  int64_t base_time = TimeUtils::millisecond_time();
  if (!TimeUtils::CheckTTL(write_options.ttl_time, base_time)) {
    return Status::InvalidArgument;
  }

  TEST_SYNC_POINT("KVEngine::stringPutImpl::BeforeLock");
  auto ul = hash_table_->AcquireLock(key);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimestampType new_ts = holder.Timestamp();

  // Lookup key in hashtable
  auto lookup_result = lookupKey<true>(key, RecordType::String);
  if (lookup_result.s == Status::MemoryOverflow ||
      lookup_result.s == Status::WrongType) {
    return lookup_result.s;
  }

  kvdk_assert(lookup_result.s == Status::NotFound ||
                  lookup_result.s == Status::Ok ||
                  lookup_result.s == Status::Outdated,
              "Wrong return status in lookupKey in stringPutImpl");
  StringRecord* existing_record =
      lookup_result.s == Status::NotFound
          ? nullptr
          : lookup_result.entry.GetIndex().string_record;
  kvdk_assert(!existing_record || new_ts > existing_record->GetTimestamp(),
              "existing record has newer timestamp or wrong return status in "
              "string set");
  ExpireTimeType expired_time =
      lookup_result.s == Status::Ok && !write_options.update_ttl
          ? existing_record->GetExpireTime()
          : TimeUtils::TTLToExpireTime(write_options.ttl_time, base_time);

  // Save key-value pair
  SpaceEntry space_entry =
      kv_allocator_->Allocate(StringRecord::RecordSize(key, value));
  if (space_entry.size == 0) {
    return Status::MemoryOverflow;
  }

  StringRecord* new_record =
      kv_allocator_->offset2addr_checked<StringRecord>(space_entry.offset);
  StringRecord::PersistStringRecord(new_record, space_entry.size, new_ts,
                                    RecordType::String, RecordStatus::Normal,
                                    kv_allocator_->addr2offset(existing_record),
                                    key, value, expired_time);

  insertKeyOrElem(lookup_result, RecordType::String, RecordStatus::Normal,
                  new_record);

  if (existing_record) {
    removeAndCacheOutdatedVersion(new_record);
  }
  tryCleanCachedOutdatedRecord();

  return Status::Ok;
}

Status KVEngine::stringWritePrepare(StringWriteArgs& args, TimestampType ts) {
  args.res = lookupKey<true>(args.key, RecordType::String);
  if (args.res.s != Status::Ok && args.res.s != Status::NotFound &&
      args.res.s != Status::Outdated) {
    return args.res.s;
  }
  args.ts = ts;
  if (args.op == WriteOp::Delete && args.res.s != Status::Ok) {
    return Status::Ok;
  }
  args.space =
      kv_allocator_->Allocate(StringRecord::RecordSize(args.key, args.value));
  if (args.space.size == 0) {
    return Status::MemoryOverflow;
  }
  return Status::Ok;
}

Status KVEngine::stringWrite(StringWriteArgs& args) {
  RecordStatus record_status =
      args.op == WriteOp::Put ? RecordStatus::Normal : RecordStatus::Outdated;
  void* new_addr = kv_allocator_->offset2addr_checked(args.space.offset);
  MemoryOffsetType old_off;
  if (args.res.s == Status::NotFound) {
    kvdk_assert(args.op == WriteOp::Put, "");
    old_off = kNullMemoryOffset;
  } else {
    kvdk_assert(args.res.s == Status::Ok || args.res.s == Status::Outdated, "");
    old_off = kv_allocator_->addr2offset_checked(
        args.res.entry.GetIndex().string_record);
  }
  args.new_rec = StringRecord::PersistStringRecord(
      new_addr, args.space.size, args.ts, RecordType::String, record_status,
      old_off, args.key, args.value);
  return Status::Ok;
}

Status KVEngine::stringWritePublish(StringWriteArgs const& args) {
  RecordStatus record_status =
      args.op == WriteOp::Put ? RecordStatus::Normal : RecordStatus::Outdated;
  insertKeyOrElem(args.res, RecordType::String, record_status,
                  const_cast<StringRecord*>(args.new_rec));
  return Status::Ok;
}

Status KVEngine::stringRollback(TimestampType,
                                BatchWriteLog::StringLogEntry const& log) {
  static_cast<DataEntry*>(kv_allocator_->offset2addr_checked(log.offset))
      ->Destroy();
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE
