#pragma once

#include "../dl_list.hpp"
#include "../hash_table.hpp"
#include "../version/version_controller.hpp"
#include "../write_batch_impl.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"
#include "kvdk/types.hpp"

namespace KVDK_NAMESPACE {

class HashIteratorImpl;

struct HashWriteArgs {
  StringView collection;
  StringView key;
  StringView value;
  WriteBatchImpl::Op op;
  HashList* hlist;
  SpaceEntry space;
  TimeStampType ts;
  HashTable::LookupResult lookup_result;
};

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

  const DLRecord* HeaderRecord() const { return dl_list_.Header(); }

  DLRecord* HeaderRecord() { return dl_list_.Header(); }

  size_t Size() { return size_; }

  WriteResult Put(const StringView& key, const StringView& value,
                  TimeStampType timestamp);

  Status Get(const StringView& key, std::string* value);

  WriteResult Delete(const StringView& key, TimeStampType timestamp);

  WriteResult Modify(const StringView key, ModifyFunc modify_func,
                     void* modify_args, TimeStampType ts);

  HashWriteArgs InitWriteArgs(const StringView& key, const StringView& value,
                              WriteBatchImpl::Op op);

  Status PrepareWrite(HashWriteArgs& args, TimeStampType ts);

  WriteResult Write(HashWriteArgs& args);

  WriteResult SetExpireTime(ExpireTimeType expired_time,
                            TimeStampType timestamp);

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

  // Destroy and free the whole hash list with old version list.
  void DestroyAll() {}

  void Destroy();

  void UpdateSize(int64_t delta) {
    kvdk_assert(delta >= 0 || size_.load() >= static_cast<size_t>(-delta),
                "Update hash list size to negative");
    size_.fetch_add(delta, std::memory_order_relaxed);
  }

  Status CheckIndex();

  static CollectionIDType HashListID(const DLRecord* record);

 private:
  friend HashIteratorImpl;
  DLList dl_list_;
  std::atomic<size_t> size_;
  PMEMAllocator* pmem_allocator_;
  HashTable* hash_table_;

  WriteResult putPrepared(const HashTable::LookupResult& lookup_result,
                          const StringView& key, const StringView& value,
                          TimeStampType timestamp, const SpaceEntry& space);

  WriteResult deletePrepared(const HashTable::LookupResult& lookup_result,
                             const StringView& key, TimeStampType timestamp,
                             const SpaceEntry& space);
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

  bool Valid() const final { return dl_iter_.Valid(); }

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
