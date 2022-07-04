#pragma once

#include <x86intrin.h>

#include <cstring>
#include <unordered_set>
#include <vector>

#include "alias.hpp"
#include "hash_table.hpp"
#include "kvdk/write_batch.hpp"
#include "utils/codec.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

struct Splice;

class WriteBatchImpl final : public WriteBatch {
 public:
  enum class Op { Put, Delete };

  struct StringOp {
    Op op;
    std::string key;
    std::string value;
  };

  struct SortedOp {
    Op op;
    std::string collection;
    std::string key;
    std::string value;
  };

  struct HashOp {
    Op op;
    std::string collection;
    std::string key;
    std::string value;
  };

  struct HashEq {
    size_t operator()(StringOp const& string_op) const {
      return xxh_hash(string_op.key);
    }
    size_t operator()(SortedOp const& sorted_op) const {
      return xxh_hash(sorted_op.collection) ^ xxh_hash(sorted_op.key);
    }
    size_t operator()(HashOp const& hash_op) const {
      return xxh_hash(hash_op.collection) ^ xxh_hash(hash_op.key);
    }
    bool operator()(StringOp const& lhs, StringOp const& rhs) const {
      return lhs.key == rhs.key;
    }
    bool operator()(SortedOp const& lhs, SortedOp const& rhs) const {
      return lhs.collection == rhs.collection && lhs.key == rhs.key;
    }
    bool operator()(HashOp const& lhs, HashOp const& rhs) const {
      return lhs.collection == rhs.collection && lhs.key == rhs.key;
    }
  };

  void StringPut(std::string const& key, std::string const& value) final {
    StringOp op{Op::Put, key, value};
    string_ops.erase(op);
    string_ops.insert(op);
  }

  void StringDelete(std::string const& key) final {
    StringOp op{Op::Delete, key, std::string{}};
    string_ops.erase(op);
    string_ops.insert(op);
  }

  void SortedPut(std::string const& key, std::string const& field,
                 std::string const& value) final {
    SortedOp op{Op::Put, key, field, value};
    sorted_ops.erase(op);
    sorted_ops.insert(op);
  }

  void SortedDelete(std::string const& key, std::string const& field) final {
    SortedOp op{Op::Delete, key, field, std::string{}};
    sorted_ops.erase(op);
    sorted_ops.insert(op);
  }

  void HashPut(std::string const& key, std::string const& field,
               std::string const& value) final {
    HashOp op{Op::Put, key, field, value};
    hash_ops.erase(op);
    hash_ops.insert(op);
  }

  void HashDelete(std::string const& key, std::string const& field) final {
    HashOp op{Op::Delete, key, field, std::string{}};
    hash_ops.erase(op);
    hash_ops.insert(op);
  }

  void Clear() final {
    string_ops.clear();
    sorted_ops.clear();
    hash_ops.clear();
  }

  size_t Size() const final {
    return string_ops.size() + sorted_ops.size() + hash_ops.size();
  }

  using StringOpBatch = std::unordered_set<StringOp, HashEq, HashEq>;
  using SortedOpBatch = std::unordered_set<SortedOp, HashEq, HashEq>;
  using HashOpBatch = std::unordered_set<HashOp, HashEq, HashEq>;

  StringOpBatch const& StringOps() const { return string_ops; }
  SortedOpBatch const& SortedOps() const { return sorted_ops; }
  HashOpBatch const& HashOps() const { return hash_ops; }

 private:
  StringOpBatch string_ops;
  SortedOpBatch sorted_ops;
  HashOpBatch hash_ops;
};

struct StringWriteArgs {
  StringView key;
  StringView value;
  WriteBatchImpl::Op op;
  SpaceEntry space;
  TimeStampType ts;
  HashTable::LookupResult res;
  StringRecord* new_rec;

  void Assign(WriteBatchImpl::StringOp const& string_op) {
    key = string_op.key;
    value = string_op.value;
    op = string_op.op;
  }
};

struct SortedWriteArgs {
  StringView collection;
  StringView key;
  StringView value;
  WriteBatchImpl::Op op;
  Skiplist* skiplist;
  SpaceEntry space;
  TimeStampType ts;
  HashTable::LookupResult lookup_result;
  std::unique_ptr<Splice> seek_result;

  void Assign(WriteBatchImpl::SortedOp const& sorted_op) {
    collection = sorted_op.collection;
    key = sorted_op.key;
    value = sorted_op.value;
    op = sorted_op.op;
  }
};

struct HashWriteArgs {
  StringView collection;
  StringView key;
  StringView value;
  WriteBatchImpl::Op op;
  HashList* hlist;
  SpaceEntry space;
  TimeStampType ts;
  HashTable::LookupResult lookup_result;
  // returned by write, used by publish
  DLRecord* new_rec;

  void Assign(WriteBatchImpl::HashOp const& hash_op) {
    collection = hash_op.collection;
    key = hash_op.key;
    value = hash_op.value;
    op = hash_op.op;
  }
};

class BatchWriteLog {
 public:
  // The batch is first persisted to PMem with Stage::Initializing.
  // After persisting is done, it enters Stage::Processing.
  // When all batches are executed, it is marked as Stage::Committed
  // and then purged from PMem.
  // During recovery, a batch in
  //    Stage::Initializing is directly discarded and purged.
  //    Stage::Processing is rolled back.
  //    Stage::Committed is directly purged.
  enum class Stage : size_t {
    // Initializing must be 0 so that empty file can be skipped.
    Initializing = 0,
    Processing,
    Committed,
  };

  enum class Op : size_t { Put, Delete, Replace };

  struct StringLogEntry {
    Op op;
    PMemOffsetType offset;
  };

  struct SortedLogEntry {
    Op op;
    PMemOffsetType offset;
  };

  struct HashLogEntry {
    Op op;
    PMemOffsetType new_offset;
    PMemOffsetType old_offset;
  };

  struct ListLogEntry {
    Op op;
    PMemOffsetType offset;
  };

  explicit BatchWriteLog() {}

  void SetTimestamp(TimeStampType ts) { timestamp = ts; }

  void StringPut(PMemOffsetType offset) {
    string_logs.emplace_back(StringLogEntry{Op::Put, offset});
  }

  void StringDelete(PMemOffsetType offset) {
    string_logs.emplace_back(StringLogEntry{Op::Delete, offset});
  }

  void SortedPut(PMemOffsetType offset) {
    sorted_logs.emplace_back(SortedLogEntry{Op::Put, offset});
  }

  void SortedDelete(PMemOffsetType offset) {
    sorted_logs.emplace_back(SortedLogEntry{Op::Delete, offset});
  }

  void HashEmplace(PMemOffsetType new_offset) {
    hash_logs.emplace_back(HashLogEntry{Op::Put, new_offset, kNullPMemOffset});
  }

  void HashReplace(PMemOffsetType new_offset, PMemOffsetType old_offset) {
    hash_logs.emplace_back(HashLogEntry{Op::Replace, new_offset, old_offset});
  }

  void HashDelete(PMemOffsetType old_offset) {
    hash_logs.emplace_back(
        HashLogEntry{Op::Delete, kNullPMemOffset, old_offset});
  }

  void ListEmplace(PMemOffsetType offset) {
    list_logs.emplace_back(ListLogEntry{Op::Put, offset});
  }

  void ListDelete(PMemOffsetType offset) {
    list_logs.emplace_back(ListLogEntry{Op::Delete, offset});
  }

  void Clear() {
    string_logs.clear();
    sorted_logs.clear();
    hash_logs.clear();
    list_logs.clear();
  }

  size_t Size() const {
    return string_logs.size() + sorted_logs.size() + hash_logs.size() +
           list_logs.size();
  }

  static size_t Capacity() { return (1UL << 20); }

  static size_t MaxBytes() {
    static_assert(sizeof(HashLogEntry) >= sizeof(StringLogEntry), "");
    static_assert(sizeof(HashLogEntry) >= sizeof(SortedLogEntry), "");
    static_assert(sizeof(HashLogEntry) >= sizeof(ListLogEntry), "");
    return sizeof(size_t) + sizeof(TimeStampType) + sizeof(Stage) +
           sizeof(size_t) + Capacity() * sizeof(HashLogEntry);
  }

  // Format of the BatchWriteLog
  // total_bytes | timestamp | stage |
  // N | StringLogEntry*N |
  // M | SortedLogEntry*M
  // K | HashLogEntry*K
  // L | ListLogEntry*K
  // dst is expected to have capacity of MaxBytes().
  void EncodeTo(char* dst);

  void DecodeFrom(char const* src);

  static void MarkProcessing(char* dst) {
    dst = &dst[sizeof(size_t) + sizeof(TimeStampType)];
    *reinterpret_cast<Stage*>(dst) = Stage::Processing;
    _mm_clflush(dst);
    _mm_mfence();
  }

  static void MarkCommitted(char* dst) {
    dst = &dst[sizeof(size_t) + sizeof(TimeStampType)];
    *reinterpret_cast<Stage*>(dst) = Stage::Committed;
    _mm_clflush(dst);
    _mm_mfence();
  }

  // For rollback
  static void MarkInitializing(char* dst) {
    dst = &dst[sizeof(size_t) + sizeof(TimeStampType)];
    *reinterpret_cast<Stage*>(dst) = Stage::Initializing;
    _mm_clflush(dst);
    _mm_mfence();
  }

  using StringLog = std::vector<StringLogEntry>;
  using SortedLog = std::vector<SortedLogEntry>;
  using HashLog = std::vector<HashLogEntry>;
  using ListLog = std::vector<ListLogEntry>;

  StringLog const& StringLogs() const { return string_logs; }
  SortedLog const& SortedLogs() const { return sorted_logs; }
  HashLog const& HashLogs() const { return hash_logs; }
  ListLog const& ListLogs() const { return list_logs; }
  TimeStampType Timestamp() const { return timestamp; }

 private:
  Stage stage{Stage::Initializing};
  TimeStampType timestamp;
  StringLog string_logs;
  SortedLog sorted_logs;
  HashLog hash_logs;
  ListLog list_logs;
};

}  // namespace KVDK_NAMESPACE
