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
  struct StringOp {
    WriteOp op;
    std::string key;
    std::string value;
  };

  struct SortedOp {
    WriteOp op;
    std::string collection;
    std::string key;
    std::string value;
  };

  struct HashOp {
    WriteOp op;
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
    StringOp op{WriteOp::Put, key, value};
    string_ops_.erase(op);
    string_ops_.insert(op);
  }

  void StringDelete(std::string const& key) final {
    StringOp op{WriteOp::Delete, key, std::string{}};
    string_ops_.erase(op);
    string_ops_.insert(op);
  }

  void SortedPut(std::string const& key, std::string const& field,
                 std::string const& value) final {
    SortedOp op{WriteOp::Put, key, field, value};
    sorted_ops_.erase(op);
    sorted_ops_.insert(op);
  }

  void SortedDelete(std::string const& key, std::string const& field) final {
    SortedOp op{WriteOp::Delete, key, field, std::string{}};
    sorted_ops_.erase(op);
    sorted_ops_.insert(op);
  }

  void HashPut(std::string const& key, std::string const& field,
               std::string const& value) final {
    HashOp op{WriteOp::Put, key, field, value};
    hash_ops_.erase(op);
    hash_ops_.insert(op);
  }

  void HashDelete(std::string const& key, std::string const& field) final {
    HashOp op{WriteOp::Delete, key, field, std::string{}};
    hash_ops_.erase(op);
    hash_ops_.insert(op);
  }

  void Clear() final {
    string_ops_.clear();
    sorted_ops_.clear();
    hash_ops_.clear();
  }

  size_t Size() const final {
    return string_ops_.size() + sorted_ops_.size() + hash_ops_.size();
  }

  using StringOpBatch = std::unordered_set<StringOp, HashEq, HashEq>;
  using SortedOpBatch = std::unordered_set<SortedOp, HashEq, HashEq>;
  using HashOpBatch = std::unordered_set<HashOp, HashEq, HashEq>;

  StringOpBatch const& StringOps() const { return string_ops_; }
  SortedOpBatch const& SortedOps() const { return sorted_ops_; }
  HashOpBatch const& HashOps() const { return hash_ops_; }

 private:
  StringOpBatch string_ops_;
  SortedOpBatch sorted_ops_;
  HashOpBatch hash_ops_;
};

struct StringWriteArgs {
  StringView key;
  StringView value;
  WriteOp op;
  SpaceEntry space;
  TimestampType ts;
  HashTable::LookupResult res;
  StringRecord* new_rec;

  void Assign(WriteBatchImpl::StringOp const& string_op) {
    key = string_op.key;
    value = string_op.value;
    op = string_op.op;
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

  enum class Op : size_t { Put, Delete };

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
    PMemOffsetType offset;
  };

  struct ListLogEntry {
    Op op;
    PMemOffsetType offset;
  };

  explicit BatchWriteLog() {}

  void SetTimestamp(TimestampType ts) { timestamp_ = ts; }

  void StringPut(PMemOffsetType offset) {
    string_logs_.emplace_back(StringLogEntry{Op::Put, offset});
  }

  void StringDelete(PMemOffsetType offset) {
    string_logs_.emplace_back(StringLogEntry{Op::Delete, offset});
  }

  void SortedPut(PMemOffsetType offset) {
    sorted_logs_.emplace_back(SortedLogEntry{Op::Put, offset});
  }

  void SortedDelete(PMemOffsetType offset) {
    sorted_logs_.emplace_back(SortedLogEntry{Op::Delete, offset});
  }

  void HashPut(PMemOffsetType offset) {
    hash_logs_.emplace_back(HashLogEntry{Op::Put, offset});
  }

  void HashDelete(PMemOffsetType offset) {
    hash_logs_.emplace_back(HashLogEntry{Op::Delete, offset});
  }

  void ListEmplace(PMemOffsetType offset) {
    list_logs_.emplace_back(ListLogEntry{Op::Put, offset});
  }

  void ListDelete(PMemOffsetType offset) {
    list_logs_.emplace_back(ListLogEntry{Op::Delete, offset});
  }

  void Clear() {
    string_logs_.clear();
    sorted_logs_.clear();
    hash_logs_.clear();
    list_logs_.clear();
  }

  size_t Size() const {
    return string_logs_.size() + sorted_logs_.size() + hash_logs_.size() +
           list_logs_.size();
  }

  static size_t Capacity() { return (1UL << 20); }

  static size_t MaxBytes() {
    static_assert(sizeof(HashLogEntry) >= sizeof(StringLogEntry), "");
    static_assert(sizeof(HashLogEntry) >= sizeof(SortedLogEntry), "");
    static_assert(sizeof(HashLogEntry) >= sizeof(ListLogEntry), "");
    return sizeof(size_t) + sizeof(TimestampType) + sizeof(Stage) +
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
    dst = &dst[sizeof(size_t) + sizeof(TimestampType)];
    *reinterpret_cast<Stage*>(dst) = Stage::Processing;
    _mm_clflush(dst);
    _mm_mfence();
  }

  static void MarkCommitted(char* dst) {
    dst = &dst[sizeof(size_t) + sizeof(TimestampType)];
    *reinterpret_cast<Stage*>(dst) = Stage::Committed;
    _mm_clflush(dst);
    _mm_mfence();
  }

  // For rollback
  static void MarkInitializing(char* dst) {
    dst = &dst[sizeof(size_t) + sizeof(TimestampType)];
    *reinterpret_cast<Stage*>(dst) = Stage::Initializing;
    _mm_clflush(dst);
    _mm_mfence();
  }

  using StringLog = std::vector<StringLogEntry>;
  using SortedLog = std::vector<SortedLogEntry>;
  using HashLog = std::vector<HashLogEntry>;
  using ListLog = std::vector<ListLogEntry>;

  StringLog const& StringLogs() const { return string_logs_; }
  SortedLog const& SortedLogs() const { return sorted_logs_; }
  HashLog const& HashLogs() const { return hash_logs_; }
  ListLog const& ListLogs() const { return list_logs_; }
  TimestampType Timestamp() const { return timestamp_; }

 private:
  Stage stage{Stage::Initializing};
  TimestampType timestamp_;
  StringLog string_logs_;
  SortedLog sorted_logs_;
  HashLog hash_logs_;
  ListLog list_logs_;
};

}  // namespace KVDK_NAMESPACE
