#pragma once

#include <x86intrin.h>

#include <cstring>

#include "alias.hpp"
#include "hash_table.hpp"
#include "kvdk/write_batch.hpp"
#include "utils/codec.hpp"

namespace KVDK_NAMESPACE {

class WriteBatchImpl final : public WriteBatch2 {
 public:
  enum class Op { Put, Delete };

  struct StringOp {
    Op op;
    std::string key;
    std::string value;
  };

  struct SortedOp {
    Op op;
    std::string key;
    std::string field;
    std::string value;
  };

  struct HashOp {
    Op op;
    std::string key;
    std::string field;
    std::string value;
  };

  void StringPut(std::string const& key, std::string const& value) final {
    string_ops.emplace_back(StringOp{Op::Put, key, value});
  }

  void StringDelete(std::string const& key) final {
    string_ops.emplace_back(StringOp{Op::Delete, key, std::string{}});
  }

  void SortedPut(std::string const& key, std::string const& field,
                 std::string const& value) final {
    sorted_ops.emplace_back(SortedOp{Op::Put, key, field, value});
  }

  void SortedDelete(std::string const& key, std::string const& field) final {
    sorted_ops.emplace_back(SortedOp{Op::Delete, key, field, std::string{}});
  }

  void HashPut(std::string const& key, std::string const& field,
               std::string const& value) final {
    hash_ops.emplace_back(HashOp{Op::Put, key, field, value});
  }

  void HashDelete(std::string const& key, std::string const& field) final {
    hash_ops.emplace_back(HashOp{Op::Delete, key, field, std::string{}});
  }

  void Clear() final {
    string_ops.clear();
    sorted_ops.clear();
    hash_ops.clear();
  }

  size_t Size() const {
    return string_ops.size() + sorted_ops.size() + hash_ops.size();
  }

  using StringOpBatch = std::vector<StringOp>;
  using SortedOpBatch = std::vector<SortedOp>;
  using HashOpBatch = std::vector<HashOp>;

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
  StringView key;
  StringView field;
  StringView value;
  WriteBatchImpl::Op op;
  SpaceEntry space;
  TimeStampType ts;
  HashTable::LookupResult res;
  PointerWithTag<void*, PointerType> new_rec;

  void Assign(WriteBatchImpl::SortedOp const& sorted_op) {
    key = sorted_op.key;
    field = sorted_op.field;
    value = sorted_op.value;
    op = sorted_op.op;
  }
};

struct HashWriteArgs {
  StringView key;
  StringView field;
  StringView value;
  WriteBatchImpl::Op op;
  SpaceEntry space;
  TimeStampType ts;
  HashTable::LookupResult res;
  DLRecord* new_rec;

  void Assign(WriteBatchImpl::HashOp const& hash_op) {
    key = hash_op.key;
    field = hash_op.field;
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

  explicit BatchWriteLog() {}

  void StringPut(PMemOffsetType offset) {
    string_ops.emplace_back(StringLogEntry{Op::Put, offset});
  }

  void StringDelete(PMemOffsetType offset) {
    string_ops.emplace_back(StringLogEntry{Op::Delete, offset});
  }

  void SortedPut(PMemOffsetType offset) {
    sorted_ops.emplace_back(SortedLogEntry{Op::Put, offset});
  }

  void SortedDelete(PMemOffsetType offset) {
    sorted_ops.emplace_back(SortedLogEntry{Op::Delete, offset});
  }

  void HashPut(PMemOffsetType offset) {
    hash_ops.emplace_back(HashLogEntry{Op::Put, offset});
  }

  void HashDelete(PMemOffsetType offset) {
    hash_ops.emplace_back(HashLogEntry{Op::Delete, offset});
  }

  void Clear() {
    string_ops.clear();
    sorted_ops.clear();
    hash_ops.clear();
  }

  size_t Size() const {
    return string_ops.size() + sorted_ops.size() + hash_ops.size();
  }

  static size_t Capacity() { return (1UL << 20); }

  static size_t MaxBytes() {
    return sizeof(size_t) + sizeof(TimeStampType) + sizeof(Stage) +
           sizeof(size_t) + Capacity() * sizeof(StringLog);
  }

  // Format of the BatchWriteLog
  // total_bytes | Stage | timestamp | N | StringLogEntry*N | M |
  // SortedLogEntry*M | K | HashLogEntry*K
  std::string Serialize();

  void Deserialize(char const* src);

  static void Persist(char* dst, std::string const& seq) {
    kvdk_assert(
        *reinterpret_cast<Stage const*>(seq.data()) == Stage::Initializing, "");
    kvdk_assert(*reinterpret_cast<size_t const*>(&seq[1]) == seq.size(), "");
    memcpy(dst, seq.data(), seq.size());
    for (size_t i = 0; i < seq.size(); i += 64) {
      _mm_clflushopt(&dst[i]);
    }
    _mm_mfence();
  }

  static void MarkProcessing(char* dst) {
    *reinterpret_cast<Stage*>(dst) = Stage::Processing;
    _mm_clflush(dst);
    _mm_mfence();
  }

  static void MarkCommitted(char* dst) {
    *reinterpret_cast<Stage*>(dst) = Stage::Committed;
    _mm_clflush(dst);
    _mm_mfence();
  }

  using StringLog = std::vector<StringLogEntry>;
  using SortedLog = std::vector<SortedLogEntry>;
  using HashLog = std::vector<HashLogEntry>;

 private:
  Stage stage{Stage::Initializing};
  TimeStampType timestamp;
  StringLog string_ops;
  SortedLog sorted_ops;
  HashLog hash_ops;
};

}  // namespace KVDK_NAMESPACE
