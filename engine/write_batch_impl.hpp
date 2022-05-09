#include "alias.hpp"
#include "kvdk/write_batch.hpp"

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

  using StringOpBatch = std::vector<StringOp>;
  using SortedOpBatch = std::vector<SortedOp>;
  using HashOpBatch = std::vector<HashOp>;

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

  StringOpBatch const& StringOps() { return string_ops; }
  SortedOpBatch const& SortedOps() { return sorted_ops; }
  HashOpBatch const& HashOps() { return hash_ops; }

 private:
  StringOpBatch string_ops;
  SortedOpBatch sorted_ops;
  HashOpBatch hash_ops;
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

  struct StringOp {
    Op op;
    PMemOffsetType offset;
  };

  struct SortedOp {
    Op op;
    PMemOffsetType offset;
  };

  struct HashOp {
    Op op;
    PMemOffsetType offset;
  };

  explicit BatchWriteLog() {}

  void StringPut(PMemOffsetType offset) {
    string_ops.emplace_back(StringOp{Op::Put, offset});
  }

  void StringDelete(PMemOffsetType offset) {
    string_ops.emplace_back(StringOp{Op::Delete, offset});
  }

  void SortedPut(PMemOffsetType offset) {
    sorted_ops.emplace_back(SortedOp{Op::Put, offset});
  }

  void SortedDelete(PMemOffsetType offset) {
    sorted_ops.emplace_back(SortedOp{Op::Delete, offset});
  }

  void HashPut(PMemOffsetType offset) {
    hash_ops.emplace_back(HashOp{Op::Put, offset});
  }

  void HashDelete(PMemOffsetType offset) {
    hash_ops.emplace_back(HashOp{Op::Delete, offset});
  }

  void Clear() {
    string_ops.clear();
    sorted_ops.clear();
    hash_ops.clear();
  }

  // Format of the BatchWriteLog
  // Stage | total_bytes | N | StringOp*N | M | SortedOp*M | K | HashOp*K
  std::string Serialize() {
    kvdk_assert(stage == Stage::Initializing, "");

    size_t string_cnt = string_ops.size();
    size_t string_sz = string_cnt * sizeof(StringOp);

    size_t sorted_cnt = sorted_ops.size();
    size_t sorted_sz = sorted_cnt * sizeof(SortedOp);

    size_t hash_cnt = hash_ops.size();
    size_t hash_sz = hash_cnt * sizeof(HashOp);

    size_t total_bytes = sizeof(Stage) + sizeof(size_t) * 4 + string_sz +
                         sorted_sz + hash_sz + sizeof(char);

    std::string ret(total_bytes, ' ');
    size_t pos = 0;

    memcpy(&ret[pos], &stage, sizeof(Stage));
    pos += sizeof(Stage);

    memcpy(&ret[pos], &total_bytes, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(&ret[pos], &string_cnt, sizeof(size_t));
    pos += sizeof(size_t);
    memcpy(&ret[pos], &string_ops[0], string_sz);
    pos += string_sz;

    memcpy(&ret[pos], &sorted_cnt, sizeof(size_t));
    pos += sizeof(size_t);
    memcpy(&ret[pos], &sorted_ops[0], sorted_sz);
    pos += sorted_sz;

    memcpy(&ret[pos], &hash_cnt, sizeof(size_t));
    pos += sizeof(size_t);
    memcpy(&ret[pos], &hash_ops[0], hash_sz);
    pos += hash_sz;

    kvdk_assert(pos == total_bytes, "");

    return ret;
  }

  void Deserialize(char const* src) {
    kvdk_assert(string_ops.empty() && sorted_ops.empty() && hash_ops.empty(),
                "");

    size_t pos = 0;

    memcpy(&stage, &src[pos], sizeof(Stage));
    pos += sizeof(Stage);

    if (stage == Stage::Initializing || stage == Stage::Committed) {
      // No need to deserialize furthermore.
      return;
    }
    if (stage != Stage::Processing) {
      kvdk_assert(false, "Invalid Stage, invalid Log!");
      return;
    }

    size_t total_bytes;
    memcpy(&total_bytes, &src[pos], sizeof(size_t));
    pos += sizeof(size_t);

    {
      size_t string_cnt;
      kvdk_assert(pos + sizeof(size_t) < total_bytes, "");
      memcpy(&string_cnt, &src[pos], sizeof(size_t));
      pos += sizeof(size_t);

      size_t string_sz = string_cnt * sizeof(StringOp);
      kvdk_assert(pos + string_sz < total_bytes, "");
      std::vector<StringOp> temp(string_cnt);
      memcpy(&temp[0], &src[pos], string_sz);
      string_ops.swap(temp);
      pos += string_sz;
    }

    {
      size_t sorted_cnt;
      kvdk_assert(pos + sizeof(size_t) < total_bytes, "");
      memcpy(&sorted_cnt, &src[pos], sizeof(size_t));
      pos += sizeof(size_t);

      size_t sorted_sz = sorted_cnt * sizeof(SortedOp);
      kvdk_assert(pos + sorted_sz < total_bytes, "");
      std::vector<SortedOp> temp(sorted_cnt);
      memcpy(&temp[0], &src[pos], sorted_sz);
      sorted_ops.swap(temp);
      pos += sorted_sz;
    }

    {
      size_t hash_cnt;
      kvdk_assert(pos + sizeof(size_t) < total_bytes, "");
      memcpy(&hash_cnt, &src[pos], sizeof(size_t));
      pos += sizeof(size_t);

      size_t hash_sz = hash_cnt * sizeof(HashOp);
      kvdk_assert(pos + hash_sz == total_bytes, "");
      std::vector<HashOp> temp(hash_cnt);
      memcpy(&temp[0], &src[pos], hash_sz);
      hash_ops.swap(temp);
      pos += hash_sz;
    }
  }

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

  using StringOpBatch = std::vector<StringOp>;
  using SortedOpBatch = std::vector<SortedOp>;
  using HashOpBatch = std::vector<HashOp>;

 private:
  Stage stage{Stage::Initializing};
  StringOpBatch string_ops;
  SortedOpBatch sorted_ops;
  HashOpBatch hash_ops;
};

}  // namespace KVDK_NAMESPACE
