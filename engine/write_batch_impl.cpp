#include "write_batch_impl.hpp"

#include "alias.hpp"

namespace KVDK_NAMESPACE {

std::string BatchWriteLog::Serialize() {
  kvdk_assert(stage == Stage::Initializing, "");

  size_t total_bytes;
  total_bytes =
      sizeof(total_bytes) + sizeof(timestamp) + sizeof(stage) +
      sizeof(string_ops.size()) + string_ops.size() * sizeof(StringLogEntry) +
      sizeof(sorted_ops.size()) + sorted_ops.size() * sizeof(SortedLogEntry) +
      sizeof(hash_ops.size()) + hash_ops.size() * sizeof(HashLogEntry);

  std::string ret;
  ret.reserve(total_bytes);

  AppendPOD(&ret, total_bytes);
  AppendPOD(&ret, timestamp);
  AppendPOD(&ret, stage);

  AppendPOD(&ret, string_ops.size());
  for (size_t i = 0; i < string_ops.size(); i++) {
    AppendPOD(&ret, string_ops[i]);
  }

  AppendPOD(&ret, sorted_ops.size());
  for (size_t i = 0; i < sorted_ops.size(); i++) {
    AppendPOD(&ret, sorted_ops[i]);
  }

  AppendPOD(&ret, hash_ops.size());
  for (size_t i = 0; i < hash_ops.size(); i++) {
    AppendPOD(&ret, hash_ops[i]);
  }

  kvdk_assert(ret.size() == total_bytes, "");

  return ret;
}

void BatchWriteLog::Deserialize(char const* src) {
  kvdk_assert(string_ops.empty() && sorted_ops.empty() && hash_ops.empty(), "");

  size_t total_bytes = *reinterpret_cast<size_t const*>(src);

  StringView sw{src, sizeof(total_bytes)};

  total_bytes = FetchPOD<size_t>(&sw);
  timestamp = FetchPOD<size_t>(&sw);
  stage = FetchPOD<Stage>(&sw);

  if (stage == Stage::Initializing || stage == Stage::Committed) {
    // No need to deserialize furthermore.
    return;
  }
  if (stage != Stage::Processing) {
    kvdk_assert(false, "Invalid Stage, invalid Log!");
    return;
  }

  string_ops.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_ops.size(); i++) {
    string_ops[i] = FetchPOD<StringLogEntry>(&sw);
  }

  sorted_ops.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_ops.size(); i++) {
    sorted_ops[i] = FetchPOD<SortedLogEntry>(&sw);
  }

  hash_ops.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_ops.size(); i++) {
    hash_ops[i] = FetchPOD<HashLogEntry>(&sw);
  }

  kvdk_assert(sw.size() == 0, "");
}

}  // namespace KVDK_NAMESPACE