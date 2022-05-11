#include "write_batch_impl.hpp"

#include "alias.hpp"

namespace KVDK_NAMESPACE {

std::string BatchWriteLog::Serialize() {
  kvdk_assert(stage == Stage::Initializing, "");

  size_t total_bytes;
  total_bytes =
      sizeof(total_bytes) + sizeof(timestamp) + sizeof(stage) +
      sizeof(string_logs.size()) + string_logs.size() * sizeof(StringLogEntry) +
      sizeof(sorted_logs.size()) + sorted_logs.size() * sizeof(SortedLogEntry) +
      sizeof(hash_logs.size()) + hash_logs.size() * sizeof(HashLogEntry);

  std::string ret;
  ret.reserve(total_bytes);

  AppendPOD(&ret, total_bytes);
  AppendPOD(&ret, timestamp);
  AppendPOD(&ret, stage);

  AppendPOD(&ret, string_logs.size());
  for (size_t i = 0; i < string_logs.size(); i++) {
    AppendPOD(&ret, string_logs[i]);
  }

  AppendPOD(&ret, sorted_logs.size());
  for (size_t i = 0; i < sorted_logs.size(); i++) {
    AppendPOD(&ret, sorted_logs[i]);
  }

  AppendPOD(&ret, hash_logs.size());
  for (size_t i = 0; i < hash_logs.size(); i++) {
    AppendPOD(&ret, hash_logs[i]);
  }

  kvdk_assert(ret.size() == total_bytes, "");

  return ret;
}

void BatchWriteLog::Deserialize(char const* src) {
  kvdk_assert(string_logs.empty() && sorted_logs.empty() && hash_logs.empty(),
              "");

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

  string_logs.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_logs.size(); i++) {
    string_logs[i] = FetchPOD<StringLogEntry>(&sw);
  }

  sorted_logs.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_logs.size(); i++) {
    sorted_logs[i] = FetchPOD<SortedLogEntry>(&sw);
  }

  hash_logs.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_logs.size(); i++) {
    hash_logs[i] = FetchPOD<HashLogEntry>(&sw);
  }

  kvdk_assert(sw.size() == 0, "");
}

}  // namespace KVDK_NAMESPACE