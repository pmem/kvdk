#include "write_batch_impl.hpp"

#include "alias.hpp"

namespace KVDK_NAMESPACE {

void BatchWriteLog::EncodeTo(char* dst) {
  kvdk_assert(stage == Stage::Initializing, "");

  size_t total_bytes;
  total_bytes =
      sizeof(total_bytes) + sizeof(timestamp) + sizeof(stage) +
      sizeof(string_logs.size()) + string_logs.size() * sizeof(StringLogEntry) +
      sizeof(sorted_logs.size()) + sorted_logs.size() * sizeof(SortedLogEntry) +
      sizeof(hash_logs.size()) + hash_logs.size() * sizeof(HashLogEntry);

  std::string buffer;
  buffer.reserve(total_bytes);

  AppendPOD(&buffer, total_bytes);
  AppendPOD(&buffer, timestamp);
  AppendPOD(&buffer, stage);

  AppendPOD(&buffer, string_logs.size());
  for (size_t i = 0; i < string_logs.size(); i++) {
    AppendPOD(&buffer, string_logs[i]);
  }

  AppendPOD(&buffer, sorted_logs.size());
  for (size_t i = 0; i < sorted_logs.size(); i++) {
    AppendPOD(&buffer, sorted_logs[i]);
  }

  AppendPOD(&buffer, hash_logs.size());
  for (size_t i = 0; i < hash_logs.size(); i++) {
    AppendPOD(&buffer, hash_logs[i]);
  }

  kvdk_assert(buffer.size() == total_bytes, "");

  memcpy(dst, buffer.data(), buffer.size());
  for (size_t i = 0; i < buffer.size(); i += 64) {
    _mm_clflushopt(&dst[i]);
  }
  _mm_mfence();
}

void BatchWriteLog::DecodeFrom(char const* src) {
  kvdk_assert(string_logs.empty() && sorted_logs.empty() && hash_logs.empty(),
              "");

  size_t total_bytes = *reinterpret_cast<size_t const*>(src);

  StringView sw{src, total_bytes};

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