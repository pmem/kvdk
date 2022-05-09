#include "write_batch_impl.hpp"

#include "alias.hpp"

namespace KVDK_NAMESPACE {

std::string BatchWriteLog::Serialize() {
  kvdk_assert(stage == Stage::Initializing, "");

  size_t total_bytes = sizeof(Stage) + sizeof(size_t) * 4 +
                       sizeof(StringEntry) * string_ops.size() +
                       sizeof(SortedEntry) * sorted_ops.size() +
                       sizeof(HashEntry) * hash_ops.size();

  std::string ret;
  ret.reserve(total_bytes);
  size_t pos = 0;

  AppendPOD(&ret, stage);
  AppendPOD(&ret, total_bytes);

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

  StringView sw{src, sizeof(Stage) + sizeof(size_t)};
  stage = FetchPOD<Stage>(&sw);

  if (stage == Stage::Initializing || stage == Stage::Committed) {
    // No need to deserialize furthermore.
    return;
  }
  if (stage != Stage::Processing) {
    kvdk_assert(false, "Invalid Stage, invalid Log!");
    return;
  }

  size_t total_bytes = FetchPOD<size_t>(&sw);
  sw = StringView{sw.data(), total_bytes - sizeof(Stage) - sizeof(size_t)};

  string_ops.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_ops.size(); i++) {
    string_ops[i] = FetchPOD<StringEntry>(&sw);
  }

  sorted_ops.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_ops.size(); i++) {
    sorted_ops[i] = FetchPOD<SortedEntry>(&sw);
  }

  hash_ops.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_ops.size(); i++) {
    hash_ops[i] = FetchPOD<HashEntry>(&sw);
  }

  kvdk_assert(sw.size() == 0, "");
}

}  // namespace KVDK_NAMESPACE