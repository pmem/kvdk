#include "write_batch_impl.hpp"

#include "alias.hpp"

namespace KVDK_NAMESPACE {

std::string BatchWriteLog::Serialize() {
  kvdk_assert(stage == Stage::Initializing, "");

  size_t total_bytes = sizeof(Stage) + sizeof(size_t) * 4 +
                       sizeof(StringOp) * string_ops.size() +
                       sizeof(SortedOp) * sorted_ops.size() +
                       sizeof(HashOp) * hash_ops.size();

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

  {
    std::vector<StringOp> temp(FetchPOD<size_t>(&sw));
    for (size_t i = 0; i < temp.size(); i++) {
      temp[i] = FetchPOD<StringOp>(&sw);
    }
    string_ops.swap(temp);
  }

  {
    std::vector<SortedOp> temp(FetchPOD<size_t>(&sw));
    for (size_t i = 0; i < temp.size(); i++) {
      temp[i] = FetchPOD<SortedOp>(&sw);
    }
    sorted_ops.swap(temp);
  }

  {
    std::vector<HashOp> temp(FetchPOD<size_t>(&sw));
    for (size_t i = 0; i < temp.size(); i++) {
      temp[i] = FetchPOD<HashOp>(&sw);
    }
    hash_ops.swap(temp);
  }

  kvdk_assert(sw.size() == 0, "");
}

}  // namespace KVDK_NAMESPACE