/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include "write_batch_impl.hpp"

#include "alias.hpp"

namespace KVDK_NAMESPACE {

void BatchWriteLog::EncodeTo(char* dst) {
  if (dst == nullptr) {
    return;
  }
  kvdk_assert(stage == Stage::Initializing, "");

  size_t total_bytes;
  total_bytes =
      sizeof(total_bytes) + sizeof(timestamp_) + sizeof(stage) +
      sizeof(string_logs_.size()) +
      string_logs_.size() * sizeof(StringLogEntry) +
      sizeof(sorted_logs_.size()) +
      sorted_logs_.size() * sizeof(SortedLogEntry) + sizeof(hash_logs_.size()) +
      hash_logs_.size() * sizeof(HashLogEntry) + sizeof(list_logs_.size()) +
      list_logs_.size() * sizeof(ListLogEntry);

  std::string buffer;
  buffer.reserve(total_bytes);

  AppendPOD(&buffer, total_bytes);
  AppendPOD(&buffer, timestamp_);
  AppendPOD(&buffer, stage);

  AppendPOD(&buffer, string_logs_.size());
  for (size_t i = 0; i < string_logs_.size(); i++) {
    AppendPOD(&buffer, string_logs_[i]);
  }

  AppendPOD(&buffer, sorted_logs_.size());
  for (size_t i = 0; i < sorted_logs_.size(); i++) {
    AppendPOD(&buffer, sorted_logs_[i]);
  }

  AppendPOD(&buffer, hash_logs_.size());
  for (size_t i = 0; i < hash_logs_.size(); i++) {
    AppendPOD(&buffer, hash_logs_[i]);
  }

  AppendPOD(&buffer, list_logs_.size());
  for (size_t i = 0; i < list_logs_.size(); i++) {
    AppendPOD(&buffer, list_logs_[i]);
  }

  kvdk_assert(buffer.size() == total_bytes, "");

  memcpy(dst, buffer.data(), buffer.size());
  for (size_t i = 0; i < buffer.size(); i += 64) {
    _mm_clflushopt(&dst[i]);
  }
  _mm_mfence();
}

void BatchWriteLog::DecodeFrom(char const* src) {
  if (src == nullptr) {
    return;
  }
  kvdk_assert(
      string_logs_.empty() && sorted_logs_.empty() && hash_logs_.empty(), "");

  size_t total_bytes = *reinterpret_cast<size_t const*>(src);
  if (total_bytes == 0) {
    return;
  }

  StringView sw{src, total_bytes};

  total_bytes = FetchPOD<size_t>(&sw);
  timestamp_ = FetchPOD<TimestampType>(&sw);
  stage = FetchPOD<Stage>(&sw);

  if (stage == Stage::Initializing || stage == Stage::Committed) {
    // No need to deserialize furthermore.
    return;
  }
  if (stage != Stage::Processing) {
    kvdk_assert(false, "Invalid Stage, invalid Log!");
    return;
  }

  string_logs_.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < string_logs_.size(); i++) {
    string_logs_[i] = FetchPOD<StringLogEntry>(&sw);
  }

  sorted_logs_.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < sorted_logs_.size(); i++) {
    sorted_logs_[i] = FetchPOD<SortedLogEntry>(&sw);
  }

  hash_logs_.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < hash_logs_.size(); i++) {
    hash_logs_[i] = FetchPOD<HashLogEntry>(&sw);
  }

  list_logs_.resize(FetchPOD<size_t>(&sw));
  for (size_t i = 0; i < list_logs_.size(); i++) {
    list_logs_[i] = FetchPOD<ListLogEntry>(&sw);
  }

  kvdk_assert(sw.size() == 0, "");
}

}  // namespace KVDK_NAMESPACE