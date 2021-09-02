/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>
#include <malloc.h>

#include "kvdk/namespace.hpp"
#include "logger.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

struct PendingBatch {
  enum class Stage {
    Done = 0,
    Processing = 1,
  };

  PendingBatch(Stage s, uint32_t nkv, uint64_t ts)
      : stage(s), num_kv(nkv), timestamp(ts) {}

  void PersistProcessing(void *target,
                         const std::vector<uint64_t> &entry_offsets);

  void Restore(char *target, std::vector<uint64_t> *entry_offsets);

  void PersistStage(Stage s);

  bool Unfinished() { return stage == Stage::Processing; }

  Stage stage;
  uint32_t num_kv;
  uint64_t timestamp;
};

class PersistentList {
public:
  virtual uint64_t id() = 0;

  inline static std::string ListKey(const pmem::obj::string_view &user_key,
                                    uint64_t list_id) {
    return std::string((char *)&list_id, 8)
        .append(user_key.data(), user_key.size());
  }
};
} // namespace KVDK_NAMESPACE
