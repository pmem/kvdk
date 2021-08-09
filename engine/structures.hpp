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

struct HashHeader {
  uint32_t key_prefix;
  uint16_t reference;
  uint16_t type;
};

struct HashEntry {
  HashEntry() = default;
  HashEntry(uint32_t kp, uint16_t r, uint16_t t, uint64_t bo)
      : header({kp, r, t}), offset(bo) {}

  HashHeader header;
  uint64_t offset;

  static void CopyHeader(HashEntry *dst, HashEntry *src) { memcpy_8(dst, src); }
  static void CopyOffset(HashEntry *dst, HashEntry *src) {
    dst->offset = src->offset;
  }
};

struct HashCache {
  HashEntry *entry_base = nullptr;
};

struct Slot {
  HashCache hash_cache;
  SpinMutex spin;
};

struct PendingBatch {
  enum Stage {
    Done = 0,
    Processing = 1,
  };

  PendingBatch(Stage s, uint32_t nkv, uint64_t ts)
      : stage(s), num_kv(nkv), timestamp(ts) {}

  void PersistProcessing(void *target,
                         const std::vector<uint64_t> &entry_offsets);

  void Restore(char *target, std::vector<uint64_t> *entry_offsets);

  void PersistStage(Stage s);

  bool Unfinished() { return stage == Processing; }

  Stage stage;
  uint32_t num_kv;
  uint64_t timestamp;
};

class PersistentList {
public:
  virtual uint64_t id() = 0;

  inline static std::string ListKey(const std::string &user_key,
                                    uint64_t list_id) {
    return std::string((char *)&list_id, 8).append(user_key);
  }
};
} // namespace KVDK_NAMESPACE