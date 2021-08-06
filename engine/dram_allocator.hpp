/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "allocator.hpp"
#include "assert.h"
#include "atomic"
#include "kvdk/engine.hpp"
#include "logger.hpp"
#include "structures.hpp"
#include <string>
#include <sys/mman.h>

namespace KVDK_NAMESPACE {

// Chunk based simple implementation
// TODO: optimize, implement free
class DRAMAllocator : Allocator {
public:
  SizedSpaceEntry Allocate(uint64_t size) override;
  void Free(const SizedSpaceEntry &entry) override;
  inline char *offset2addr(uint64_t offset) { return (char *)offset; }
  inline uint64_t addr2offset(void *addr) { return (uint64_t)addr; }
  DRAMAllocator(uint32_t write_threads) : thread_cache_(write_threads) {}

private:
  struct ThreadCache {
    alignas(64) char *chunk_addr = nullptr;
    uint64_t usable_bytes = 0;
  };

  const uint32_t chunk_size_ = (1 << 20);
  std::vector<ThreadCache> thread_cache_;
};
} // namespace KVDK_NAMESPACE