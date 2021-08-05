/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "assert.h"
#include "atomic"
#include "logger.hpp"
#include "pmemdb/db.hpp"
#include <string>
#include <sys/mman.h>

namespace PMEMDB_NAMESPACE {

class MemoryPool {
public:
  MemoryPool(uint64_t chunk_size, uint64_t pool_size)
      : chunk_size_(chunk_size), pool_size_(pool_size), offset_(0) {
    assert(pool_size <= chunk_size << 32);
    pool_ = (char *)mmap64(nullptr, pool_size, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANON, -1, 0);
  }

  ~MemoryPool() { munmap(pool_, pool_size_); }

  Status GetChunk(uint64_t num_chunk, uint32_t &chunk_offset) {
    chunk_offset = offset_.fetch_add(num_chunk);
    if (chunk_offset + num_chunk >= pool_size_) {
#ifdef DO_LOG
      GlobalLogger.Error("out of dram\n");
#endif
      return Status::MemoryOverflow;
    }
    return Status::Ok;
  }

  char *GetAddress(uint32_t chunk_offset) {
    return pool_ + (uint64_t)chunk_offset * chunk_size_;
  }

private:
  const uint64_t chunk_size_;
  const uint64_t pool_size_;
  std::atomic<uint32_t> offset_;
  char *pool_;
};
} // namespace PMEMDB_NAMESPACE