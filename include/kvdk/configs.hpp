/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <string>

#include "namespace.hpp"

namespace KVDK_NAMESPACE {

struct Configs {
  // Max number of write threads.
  //
  // Notice that the allocated resources of a write thread would be released
  // only if the thread exited.
  uint64_t max_write_threads = 48;

  // Size of PMem space to store KV data, this is not scalable in current
  // edition.
  //
  // Notice that it should be larger than (max_write_threads *
  // pmem_segment_blocks  * pmem_block_size)
  // TODO: sacle out
  uint64_t pmem_file_size = (256ULL << 30);

  // Populate PMem space while creating a new instance.
  //
  // This can improve write performance in runtime, but will take long time to
  // init the newly created instance.
  bool populate_pmem_space = true;

  // The minimum allocation unit of PMem space
  //
  // It should align to 8 bytes.
  uint32_t pmem_block_size = 64;

  // The number of blocks in a PMem segment
  //
  // A PMem segment is a piece of private space of a write thread, so each
  // thread can allocate space without contention. It also decides the max size
  // of (key + value), which is slightly smaller than
  // (pmem_block_size * pmem_segment_blocks)
  uint64_t pmem_segment_blocks = 2 * 1024 * 1024;

  // Size of each hash bucket
  //
  // It should be larger than hans entry size (which is 16) plus 8 (the pointer
  // to next bucket). It is recommended to set it align to cache line
  uint32_t hash_bucket_size = 128;

  // The number of bucket groups in the hash table.
  //
  // It should be 2^n and should smaller than 2^32.
  // The original hash table size would be (hash_bucket_size *
  // hash_bucket_num)
  // More buckets means less write contention, faster hash
  // search, but more internal memory fragmentation.
  uint64_t hash_bucket_num = (1 << 27);

  // The number of buckets per hash slot.
  //
  // The hash slot is the minimum unit of write lock and hot-spot cache.
  uint32_t num_buckets_per_slot = 1 << 4;

  // Time interval to do background work in seconds
  //
  // In KVDK, a background thread will regularly organize PMem free space,
  // frequent execution will lead to better space utilization, but more
  // influence to foreground performance
  uint64_t background_work_interval = 5;
};

} // namespace KVDK_NAMESPACE
