/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <string>

#include "namespace.hpp"

namespace KVDK_NAMESPACE {

enum class LogLevel : uint8_t {
  All = 0,
  Debug,
  Info,
  Error,
};

struct Snapshot {
public:
  virtual uint64_t GetTimestamp() = 0;
};

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
  // The hash slot is the minimum unit of write lock and hot-spot cache, less
  // buckets in a slot means better hot spot read performance, less write
  // contentions and more memory consumption
  uint32_t num_buckets_per_slot = 1;

  // Time interval to do background work in seconds
  //
  // In KVDK, a background thread will regularly organize PMem free space,
  // frequent execution will lead to better space utilization, but more
  // influence to foreground performance
  double background_work_interval = 5;

  // Support the devdax model with PMem
  //
  // The devdax mode will create a char device on a pmem region, we will
  // use mmap to operator the devdax device.
  bool use_devdax_mode = false;

  // Store the immutable config and batch-write stats on devdax mode
  //
  // To use the config, you must create the devdax namespace with the shell
  // scripts/init_devdax.sh.
  // The shell will create a devdax namespace and a 32MB(the minum size of
  // fsdax) fsdax namespace, and the fsdax namespace will mount on this dir.
  //
  // ps:
  // If you want change the dir, you need change the scripts/init_devdax.sh's
  // fsdax mount dir, and this dir should be same with shell's fsdax dir.
  //
  // TODO(zhg): organize the immutable config and pending_batch_dir on a same
  // file of the data file.
  std::string devdax_meta_dir = "/mnt/kvdk-pmem-meta";

  // Log information to show
  LogLevel log_level = LogLevel::Info;

  // Optional optimization strategy for few large skiplists by multi-thread
  // recovery a skiplist. The optimization can get better performance when
  // having few large skiplists. Default is to close optimization.
  bool opt_large_sorted_collection_restore = false;
};

} // namespace KVDK_NAMESPACE
