/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "namespace.hpp"
#include <string>

namespace PMEMDB_NAMESPACE {

struct DBOptions {
  // Max threads num to write db
  // Recommend set it to the hyper-threads number
  uint64_t max_write_threads = 48;

  // Original pmem file size
  // Not changeable in the runtime now
  // Make sure it's larger than (pmem_segment_blocks * max_write_threads)
  // TODO: sacle out
  uint64_t pmem_file_size = (256ULL << 30);

  // Populate pmem space while creating a new db
  // This can improve write performance in runtime, but will take long time to
  // init while creating the db at the first time
  bool populate_pmem_space = true;

  // The minimum allocation unit of PMEM space
  uint32_t pmem_block_size = 64;

  // The number of blocks in a PMEM segment
  uint64_t pmem_segment_blocks = 1024 * 1024;

  // Size of each hash bucket (hash entry size is 16 and the pointer size to the
  // next bucket is 4)
  uint32_t hash_bucket_size = 128;

  // How many buckets in the hash table, should smaller than 2^32
  // the original hash table size would be (hash_bucket_size * hash_bucket_num)
  uint64_t hash_bucket_num = (1 << 27);

  // max memory space to store hash table
  // make sure it's larger than (hash_bucket_size * hash_bucket_num)
  uint64_t max_memory_usage = (32ULL << 30);

  // How many buckets in a slot
  uint32_t slot_grain = 16;

  /* below is for kv server, please ignore */
  bool network_client = false;

  std::string server_url = "127.0.0.1";

  int port = 9999;

  int workers = 48;
};

} // namespace PMEMDB_NAMESPACE