/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <string>

#include "comparator.hpp"
#include "types.hpp"

namespace KVDK_NAMESPACE {

enum class LogLevel : uint8_t {
  All = 0,
  Debug,
  Info,
  Error,
  None,
};

// Configs of created sorted collection
// For correctness of encoding, please add new config field in the end of the
// existing fields
struct SortedCollectionConfigs {
  std::string comparator_name = "default";
  int index_with_hashtable = 1;
};

struct Configs {
  // TODO: rename to concurrent internal threads
  //
  // Max number of concurrent threads read/write the kvdk instance internally.
  // Set it to the number of the hyper-threads to get best performance
  //
  // Notice:  you can call KVDK API with any number of threads, but if your
  // parallel threads more than max_access_threads, the performance will be
  // degraded due to synchronization cost
  uint64_t max_access_threads = 64;

  // The number of bucket groups in the hash table.
  //
  // It should be 2^n and should smaller than 2^32.
  // The original hash table size would be (kHashBucketSize(128 by default) *
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
  // In KVDK, a background thread will regularly organize free space,
  // frequent execution will lead to better space utilization, but more
  // influence to foreground performance
  double background_work_interval = 5.0;

  // Time interval that the background thread report memory usage by
  // GlobalLogger. report_memory_usage_interval less than
  // background_work_interval will be ignored.
  double report_memory_usage_interval = 1000000.0;

  // Log information to show
  LogLevel log_level = LogLevel::Info;

  // Optional optimization strategy for few large skiplists by multi-thread
  // recovery a skiplist. The optimization can get better performance when
  // having few large skiplists. Default is to close optimization.
  bool opt_large_sorted_collection_recovery = false;

  // If a checkpoint is made in last open, recover the instance to the
  // checkpoint version if this true
  //
  // Notice: If opening a backup instance, this will always be set to true
  // during recovery.
  bool recover_to_checkpoint = false;

  // If customer compare functions is used in a kvdk engine, these functions
  // should be registered to the comparator before open engine
  ComparatorTable comparator;

  // Background clean thread numbers.
  uint64_t clean_threads = 8;

  // Set the memory nodes where volatile KV memory allocator binds to.
  // There is no effect when the configured value is empty or invalid.
  // Note: Currently, only the jemalloc allocator support memory binding.
  // jemalloc is enabled by cmake option -DWITH_JEMALLOC=ON.
  std::string dest_memory_nodes = "";
};

struct WriteOptions {
  WriteOptions(TTLType _ttl_time = kPersistTTL, bool _update_ttl = true)
      : ttl_time(_ttl_time), update_ttl(_update_ttl) {}

  // expired time in milliseconod, should be kPersistTime for no expiration
  // data, or a certain interge which be in the range [INT64_MIN , INT64_MAX].
  TTLType ttl_time;

  // determine whether to update expired time if key already existed
  bool update_ttl;
};

}  // namespace KVDK_NAMESPACE
