#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <string>
#include <thread>

#include <gflags/gflags.h>

#include "sys/time.h"

#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"

#include "engine/alias.hpp"

#include "generator.hpp"

using namespace google;
using namespace KVDK_NAMESPACE;

#define MAX_LAT (10000000)

// Benchmark configs
DEFINE_string(path, "/mnt/pmem0/kvdk", "Instance path");

DEFINE_uint64(num, (1 << 30), "Number of KVs to place");

DEFINE_uint64(num_operations, (1 << 30),
              "Number of total operations. Asserted to be equal to num if "
              "(fill == true).");

DEFINE_bool(fill, false, "Fill num uniform kv pairs to a new instance");

DEFINE_uint64(time, 600, "Time to benchmark, this is valid only if fill=false");

DEFINE_uint64(value_size, 120, "Value size of KV");

DEFINE_string(value_size_distribution, "constant",
              "Distribution of value size to write, can be constant/random, "
              "default is constant. If set to random, the max value size "
              "will be FLAGS_value_size.");

DEFINE_uint64(threads, 10, "Number of concurrent threads to run benchmark");

DEFINE_double(read_ratio, 0, "Read threads = threads * read_ratio");

DEFINE_double(
    existing_keys_ratio, 1,
    "Ratio of keys to read / write that existed in the filled instance, for "
    "example, if set to "
    "1, all writes will be updates, and all read keys will be existed");

DEFINE_bool(latency, false, "Stat operation latencies");

DEFINE_string(type, "string",
              "Storage engine to benchmark, can be string, sorted, hash, queue "
              "or blackhole");

DEFINE_bool(scan, false,
            "If set true, read threads will do scan operations, this is valid "
            "only if we benchmark sorted or hash engine");

DEFINE_uint64(num_collection, 1,
              "Number of collections in the instance to benchmark");

DEFINE_uint64(
    batch_size, 0,
    "Size of write batch. If batch>0, write string type kv with atomic batch "
    "write, this is valid only if we benchmark string engine");

DEFINE_string(key_distribution, "random",
              "Distribution of benchmark keys, if fill is true, this para will "
              "be ignored and only uniform distribution will be used");

// Engine configs
DEFINE_bool(
    populate, false,
    "Populate pmem space while creating a new instance. This can improve write "
    "performance in runtime, but will take long time to init the instance");

DEFINE_int32(max_write_threads, 32, "Max write threads of the instance");

DEFINE_uint64(space, (uint64_t)256 << 30,
              "Max usable PMem space of the instance");

DEFINE_bool(opt_large_sorted_collection_restore, false,
            " Optional optimization strategy which Multi-thread recovery a "
            "skiplist. When having few large skiplists, the optimization can "
            "get better performance");

DEFINE_bool(use_devdax_mode, false, "Use devdax device for kvdk");

class Timer {
public:
  void Start() { clock_gettime(CLOCK_REALTIME, &start); }

  uint64_t End() {
    struct timespec end;
    clock_gettime(CLOCK_REALTIME, &end);
    return (end.tv_sec - start.tv_sec) * 1000000000 +
           (end.tv_nsec - start.tv_nsec);
  }

private:
  struct timespec start;
};

std::atomic<uint64_t> read_ops{0};
std::atomic<uint64_t> write_ops{0};
std::atomic<uint64_t> read_not_found{0};
std::atomic<uint64_t> read_cnt{UINT64_MAX};
std::vector<std::vector<uint64_t>> read_latencies;
std::vector<std::vector<uint64_t>> write_latencies;
std::vector<std::string> collections;
Engine *engine;
std::string value_pool;
size_t operations_per_thread;
bool has_timed_out;
std::vector<int> has_finished; // std::vector<bool> is a trap!

std::vector<PaddedEngine> engines;
std::vector<PaddedRangeIterators> ranges;

enum class DataType {
  String,
  Sorted,
  Hashes,
  Queue,
  Blackhole
} bench_data_type;

enum class KeyDistribution { Range, Random, Zipf } key_dist;

enum class ValueSizeDistribution { Constant, Random } vsz_dist;

std::uint64_t generate_key(size_t tid) {
  static std::uint64_t max_key = FLAGS_existing_keys_ratio == 0
                                     ? UINT64_MAX
                                     : FLAGS_num / FLAGS_existing_keys_ratio;
  static extd::zipfian_distribution<std::uint64_t> zipf{max_key, 0.99};
  switch (key_dist) {
  case KeyDistribution::Range: {
    return ranges[tid].gen();
  }
  case KeyDistribution::Random: {
    return engines[tid].gen() % max_key;
  }
  case KeyDistribution::Zipf: {
    return zipf(engines[tid].gen);
  }
  default: {
    throw;
  }
  }
}

size_t generate_vsz(size_t tid) {
  switch (vsz_dist) {
  case ValueSizeDistribution::Constant: {
    return FLAGS_value_size;
  }
  case ValueSizeDistribution::Random: {
    return engines[tid].gen() % FLAGS_value_size + 1;
  }
  default: {
    throw;
  }
  }
}

void DBWrite(int tid) {
  Timer timer;
  uint64_t lat = 0;
  WriteBatch batch;

  Status s;
  std::string key;
  key.resize(8);
  for (size_t operations = 0; operations < operations_per_thread;
       ++operations) {
    if (has_timed_out) {
      break;
    }

    // generate key
    std::uint64_t num = generate_key(tid);
    memcpy(&key[0], &num, 8);

    StringView value = StringView(value_pool.data(), generate_vsz(tid));

    if (FLAGS_latency)
      timer.Start();
    switch (bench_data_type) {
    case DataType::String: {
      if (FLAGS_batch_size == 0) {
        s = engine->Set(key, value);
      } else {
        batch.Put(key, std::string(value.data(), value.size()));
        if (batch.Size() == FLAGS_batch_size) {
          engine->BatchWrite(batch);
          batch.Clear();
        }
      }
      break;
    }
    case DataType::Sorted: {
      s = engine->SSet(collections[num % FLAGS_num_collection], key, value);
      break;
    }
    case DataType::Hashes: {
      s = engine->HSet(collections[num % FLAGS_num_collection], key, value);
      break;
    }
    case DataType::Queue: {
      if ((num / FLAGS_num_collection) % 2 == 0)
        s = engine->LPush(collections[num % FLAGS_num_collection], value);
      break;
    }
    case DataType::Blackhole: {
      s = Status::Ok;
      if (operations == 0) {
        s = engine->Set(
            key, value); // Write something so that blackhole won't re-populate
      }
      break;
    }
    default: {
      throw std::runtime_error{"Unsupported data type!"};
    }
    }

    if (FLAGS_latency) {
      lat = timer.End();
      if (lat / 100 >= MAX_LAT) {
        throw std::runtime_error{"Write latency overflow"};
      }
      write_latencies[tid][lat / 100]++;
    }

    if (s != Status::Ok) {
      throw std::runtime_error{"Set error"};
    }

    if ((operations + 1) % 1000 == 0) {
      write_ops.fetch_add(1000);
    };
  }
  has_finished[tid] = 1;
  return;
}

void DBScan(int tid) {
  uint64_t operations = 0;
  uint64_t operations_counted = 0;
  std::string key;
  std::string value;
  key.resize(8);
  int scan_length = 100;
  for (size_t operations = 0; operations < operations_per_thread;) {
    if (has_timed_out) {
      break;
    }

    uint64_t num = generate_key(tid);
    memcpy(&key[0], &num, 8);
    switch (bench_data_type) {
    case DataType::Sorted: {
      auto iter =
          engine->NewSortedIterator(collections[num % FLAGS_num_collection]);
      if (iter) {
        iter->Seek(key);
        for (size_t i = 0; i < scan_length && iter->Valid();
             i++, iter->Next()) {
          key = iter->Key();
          value = iter->Value();
          ++operations;
          if (operations > operations_counted + 1000) {
            read_ops.fetch_add(operations - operations_counted);
            operations_counted = operations;
          }
        }
      } else {
        throw std::runtime_error{"Error creating SortedIterator"};
      }
      break;
    }
    case DataType::Hashes: {
      auto iter =
          engine->NewUnorderedIterator(collections[num % FLAGS_num_collection]);
      if (iter) {
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
          key = iter->Key();
          value = iter->Value();
          ++operations;
          if (operations > operations_counted + 1000) {
            read_ops += (operations - operations_counted);
            operations_counted = operations;
          }
        }
      } else {
        throw std::runtime_error{"Error creating UnorderedIterator"};
      }
      break;
    }
    case DataType::Blackhole: {
      read_ops.fetch_add(1024);
      break;
    }
    case DataType::String:
    case DataType::Queue:
    default: {
      throw std::runtime_error{"Unsupported data type!"};
    }
    }
  }

  has_finished[tid] = 1;
  return;
}

void DBRead(int tid) {
  std::string value;
  std::string key;
  key.resize(8);
  Timer timer;
  uint64_t lat = 0;
  Status s;

  uint64_t not_found = 0;
  for (size_t operations = 0; operations < operations_per_thread;
       ++operations) {
    if (has_timed_out) {
      break;
    }

    std::uint64_t num = generate_key(tid);
    memcpy(&key[0], &num, 8);
    if (FLAGS_latency)
      timer.Start();
    switch (bench_data_type) {
    case DataType::String: {
      s = engine->Get(key, &value);
      break;
    }
    case DataType::Sorted: {
      s = engine->SGet(collections[num % FLAGS_num_collection], key, &value);
      break;
    }
    case DataType::Hashes: {
      s = engine->HGet(collections[num % FLAGS_num_collection], key, &value);
      break;
    }
    case DataType::Queue: {
      s = engine->RPop(collections[num % FLAGS_num_collection], &value);
      break;
    }
    case DataType::Blackhole: {
      s = Status::Ok;
      break;
    }
    default: {
      throw std::runtime_error{"Unsupported!"};
    }
    }

    if (FLAGS_latency) {
      lat = timer.End();
      if (lat / 100 >= MAX_LAT) {
        fprintf(stderr, "Read latency overflow: %ld us\n", lat / 100);
        std::abort();
      }
      read_latencies[tid][lat / 100]++;
    }

    if (s != Status::Ok) {
      if (s != Status::NotFound) {
        throw std::runtime_error{"Fail to Read"};
      } else {
        if (++not_found % 1000 == 0) {
          read_not_found.fetch_add(1000);
        }
      }
    }

    if ((operations + 1) % 1000 == 0) {
      read_ops.fetch_add(1000);
    }
  }

  has_finished[tid] = 1;
  return;
}

bool ProcessBenchmarkConfigs() {
  if (FLAGS_type == "sorted") {
    bench_data_type = DataType::Sorted;
  } else if (FLAGS_type == "string") {
    bench_data_type = DataType::String;
  } else if (FLAGS_type == "hash") {
    bench_data_type = DataType::Hashes;
  } else if (FLAGS_type == "queue") {
    bench_data_type = DataType::Queue;
  } else if (FLAGS_type == "blackhole") {
    bench_data_type = DataType::Blackhole;
  } else {
    return false;
  }
  // Initialize collections and batch parameters
  switch (bench_data_type) {
  case DataType::String:
  case DataType::Blackhole: {
    break;
  }
  case DataType::Queue:
  case DataType::Hashes:
  case DataType::Sorted: {
    if (FLAGS_batch_size > 0) {
      std::cerr << R"(Batch is only supported for "hash" type data.)"
                << std::endl;
      return false;
    }
    collections.resize(FLAGS_num_collection);
    for (uint64_t i = 0; i < FLAGS_num_collection; i++) {
      collections[i] = "Collection_" + std::to_string(i);
    }
    break;
  }
  default:
    throw std::runtime_error{"Unsupported data type!"};
  }
  // Check for scan flag
  switch (bench_data_type) {
  case DataType::String:
  case DataType::Queue: {
    if (FLAGS_scan) {
      throw std::runtime_error{
          R"(Scan is not supported for "String" and "Que" type data.)"};
    }
    break;
  }
  case DataType::Hashes:
  case DataType::Sorted:
  case DataType::Blackhole: {
    break;
  }
  default:
    throw std::runtime_error{"Unsupported data type!"};
  }

  if (FLAGS_value_size > 102400) {
    throw std::runtime_error{"value size too large"};
  }

  if (FLAGS_fill || FLAGS_key_distribution == "uniform") {
    assert(FLAGS_read_ratio == 0);
    key_dist = KeyDistribution::Range;
    operations_per_thread = FLAGS_num / FLAGS_max_write_threads + 1;
    for (size_t i = 0; i < FLAGS_max_write_threads; i++) {
      ranges.emplace_back(i * operations_per_thread,
                          (i + 1) * operations_per_thread);
    }
  } else {
    operations_per_thread = FLAGS_num_operations / FLAGS_threads;
    engines.resize(FLAGS_threads);
    if (FLAGS_key_distribution == "random") {
      key_dist = KeyDistribution::Random;
    } else if (FLAGS_key_distribution == "zipf") {
      key_dist = KeyDistribution::Zipf;
    } else {
      throw std::runtime_error{"key distribution " + FLAGS_key_distribution +
                               " is not supported"};
    }
  }

  if (FLAGS_value_size_distribution == "constant") {
    vsz_dist = ValueSizeDistribution::Constant;
  } else if (FLAGS_value_size_distribution == "random") {
    vsz_dist = ValueSizeDistribution::Random;
  } else {
    throw std::runtime_error{"value size distribution " +
                             FLAGS_value_size_distribution +
                             " is not supported"};
  }

  return true;
}

int main(int argc, char **argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (!ProcessBenchmarkConfigs()) {
    throw std::runtime_error{"Fail To Process Benchmark config parameters"};
  }

  Configs configs;
  configs.populate_pmem_space = FLAGS_populate;
  configs.max_write_threads = FLAGS_max_write_threads;
  configs.pmem_file_size = FLAGS_space;
  configs.opt_large_sorted_collection_restore =
      FLAGS_opt_large_sorted_collection_restore;
  configs.use_devdax_mode = FLAGS_use_devdax_mode;

  Status s = Engine::Open(FLAGS_path, &engine, configs, stdout);

  if (s != Status::Ok) {
    printf("open KVDK instance %s error\n", FLAGS_path.c_str());
    std::abort();
  }

  value_pool.clear();
  value_pool.reserve(1ULL << 20);
  std::default_random_engine rand_engine{42};
  for (size_t i = 0; i < (1ULL << 20); i++) {
    value_pool.push_back('a' + rand_engine() % 26);
  }

  int write_threads =
      FLAGS_fill ? FLAGS_threads
                 : FLAGS_threads - FLAGS_read_ratio * 100 * FLAGS_threads / 100;
  int read_threads = FLAGS_threads - write_threads;
  std::vector<std::thread> ts;

  if (FLAGS_latency) {
    printf("calculate latencies\n");
    read_latencies.resize(read_threads, std::vector<uint64_t>(MAX_LAT, 0));
    write_latencies.resize(write_threads, std::vector<uint64_t>(MAX_LAT, 0));
  }

  if (bench_data_type == DataType::Sorted) {
    printf("Create %ld Sorted Collections\n", FLAGS_num_collection);
    for (auto col : collections) {
      Collection *collection_ptr;
      s = engine->CreateSortedCollection(col, &collection_ptr);
      if (s != Status::Ok) {
        throw std::runtime_error{"Fail to create Sorted collection"};
      }
    }
    engine->ReleaseWriteThread();
  }

  has_finished.resize(FLAGS_threads, 0);

  printf("init %d write threads\n", write_threads);
  for (int i = 0; i < write_threads; i++) {
    ts.emplace_back(DBWrite, i);
  }

  printf("init %d read threads\n", read_threads);
  for (int i = write_threads; i < FLAGS_threads; i++) {
    ts.emplace_back(FLAGS_scan ? DBScan : DBRead, i);
  }

  size_t last_read_ops = 0;
  size_t last_read_notfound = 0;
  size_t last_write_ops = 0;
  size_t run_time = 0;
  auto start_ts = std::chrono::system_clock::now();
  printf("------- ops in seconds -----------\n");
  printf("time (ms),   read ops,   not found,  write ops,  total read,  total "
         "write\n");
  size_t total_read = 0;
  size_t total_write = 0;
  size_t total_not_found = 0;

  // To ignore warm up time and thread join time
  // Ignore first 2 seconds and last few seconds when a thread has joined.
  size_t total_read_head = 0;
  size_t total_read_tail = 0;
  size_t total_write_head = 0;
  size_t total_write_tail = 0;
  size_t effective_runtime = 0;
  bool stop_counting = false;
  while (true) {
    if (!FLAGS_fill && run_time >= FLAGS_time) {
      // Read, scan, update and insert
      has_timed_out = true;
    } else {
      // Fill will never timeout
      has_timed_out = false;
    }
    std::this_thread::sleep_for(std::chrono::seconds{1});

    // for latency, the last second may not accurate
    run_time++;
    total_read = read_ops.load();
    total_write = write_ops.load();
    total_not_found = read_not_found.load();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now() - start_ts);
    printf("%-10lu  %-10lu  %-10lu  %-10lu  %-11lu  %-10lu\n", duration.count(),
           total_read - last_read_ops, read_not_found - last_read_notfound,
           total_write - last_write_ops, total_read, total_write);
    fflush(stdout);

    last_read_ops = total_read;
    last_write_ops = total_write;
    last_read_notfound = total_not_found;

    int num_finished =
        std::accumulate(has_finished.begin(), has_finished.end(), 0);

    if (run_time == 2) {
      total_read_head = total_read;
      total_write_head = total_write;
    }
    if (num_finished > 0 && !stop_counting) {
      total_read_tail = total_read;
      total_write_tail = total_write;
      effective_runtime = run_time - 2;
      stop_counting = true;
    }
    if (num_finished == FLAGS_threads) {
      break;
    }
  }

  printf("finish bench\n");

  for (auto &t : ts)
    t.join();

  uint64_t read_thpt = (total_read_tail - total_read_head) / effective_runtime;
  uint64_t write_thpt =
      (total_write_tail - total_write_head) / effective_runtime;

  printf(" ------------ statistics ------------\n");
  printf("read ops %lu, write ops %lu\n", read_thpt, write_thpt);

  if (FLAGS_latency) {
    auto ro = read_ops.load();
    if (ro > 0 && read_latencies.size() > 0) {
      double total = 0;
      double avg = 0;
      double cur = 0;
      double l50 = 0;
      double l99 = 0;
      double l995 = 0;
      double l999 = 0;
      double l9999 = 0;
      for (uint64_t i = 1; i <= MAX_LAT; i++) {
        for (auto j = 0; j < read_threads; j++) {
          cur += read_latencies[j][i];
          total += read_latencies[j][i] * i;
          if (l50 == 0 && (double)cur / ro > 0.5) {
            l50 = (double)i / 10;
          } else if (l99 == 0 && (double)cur / ro > 0.99) {
            l99 = (double)i / 10;
          } else if (l995 == 0 && (double)cur / ro > 0.995) {
            l995 = (double)i / 10;
          } else if (l999 == 0 && (double)cur / ro > 0.999) {
            l999 = (double)i / 10;
          } else if (l9999 == 0 && (double)cur / ro > 0.9999) {
            l9999 = (double)i / 10;
          }
        }
      }
      avg = total / ro / 10;

      printf("read lantencies (us): Avg: %.2f, P50: %.2f, P99: %.2f, P99.5: "
             "%.2f, "
             "P99.9: %.2f, P99.99: %.2f\n",
             avg, l50, l99, l995, l999, l9999);
    }

    auto wo = write_ops.load();
    if (wo > 0 && write_latencies.size() > 0) {
      double total = 0;
      double avg = 0;
      double cur = 0;
      double l50 = 0;
      double l99 = 0;
      double l995 = 0;
      double l999 = 0;
      double l9999 = 0;
      for (uint64_t i = 1; i <= MAX_LAT; i++) {
        for (auto j = 0; j < write_threads; j++) {
          cur += write_latencies[j][i];
          total += write_latencies[j][i] * i;
          if (l50 == 0 && (double)cur / wo > 0.5) {
            l50 = (double)i / 10;
          } else if (l99 == 0 && (double)cur / wo > 0.99) {
            l99 = (double)i / 10;
          } else if (l995 == 0 && (double)cur / wo > 0.995) {
            l995 = (double)i / 10;
          } else if (l999 == 0 && (double)cur / wo > 0.999) {
            l999 = (double)i / 10;
          } else if (l9999 == 0 && (double)cur / wo > 0.9999) {
            l9999 = (double)i / 10;
          }
        }
      }
      avg = total / wo / 10;

      printf("write lantencies (us): Avg: %.2f, P50: %.2f, P99: %.2f, P99.5: "
             "%.2f, "
             "P99.9: %.2f, P99.99: %.2f\n",
             avg, l50, l99, l995, l999, l9999);
    }
  }

  delete engine;

  return 0;
}
