#include <gflags/gflags.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>

#include "generator.hpp"
#include "kvdk/volatile/engine.hpp"
#include "kvdk/volatile/types.hpp"

using namespace google;
using namespace KVDK_NAMESPACE;

#define MAX_LAT (10000000)

// Benchmark configs
DEFINE_string(path, "/mnt/pmem0/kvdk", "Instance path");

DEFINE_uint64(num_kv, (1 << 30), "Number of KVs to place");

DEFINE_uint64(num_operations, (1 << 30),
              "Number of total operations. Asserted to be equal to num_kv if "
              "(fill == true).");

DEFINE_bool(fill, false, "Fill num_kv uniform kv pairs to a new instance");

DEFINE_int64(timeout, 30,
             "Time to benchmark, this is valid only if fill=false");

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
              "Storage engine to benchmark, can be string, sorted, hash, list "
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

DEFINE_uint64(max_access_threads, 64, "Max access threads of the instance");

DEFINE_uint64(space, (256ULL << 30), "Max usable PMem space of the instance");

DEFINE_bool(opt_large_sorted_collection_restore, true,
            " Optional optimization strategy which Multi-thread recovery a "
            "skiplist. When having few large skiplists, the optimization can "
            "get better performance");

DEFINE_bool(use_devdax_mode, false, "Use devdax device for kvdk");

class Timer {
 public:
  void Start() { clock_gettime(CLOCK_REALTIME, &start); }

  std::uint64_t End() {
    struct timespec end;
    clock_gettime(CLOCK_REALTIME, &end);
    return (end.tv_sec - start.tv_sec) * 1000000000 +
           (end.tv_nsec - start.tv_nsec);
  }

 private:
  struct timespec start;
};

std::atomic_uint64_t read_ops{0};
std::atomic_uint64_t write_ops{0};
std::atomic_uint64_t read_not_found{0};
std::atomic_uint64_t read_cnt{UINT64_MAX};
std::vector<std::string> collections;
Engine* engine{nullptr};
std::string value_pool;
size_t operations_per_thread;
bool has_timed_out;
std::vector<int> has_finished;  // std::vector<bool> is a trap!
// Record operation latencies of access threads. Latencies of write threads
// stored in first part of the vector, latencies of read threads stored in
// second part
std::vector<std::vector<std::uint64_t>> latencies;

std::vector<PaddedEngine> random_engines;
std::vector<PaddedRangeIterators> ranges;

enum class DataType { String, Sorted, Hashes, List, Blackhole } bench_data_type;

enum class KeyDistribution { Range, Uniform, Zipf } key_dist;

enum class ValueSizeDistribution { Constant, Uniform } vsz_dist;

std::uint64_t generate_key(size_t tid) {
  static std::uint64_t max_key = FLAGS_existing_keys_ratio == 0
                                     ? UINT64_MAX
                                     : FLAGS_num_kv / FLAGS_existing_keys_ratio;
  static extd::zipfian_distribution<std::uint64_t> zipf{max_key, 0.99};
  static std::uniform_int_distribution<std::uint64_t> uniform{0, max_key};
  switch (key_dist) {
    case KeyDistribution::Range: {
      return ranges[tid].gen();
    }
    case KeyDistribution::Uniform: {
      return uniform(random_engines[tid].gen);
    }
    case KeyDistribution::Zipf: {
      return zipf(random_engines[tid].gen);
    }
    default: {
      throw;
    }
  }
}

size_t generate_value_size(size_t tid) {
  switch (vsz_dist) {
    case ValueSizeDistribution::Constant: {
      return FLAGS_value_size;
    }
    case ValueSizeDistribution::Uniform: {
      return random_engines[tid].gen() % FLAGS_value_size + 1;
    }
    default: {
      throw;
    }
  }
}

void DBWrite(int tid) {
  std::string key(8, ' ');
  std::unique_ptr<WriteBatch> batch;
  if (engine != nullptr) {
    batch = engine->WriteBatchCreate();
  }

  for (size_t operations = 0; operations < operations_per_thread;
       ++operations) {
    if (has_timed_out) {
      break;
    }

    // generate key
    std::uint64_t num = generate_key(tid);
    std::uint64_t cid = num % FLAGS_num_collection;
    memcpy(&key[0], &num, 8);
    StringView value = StringView(value_pool.data(), generate_value_size(tid));

    Timer timer;
    if (FLAGS_latency) timer.Start();

    Status s;
    switch (bench_data_type) {
      case DataType::String: {
        if (FLAGS_batch_size == 0) {
          s = engine->Put(key, value, WriteOptions());
        } else {
          batch->StringPut(key, std::string{value.data(), value.size()});
          if (operations % FLAGS_batch_size == 0) {
            s = engine->BatchWrite(batch);
            batch->Clear();
          }
        }
        break;
      }
      case DataType::Sorted: {
        if (FLAGS_batch_size == 0) {
          s = engine->SortedPut(collections[cid], key, value);
        } else {
          batch->SortedPut(collections[cid], key,
                           std::string{value.data(), value.size()});
          if (operations % FLAGS_batch_size == 0) {
            s = engine->BatchWrite(batch);
            batch->Clear();
          }
        }
        break;
      }
      case DataType::Hashes: {
        if (FLAGS_batch_size == 0) {
          s = engine->HashPut(collections[cid], key, value);
        } else {
          batch->HashPut(collections[cid], key,
                         std::string{value.data(), value.size()});
          if (operations % FLAGS_batch_size == 0) {
            s = engine->BatchWrite(batch);
            batch->Clear();
          }
        }
        break;
      }
      case DataType::List: {
        s = engine->ListPushFront(collections[cid], value);
        break;
      }
      case DataType::Blackhole: {
        s = Status::Ok;
        break;
      }
      default: {
        throw std::runtime_error{"Unsupported data type!"};
      }
    }

    if (FLAGS_latency) {
      std::uint64_t lat = timer.End();
      if (lat / 100 >= MAX_LAT) {
        throw std::runtime_error{"Write latency overflow"};
      }
      latencies[tid][lat / 100]++;
    }

    if (s != Status::Ok) {
      throw std::runtime_error{"Put error"};
    }

    if ((operations + 1) % 1000 == 0) {
      write_ops.fetch_add(1000);
    };
  }
  has_finished[tid] = 1;
  return;
}

void DBScan(int tid) {
  std::string key(8, ' ');
  std::string value_sink;

  for (size_t operations = 0, operations_counted = 0;
       operations < operations_per_thread;) {
    if (has_timed_out) {
      break;
    }

    std::uint64_t num = generate_key(tid);
    std::uint64_t cid = num % FLAGS_num_collection;
    memcpy(&key[0], &num, 8);

    switch (bench_data_type) {
      case DataType::Sorted: {
        size_t const scan_length = 100;
        auto iter = engine->SortedIteratorCreate(collections[cid]);
        if (iter) {
          iter->Seek(key);
          for (size_t i = 0; (i < scan_length) && (iter->Valid());
               i++, iter->Next()) {
            key = iter->Key();
            value_sink = iter->Value();
            ++operations;
            if (operations > operations_counted + 1000) {
              read_ops.fetch_add(operations - operations_counted);
              operations_counted = operations;
            }
          }
        } else {
          throw std::runtime_error{"Error creating SortedIterator"};
        }
        engine->SortedIteratorRelease(iter);
        break;
      }
      case DataType::Hashes: {
        auto iter = engine->HashIteratorCreate(collections[cid]);
        if (iter) {
          for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            key = iter->Key();
            value_sink = iter->Value();
            ++operations;
            if (operations > operations_counted + 1000) {
              read_ops += (operations - operations_counted);
              operations_counted = operations;
            }
          }
        } else {
          throw std::runtime_error{"Error creating HashIterator"};
        }
        engine->HashIteratorRelease(iter);
        break;
      }
      case DataType::Blackhole: {
        operations += 1024;
        read_ops.fetch_add(1024);
        break;
      }
      case DataType::String:
      case DataType::List:
      default: {
        throw std::runtime_error{"Unsupported data type!"};
      }
    }
  }

  has_finished[tid] = 1;
  return;
}

void DBRead(int tid) {
  std::string key(8, ' ');
  std::string value_sink;

  std::uint64_t not_found = 0;
  for (size_t operations = 0; operations < operations_per_thread;
       ++operations) {
    if (has_timed_out) {
      break;
    }

    std::uint64_t num = generate_key(tid);
    std::uint64_t cid = num % FLAGS_num_collection;
    memcpy(&key[0], &num, 8);

    Timer timer;
    if (FLAGS_latency) timer.Start();

    Status s;
    switch (bench_data_type) {
      case DataType::String: {
        s = engine->Get(key, &value_sink);
        break;
      }
      case DataType::Sorted: {
        s = engine->SortedGet(collections[cid], key, &value_sink);
        break;
      }
      case DataType::Hashes: {
        s = engine->HashGet(collections[cid], key, &value_sink);
        break;
      }
      case DataType::List: {
        s = engine->ListPopBack(collections[cid], &value_sink);
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
      auto lat = timer.End();
      if (lat / 100 >= MAX_LAT) {
        fprintf(stderr, "Read latency overflow: %ld us\n", lat / 100);
        std::abort();
      }
      latencies[tid][lat / 100]++;
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

void ProcessBenchmarkConfigs() {
  if (FLAGS_type == "sorted") {
    bench_data_type = DataType::Sorted;
  } else if (FLAGS_type == "string") {
    bench_data_type = DataType::String;
  } else if (FLAGS_type == "hash") {
    bench_data_type = DataType::Hashes;
  } else if (FLAGS_type == "list") {
    bench_data_type = DataType::List;
  } else if (FLAGS_type == "blackhole") {
    bench_data_type = DataType::Blackhole;
  } else {
    throw std::invalid_argument{"Unsupported data type"};
  }
  // Initialize collections and batch parameters
  switch (bench_data_type) {
    case DataType::String:
    case DataType::Blackhole: {
      break;
    }
    case DataType::Hashes:
    case DataType::List:
    case DataType::Sorted: {
      collections.resize(FLAGS_num_collection);
      for (size_t i = 0; i < FLAGS_num_collection; i++) {
        collections[i] = "Collection_" + std::to_string(i);
      }
      break;
    }
  }

  if (FLAGS_batch_size > 0 && (bench_data_type == DataType::List)) {
    throw std::invalid_argument{R"(List does not support batch write.)"};
  }

  // Check for scan flag
  switch (bench_data_type) {
    case DataType::String:
    case DataType::List: {
      if (FLAGS_scan) {
        throw std::invalid_argument{
            R"(Scan is not supported for "String" and "List" type data.)"};
      }
    }
    default: {
      break;
    }
  }

  if (FLAGS_value_size > 102400) {
    throw std::invalid_argument{"value size too large"};
  }

  random_engines.resize(FLAGS_threads);
  if (FLAGS_fill) {
    assert(FLAGS_read_ratio == 0);
    key_dist = KeyDistribution::Range;
    operations_per_thread = FLAGS_num_kv / FLAGS_threads + 1;
    for (size_t i = 0; i < FLAGS_threads; i++) {
      ranges.emplace_back(i * operations_per_thread,
                          (i + 1) * operations_per_thread);
    }
  } else {
    operations_per_thread = FLAGS_num_operations / FLAGS_threads;
    if (FLAGS_key_distribution == "random") {
      key_dist = KeyDistribution::Uniform;
    } else if (FLAGS_key_distribution == "zipf") {
      key_dist = KeyDistribution::Zipf;
    } else {
      throw std::invalid_argument{"Invalid key distribution"};
    }
  }

  if (FLAGS_value_size_distribution == "constant") {
    vsz_dist = ValueSizeDistribution::Constant;
  } else if (FLAGS_value_size_distribution == "random") {
    vsz_dist = ValueSizeDistribution::Uniform;
  } else {
    throw std::runtime_error{"Invalid value size distribution"};
  }
}

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);
  ProcessBenchmarkConfigs();

  if (bench_data_type != DataType::Blackhole) {
    Configs configs;
    configs.max_access_threads = FLAGS_max_access_threads;
    configs.opt_large_sorted_collection_recovery =
        FLAGS_opt_large_sorted_collection_restore;
    Status s = Engine::Open(FLAGS_path, &engine, configs, stdout);
    if (s != Status::Ok) {
      throw std::runtime_error{
          std::string{"Fail to open KVDK instance. Status: "} +
          KVDKStatusStrings[static_cast<int>(s)]};
    }
  }

  {
    value_pool.clear();
    value_pool.reserve(FLAGS_value_size);
    std::default_random_engine rand_engine{42};
    for (size_t i = 0; i < FLAGS_value_size; i++) {
      value_pool.push_back('a' + rand_engine() % 26);
    }
  }

  size_t write_threads =
      FLAGS_fill ? FLAGS_threads
                 : FLAGS_threads - FLAGS_read_ratio * 100 * FLAGS_threads / 100;
  int read_threads = FLAGS_threads - write_threads;
  std::vector<std::thread> ts;

  if (FLAGS_latency) {
    printf("calculate latencies\n");
    latencies.resize(FLAGS_threads, std::vector<std::uint64_t>(MAX_LAT, 0));
  }

  switch (bench_data_type) {
    case DataType::Sorted: {
      printf("Create %ld Sorted Collections\n", FLAGS_num_collection);
      for (auto col : collections) {
        SortedCollectionConfigs s_configs;
        Status s = engine->SortedCreate(col, s_configs);
        if (s != Status::Ok && s != Status::Existed) {
          throw std::runtime_error{"Fail to create Sorted collection"};
        }
      }
      break;
    }
    case DataType::Hashes: {
      for (auto col : collections) {
        Status s = engine->HashCreate(col);
        if (s != Status::Ok && s != Status::Existed) {
          throw std::runtime_error{"Fail to create Hashset"};
        }
      }
      break;
    }
    case DataType::List: {
      for (auto col : collections) {
        Status s = engine->ListCreate(col);
        if (s != Status::Ok && s != Status::Existed) {
          throw std::runtime_error{"Fail to create List"};
        }
      }
      break;
    }
    default: {
      break;
    }
  }

  has_finished.resize(FLAGS_threads, 0);

  std::cout << "Init " << read_threads << " readers "
            << "and " << write_threads << " writers." << std::endl;

  for (size_t i = 0; i < write_threads; i++) {
    ts.emplace_back(DBWrite, i);
  }
  for (size_t i = write_threads; i < FLAGS_threads; i++) {
    ts.emplace_back(FLAGS_scan ? DBScan : DBRead, i);
  }

  size_t const field_width = 15;
  std::cout << "----------------------------------------------------------\n"
            << std::setw(field_width) << "Time(ms)" << std::setw(field_width)
            << "Read Ops" << std::setw(field_width) << "Write Ops"
            << std::setw(field_width) << "Not Found" << std::setw(field_width)
            << "Total Read" << std::setw(field_width) << "Total Write"
            << std::endl;

  std::vector<size_t> read_cnt{0};
  std::vector<size_t> write_cnt{0};
  std::vector<size_t> notfound_cnt{0};
  size_t last_effective_idx = read_cnt.size();
  auto start_ts = std::chrono::system_clock::now();
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds{1});
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now() - start_ts);

    read_cnt.push_back(read_ops.load());
    write_cnt.push_back(write_ops.load());
    notfound_cnt.push_back(read_not_found.load());

    size_t idx = read_cnt.size() - 1;
    std::cout << std::setw(field_width) << duration.count()
              << std::setw(field_width) << read_cnt[idx] - read_cnt[idx - 1]
              << std::setw(field_width) << write_cnt[idx] - write_cnt[idx - 1]
              << std::setw(field_width)
              << notfound_cnt[idx] - notfound_cnt[idx - 1]
              << std::setw(field_width) << read_cnt[idx]
              << std::setw(field_width) << write_cnt[idx] << std::endl;

    size_t num_finished =
        std::accumulate(has_finished.begin(), has_finished.end(), 0UL);

    if (num_finished == 0 || idx < 2) {
      last_effective_idx = idx;
    }
    if (num_finished == FLAGS_threads) {
      break;
    }
    if (!FLAGS_fill && (duration.count() >= FLAGS_timeout * 1000)) {
      // Signal a timeout for read, scan, update and insert
      // Fill will never timeout
      has_timed_out = true;
      break;
    }
  }

  std::cout << "Benchmark finished." << std::endl;
  printf("finish bench\n");

  for (size_t i = 0; i < ts.size(); i++) {
    ts[i].join();
  }

  size_t time_elapsed;
  size_t total_effective_read;
  size_t total_effective_write;
  size_t const warmup_time = 2;
  if (last_effective_idx <= warmup_time) {
    time_elapsed = last_effective_idx;
    total_effective_read = read_cnt[last_effective_idx];
    total_effective_write = write_cnt[last_effective_idx];
  } else {
    time_elapsed = last_effective_idx - warmup_time;
    total_effective_read = read_cnt[last_effective_idx] - read_cnt[warmup_time];
    total_effective_write =
        write_cnt[last_effective_idx] - write_cnt[warmup_time];
  }

  std::cout << "----------------------------------------------------------\n"
            << "Average Read Ops:\t" << total_effective_read / time_elapsed
            << ". "
            << "Average Write Ops:\t" << total_effective_write / time_elapsed
            << std::endl;

  if (FLAGS_latency) {
    auto ro = read_ops.load();
    if (ro > 0 && read_threads > 0) {
      double total = 0;
      double avg = 0;
      double cur = 0;
      double l50 = 0;
      double l99 = 0;
      double l995 = 0;
      double l999 = 0;
      double l9999 = 0;
      for (std::uint64_t i = 1; i <= MAX_LAT; i++) {
        for (auto j = 0; j < read_threads; j++) {
          cur += latencies[write_threads + j][i];
          total += latencies[write_threads + j][i] * i;
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

      printf(
          "read lantencies (us): Avg: %.2f, P50: %.2f, P99: %.2f, P99.5: "
          "%.2f, "
          "P99.9: %.2f, P99.99: %.2f\n",
          avg, l50, l99, l995, l999, l9999);
    }

    auto wo = write_ops.load();
    if (wo > 0 && write_threads > 0) {
      double total = 0;
      double avg = 0;
      double cur = 0;
      double l50 = 0;
      double l99 = 0;
      double l995 = 0;
      double l999 = 0;
      double l9999 = 0;
      for (std::uint64_t i = 1; i <= MAX_LAT; i++) {
        for (size_t j = 0; j < write_threads; j++) {
          cur += latencies[j][i];
          total += latencies[j][i] * i;
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

      printf(
          "write lantencies (us): Avg: %.2f, P50: %.2f, P99: %.2f, P99.5: "
          "%.2f, "
          "P99.9: %.2f, P99.99: %.2f\n",
          avg, l50, l99, l995, l999, l9999);
    }
  }

  if (bench_data_type != DataType::Blackhole) delete engine;

  return 0;
}
