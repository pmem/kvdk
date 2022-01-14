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

DEFINE_bool(use_experimental_hashmap, false, "Enable experimental hashmap. Only string data type is supported");

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

bool done{false};
std::atomic<uint64_t> read_ops{0};
std::atomic<uint64_t> write_ops{0};
std::atomic<uint64_t> read_not_found{0};
std::atomic<uint64_t> read_cnt{UINT64_MAX};
std::vector<std::vector<uint64_t>> read_latencies;
std::vector<std::vector<uint64_t>> write_latencies;
std::vector<std::string> collections;
Engine *engine;
char *value_pool = nullptr;

enum class DataType {
  String,
  Sorted,
  Hashes,
  Queue,
  Blackhole
} bench_data_type;

std::shared_ptr<Generator> key_generator;
std::shared_ptr<Generator> value_size_generator;

char *random_str(unsigned int size) {
  char *str = (char *)malloc(size + 1);
  for (unsigned int i = 0; i < size; i++) {
    switch (rand() % 3) {
    case 0:
      str[i] = rand() % 10 + '0';
      break;
    case 1:
      str[i] = rand() % 26 + 'A';
      break;
    case 2:
      str[i] = rand() % 26 + 'a';
      break;
    default:
      break;
    }
  }
  str[size] = 0;

  return str;
}

uint64_t generate_key() { return key_generator->Next(); }

void DBWrite(int tid) {
  std::string key;
  key.resize(8);
  uint64_t num;
  uint64_t ops = 0;
  Timer timer;
  uint64_t lat = 0;
  WriteBatch batch;
  Status s;

  while (true) {
    if (done)
      return;

    // generate key
    num = generate_key();
    memcpy(&key[0], &num, 8);

    StringView value = StringView(value_pool, value_size_generator->Next());

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
      else
        s = engine->RPush(collections[num % FLAGS_num_collection], value);
      break;
    }
    case DataType::Blackhole: {
      s = Status::Ok;
      if (ops == 0) {
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
        fprintf(stderr, "Write latency overflow: %ld us\n", lat / 100);
        std::abort();
      }
      write_latencies[tid][lat / 100]++;
    }

    if (s != Status::Ok) {
      fprintf(stderr, "Set error\n");
      std::abort();
    }

    if ((++ops % 1000) == 0) {
      write_ops += 1000;
    };
  }
}

void DBScan(int tid) {
  uint64_t operations = 0;
  uint64_t operations_counted = 0;
  std::string key;
  std::string value;
  key.resize(8);
  int scan_length = 100;
  while (!done) {
    uint64_t num = generate_key();
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
            read_ops += (operations - operations_counted);
            operations_counted = operations;
          }
        }
      } else {
        fprintf(stderr, "Error creating SortedIterator\n");
        std::abort();
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
        fprintf(stderr, "Error creating UnorderedIterator\n");
        std::abort();
      }
      break;
    }
    case DataType::Blackhole: {
      read_ops += 1024;
      break;
    }
    case DataType::String:
    case DataType::Queue:
    default: {
      throw std::runtime_error{"Unsupported data type!"};
    }
    }
  }
}

void DBRead(int tid) {
  std::string value;
  std::string key;
  key.resize(8);
  uint64_t num;
  uint64_t ops = 0;
  uint64_t not_found = 0;
  Timer timer;
  uint64_t lat = 0;

  while (true) {
    if (done) {
      return;
    }
    num = generate_key();
    memcpy(&key[0], &num, 8);
    if (FLAGS_latency)
      timer.Start();
    Status s;
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
      std::string sink;
      if ((num / FLAGS_num_collection) % 2 == 0)
        s = engine->LPop(collections[num % FLAGS_num_collection], &sink);
      else
        s = engine->RPop(collections[num % FLAGS_num_collection], &sink);
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
        fprintf(stderr, "get error\n");
        std::abort();
      } else {
        if (++not_found % 1000 == 0) {
          read_not_found += 1000;
        }
      }
    }

    if (++ops % 1000 == 0) {
      read_ops += 1000;
    }
  }
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
  if (FLAGS_use_experimental_hashmap)
  {
    if (bench_data_type != DataType::String && bench_data_type != DataType::Blackhole)
    {
      throw std::invalid_argument{"Data type must be string or blackhole for experimental hashmap"};
    }
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
      std::cerr
          << R"(Scan is only supported for "hash" and "sorted" type data.)"
          << std::endl;
      return false;
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
    printf("value size too large\n");
    return false;
  }

  uint64_t max_key = FLAGS_existing_keys_ratio == 0
                         ? UINT64_MAX
                         : FLAGS_num / FLAGS_existing_keys_ratio;
  if (FLAGS_fill || FLAGS_key_distribution == "uniform") {
    key_generator.reset(
        new MultiThreadingRangeIterator(FLAGS_threads, 0, FLAGS_num));
  } else if (FLAGS_key_distribution == "random") {
    key_generator.reset(new RandomGenerator(max_key));
  } else if (FLAGS_key_distribution == "zipf") {
    key_generator.reset(new ZipfianGenerator(FLAGS_threads, max_key));
  } else {
    printf("key distribution %s is not supported\n",
           FLAGS_key_distribution.c_str());
    return false;
  }

  if (FLAGS_value_size_distribution == "constant") {
    value_size_generator.reset(new ConstantGenerator(FLAGS_value_size));
  } else if (FLAGS_value_size_distribution == "random") {
    value_size_generator.reset(new RandomGenerator(FLAGS_value_size));
  } else {
    printf("value size distribution %s is not supported\n",
           FLAGS_value_size_distribution.c_str());
    return false;
  }

  return true;
}

int main(int argc, char **argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (!ProcessBenchmarkConfigs()) {
    std::abort();
  }

  Configs configs;
  configs.populate_pmem_space = FLAGS_populate;
  configs.max_write_threads = FLAGS_max_write_threads;
  configs.pmem_file_size = FLAGS_space;
  configs.opt_large_sorted_collection_restore =
      FLAGS_opt_large_sorted_collection_restore;
  configs.use_devdax_mode = FLAGS_use_devdax_mode;
  configs.use_experimental_hashmap = FLAGS_use_experimental_hashmap;

  Status s = Engine::Open(FLAGS_path, &engine, configs, stdout);

  if (s != Status::Ok) {
    printf("open KVDK instance %s error\n", FLAGS_path.c_str());
    std::abort();
  }

  value_pool = random_str(102400);

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
        fprintf(stderr, "Create Sorted collection error\n");
        std::abort();
      }
    }
    engine->ReleaseWriteThread();
  }

  printf("init %d write threads\n", write_threads);
  for (int i = 0; i < write_threads; i++) {
    ts.emplace_back(DBWrite, i);
  }

  printf("init %d read threads\n", read_threads);
  for (int i = 0; i < read_threads; i++) {
    ts.emplace_back(FLAGS_scan ? DBScan : DBRead, i);
  }

  uint64_t last_read_ops = 0;
  uint64_t last_read_notfound = 0;
  uint64_t last_write_ops = 0;
  uint64_t run_time = 0;
  auto start_ts = std::chrono::system_clock::now();
  printf("------- ops in seconds -----------\n");
  printf("time (ms),   read ops,   not found,  write ops,  total read,  total "
         "write\n");
  uint64_t total_read = 0;
  uint64_t total_write = 0;
  uint64_t total_not_found = 0;
  while (!done) {
    sleep(1);
    {
      // for latency, the last second may not accurate
      run_time++;
      total_read = read_ops.load();
      total_write = write_ops.load();
      total_not_found = read_not_found.load();

      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now() - start_ts);
      printf("%-10lu  %-10lu  %-10lu  %-10lu  %-11lu  %-10lu\n",
             duration.count(), total_read - last_read_ops,
             read_not_found - last_read_notfound, total_write - last_write_ops,
             total_read, total_write);
      fflush(stdout);

      last_read_ops = total_read;
      last_write_ops = total_write;
      last_read_notfound = total_not_found;

      if (FLAGS_fill && total_write >= FLAGS_num * 99 / 100) {
        // Fill
        /// TODO: Introduce mechanism to signal that every thread has done
        /// filling.
        done = true;
      } else if (!FLAGS_fill && run_time >= FLAGS_time) {
        // Read, scan, update and insert
        done = true;
      } else {
        done = false;
      }
    }
  }

  printf("finish bench\n");
  if (FLAGS_fill) {
    // Make sure every thread has done its job.
    sleep(30);
  }

  for (auto &t : ts)
    t.join();

  uint64_t read_thpt = total_read / run_time;
  uint64_t write_thpt = total_write / run_time;

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
