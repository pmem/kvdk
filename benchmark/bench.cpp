#include "algorithm"
#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include "sys/time.h"
#include <atomic>
#include <cassert>
#include <gflags/gflags.h>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>

using namespace google;
using namespace KVDK_NAMESPACE;

#define MAX_LAT (10000000)

// Benchmark configs
DEFINE_string(path, "/mnt/pmem0/kvdk", "Instance path");

DEFINE_uint64(num, 1 << 30, "Number of KVs to place");

DEFINE_bool(fill, false, "Fill num uniform kv pairs to a new instance");

DEFINE_uint64(time, 600, "Time to benchmark, this is valid only if fill=false");

DEFINE_uint64(value_size, 120, "Value size of KV");

DEFINE_uint64(threads, 10, "Number of concurrent threads to run benchmark");

DEFINE_double(read_ratio, 0, "Read threads = threads * read_ratio");

DEFINE_double(
    existing_keys_ratio, 1,
    "Ratio of keys to read / write that existed in the filled instance, for "
    "example, if set to "
    "1, all writes will be updates, and all read keys will be existed");

DEFINE_bool(latency, false, "Stat operation latencies");

DEFINE_string(type, "string",
              "Storage engine to benchmark, can be string or sorted");

DEFINE_bool(scan, false,
            "If set true, read threads will do scan operations, this is valid "
            "only if we benchmark sorted engine");

DEFINE_uint64(collections, 1,
              "Number of collections in the instance to benchmark");

DEFINE_uint64(
    batch, 0,
    "Size of write batch. If batch>0, write string type kv with atomic batch "
    "write, this is valid only if we benchmark string engine");

// Engine configs
DEFINE_bool(
    populate, false,
    "Populate pmem space while creating a new instance. This can improve write "
    "performance in runtime, but will take long time to init the instance");

DEFINE_int32(max_write_threads, 32, "Max write threads of the instance");

DEFINE_uint64(space, (uint64_t)256 << 30,
              "Max usable PMem space of the instance");

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

class UniformGenerator {
public:
  UniformGenerator(uint64_t max_num, int scale = 64)
      : base_(max_num / scale), scale_(scale), gen_cnt_(0) {
    for (uint64_t i = 0; i < base_.size(); i++) {
      base_[i] = i + 1;
    }
    std::shuffle(base_.begin(), base_.end(), std::mt19937_64());
  }

  uint64_t Next() {
    auto next = gen_cnt_.fetch_add(1, std::memory_order_relaxed);
    auto index = next % base_.size();
    auto cur_scale = (next / base_.size()) % scale_;
    return base_[index] + base_.size() * cur_scale;
  }

private:
  std::vector<uint64_t> base_;
  uint64_t scale_;
  std::atomic<uint64_t> gen_cnt_;
};

bool done{false};
std::atomic<uint64_t> read_ops{0};
std::atomic<uint64_t> write_ops{0};
std::atomic<uint64_t> read_not_found{0};
std::atomic<uint64_t> read_cnt{UINT64_MAX};
std::vector<std::vector<uint64_t>> read_latencies;
std::vector<std::vector<uint64_t>> write_latencies;
std::unique_ptr<UniformGenerator> generator{nullptr};
std::vector<std::string> collections;
Engine *engine;
char *value_pool = nullptr;
uint64_t num_keys = 0;
uint64_t value_size = 0;

int batch_num;
bool fill;
bool stat_latencies;
double existing_keys_ratio;
bool sorted;
uint64_t num_collections;

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

inline uint64_t fast_random() {
  static std::mt19937_64 generator;
  thread_local uint64_t seed = 0;
  if (seed == 0) {
    seed = generator();
  }
  uint64_t x = seed; /* The state must be seeded with a nonzero value. */
  x ^= x >> 12;      // a
  x ^= x << 25;      // b
  x ^= x >> 27;      // c
  seed = x;
  return x * 0x2545F4914F6CDD1D;
}

uint64_t generate_key(double hit_ratio = 1) {
  if (fill) {
    return generator->Next();
  }

  if (hit_ratio == 0) {
    return fast_random();
  } else {
    return fast_random() % (uint64_t)(num_keys / hit_ratio);
  }
}

void DBWrite(int id) {
  std::string k;
  std::string v;
  k.resize(8);
  v.assign(value_pool + (rand() % 102400 - value_size), value_size);
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
    num = generate_key(existing_keys_ratio);

    memcpy(&k[0], &num, 8);
    std::vector<std::string> sv;
    if (stat_latencies)
      timer.Start();
    if (!sorted) {
      if (batch_num == 0) {
        s = engine->Set(k, v);
      } else {
        batch.Put(k, v);
        if (batch.Size() == batch_num) {
          engine->BatchWrite(batch);
          batch.Clear();
        }
      }
    } else {
      s = engine->SSet(collections[num % num_collections], k, v);
    }

    if (stat_latencies) {
      lat = timer.End();
      if (lat / 100 >= MAX_LAT) {
        fprintf(stderr, "Write latency overflow: %ld us\n", lat / 100);
        exit(-1);
      }
      write_latencies[id][lat / 100]++;
    }

    if (s != Status::Ok) {
      fprintf(stderr, "Set error\n");
      exit(-1);
    }

    if (++ops % 1000 == 0) {
      write_ops += 1000;
    };
  }
}

void DBScan(int id) {
  uint64_t ops = 0;
  uint64_t num;
  std::string k;
  std::string v;
  k.resize(8);
  int scan_length = 100;
  while (true) {
    if (done) {
      return;
    }

    num = generate_key(existing_keys_ratio);
    memcpy(&k[0], &num, 8);
    auto iter = engine->NewSortedIterator(collections[num % num_collections]);
    if (iter) {
      iter->Seek(k);
      int cnt = scan_length;
      while (cnt > 0 && iter->Valid()) {
        cnt--;
        ++ops;
        k = iter->Key();
        v = iter->Value();
        iter->Next();
      }
    } else {
      fprintf(stderr, "Seek error\n");
      exit(-1);
    }

    if (ops % 100 == 0) {
      read_ops += 100;
    }
  }
}

void DBRead(int id) {
  std::string value;
  std::string k;
  k.resize(8);
  uint64_t num;
  uint64_t ops = 0;
  uint64_t not_found = 0;
  Timer timer;
  uint64_t lat = 0;
  bool sorted = FLAGS_type == "sorted";

  while (true) {
    if (done) {
      return;
    }
    num = generate_key(existing_keys_ratio);
    memcpy(&k[0], &num, 8);
    if (stat_latencies)
      timer.Start();
    Status s;
    if (sorted) {
      s = engine->SGet(collections[num % num_collections], k, &value);
    } else {
      s = engine->Get(k, &value);
    }
    if (stat_latencies) {
      lat = timer.End();
      if (lat / 100 >= MAX_LAT) {
        fprintf(stderr, "Read latency overflow: %ld us\n", lat / 100);
        exit(-1);
      }
      read_latencies[id][lat / 100]++;
    }

    if (s != Status::Ok) {
      if (s != Status::NotFound) {
        fprintf(stderr, "get error\n");
        exit(-1);
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
    sorted = true;
    if (FLAGS_batch > 0) {
      printf("Batch is not supported for \"sorted\" type data\n");
      return false;
    }
    collections.resize(FLAGS_collections);
    for (uint64_t i = 0; i < FLAGS_collections; i++) {
      collections[i] = "skiplist" + std::to_string(i);
    }
  } else if (FLAGS_type == "string") {
    sorted = false;
    batch_num = FLAGS_batch;
    if (FLAGS_scan) {
      printf("scan is not supported for \"string\" type data\n");
      return false;
    }
  } else {
    printf("Only support \"string\" or \"sorted\" type data");
    return false;
  }

  fill = FLAGS_fill;
  stat_latencies = FLAGS_latency;
  existing_keys_ratio = FLAGS_existing_keys_ratio;
  value_size = FLAGS_value_size;
  num_keys = FLAGS_num;
  num_collections = FLAGS_collections;

  if (fill) {
    printf("to fill %lu uniform keys\n", num_keys);
    generator.reset(new UniformGenerator(num_keys));
  }

  return true;
}

int main(int argc, char **argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (!ProcessBenchmarkConfigs()) {
    exit(1);
  }

  Configs configs;
  configs.populate_pmem_space = FLAGS_populate;
  configs.max_write_threads = FLAGS_max_write_threads;
  configs.pmem_file_size = FLAGS_space;

  Status s = Engine::Open(FLAGS_path, &engine, configs, stdout);

  if (s != Status::Ok) {
    printf("open KVDK instance %s error\n", FLAGS_path.c_str());
    exit(1);
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
  while (!done) {
    sleep(1);
    if (!done) { // for latency, the last second may not accurate
      run_time++;
      uint64_t total_read = read_ops.load();
      uint64_t total_write = write_ops.load();
      uint64_t total_not_found = read_not_found.load();
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

      if (FLAGS_fill) {
        if (total_write >= num_keys)
          done = true;
      } else if (run_time >= FLAGS_time) {
        done = true;
      }
    }
  }

  printf("finish bench\n");
  done = true;

  for (auto &t : ts)
    t.join();

  uint64_t read_thpt = read_ops.load() / run_time;
  uint64_t write_thpt = write_ops.load() / run_time;

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
