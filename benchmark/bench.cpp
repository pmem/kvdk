/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "algorithm"
#include "pmemdb/db.hpp"
#include "pmemdb/namespace.hpp"
#include "safe_lib.h"
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
using namespace PMEMDB_NAMESPACE;

#define DATA_SIZE (10LL * 1024 * 1024 * 1024)

DEFINE_double(thread_num, 10, "total threads");
DEFINE_int64(time, 600, "runtime");
DEFINE_bool(load, false, "load new db");
DEFINE_double(read_ratio, 0.9, "read threads ratio");
DEFINE_int64(max_op, ((uint64_t)1 << 30), "max operations to bench");
DEFINE_bool(latency, false, "stat P99 etc");
DEFINE_int64(value_size, 120, "value size");
DEFINE_string(db_path, "/mnt/pmem0/DB",
              "file path means local db, socket path means remote db");
DEFINE_bool(populate, true, "");
DEFINE_bool(update, false, "");
DEFINE_double(hit_ratio, 1, "");
DEFINE_double(delete_ratio, 0, "delete ratio of writes");
DEFINE_bool(sorted, false, "bench skiplist structure");

class Timer {
public:
  void Start() { clock_gettime(CLOCK_REALTIME, &start); }

  uint64_t End() {
    struct timespec end{0};
    clock_gettime(CLOCK_REALTIME, &end);
    // gettimeofday(&end, nullptr);
    return (end.tv_sec - start.tv_sec) * 1000000000 +
           (end.tv_nsec - start.tv_nsec);
  }

private:
  struct timespec start{0};
};

bool done{false};
std::atomic<uint64_t> read_ops{0};
std::atomic<uint64_t> write_ops{0};
std::atomic<uint64_t> read_not_found{0};
std::atomic<uint64_t> load_cnt{0};
std::atomic<uint64_t> read_cnt{UINT64_MAX};
std::vector<uint32_t> read_latencies;
std::vector<uint32_t> write_latencies;
uint64_t max_op_per_thread = 0;
DB *db;
char *value_pool = nullptr;
uint64_t max_num_keys = 0;
uint64_t value_size = 0;

std::mt19937_64 generator_;
std::uniform_int_distribution<uint64_t> dist_;

char *random_str(unsigned int size) {
  char *str = (char *)malloc(size + 1);
  if (!str)
    exit(1);
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

uint64_t my_random(uint64_t &seed) {
  uint64_t x = seed; /* The state must be seeded with a nonzero value. */
  x ^= x >> 12;      // a
  x ^= x << 25;      // b
  x ^= x >> 27;      // c
  seed = x;
  return x * 0x2545F4914F6CDD1D;
}

uint64_t generate_key(bool uniform = false, bool hit = true) {
  thread_local uint64_t seed = 0;
  if (seed == 0) {
    seed = generator_() + dist_(generator_);
    if (seed < 10000000000)
      seed += 10000000000;
  }
  if (uniform) {
    if (hit)
      return dist_(generator_);
    else
      return generator_();
  } else {
    if (hit) {
      return my_random(seed) % (uint64_t)(max_num_keys / FLAGS_hit_ratio);
    } else {
      return my_random(seed);
    }
  }
}

void DBWrite(int id) {
  char value[102400];
  memcpy_s(value, 102400, value_pool + (rand() % 102400 - value_size),
           value_size);
  std::string k;
  std::string v;
  k.resize(8);
  v.assign(value, value_size);
  uint64_t num;
  uint64_t ops = 0;
  Timer timer;
  uint64_t lat = 0;
  int to_write = 100;
  int to_delete = to_write * FLAGS_delete_ratio;
  Status s;
  while (true) {
    if (done)
      return;
    // generate key
    if (FLAGS_load) {
      num = load_cnt.fetch_add(1, std::memory_order_relaxed);
    } else {
      num = generate_key(false, FLAGS_update);
    }
    memcpy_s(&k[0], 8, &num, 8);

    if (FLAGS_latency)
      timer.Start();
    if (to_delete > 0) {
      to_delete--;
      s = db->Delete(k);
    } else {
      if (FLAGS_sorted) {
        s = db->SSet(k, v);
      } else {
        s = db->Set(k, v);
      }
      if (s != Status::Ok) {
        fprintf(stderr, "Set error\n");
      }
      to_write--;
      if (to_write == 0) {
        to_write = 100;
        to_delete = to_write * FLAGS_delete_ratio;
      }
    }
    if (FLAGS_latency)
      lat = timer.End();
    if (s != Status::Ok) {
      fprintf(stderr, "set error\n");
      exit(-1);
    }
    if (FLAGS_latency && max_op_per_thread > 0) {
      write_latencies[ops + id * max_op_per_thread] = lat;
      if (ops >= max_op_per_thread)
        done = true;
    }
    if (++ops % 1000 == 0) {
      write_ops += 1000;
    };
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

  while (true) {
    if (done) {
      return;
    }
    num = generate_key();
    memcpy_s(&k[0], 8, &num, 8);
    if (FLAGS_latency)
      timer.Start();
    Status s;
    if (FLAGS_sorted) {
      s = db->SGet(k, &value);
    } else {
      s = db->Get(k, &value);
    }
    if (FLAGS_latency)
      lat = timer.End();
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
    if (FLAGS_latency && max_op_per_thread > 0) {
      read_latencies[ops + id * max_op_per_thread] = lat;
      if (ops >= max_op_per_thread) {
        done = true;
      }
    }
    if (++ops % 1000 == 0) {
      read_ops += 1000;
    }
  }
}

int main(int argc, char **argv) {
  ParseCommandLineFlags(&argc, &argv, true);
  value_size = FLAGS_value_size;
  max_num_keys = DATA_SIZE / (8 + value_size);
  dist_.param(std::uniform_int_distribution<uint64_t>::param_type(
      0, (uint64_t)(max_num_keys / FLAGS_hit_ratio)));
  max_op_per_thread = FLAGS_max_op / FLAGS_thread_num;
  auto start_res = std::chrono::system_clock::now();

  DBOptions options;
  options.populate_pmem_space = FLAGS_populate;
  options.max_write_threads = FLAGS_thread_num;

  DB::Open(FLAGS_db_path, &db, options, stdout);
  std::cout << "restored time "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now() - start_res)
                   .count()
            << std::endl;

  value_pool = random_str(102400);

  int write_threads =
      FLAGS_thread_num - FLAGS_read_ratio * 100 * FLAGS_thread_num / 100;
  int read_threads = FLAGS_thread_num - write_threads;
  std::vector<std::thread> ts;

  if (FLAGS_load) {
    printf("to load %lu keys\n", max_num_keys);
  }

  if (FLAGS_latency && max_op_per_thread > 0) {
    std::cout << "calculate latencies, max operations per thread "
              << max_op_per_thread << std::endl;
    read_latencies.resize(max_op_per_thread * read_threads, 0);
    write_latencies.resize(max_op_per_thread * write_threads, 0);
  }

  printf("init %d write threads\n", write_threads);
  for (int i = 0; i < write_threads; i++) {
    ts.emplace_back(DBWrite, i);
  }

  printf("init %d read threads\n", read_threads);
  for (int i = 0; i < read_threads; i++) {
    ts.emplace_back(DBRead, i);
  }

  uint64_t last_read_ops = 0;
  uint64_t last_read_notfound = 0;
  uint64_t last_write_ops = 0;
  uint64_t count = 0;
  auto start_ts = std::chrono::system_clock::now();
  printf("------- ops in seconds -----------\n");
  printf(
      "time,   read ops,   not found,  write ops,  total read,  total write\n");
  while (!done) {
    sleep(1);
    if (!done) { // for latency, the last second may not accurate
      count++;
      uint64_t total_read = read_ops.load();
      uint64_t total_write = write_ops.load();
      uint64_t total_not_found = read_not_found.load();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now() - start_ts);
      printf("%-6lu  %-10lu  %-10lu  %-10lu  %-11lu  %-10lu\n",
             duration.count(), total_read - last_read_ops,
             total_not_found - last_read_notfound, total_write - last_write_ops,
             total_read, total_write);
      fflush(stdout);

      last_read_ops = total_read;
      last_write_ops = total_write;
      last_read_notfound = total_not_found;

      if (FLAGS_load) {
        if (total_write >= max_num_keys)
          done = true;
      } else if (count >= FLAGS_time) {
        done = true;
      }
    }
  }

  printf("finish bench\n");
  done = true;

  for (auto &t : ts)
    t.join();

  if (count == 0) count = 1;
  uint64_t read_thpt = read_ops.load() / count;
  uint64_t write_thpt = write_ops.load() / count;

  printf(" ------------ statistics ------------\n");
  printf("read ops %lu, write ops %lu\n", read_thpt, write_thpt);

  if (FLAGS_latency) {
    std::sort(read_latencies.begin(), read_latencies.end());
    std::sort(write_latencies.begin(), write_latencies.end());
    uint64 start = 0;
    uint64 valid = 0;
    if (read_ops.load() > 0 && read_latencies.size() > 0) {
      start = read_latencies.size() - 1;
      while (start != 0 && read_latencies[start] != 0) {
        start--;
      }
      valid = read_latencies.size() - start;
      printf("read lantencies (ns): Avg: %lu, P50: %lu, P99: %lu, P99.5: %lu, "
             "P99.9: %lu P99.99: %lu, P99.999: %lu, P100: %lu\n",
             (uint64_t)1000000000 /* 1s in ns*/ * read_threads / read_thpt,
             read_latencies[start + 0.5 * valid],
             read_latencies[start + 0.99 * valid],
             read_latencies[start + 0.995 * valid],
             read_latencies[start + 0.999 * valid],
             read_latencies[start + 0.9999 * valid],
             read_latencies[start + 0.99999 * valid], read_latencies.back());
    }

    if (write_ops.load() > 0 && write_latencies.size() > 0) {
      start = write_latencies.size() - 1;
      while (start != 0 && write_latencies[start] != 0) {
        start--;
      }
      valid = write_latencies.size() - start;
      printf("write lantencies (ns): Avg: %lu, P50: %lu, P99: %lu, P99.5: %lu, "
             "P99.9: %lu P99.99: %lu, P99.999: %lu, P100 %lu\n",
             (uint64_t)1000000000 * write_threads / write_thpt,
             write_latencies[start + 0.5 * valid],
             write_latencies[start + 0.99 * valid],
             write_latencies[start + 0.995 * valid],
             write_latencies[start + 0.999 * valid],
             write_latencies[start + 0.9999 * valid],
             write_latencies[start + 0.99999 * valid], write_latencies.back());
    }
  }

  delete db;

  return 0;
}
