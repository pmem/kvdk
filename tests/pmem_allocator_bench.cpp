/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <gflags/gflags.h>
#include <sys/time.h>

#include <chrono>
#include <ctime>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "../engine/kv_engine.hpp"
#include "../engine/thread_manager.hpp"
#include "allocator.hpp"
#include "kvdk/engine.hpp"
#include "test_util.h"

using namespace KVDK_NAMESPACE;

#define MIN_ALLOC_SIZE 8
#define MAX_ALLOC_SIZE 524288  // 512KB

constexpr size_t NUM_SIZES = 10000;
constexpr size_t NUM_OFFSET = 4096;

// Benchmark configs
DEFINE_string(pmem_path, "/mnt/pmem0/allcator_bench", "Instance path");

DEFINE_uint64(num_thread, 16, "Number of threads");

DEFINE_uint64(iter_num, 10000,
              "Number of iterating operators(random free and allocate)");

DEFINE_uint64(pmem_size, 64ULL << 30, "PMem total size");

DEFINE_uint64(num_segment_blocks, 1024ULL, "PMem num blocks per segment");

DEFINE_uint64(block_size, 16, "PMem block size");

// Allocator Performance Class
// TODO: add access_memory bench
class AllocatorBench {
 private:
  std::vector<std::uint64_t> random_alloc_sizes;
  std::vector<std::uint64_t> random_offsets;

 private:
  /* Get a random block size with an inverse square distribution.  */
  static uint64_t get_block_size(uint64_t rand_data) {
    /* Inverse square.  */
    const float exponent = -2;
    const float dist_min = MIN_ALLOC_SIZE;
    const float dist_max = MAX_ALLOC_SIZE;

    float min_pow = powf(dist_min, exponent + 1);
    float max_pow = powf(dist_max, exponent + 1);

    float r = (float)rand_data / RAND_MAX;

    return (uint64_t)powf((max_pow - min_pow) * r + min_pow,
                          1 / (exponent + 1));
  }

 public:
  void FixedSizePerf(uint32_t num_thread, uint64_t fixed_alloc_size,
                     int64_t iter_num, int, AllocatorAdaptor* allocator) {
    double execute_time = 0;
    auto FixedSizeBench = [&](size_t) {
      std::vector<op_alloc_info> res(iter_num);
      std::chrono::system_clock::time_point to_begin =
          std::chrono::high_resolution_clock::now();
      for (int64_t i = 0; i < iter_num; ++i) {
        res[i] = allocator->wrapped_malloc(fixed_alloc_size);
      }
      for (int64_t i = 0; i < iter_num; ++i) {
        allocator->wrapped_free(&res[i]);
      }

      std::chrono::system_clock::time_point to_end =
          std::chrono::high_resolution_clock::now();
      std::chrono::duration<double, std::milli> elapsedTime(to_end - to_begin);
      execute_time += (elapsedTime.count() / 1000);
    };
    LaunchNThreads(num_thread, FixedSizeBench);
    std::cout << "Allocated " << std::fixed << std::setprecision(5)
              << iter_num * fixed_alloc_size
              << " bytes and free all, executed time: " << std::fixed
              << std::setprecision(5) << execute_time << std::endl;
  }

  void RandomSizePerf(uint32_t num_thread, int64_t iter_num,
                      AllocatorAdaptor* allocator) {
    std::vector<double> args(num_thread);
    std::vector<std::vector<op_alloc_info>> records(num_thread);
    double elapesd_time = 0;
    auto RandomBench = [&](uint64_t id) {
      allocator->InitThread();
      std::vector<op_alloc_info> work_sets(iter_num);
      std::chrono::system_clock::time_point to_begin =
          std::chrono::high_resolution_clock::now();
      for (int64_t i = 0; i < iter_num; ++i) {
        std::uint64_t idx = random_offsets[i % NUM_OFFSET];
        // random free and allocate.
        allocator->wrapped_free(&work_sets[idx]);
        work_sets[idx] =
            allocator->wrapped_malloc(random_alloc_sizes[idx % NUM_SIZES]);
      }
      std::chrono::system_clock::time_point to_end =
          std::chrono::high_resolution_clock::now();
      std::chrono::duration<double, std::milli> elapsedTime(to_end - to_begin);
      args[id] = elapsedTime.count() / 1000;
      records[id] = work_sets;
    };
    LaunchNThreads(num_thread, RandomBench);

    // caculate
    for (size_t i = 0; i < num_thread; ++i) {
      elapesd_time += args[i];
    }
    std::cout << "Random allocated sized, Random allocated and free";
    std::cout << "Total execute time: " << std::fixed << std::setprecision(5)
              << elapesd_time << " seconds\n";

    // Clear all memory to avoid memory leak
    allocator->InitThread();
    for (auto record : records) {
      for (auto r : record) {
        allocator->wrapped_free(&r);
      }
    }
  }

  AllocatorBench() {
    std::default_random_engine rand_engine{std::random_device()()};
    random_alloc_sizes.reserve(NUM_SIZES);
    for (size_t i = 0; i < NUM_SIZES; ++i) {
      random_alloc_sizes.emplace_back(get_block_size(rand_engine() % RAND_MAX));
    }

    random_offsets.reserve(NUM_OFFSET);
    for (size_t i = 0; i < NUM_OFFSET; ++i) {
      random_offsets.emplace_back(rand() % (NUM_OFFSET / 2));
    }
  }

  ~AllocatorBench() {
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", FLAGS_pmem_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }
};

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  AllocatorBench allcator_bench;

  // For standard allocator
  StandardAllocatorWrapper* standard_allocator = new StandardAllocatorWrapper();
  std::cout << "Standard Allocator Performance: \n";
  allcator_bench.RandomSizePerf(FLAGS_num_thread, FLAGS_iter_num,
                                standard_allocator);
  delete standard_allocator;

  // For pmem allocator
  PMemAllocatorWrapper* pmem_allocator = new PMemAllocatorWrapper();
  pmem_allocator->InitPMemAllocator(FLAGS_pmem_path, FLAGS_pmem_size,
                                    FLAGS_num_segment_blocks, FLAGS_block_size,
                                    FLAGS_num_thread);

  std::cout << "PMmem Allocator Performance: \n";
  allcator_bench.RandomSizePerf(FLAGS_num_thread, FLAGS_iter_num,
                                pmem_allocator);

  delete pmem_allocator;

  return 0;
}
