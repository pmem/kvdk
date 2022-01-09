/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <chrono>
#include <ctime>
#include <future>
#include <queue>
#include <string>
#include <sys/time.h>
#include <thread>
#include <vector>

#include "../engine/kv_engine.hpp"
#include "../engine/logger.hpp"
#include "../engine/thread_manager.hpp"
#include "allocator.hpp"
#include "kvdk/engine.hpp"
#include "test_util.h"

using namespace KVDK_NAMESPACE;

class EnginePMemAllocatorTest : public testing::Test {
protected:
  Engine *engine = nullptr;
  Configs configs;
  std::shared_ptr<ThreadManager> thread_manager_;
  std::string pmem_path;

  virtual void SetUp() override {
    pmem_path = "/mnt/pmem0/kvdk_pmem_allocator";
    GlobalLogger.Init(stdout, LogLevel::All);
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", pmem_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }

  virtual void TearDown() { // delete db_path
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", pmem_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }

  perf_time run(uint32_t num_thread, uint64_t alloc_size,
                unsigned mem_operations_num, int seed,
                TestPMemAllocator *pmem_allocator = nullptr) {
    std::vector<std::vector<op_alloc_info>> all_results(num_thread);

    auto TestMallocPerf = [&](uint64_t id) {
      std::vector<op_alloc_info> results;
      TestAllocator *test_alloc;
      if (pmem_allocator) {
        thread_manager_->MaybeInitThread(write_thread);
        test_alloc = pmem_allocator;
      } else {
        test_alloc = new TestStandardAllocator();
      }

      auto func_calls =
          generate_random_allocator_func(mem_operations_num, seed + id);
      for (auto func_call : func_calls) {
        if (func_call == AllocatorFunc::FREE) {
          op_alloc_info *data = nullptr;
          for (int i = results.size() - 1; i >= 0; i--) {
            data = &results[i];
            if (data->is_allocated)
              break;
          }
          if (data != nullptr) {
            results.push_back(test_alloc->wrapped_free(data));
          }
        } else if (func_call == AllocatorFunc::ALLOCATE) {
          results.push_back(test_alloc->wrapped_malloc(alloc_size));
        } else {
          GlobalLogger.Error("Don't support other allocator type");
          assert("Don't support other allocator type");
        }
      }
      all_results[id] = results;
      if (pmem_allocator == nullptr) {
        delete test_alloc;
      }
    };

    LaunchNThreads(num_thread, TestMallocPerf);

    // analaysis data.
    perf_time statistics_perf_time;
    uint64_t allocated = 0;
    uint64_t deallocated = 0;
    for (int i = 0; i < num_thread; i++) {
      for (size_t j = 0; j < all_results[i].size(); ++j) {
        statistics_perf_time.alloc_total_time +=
            all_results[i][j].time.alloc_total_time;
        statistics_perf_time.free_total_time +=
            all_results[i][j].time.free_total_time;
        statistics_perf_time.pmem_merge_time +=
            all_results[i][j].time.pmem_merge_time;
        if (all_results[i][j].allocator_func != AllocatorFunc::FREE) {
          allocated += all_results[i][j].allocate_size;
          if (all_results[i][j].ptr != nullptr) {
            free(all_results[i][j].ptr);
          }
        } else {
          deallocated += all_results[i][j].allocate_size;
        }
      }
    }

    PrintRecord("total alloced size: ", allocated + deallocated);
    PrintRecord("total dealloced size: ", deallocated);
    PrintRecord("alloc_operations_per_thread", mem_operations_num);
    statistics_perf_time.allocated_ops_per_seconds =
        (allocated + deallocated) / alloc_size /
        statistics_perf_time.alloc_total_time;
    ;
    statistics_perf_time.deallocated_ops_per_seconds =
        (deallocated) / alloc_size / statistics_perf_time.free_total_time;

    return statistics_perf_time;
  }

  void run_test(uint32_t num_thread, uint64_t pmem_size,
                uint64_t num_segment_blocks, uint32_t block_size,
                uint64_t alloc_size, unsigned mem_operations_num) {
    PrintRecord("alloc_operations_per_thread", mem_operations_num);
    perf_time ref_time = run(num_thread, alloc_size, mem_operations_num, 11);
    PrintRecord("malloc spend_on_alloc(ops/ms)",
                ref_time.allocated_ops_per_seconds);
    PrintRecord("malloc spend_on_free(ops/ms)",
                ref_time.deallocated_ops_per_seconds);

    // For PMem
    thread_manager_.reset(new (std::nothrow) ThreadManager(num_thread));

    TestPMemAllocator *pmem_alloc = new TestPMemAllocator();
    pmem_alloc->InitPMemAllocator(pmem_path, pmem_size, num_segment_blocks,
                                  block_size, num_thread);

    perf_time pmem_time =
        run(num_thread, alloc_size, mem_operations_num, 11, pmem_alloc);

    PrintRecord("pmem spend_on_alloc(ops/ms)",
                pmem_time.allocated_ops_per_seconds);
    PrintRecord("pmem spend_on_free(ops/ms)",
                pmem_time.deallocated_ops_per_seconds);
    PrintRecord("pmem spend_on_merge_time", pmem_time.pmem_merge_time);
    float alloc_ref_delta_time_percent =
        ((pmem_time.alloc_total_time / ref_time.alloc_total_time) - 1.0) *
        100.0;
    delete pmem_alloc;
  }

private:
  std::vector<int> generate_random_allocator_func(int call_num, int seed) {

    std::srand(seed);
    std::vector<int> calls;
    for (int i = 0; i < call_num; i++) {
      calls.push_back(rand() % NUM_FUNCS);
    }
    return calls;
  }
};

TEST_F(EnginePMemAllocatorTest, TestBasicAlloc) {
  uint64_t pmem_size = 128ULL << 20; // 32MB
  uint64_t alloc_size = 8;

  // params config
  std::vector<uint64_t> num_segment_blocks{1024, 2 * 1024, 2 * 1024 * 1024};
  std::vector<uint32_t> block_sizes{16, 32, 64};
  std::vector<uint32_t> num_threads = {1, 16};

  for (int i = 0; i < num_segment_blocks.size(); ++i) {
    for (auto num_thread : num_threads) {
      thread_manager_.reset(new (std::nothrow) ThreadManager(num_thread));

      // Test function.
      auto TestPmemAlloc = [&](uint64_t id) {
        std::vector<SpaceEntry> records;
        thread_manager_->MaybeInitThread(write_thread);
        PMEMAllocator *pmem_alloc = PMEMAllocator::NewPMEMAllocator(
            pmem_path, pmem_size, num_segment_blocks[i], block_sizes[i],
            num_thread, false);
        ASSERT_NE(pmem_alloc, nullptr);

        uint64_t kvpairs = pmem_size / block_sizes[i];
        for (uint64_t j = 0; j < kvpairs; ++j) {
          auto space_entry = pmem_alloc->Allocate(alloc_size);
          records.push_back(space_entry);
        }
        for (uint64_t j = 0; j < records.size(); ++j) {
          pmem_alloc->Free(records[j]);
        }
        records.clear();

        // again allocate pmem
        while (true) {
          auto space_entry = pmem_alloc->Allocate(alloc_size);
          if (space_entry.size == 0)
            break;
          records.push_back(space_entry);
        }
        ASSERT_EQ(kvpairs, records.size());
        // TODO: add check pmem usage.
        delete pmem_alloc;
      };

      LaunchNThreads(num_thread, TestPmemAlloc);
    }
  }
}

TEST_F(EnginePMemAllocatorTest, TestPMemPopulateSpace) {
  // pmem size can't less than 64?
  std::vector<uint64_t> pmem_sizes{16ULL << 30, 16ULL << 20, 16ULL << 10};
  uint64_t num_segment_block = 2 * 1024;
  uint32_t block_size = 16;
  std::vector<uint32_t> num_threads = {1, 16};
  std::vector<bool> use_devdax_modes = {false};
  for (auto pmem_size : pmem_sizes) {
    for (auto num_thread : num_threads) {
      thread_manager_.reset(new (std::nothrow) ThreadManager(num_thread));
      auto TestPmemPopulate = [&](uint64_t id) {
        thread_manager_->MaybeInitThread(write_thread);
        PMEMAllocator *pmem_alloc = PMEMAllocator::NewPMEMAllocator(
            pmem_path, pmem_size, num_segment_block, block_size, num_thread,
            false);
        ASSERT_NE(pmem_alloc, nullptr);
        pmem_alloc->PopulateSpace();
        delete pmem_alloc;
      };
      LaunchNThreads(num_thread, TestPmemPopulate);
    }
  }
}

TEST_F(EnginePMemAllocatorTest, TestPMemFragmentation) {
  uint32_t num_thread = 16;
  uint64_t pmem_size = 64ULL << 10;
  uint64_t num_segment_block = 1024;
  uint64_t block_size = 64;
  std::vector<uint64_t> alloc_size{8 * 64, 8 * 64, 16 * 64, 32 * 64};
  thread_manager_.reset(new (std::nothrow) ThreadManager(num_thread));
  PMEMAllocator *pmem_alloc = PMEMAllocator::NewPMEMAllocator(
      pmem_path, pmem_size, num_segment_block, block_size, num_thread, false);
  ASSERT_NE(pmem_alloc, nullptr);

  /* Allocated pmem status (block nums):
   * | 8 | 8 | 16 | 32 | 8 | 8 | 16 | 32 | 8 | 8 | 16 | 32 | 8 | 8 | 16 | 32 |
   */

  std::vector<SpaceEntry> records(num_thread);
  thread_manager_->MaybeInitThread(write_thread);
  for (uint32_t i = 0; i < records.size(); ++i) {
    SpaceEntry space_entry = pmem_alloc->Allocate(alloc_size[i % 4]);
    ASSERT_NE(space_entry.size, 0);
  }

  /* Allocated pmem status:
   * | null | null | null | 32 | null | null | null | 32 | null | null | null
   * | 32 | null | null | null | 32 |
   */
  auto TestPmemFree = [&](uint64_t id) {
    thread_manager_->MaybeInitThread(write_thread);
    if ((id + 1) % 4 != 0) {
      pmem_alloc->Free(records[id]);
      pmem_alloc->BackgroundWork();
    }
  };

  // Test merge free memory
  auto TestPmemFrage = [&](uint64_t id) {
    thread_manager_->MaybeInitThread(write_thread);
    if ((id + 1 % 4) == 0) {
      SpaceEntry space_entry = pmem_alloc->Allocate(alloc_size[id % 4]);
      ASSERT_NE(space_entry.size, 0);
    }
  };
  LaunchNThreads(num_thread, TestPmemFree);
  LaunchNThreads(num_thread, TestPmemFrage);

  // TODO: add check pmem usage.
  delete pmem_alloc;
}

// PMemAllocPerformaceTest
TEST_F(EnginePMemAllocatorTest, TestPMemPerf_16_threads_8_bytes_64_bsize) {
  // Init 64G PMem
  run_test(16, 64ULL << 30, 1024, 64, 8, 1000);
}

// PMemAllocPerformaceTest
TEST_F(EnginePMemAllocatorTest, TestPMemPerf_16_threads_8_bytes_32_bsize) {
  // Init 64G PMem
  run_test(16, 64ULL << 30, 1024, 32, 8, 1000);
}

// PMemAllocPerformaceTest
TEST_F(EnginePMemAllocatorTest, TestPMemPerf_16_threads_64_bytes_32_bsize) {
  // Init 64G PMem
  run_test(16, 64ULL << 30, 1024, 32, 64, 1000);
}

// PMemAllocPerformaceTest
TEST_F(EnginePMemAllocatorTest, TestPMemPerf_16_threads_65536_bytes_64_bsize) {
  // Init 64G PMem
  run_test(16, 64ULL << 30, 1024, 64, 65536, 1000);
}