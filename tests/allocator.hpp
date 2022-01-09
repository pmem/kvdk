/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2022 Intel Corporation
 */

#pragma once

#include <stdio.h>

#include "pmem_allocator/free_list.hpp"
#include "pmem_allocator/pmem_allocator.hpp"

#include "gtest/gtest.h"

using namespace KVDK_NAMESPACE;

#define START(ALLOCATOR, FUNC)                                                 \
  op_alloc_info mem_op;                                                        \
  mem_op.allocator_type = ALLOCATOR;                                           \
  mem_op.allocator_func = FUNC;                                                \
  std::chrono::system_clock::time_point to_begin =                             \
      std::chrono::high_resolution_clock::now();
#define END                                                                    \
  std::chrono::system_clock::time_point to_end =                               \
      std::chrono::high_resolution_clock::now();                               \
  std::chrono::duration<double, std::milli> elapsedTime(to_end - to_begin);    \
  mem_op.time.alloc_total_time = elapsedTime.count() / 1000.0;                 \
  mem_op.allocate_size = alloc_size;                                           \
  mem_op.is_allocated = true;                                                  \
  return mem_op;

enum AllocatorTypes { STANDARD_ALLOCATOR, PMEM_ALLOCATOR, NUM_ALLOCATOR_TYPE };

enum AllocatorFunc { FREE, ALLOCATE, NUM_FUNCS };

struct perf_time {
  double alloc_total_time;
  double free_total_time;
  double pmem_merge_time;
  uint64_t allocated_ops_per_seconds;
  uint64_t deallocated_ops_per_seconds;
};

struct op_alloc_info {
  void *ptr = nullptr;
  SpaceEntry entry;

  perf_time time;
  uint64_t allocate_size;
  unsigned allocator_type;
  unsigned allocator_func;
  bool is_allocated;
  bool is_pmem;

  op_alloc_info() {}
};

class TestAllocator {
public:
  virtual op_alloc_info wrapped_malloc(uint64_t size) = 0;
  virtual op_alloc_info wrapped_free(op_alloc_info *data) = 0;

  virtual ~TestAllocator(void) {}
};

class TestStandardAllocator : public TestAllocator {
public:
  op_alloc_info wrapped_free(op_alloc_info *data) {
    std::chrono::system_clock::time_point to_begin =
        std::chrono::high_resolution_clock::now();
    free(data->ptr);
    std::chrono::system_clock::time_point to_end =
        std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsedTime(to_end - to_begin);
    data->ptr = nullptr;
    data->is_allocated = false;
    data->allocator_func = AllocatorFunc::FREE;
    data->time.free_total_time = elapsedTime.count() / 1000.0;
    return *data;
  }

  op_alloc_info wrapped_malloc(uint64_t alloc_size) override {
    START(AllocatorTypes::STANDARD_ALLOCATOR, AllocatorFunc::ALLOCATE)
    mem_op.ptr = malloc(alloc_size);
    END
  }
};

class TestPMemAllocator : public TestAllocator {
public:
  void InitPMemAllocator(const std::string &pmem_path, uint64_t pmem_size,
                         uint64_t num_segment_blocks, uint32_t block_size,
                         uint32_t num_write_threads) {
    pmem_alloc_ = PMEMAllocator::NewPMEMAllocator(
        pmem_path, pmem_size, num_segment_blocks, block_size, num_write_threads,
        false);
    ASSERT_NE(pmem_alloc_, nullptr);
    pmem_alloc_->PopulateSpace();
  }

  op_alloc_info wrapped_malloc(uint64_t alloc_size) {
    START(AllocatorTypes::PMEM_ALLOCATOR, AllocatorFunc::ALLOCATE)
    mem_op.entry = pmem_alloc_->Allocate(alloc_size);
    END
  }

  op_alloc_info wrapped_free(op_alloc_info *data) {
    std::chrono::system_clock::time_point to_begin =
        std::chrono::high_resolution_clock::now();
    pmem_alloc_->Free(data->entry);
    std::chrono::system_clock::time_point to_end =
        std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsedTime(to_end - to_begin);
    data->time.free_total_time = elapsedTime.count() / 1000.0;
    pmem_alloc_->BackgroundWork();
    std::chrono::system_clock::time_point to_end2 =
        std::chrono::high_resolution_clock::now();
    elapsedTime = to_end2 - to_end;
    data->time.pmem_merge_time = elapsedTime.count() / 1000;
    data->is_allocated = false;
    data->allocator_func = AllocatorFunc::FREE;
    return *data;
  }

  ~TestPMemAllocator(void) { delete pmem_alloc_; }

private:
  PMEMAllocator *pmem_alloc_;
};
