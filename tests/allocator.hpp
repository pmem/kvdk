/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2022 Intel Corporation
 */

#pragma once

#include <stdio.h>

#include "../engine/thread_manager.hpp"
#include "kvdk/namespace.hpp"
#include "pmem_allocator/free_list.hpp"
#include "pmem_allocator/pmem_allocator.hpp"

#include "gtest/gtest.h"

using namespace KVDK_NAMESPACE;

struct op_alloc_info {
  void *ptr = nullptr;
  SpaceEntry entry;
  op_alloc_info() {}
};

class TestAllocator {
public:
  virtual op_alloc_info wrapped_malloc(uint64_t size) = 0;
  virtual op_alloc_info wrapped_free(op_alloc_info *data) = 0;
  virtual void InitThread() {}
  virtual ~TestAllocator(void) {}
};

class TestStandardAllocator : public TestAllocator {
public:
  op_alloc_info wrapped_free(op_alloc_info *data) override {
    free(data->ptr);
    return *data;
  }

  op_alloc_info wrapped_malloc(uint64_t alloc_size) override {
    op_alloc_info data;
    data.ptr = malloc(alloc_size);
    return data;
  }
};

class TestPMemAllocator : public TestAllocator {
public:
  void InitPMemAllocator(const std::string &pmem_path, uint64_t pmem_size,
                         uint64_t num_segment_blocks, uint32_t block_size,
                         uint32_t num_write_threads) {
    // // remove old pmem
    // char cmd[1024];
    // sprintf(cmd, "rm -rf %s\n", pmem_path.c_str());
    // int res __attribute__((unused)) = system(cmd);

    thread_manager_.reset(new (std::nothrow) ThreadManager(num_write_threads));
    pmem_alloc_ = PMEMAllocator::NewPMEMAllocator(
        pmem_path, pmem_size, num_segment_blocks, block_size, num_write_threads,
        true, false, nullptr);
    kvdk_assert(pmem_alloc_ != nullptr, "New pmem allocator failed!");
    background.emplace_back(std::thread(&TestPMemAllocator::BackGround, this));
  }

  op_alloc_info wrapped_malloc(uint64_t alloc_size) override {
    op_alloc_info data;
    data.entry = pmem_alloc_->Allocate(alloc_size);
  }

  op_alloc_info wrapped_free(op_alloc_info *data) override {
    pmem_alloc_->Free(data->entry);
    return *data;
  }

  void BackGround() {
    while (!closing_) {
      pmem_alloc_->BackgroundWork();
    }
  }

  void InitThread() override { thread_manager_->MaybeInitThread(access_thread); }

  ~TestPMemAllocator(void) {
    closing_ = true;
    // background thread exit;
    for (auto &t : background) {
      t.join();
    }
    delete pmem_alloc_;
  }

private:
  PMEMAllocator *pmem_alloc_;
  bool closing_ = false;
  std::vector<std::thread> background;
  std::shared_ptr<ThreadManager> thread_manager_;
};
