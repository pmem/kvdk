#pragma once
#ifndef KVDK_C_API_TEST_HPP
#define KVDK_C_API_TEST_HPP

/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include <x86intrin.h>

#include <string>

#include "gtest/gtest.h"
#include "kvdk/engine.h"
#include "test_util.h"

class EngineCAPITestBase : public testing::Test {
 protected:
  KVDKEngine* engine{nullptr};
  KVDKConfigs* configs{KVDKCreateConfigs()};
  const std::string db_path{"/mnt/pmem0/kvdk_c_api_test_" +
                            std::to_string(__rdtsc())};

  virtual void SetUp() override {
    KVDKSetConfigs(configs, 32, (4UL << 30), false, 64, (1UL << 20), 128, 1024,
                   1);
    KVDKOpen(db_path.c_str(), configs, stdout, &engine);
    ASSERT_EQ(KVDKOpen(db_path.c_str(), configs, stdout, &engine),
              KVDKStatus::Ok)
        << "Fail to open the KVDK instance";
  }

  virtual void TearDown() {
    KVDKCloseEngine(engine);
    KVDKDestroyConfigs(configs);
    purgeDB();
  }

  void RebootDB() {
    KVDKCloseEngine(engine);
    ASSERT_EQ(KVDKOpen(db_path.c_str(), configs, stdout, &engine),
              KVDKStatus::Ok)
        << "Fail to open the KVDK instance";
  }

 private:
  void purgeDB() {
    std::string cmd = "rm -rf " + db_path + "\n";
    [[gnu::unused]] int _sink = system(cmd.c_str());
  }
};

#endif  // KVDK_C_API_TEST_HPP
