/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <chrono>
#include <cstdarg>
#include <mutex>
#include <unistd.h>

#include "kvdk/namespace.hpp"

#define DO_LOG 1

namespace KVDK_NAMESPACE {

class Logger {
public:
  void Log(const char *format, ...);
  void Error(const char *format, ...);
  void Init(FILE *fp);

private:
  FILE *log_file_ = NULL;
  std::mutex mut_;

  std::chrono::time_point<std::chrono::system_clock> start_ts_;
};

extern Logger GlobalLogger;

} // namespace KVDK_NAMESPACE