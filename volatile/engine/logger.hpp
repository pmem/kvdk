/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <unistd.h>

#include <chrono>
#include <cstdarg>
#include <mutex>

#include "alias.hpp"
#include "kvdk/volatile/configs.hpp"

#define DO_LOG 1

namespace KVDK_NAMESPACE {

class Logger {
 public:
  void Info(const char* format, ...);
  void Error(const char* format, ...);
  void Debug(const char* format, ...);
  void Init(FILE* fp, LogLevel level);

 private:
  void Log(const char* log_type, const char* format, va_list& args);

  FILE* log_file_ = NULL;
  LogLevel level_;
  std::mutex mut_;

  std::chrono::time_point<std::chrono::system_clock> start_ts_;
};

extern Logger GlobalLogger;

}  // namespace KVDK_NAMESPACE