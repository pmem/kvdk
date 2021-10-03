/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include <array>

#include "logger.hpp"

namespace KVDK_NAMESPACE {

void Logger::Info(const char *format, ...) {
  if (level_ <= LogLevel::INFO) {
    va_list args;
    va_start(args, format);
    log_impl("[INFO]", format, args);
    va_end(args);
  }
}

void Logger::Error(const char *format, ...) {
  if (level_ <= LogLevel::ERROR) {
    va_list args;
    va_start(args, format);
    log_impl("[ERROR]", format, args);
    va_end(args);
  }
}

void Logger::log_impl(const char *log_type, const char *format, va_list &args) {
  if (log_file_ != nullptr) {
    std::lock_guard<std::mutex> lg(mut_);
    auto now = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - start_ts_);
    fprintf(log_file_, "%s time %ld ms: ", log_type, duration.count());
    vfprintf(log_file_, format, args);
    fflush(log_file_);
    fsync(fileno(log_file_));
  }
}

void Logger::Init(FILE *fp, LogLevel level) {
  log_file_ = fp;
  level_ = level;
  start_ts_ = std::chrono::system_clock::now();
}

Logger GlobalLogger;

} // namespace KVDK_NAMESPACE