/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <stdio.h>

#include <string>

#include "configs.hpp"
#include "data_record.hpp"
#include "fcntl.h"
#include "kvdk/configs.hpp"

namespace KVDK_NAMESPACE {

constexpr size_t kMaxBackupLogBufferSize = 128 << 20;

class BackupLog {
 private:
  std::string delta_;
  int log_file_{-1};

  Status persistDelta() {
    int ret = write(log_file_, delta_.data(), delta_.size());
    if (ret != static_cast<int>(delta_.size())) {
      GlobalLogger.Error("IOError while persist backup log: %s\n",
                         strerror(errno));
      return Status::IOError;
    }
    delta_.clear();
    return Status::Ok;
  }

 public:
  struct LogRecord {
    RecordType type;
    StringView key;
    StringView val;
  };

  class LogIterator {
   public:
    LogRecord Record() { return LogRecord(); }
    void Next() {}
    bool Valid() {}

   private:
  };

  ~BackupLog() {
    if (log_file_ >= 0) {
      close(log_file_);
    }
  }

  BackupLog() = default;
  BackupLog(const BackupLog&) = delete;

  LogIterator GetIterator() { return LogIterator(); }

  // Init a new backup log
  Status Init(const std::string& backup_log, const Configs& configs) {
    log_file_ = open(backup_log.c_str(), O_CREAT | O_RDWR, 0666);
    if (log_file_ < 0) {
      GlobalLogger.Error("Init bakcup log file %s error: %s\n",
                         backup_log.c_str(), strerror(errno));
      return Status::IOError;
    }
    ImmutableConfigs imm_configs;
    imm_configs.pmem_block_size = configs.pmem_block_size;
    imm_configs.pmem_segment_blocks = configs.pmem_segment_blocks;
    imm_configs.validation_flag = 1;
    // TODO persist immutable configs
    int ret = write(log_file_, &imm_configs, sizeof(ImmutableConfigs));
    if (ret != sizeof(ImmutableConfigs)) {
      GlobalLogger.Error(
          "Write immutable configs error while init backup log file %s: %s\n",
          backup_log.c_str(), strerror(errno));
      return Status::IOError;
    }
    return Status::Ok;
  }

  // Open a existing backup log
  Status Open(const std::string& backup_log) {
    log_file_ = open(backup_log.c_str(), O_RDWR, 0666);
    if (log_file_ < 0) {
      GlobalLogger.Error("Open bakcup log file %s error: %s\n",
                         backup_log.c_str(), strerror(errno));
      return Status::IOError;
    }
    return Status::Ok;
  }
  // Append a record to backup log
  void Append(RecordType type, const StringView& key, const StringView& val) {
    AppendUint32(&delta_, type);
    AppendFixedString(&delta_, key);
    AppendFixedString(&delta_, val);
    if (delta_.size() >= kMaxBackupLogBufferSize) {
      persistDelta();
    }
  }

  Status Finish() {
    Status s = persistDelta();
    if (s != Status::Ok) {
      return s;
    }
    fsync(log_file_);
    return Status::Ok;
  }
};
}  // namespace KVDK_NAMESPACE