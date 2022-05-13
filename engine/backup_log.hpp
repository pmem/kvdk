/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <stdio.h>

#include <string>

#include "configs.hpp"
#include "data_record.hpp"
#include "fcntl.h"
#include "kvdk/configs.hpp"
#include "sys/mman.h"

namespace KVDK_NAMESPACE {

constexpr size_t kMaxBackupLogBufferSize = 128 << 20;

class BackupLog {
 private:
  std::string delta_;
  char* log_file_;
  size_t file_size_;
  int fd_{-1};

  Status persistDelta() {
    // TODO: expand map file
    if (false) {
      GlobalLogger.Error("IOError while persist backup log: %s\n",
                         strerror(errno));
      return Status::IOError;
    }
    memcpy(log_file_ + file_size_, delta_.data(), delta_.size());
    file_size_ += delta_.size();
    delta_.clear();
    return Status::Ok;
  }

 public:
  struct LogRecord {
    RecordType type;
    std::string key;
    std::string val;
  };

  class LogIterator {
   public:
    LogRecord Record() { return curr_; }
    void Next() {
      if (Valid()) {
        valid_ = FetchUint32(&records_on_file_, (uint32_t*)&curr_.type) &&
                 FetchFixedString(&records_on_file_, &curr_.key) &&
                 FetchFixedString(&records_on_file_, &curr_.val);
      }
    }
    bool Valid() { return valid_; }

   private:
    StringView records_on_file_;
    LogRecord curr_;
    bool valid_ = true;
  };

  ~BackupLog() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  BackupLog() = default;
  BackupLog(const BackupLog&) = delete;

  LogIterator GetIterator() { return LogIterator(); }

  // Init a new backup log
  Status Init(const std::string& backup_log, const Configs& configs) {
    fd_ = open(backup_log.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd_ >= 0) {
      log_file_ = (char*)mmap(nullptr, 0, PROT_WRITE | PROT_READ, 0666, fd_, 0);
      file_size_ = 0;
    };
    if (log_file_ == nullptr) {
      GlobalLogger.Error("Init bakcup log file %s error: %s\n",
                         backup_log.c_str(), strerror(errno));
      return Status::IOError;
    }
    return Status::Ok;
  }

  // Open a existing backup log
  Status Open(const std::string& backup_log) {
    fd_ = open(backup_log.c_str(), O_RDWR, 0666);
    if (fd_ >= 0) {
      log_file_ = (char*)mmap(nullptr, 0, PROT_WRITE | PROT_READ, 0666, fd_, 0);
      file_size_ = 0;
    }
    if (log_file_ == nullptr) {
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
    fsync(fd_);
    return Status::Ok;
  }
};
}  // namespace KVDK_NAMESPACE