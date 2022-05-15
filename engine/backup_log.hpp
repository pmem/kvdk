/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <stdio.h>

#include <string>

#include "configs.hpp"
#include "data_record.hpp"
#include "fcntl.h"
#include "kvdk/configs.hpp"
#include "kvdk/status.hpp"
#include "logger.hpp"
#include "sys/mman.h"

namespace KVDK_NAMESPACE {

constexpr size_t kMaxBackupLogBufferSize = 128 << 20;

enum BackupStage {
  NotFinished = 0,
  Finished,
};

// Persist/Read backup of a instead as log-like manner
// Format:
// stage | string record 1 | string record 2 | sorted header record 1 | sorted
// elems of header 1 | ... sorted header n | sorted elems of header n | ... |
// string record n| ...
class BackupLog {
 public:
  struct LogRecord {
    RecordType type;
    std::string key;
    std::string val;
  };

  // Notice LogIterator would be invalid if append new records to log
  class LogIterator {
   public:
    LogIterator(StringView records_on_file)
        : records_on_file_(records_on_file) {
      valid_ = FetchUint32(&records_on_file_, (uint32_t*)&curr_.type) &&
               FetchFixedString(&records_on_file_, &curr_.key) &&
               FetchFixedString(&records_on_file_, &curr_.val);
    }

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
    if (log_file_) {
      munmap(log_file_, file_size_);
    }
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  BackupLog() = default;
  BackupLog(const BackupLog&) = delete;

  // Init a new backup log
  Status Init(const std::string& backup_log) {
    if (file_exist(backup_log)) {
      GlobalLogger.Error("Init backup log %s error: file already exist\n",
                         backup_log.c_str());
      return Status::Abort;
    }
    fd_ = open(backup_log.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd_ >= 0 && ftruncate(fd_, sizeof(BackupStage)) == 0) {
      log_file_ = (char*)mmap(nullptr, sizeof(BackupStage),
                              PROT_WRITE | PROT_READ, MAP_SHARED, fd_, 0);
      file_size_ = sizeof(BackupStage);
    };
    if (log_file_ == nullptr) {
      GlobalLogger.Error("Init bakcup log file %s error: %s\n",
                         backup_log.c_str(), strerror(errno));
      return Status::IOError;
    }
    changeStage(BackupStage::NotFinished);
    return Status::Ok;
  }

  // Open a existing backup log read-only
  Status Open(const std::string& backup_log) {
    fd_ = open(backup_log.c_str(), O_RDWR, 0666);
    if (fd_ >= 0) {
      file_size_ = lseek(fd_, 0, SEEK_END);
      if (file_size_ >= sizeof(BackupStage)) {
        log_file_ = (char*)mmap(nullptr, file_size_, PROT_WRITE | PROT_READ,
                                MAP_SHARED, fd_, 0);
      }
    }
    if (log_file_ == nullptr) {
      GlobalLogger.Error("Open bakcup log file %s error: %s\n",
                         backup_log.c_str(), strerror(errno));
      return Status::IOError;
    }
    stage_ = *persistedStage();
    return Status::Ok;
  }

  // Append a record to backup log
  Status Append(RecordType type, const StringView& key, const StringView& val) {
    if (finished()) {
      changeStage(BackupStage::NotFinished);
    }
    AppendUint32(&delta_, type);
    AppendFixedString(&delta_, key);
    AppendFixedString(&delta_, val);
    if (delta_.size() >= kMaxBackupLogBufferSize) {
      return persistDelta();
    }
    return Status::Ok;
  }

  Status Finish() {
    // TODO jiayu add a finish mark or checksum
    Status s = persistDelta();
    if (s != Status::Ok) {
      return s;
    }
    changeStage(BackupStage::Finished);
    return Status::Ok;
  }

  // Get iterator of log records
  // Notice: the iterator will be corrupted if append new records to log
  std::unique_ptr<LogIterator> GetIterator() {
    return finished()
               ? std::unique_ptr<LogIterator>(new LogIterator(logRecordsView()))
               : nullptr;
  }

 private:
  Status persistDelta() {
    if (ftruncate64(fd_, file_size_ + delta_.size())) {
      GlobalLogger.Error("Allocate space for backup log file error: %s\n",
                         strerror(errno));
      return Status::IOError;
    }
    munmap(log_file_, file_size_);
    log_file_ = (char*)mmap(log_file_, file_size_ + delta_.size(),
                            PROT_WRITE | PROT_READ, MAP_SHARED, fd_, 0);

    if (log_file_ == nullptr) {
      GlobalLogger.Error("Map backup log file error: %s\n", strerror(errno));
      return Status::IOError;
    }
    memcpy(log_file_ + file_size_, delta_.data(), delta_.size());
    msync(log_file_ + file_size_, delta_.size(), MS_SYNC);
    file_size_ += delta_.size();
    delta_.clear();
    return Status::Ok;
  }

  StringView logRecordsView() {
    kvdk_assert(log_file_ != nullptr && file_size_ >= sizeof(BackupStage), "");
    return StringView(log_file_ + sizeof(BackupStage),
                      file_size_ - sizeof(BackupStage));
  }

  BackupStage* persistedStage() {
    kvdk_assert(log_file_ != nullptr && file_size_ >= sizeof(BackupStage), "");
    return (BackupStage*)log_file_;
  }

  void changeStage(BackupStage stage) {
    memcpy(persistedStage(), &stage, sizeof(BackupStage));
    msync(log_file_, sizeof(BackupStage), MS_SYNC);
  }

  bool finished() { return stage_ == BackupStage::Finished; }

  std::string delta_{};
  char* log_file_{nullptr};
  size_t file_size_{0};
  int fd_{-1};
  BackupStage stage_{BackupStage::Finished};
};
}  // namespace KVDK_NAMESPACE