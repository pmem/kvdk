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

enum class BackupStage {
  NotFinished = 0,
  Finished,
};

// Persist/Read backup of a kvdk instance as log-like manner
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
    ExpireTimeType expire_time;

    bool DecodeFrom(StringView* str) {
      return FetchUint32(str, (uint32_t*)&type) &&
             FetchFixedString(str, &key) & FetchFixedString(str, &val) &&
             FetchUint64(str, (uint64_t*)&expire_time);
    }

    void EncodeTo(std::string* str) {
      assert(str != nullptr);
      AppendUint32(str, type);
      AppendFixedString(str, key);
      AppendFixedString(str, val);
      AppendUint64(str, expire_time);
    }
  };

  // Iterate log records on a backup log
  class LogIterator {
   public:
    LogIterator(StringView records_on_file)
        : records_on_file_(records_on_file) {
      valid_ = curr_.DecodeFrom(&records_on_file_);
    }

    LogRecord Record() { return curr_; }
    void Next() {
      if (Valid()) {
        valid_ = curr_.DecodeFrom(&records_on_file_);
      }
    }

    bool Valid() { return valid_; }

   private:
    StringView records_on_file_;
    LogRecord curr_;
    bool valid_ = true;
  };

  ~BackupLog() { Close(); }

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
      Destroy();
      return Status::IOError;
    }
    changeStage(BackupStage::NotFinished);
    file_name_ = backup_log;
    return Status::Ok;
  }

  // Open a existing backup log
  Status Open(const std::string& backup_log) {
    fd_ = open(backup_log.c_str(), O_RDWR, 0666);
    if (fd_ >= 0) {
      file_size_ = lseek(fd_, 0, SEEK_END);
      if (file_size_ >= sizeof(BackupStage)) {
        log_file_ = (char*)mmap(nullptr, file_size_, PROT_WRITE | PROT_READ,
                                MAP_SHARED, fd_, 0);
      } else {
        GlobalLogger.Error(
            "Open backup log file %s error: file size %lu smaller than "
            "persisted "
            "stage flag",
            backup_log.size(), file_size_);
      }
    }
    if (log_file_ == nullptr) {
      GlobalLogger.Error("Open bakcup log file %s error: %s\n",
                         backup_log.c_str(), strerror(errno));
      return Status::IOError;
    }
    stage_ = *persistedStage();
    file_name_ = backup_log;
    return Status::Ok;
  }

  // Append a record to backup log
  Status Append(RecordType type, const StringView& key, const StringView& val,
                ExpireTimeType expire_time) {
    if (finished()) {
      changeStage(BackupStage::NotFinished);
    }
    // we do not encapsulate LogRecord here to avoid a memory copy
    // TODO: use pinable string view in LogRecord
    AppendUint32(&delta_, type);
    AppendFixedString(&delta_, key);
    AppendFixedString(&delta_, val);
    AppendUint64(&delta_, expire_time);
    if (delta_.size() >= kMaxBackupLogBufferSize) {
      return persistDelta();
    }
    return Status::Ok;
  }

  Status Finish() {
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

  // Close backup log file
  void Close() {
    if (log_file_) {
      munmap(log_file_, file_size_);
    }
    if (fd_ >= 0) {
      close(fd_);
    }
    delta_.clear();
    file_name_.clear();
    fd_ = -1;
    file_size_ = 0;
    stage_ = BackupStage::NotFinished;
    log_file_ = nullptr;
  }

  // Destroy backup log file
  void Destroy() {
    Close();
    remove(file_name_.c_str());
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
    msync(persistedStage(), sizeof(BackupStage), MS_SYNC);
  }

  bool finished() { return stage_ == BackupStage::Finished; }

  std::string file_name_{};
  std::string delta_{};
  char* log_file_{nullptr};
  size_t file_size_{0};
  int fd_{-1};
  BackupStage stage_{BackupStage::NotFinished};
};
}  // namespace KVDK_NAMESPACE