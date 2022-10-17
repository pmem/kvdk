/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>
#include <cstdio>
#include <kvdk/engine.hpp>
#include <map>
#include <string>
#include <vector>

#if defined(BUILD_ROCKSDB)
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#endif

#include "engine_factory.hpp"

typedef kvdk::Status Status;

inline void SimpleLoger(const std::string& content) {
  fprintf(stdout, "[GRAPH-BENCH]%s \n", content.c_str());
}

// The simple abstract of the different engines.
//
// Any db engine whole want to connect the graph storage bench
// should inherent the class and implementation the function.
class KVEngine {
 public:
  class Iterator {
   public:
    virtual void Seek(const std::string& key) = 0;
    virtual void SeekToFirst() = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual bool Valid() = 0;
    virtual ~Iterator() = default;

    virtual std::string Key() = 0;
    virtual std::string Value() = 0;
  };

  virtual Status Get(const std::string& key, std::string* value) = 0;
  virtual Status Put(const std::string& key, const std::string& value) = 0;
  virtual Status Delete(const std::string& key) = 0;
  virtual Iterator* NewIterator() = 0;
  virtual ~KVEngine() = default;
};

// KVDK engine
class PMemKVDK : public KVEngine {
 public:
  PMemKVDK(const std::string& db_path);
  ~PMemKVDK();
  Status Put(const std::string& key, const std::string& value) override;
  Status Get(const std::string& key, std::string* value) override;
  Status Delete(const std::string& key) override;
  KVEngine::Iterator* NewIterator() override;

 private:
  kvdk::Engine* db_;
  kvdk::Configs configs_;
  std::string path_;
  std::string collection_;  // for sorted scan in kvdk
};

#if defined(BUILD_ROCKSDB)
// rocksdb engine implementation
class RocksEngine : public KVEngine {
 public:
  explicit RocksEngine(const std::string& db_path);
  ~RocksEngine();

  Status Put(const std::string& key, const std::string& value) override;
  Status Get(const std::string& key, std::string* value) override;
  Status Delete(const std::string& key) override;
  KVEngine::Iterator* NewIterator() override;

 private:
  std::string path_;
  // Test for compare with the other engine.
  rocksdb::DB* db_;
};
#endif

// A simple in memory engine with std::map to test in memory workload convenient
class MemoryEngine : public KVEngine {
 public:
  explicit MemoryEngine(const std::string& db_path) : path_(db_path) {}
  Status Get(const std::string& key, std::string* value) override {
    if (memory_db_.find(key) != memory_db_.end()) {
      value->assign(memory_db_[key]);
      return Status::Ok;
    }
    return Status::NotFound;
  }

  Status Put(const std::string& key, const std::string& value) override {
    memory_db_[key] = value;
    return Status::Ok;
  }

  Status Delete(const std::string& key) override {
    memory_db_.erase(key);
    return Status::Ok;
  }

  class MemoryIterator : public KVEngine::Iterator {
   public:
    explicit MemoryIterator(std::map<std::string, std::string>::iterator start,
                            std::map<std::string, std::string>::iterator end)
        : iter_(start), end_(end) {}
    virtual ~MemoryIterator() = default;

    void Seek(const std::string& key) override {
      while (iter_->first != key) {
        iter_++;
      }
    }

    void SeekToFirst() override {}
    void Next() override {
      if (iter_ != end_) iter_++;
    }
    void Prev() override { iter_--; }
    bool Valid() override { return iter_ != end_; }
    std::string Key() override { return iter_->first; }
    std::string Value() override { return iter_->second; }

   private:
    std::map<std::string, std::string>::iterator iter_;
    std::map<std::string, std::string>::iterator end_;
  };

  KVEngine::Iterator* NewIterator() override {
    return new MemoryIterator(memory_db_.begin(), memory_db_.end());
  }

 private:
  std::string path_;
  std::map<std::string, std::string> memory_db_;
};

// Construct the engine's map with their engine name.
inline void Initial() {
  static bool init = false;
  if (!init) {
    static EngineImplRegister<KVEngine, PMemKVDK> pmem_kvdk("kvdk");
    static EngineImplRegister<KVEngine, MemoryEngine> memory_engine("memory");
#if defined(BUILD_ROCKSDB)
    static EngineImplRegister<KVEngine, RocksEngine> rocks_engine("rocksdb");
#endif
    init = true;
  }
}

// Create the engine name.
class CreateEngineByName {
 public:
  CreateEngineByName() = default;
  static KVEngine* Create(const std::string& name) {
    EngineFactory<KVEngine>& engine = EngineFactory<KVEngine>::Instance();
    return engine.GetEngine(name);
  }
};
