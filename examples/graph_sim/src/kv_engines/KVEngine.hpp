//
// Created by zhanghuigui on 2021/10/18.
//

#pragma once

#include <cstdint>
#include <cstdio>
#include <kvdk/engine.hpp>
#include <map>
#include <string>
#include <vector>

#include "engine_factory.hpp"

typedef kvdk::Status Status;

static void SimpleLoger(const std::string& content) {
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

    virtual std::string Key() = 0;
    virtual std::string Value() = 0;
  };

  virtual Status Get(const std::string& key, std::string* value) = 0;
  virtual Status Put(const std::string& key, const std::string& value) = 0;
  virtual Status Delete(const std::string& key) = 0;
  virtual Iterator* NewIterator() = 0;
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
  kvdk::Configs options_;
  std::string path_;
  std::string collection_;  // for sorted scan in kvdk
};

// rocksdb engine implementation
class RocksEngine : public KVEngine {
 public:
  explicit RocksEngine(const std::string& db_path) : path_(db_path) {}
  Status Get(const std::string& key, std::string* value) override {
    // return db_->Get(rocksdb::ReadOptions(), const std::string& key,
    // std::string* value);
    return Status::Ok;
  }
  Status Put(const std::string& key, const std::string& value) override {
    // return db_->Put(rocksdb::WriteOptions(), const std::string& key, const
    // std::string& value);
    return Status::Ok;
  }
  Status Delete(const std::string& key) override {
    // return db_->Delete(rocksdb::WriteOptions(), const std::string& key);
    return Status::Ok;
  }

  // We should keep the comment for other users who want to test with rocksdb
  //
  // class RocksdbIterator : public KVEngine::Iterator {
  // public:
  // 	explicit RocksdbIterator(rocksdb::Iterator* it) : itr_(it) {}
  // 	~RocksdbIterator() { delete iter_;}

  // 	void Seek(const std::string& key) { return itr_->Seek(key); }
  // 	void SeekToFirst() { return itr_->SeekToFirst(); }
  // 	void Next() { return itr_->Next(); }
  // 	void Prev() { return itr_->Prev(); }
  // 	bool Valid() { return itr_->Valid(); }

  // 	std::string Key() { return itr_->Key().ToString(); }
  // 	std::string Value() { return itr_->Value().ToString(); }
  // private:
  // 	rocksdb::Iterator* itr_;
  // };

  // Iterator* NewIterator() override {
  // 	rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
  // 	return RocksdbIterator(it);
  // }

 private:
  std::string path_;
  // Test for compare with the other engine.
  // rocksdb::DB* db_;
};

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
    explicit MemoryIterator(std::map<std::string, std::string>::iterator it)
        : iter_(it) {}
    ~MemoryIterator() = default;

    void Seek(const std::string& key) override {
      if (iter_->first == key) {
        return;
      }
      iter_++;
    }

    void SeekToFirst() override {}
    void Next() override { iter_++; }
    void Prev() override { iter_--; }
    bool Valid() override {
      return iter_->first.empty() && iter_->second.empty();
    }
    std::string Key() override { return iter_->first; }
    std::string Value() override { return iter_->second; }

   private:
    std::map<std::string, std::string>::iterator iter_;
  };

  KVEngine::Iterator* NewIterator() override {
    return new MemoryIterator(memory_db_.begin());
  }

 private:
  std::string path_;
  std::map<std::string, std::string> memory_db_;
};

// Construct the engine's map with their engin name.
static void Initial() {
  static bool init = false;
  if (!init) {
    static EngineImplRegister<KVEngine, PMemKVDK> pmem_kvdk("kvdk");
    static EngineImplRegister<KVEngine, RocksEngine> rocks_engine("rocksdb");
    // static EngineImplRegister<KVEngine, LevelEngine> level_engine("leveldb");
    static EngineImplRegister<KVEngine, MemoryEngine> memory_engine("memory");
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
