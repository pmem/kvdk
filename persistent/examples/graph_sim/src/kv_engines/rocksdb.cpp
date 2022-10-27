/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <gflags/gflags.h>

#include "KVEngine.hpp"

#if defined(BUILD_ROCKSDB)

DEFINE_string(rocksdb_path, "rocksdb", "The path of the rocksdb.");
DEFINE_int64(rocksdb_block_cache_size, 42949672960,
             "The size of the block cache.");

// We should keep the comment for other users who want to test with rocksdb
//
class RocksdbIterator : public KVEngine::Iterator {
 public:
  explicit RocksdbIterator(rocksdb::Iterator* it) : itr_(it) {}
  virtual ~RocksdbIterator() { delete itr_; }

  void Seek(const std::string& key) { itr_->Seek(key); }
  void SeekToFirst() { itr_->SeekToFirst(); }
  void Next() { itr_->Next(); }
  void Prev() { itr_->Prev(); }
  bool Valid() { return itr_->Valid(); }

  std::string Key() { return itr_->key().ToString(); }
  std::string Value() { return itr_->value().ToString(); }

 private:
  rocksdb::Iterator* itr_;
};

RocksEngine::RocksEngine(const std::string& db_path) {
  rocksdb::Options opts;
  opts.create_if_missing = true;

  // Good options for write
  opts.max_write_buffer_number = 16;
  opts.max_background_compactions = 16;
  opts.max_background_flushes = 8;
  opts.max_subcompactions = 4;
  opts.compression = rocksdb::kNoCompression;

  // Good options for read and control the total memory
  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(16, false));
  table_options.block_cache = rocksdb::NewLRUCache(
      FLAGS_rocksdb_block_cache_size);  // 40G blockcache size
  opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  auto s = rocksdb::DB::Open(opts, FLAGS_rocksdb_path, &db_);
  if (s != rocksdb::Status::OK()) {
    SimpleLoger("Rocksdb open failed.\n");
    return;
  }
}

RocksEngine::~RocksEngine() { delete db_; }

Status RocksEngine::Get(const std::string& key, std::string* value) {
  auto s = db_->Get(rocksdb::ReadOptions(), key, value);
  if (s == rocksdb::Status::OK()) {
    return Status::Ok;
  } else if (s == rocksdb::Status::NotFound()) {
    return Status::NotFound;
  } else {
    return Status::Abort;
  }
}

Status RocksEngine::Put(const std::string& key, const std::string& value) {
  auto s = db_->Put(rocksdb::WriteOptions(), key, value);
  return s == rocksdb::Status::OK() ? Status::Ok : Status::Abort;
}
Status RocksEngine::Delete(const std::string& key) {
  auto s = db_->Delete(rocksdb::WriteOptions(), key);
  return s == rocksdb::Status::OK() ? Status::Ok : Status::Abort;
}

KVEngine::Iterator* RocksEngine::NewIterator() {
  auto it = db_->NewIterator(rocksdb::ReadOptions());
  return new RocksdbIterator(it);
}
#endif  // defined to use rocksdb
