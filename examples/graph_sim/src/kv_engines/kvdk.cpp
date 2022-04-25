/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <gflags/gflags.h>

#include "KVEngine.hpp"

DEFINE_string(kvdk_collection, "",
              "The collection for construct sorted structure in kvdk.");
DEFINE_int64(kvdk_pmem_file_size, 100ULL << 30,
             "The size of kvdk pmem file size.");
DEFINE_string(kvdk_path, "/mnt/pmem0/kvdk", "The path of the kvdk pmem file.");
DEFINE_int64(kvdk_max_access_threads, 48,
             "The max access threads number in kvdk.");

PMemKVDK::PMemKVDK(const std::string&) {
  path_ = FLAGS_kvdk_path;
  collection_ = FLAGS_kvdk_collection;

  // set some options
  configs_.pmem_file_size = FLAGS_kvdk_pmem_file_size;
  configs_.max_access_threads = FLAGS_kvdk_max_access_threads;

  auto s = kvdk::Engine::Open(path_, &db_, configs_);
  if (s != kvdk::Status::Ok) {
    SimpleLoger("KVDK Open failed");
    return;
  }
  s = db_->CreateSortedCollection(collection_);
  if (s != kvdk::Status::Ok) {
    SimpleLoger("Create Sorted Collection Failed!");
    return;
  }
}

PMemKVDK::~PMemKVDK() { delete db_; }

Status PMemKVDK::Put(const std::string& key, const std::string& value) {
  Status s;
  if (!collection_.empty()) {
    s = db_->SSet(collection_, key, value);
  } else {
    s = db_->Set(key, value);
  }
  if (s != kvdk::Status::Ok) {
    SimpleLoger("KVDK Set failed");
  }
  return s;
}

Status PMemKVDK::Get(const std::string& key, std::string* value) {
  Status s;
  if (!collection_.empty()) {
    s = db_->SGet(collection_, key, value);
  } else {
    s = db_->Get(key, value);
  }
  return s;
}

Status PMemKVDK::Delete(const std::string& key) {
  return (!collection_.empty()) ? db_->SDelete(collection_, key)
                                : db_->Delete(key);
}

class PMemKVDKIterator : public KVEngine::Iterator {
 public:
  explicit PMemKVDKIterator(kvdk::Iterator* it, kvdk::Engine* engine)
      : iter_(it), engine_(engine) {}

  ~PMemKVDKIterator() {
    if (iter_) {
      engine_->ReleaseSortedIterator(iter_);
    }
  }

  void Seek(const std::string& key) override { iter_->Seek(key); }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }
  bool Valid() override { return iter_->Valid(); }
  std::string Key() override { return iter_->Key(); }
  std::string Value() override { return iter_->Value(); }

 private:
  kvdk::Iterator* iter_ = nullptr;
  kvdk::Engine* engine_ = nullptr;
};

KVEngine::Iterator* PMemKVDK::NewIterator() {
  if (!collection_.empty()) {
    // TODO fix snapshot
    auto it = db_->NewSortedIterator(collection_);
    return new PMemKVDKIterator(it, db_);
  }
  return nullptr;
}
