//
// Created by zhanghuigui on 2021/10/18.
//

#include "KVEngine.hpp"

#include <gflags/gflags.h>

DECLARE_string(topn_collection);

PMemKVDK::PMemKVDK(const std::string &db_path) {
  path_ = "/mnt/pmem/kvdk";
  collection_ = FLAGS_topn_collection;

  // set some options
  options_.pmem_file_size = 100ULL << 30;
  options_.use_devdax_mode = false;

  auto s = kvdk::Engine::Open(path_, &db_, options_);
  if (s != kvdk::Status::Ok) {
    SimpleLoger("KVDK Open failed");
    return;
  }
}

PMemKVDK::~PMemKVDK() { delete db_; }

Status PMemKVDK::Put(const std::string &key, const std::string &value) {
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

Status PMemKVDK::Get(const std::string &key, std::string *value) {
  Status s;
  if (!collection_.empty()) {
    s = db_->SGet(collection_, key, value);
  } else {
    s = db_->Get(key, value);
  }
  if (s != kvdk::Status::Ok) {
    if (s == kvdk::Status::NotFound) {
      SimpleLoger("KVDK Get " + key + " not found");
      value->clear();
      return s;
    }
    SimpleLoger("KVDK Get failed");
  }
  return s;
}

Status PMemKVDK::Delete(const std::string &key) {
  return (!collection_.empty()) ? db_->SDelete(collection_, key)
                                : db_->Delete(key);
}

class PMemKVDKIterator : public KVEngine::Iterator {
 public:
  explicit PMemKVDKIterator(Iterator *it) : iter_(it) {}
  ~PMemKVDKIterator() = default;

  void Seek(const std::string &key) override { iter_->Seek(key); }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }
  bool Valid() override { iter_->Valid(); }
  std::string Key() override { iter_->Key(); }
  std::string Value() override { iter_->Value(); }

 private:
  Iterator *iter_;
};

KVEngine::Iterator *PMemKVDK::NewIterator() {
  auto it = db_->NewSortedIterator(collection_);
  return new PMemKVDKIterator(reinterpret_cast<Iterator *>(it.get()));
}
