/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"
#include "iostream"

namespace PMEMDB_NAMESPACE {

Status KVEngine::Open(const std::string &name, DB **dbptr,
                      const DBOptions &options) {
  KVEngine *db = new KVEngine();
  db->Init(name, options);
  *dbptr = db;
  return Status::Ok;
}

KVEngine::KVEngine() {}

void KVEngine::Init(const std::string &name, const DBOptions &options) {
  pmem_.Init(name, options);
}

Status KVEngine::Get(const std::string &key, std::string *value) {
  return pmem_.Get(key, value);
}

Status KVEngine::Set(const std::string &key, const std::string &value) {
  // assert(key.size() < (1<<16) && value.size() < (1<<16));
  return pmem_.Set(key, value);
}

KVEngine::~KVEngine() {}

} // namespace PMEMDB_NAMESPACE