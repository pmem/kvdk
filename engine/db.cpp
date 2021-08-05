/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "pmemdb/db.hpp"
#include "kv_engine.hpp"

namespace PMEMDB_NAMESPACE {

Status DB::Open(const std::string &name, DB **dbptr, const DBOptions &options,
                FILE *log_file) {
#ifdef DO_LOG
  GlobalLogger.Init(log_file);
#endif
  return KVEngine::Open(name, dbptr, options);
}

DB::~DB() {}
} // namespace PMEMDB_NAMESPACE