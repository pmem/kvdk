/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kvdk/volatile/engine.hpp"

#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {
Status Engine::Open(const StringView name, Engine** engine_ptr,
                    const Configs& configs, FILE* log_file) {
  GlobalLogger.Init(log_file, configs.log_level);
  Status s = KVEngine::Open(name, engine_ptr, configs);
  return s;
}

Status Engine::Restore(const StringView engine_path,
                       const StringView backup_file, Engine** engine_ptr,
                       const Configs& configs, FILE* log_file) {
  GlobalLogger.Init(log_file, configs.log_level);
  Status s = KVEngine::Restore(engine_path, backup_file, engine_ptr, configs);
  return s;
}

Engine::~Engine() {}
}  // namespace KVDK_NAMESPACE