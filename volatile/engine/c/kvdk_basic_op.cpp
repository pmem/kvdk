/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "kvdk_c.hpp"

extern "C" {
KVDKRegex* KVDKRegexCreate(char const* data, size_t len) {
  KVDKRegex* re = new KVDKRegex;
  re->rep = std::regex{data, len};
  return re;
}

void KVDKRegexDestroy(KVDKRegex* re) { delete re; }

KVDKConfigs* KVDKCreateConfigs() { return new KVDKConfigs; }

void KVDKSetConfigs(KVDKConfigs* kv_config, uint64_t max_access_threads,
                    uint64_t hash_bucket_num, uint32_t num_buckets_per_slot) {
  kv_config->rep.max_access_threads = max_access_threads;
  kv_config->rep.hash_bucket_num = hash_bucket_num;
  kv_config->rep.num_buckets_per_slot = num_buckets_per_slot;
}

void KVDKConfigRegisterCompFunc(KVDKConfigs* kv_config,
                                const char* compara_name, size_t compara_len,
                                int (*compare)(const char* src, size_t src_len,
                                               const char* target,
                                               size_t target_len)) {
  auto comp_func = [compare](const StringView& src,
                             const StringView& target) -> int {
    return compare(src.data(), src.size(), target.data(), target.size());
  };
  kv_config->rep.comparator.RegisterComparator(
      StringView(compara_name, compara_len), comp_func);
}

void KVDKDestroyConfigs(KVDKConfigs* kv_config) { delete kv_config; }

KVDKWriteOptions* KVDKCreateWriteOptions(void) { return new KVDKWriteOptions; }

void KVDKDestroyWriteOptions(KVDKWriteOptions* kv_options) {
  delete kv_options;
}

void KVDKWriteOptionsSetTTLTime(KVDKWriteOptions* kv_options,
                                int64_t ttl_time) {
  kv_options->rep.ttl_time = ttl_time;
}

void KVDKWriteOptionsSetUpdateTTL(KVDKWriteOptions* kv_options,
                                  int update_ttl) {
  kv_options->rep.update_ttl = update_ttl;
}

KVDKStatus KVDKOpen(const char* name, const KVDKConfigs* config, FILE* log_file,
                    KVDKEngine** kv_engine) {
  Engine* engine;
  KVDKStatus s =
      Engine::Open(std::string(name), &engine, config->rep, log_file);
  *kv_engine = nullptr;
  if (s != KVDKStatus::Ok) {
    return s;
  }
  *kv_engine = new KVDKEngine;
  (*kv_engine)->rep.reset(engine);
  return s;
}

KVDKStatus KVDKBackup(KVDKEngine* engine, const char* backup_path,
                      size_t backup_path_len, KVDKSnapshot* snapshot) {
  return engine->rep->Backup(StringView(backup_path, backup_path_len),
                             snapshot->rep);
}

KVDKStatus KVDKRestore(const char* name, const char* backup_log,
                       const KVDKConfigs* config, FILE* log_file,
                       KVDKEngine** kv_engine) {
  Engine* engine;
  KVDKStatus s = Engine::Restore(std::string(name), std::string(backup_log),
                                 &engine, config->rep, log_file);
  if (s == KVDKStatus::Ok) {
    *kv_engine = new KVDKEngine;
    (*kv_engine)->rep.reset(engine);
  }
  return s;
}

KVDKSnapshot* KVDKGetSnapshot(KVDKEngine* engine, int make_checkpoint) {
  KVDKSnapshot* snapshot = new KVDKSnapshot;
  snapshot->rep = engine->rep->GetSnapshot(make_checkpoint);
  return snapshot;
}

void KVDKReleaseSnapshot(KVDKEngine* engine, KVDKSnapshot* snapshot) {
  engine->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

void KVDKCloseEngine(KVDKEngine* engine) { delete engine; }

int KVDKRegisterCompFunc(KVDKEngine* engine, const char* compara_name,
                         size_t compara_len,
                         int (*compare)(const char* src, size_t src_len,
                                        const char* target,
                                        size_t target_len)) {
  auto comp_func = [compare](const StringView& src,
                             const StringView& target) -> int {
    return compare(src.data(), src.size(), target.data(), target.size());
  };
  return engine->rep->registerComparator(StringView(compara_name, compara_len),
                                         comp_func);
}

KVDKStatus KVDKExpire(KVDKEngine* engine, const char* str, size_t str_len,
                      int64_t ttl_time) {
  return engine->rep->Expire(StringView{str, str_len}, ttl_time);
}

KVDKStatus KVDKGetTTL(KVDKEngine* engine, const char* str, size_t str_len,
                      int64_t* ttl_time) {
  return engine->rep->GetTTL(StringView{str, str_len}, ttl_time);
}

KVDKStatus KVDKTypeOf(KVDKEngine* engine, char const* key_data, size_t key_len,
                      KVDKValueType* type) {
  return engine->rep->TypeOf(StringView{key_data, key_len}, type);
}
}

// List
