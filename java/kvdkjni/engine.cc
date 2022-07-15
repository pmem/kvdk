/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <assert.h>

#include "include/io_pmem_kvdk_Engine.h"
#include "kvdkjni/kvdkjni.h"

/**
 * Class:     io_pmem_kvdk_Engine
 * Method:    open
 * Signature: (Ljava/lang/String;J)V
 */
jlong Java_io_pmem_kvdk_Engine_open(
  JNIEnv* env, jclass, jstring jengine_path, jlong jcfg_handle) {
  const char* engine_path_chars = env->GetStringUTFChars(jengine_path, nullptr);
  if (engine_path_chars == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  std::string engine_path_str = engine_path_chars;

  auto* cfg = reinterpret_cast<KVDK_NAMESPACE::Configs*>(jcfg_handle);
  KVDK_NAMESPACE::Engine* engine = nullptr;
  KVDK_NAMESPACE::Status s =
      KVDK_NAMESPACE::Engine::Open(engine_path_str, &engine, *cfg, stdout);

  env->ReleaseStringUTFChars(jengine_path, engine_path_chars);

  if (s == KVDK_NAMESPACE::Status::Ok) {
    return GET_CPLUSPLUS_POINTER(engine);
  } else {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/**
 * Class:     io_pmem_kvdk_Engine
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Engine_closeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* engine = reinterpret_cast<kvdk::Engine *>(handle);
  assert(engine != nullptr);
  delete engine;
}
