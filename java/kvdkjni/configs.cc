/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <jni.h>

#include <assert.h>

#include "cplusplus_to_java_convert.h"

#include "kvdk/engine.hpp"


/**
 * Class:     io_pmem_kvdk_Configs
 * Method:    newConfigs
 * Signature: ()J
 */
jlong Java_io_pmem_kvdk_Configs_newConfigs(
    JNIEnv*, jclass) {
  auto* cfg = new kvdk::Configs();
  return GET_CPLUSPLUS_POINTER(cfg);
}

/**
 * Class:     io_pmem_kvdk_Configs
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Configs_closeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* cfg = reinterpret_cast<kvdk::Configs *>(handle);
  assert(cfg != nullptr);
  delete cfg;
}
