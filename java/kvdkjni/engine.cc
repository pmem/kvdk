/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <jni.h>

#include <assert.h>

#include "kvdk/engine.hpp"

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
