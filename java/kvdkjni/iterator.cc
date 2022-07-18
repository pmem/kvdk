/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <assert.h>

#include "include/io_pmem_kvdk_Iterator.h"
#include "kvdkjni/kvdkjni.h"

/**
 * Class:     io_pmem_kvdk_Iterator
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Iterator_closeInternal(
    JNIEnv*, jobject, jlong iterator_handle, jlong engine_handle) {
  auto* iterator = reinterpret_cast<kvdk::Iterator *>(iterator_handle);
  auto* engine = reinterpret_cast<kvdk::Engine *>(engine_handle);
  assert(iterator != nullptr);
  assert(engine != nullptr);
  
  engine->ReleaseSortedIterator(iterator);
}

