/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#ifdef _WIN32
#ifdef KVDK_DLL
#ifdef KVDK_LIBRARY_EXPORTS
#define KVDK_LIBRARY_API __declspec(dllimport)
#else
#define KVDK_LIBRARY_API __declspec(dllexport)
#endif
#else
#define KVDK_LIBRARY_API
#endif
#else
#define KVDK_LIBRARY_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  Ok = 1,
  NotFound,
  MemoryOverflow,
  PmemOverflow,
  NotSupported,
  MapError,
  BatchOverflow,
  TooManyWriteThreads,
  InvalidDataSize,
  IOError,
  InvalidConfiguration,
  Abort,
} KVDKStatus;

#ifdef __cplusplus
} /* end extern "C" */
#endif