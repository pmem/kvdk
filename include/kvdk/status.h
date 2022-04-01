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

#define FOREACH_ENUM(GEN)   \
  GEN(Ok)                   \
  GEN(NotFound)             \
  GEN(Expired)              \
  GEN(WrongType)            \
  GEN(OperationFail)        \
  GEN(OutOfRange)           \
  GEN(MemoryOverflow)       \
  GEN(PmemOverflow)         \
  GEN(NotSupported)         \
  GEN(PMemMapFileError)     \
  GEN(BatchOverflow)        \
  GEN(TooManyAccessThreads) \
  GEN(InvalidDataSize)      \
  GEN(InvalidArgument)      \
  GEN(IOError)              \
  GEN(InvalidConfiguration) \
  GEN(Fail)                 \
  GEN(Abort)
#define GENERATE_ENUM(ENUM) ENUM,
#define GENERATE_STRING(STRING) #STRING,

typedef enum { FOREACH_ENUM(GENERATE_ENUM) } KVDKStatus;

// static char const* KVDKStatusStrings[] = {FOREACH_ENUM(GENERATE_STRING)};

#ifdef __cplusplus
} /* end extern "C" */
#endif