/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <regex>

#include "../alias.hpp"
#include "kvdk/volatile/configs.hpp"
#include "kvdk/volatile/engine.h"
#include "kvdk/volatile/engine.hpp"
#include "kvdk/volatile/iterator.hpp"
#include "kvdk/volatile/write_batch.hpp"

using kvdk::StringView;

using kvdk::Configs;
using kvdk::Engine;
using kvdk::HashIterator;
using kvdk::ListIterator;
using kvdk::Snapshot;
using kvdk::SortedCollectionConfigs;
using kvdk::SortedIterator;
using kvdk::WriteBatch;
using kvdk::WriteOptions;

extern "C" {

struct KVDKConfigs {
  Configs rep;
};

struct KVDKEngine {
  std::unique_ptr<Engine> rep;
};

struct KVDKWriteBatch {
  std::unique_ptr<WriteBatch> rep;
};

struct KVDKSortedIterator {
  SortedIterator* rep;
};

struct KVDKListIterator {
  ListIterator* rep;
};

struct KVDKHashIterator {
  HashIterator* rep;
};

struct KVDKSnapshot {
  Snapshot* rep;
};

struct KVDKWriteOptions {
  WriteOptions rep;
};

struct KVDKSortedCollectionConfigs {
  SortedCollectionConfigs rep;
};

struct KVDKRegex {
  std::regex rep;
};

inline char* CopyStringToChar(const std::string& str) {
  char* result = static_cast<char*>(malloc(str.size()));
  memcpy(result, str.data(), str.size());
  return result;
}

}  // extern "C"