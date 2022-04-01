/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "iterator.h"
#include "types.hpp"

namespace KVDK_NAMESPACE {

using IteratorType = KVDKIteratorType;

class Iterator {
 public:
  virtual void Seek(const std::string& key) = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual bool Valid() = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  virtual std::string Key() = 0;

  virtual std::string Value() = 0;

  virtual IteratorType Type() const = 0;

  virtual ~Iterator() = default;
};

class ListIterator {
 public:
  virtual void Seek(StringView key) = 0;

  virtual void Seek(IndexType pos) = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual bool Valid() const = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  virtual std::string Value() const = 0;

  virtual ~ListIterator() = default;
};

}  // namespace KVDK_NAMESPACE