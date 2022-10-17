/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <regex>

#include "types.hpp"

namespace KVDK_NAMESPACE {

class SortedIterator {
 public:
  virtual void Seek(const std::string& key) = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual bool Valid() = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  virtual std::string Key() = 0;

  virtual std::string Value() = 0;

  virtual ~SortedIterator() = default;
};

class ListIterator {
 public:
  virtual void Seek(long index) = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToFirst(StringView elem) = 0;

  virtual void SeekToLast() = 0;

  virtual void SeekToLast(StringView elem) = 0;

  virtual void Next() = 0;

  virtual void Next(StringView elem) = 0;

  virtual void Prev() = 0;

  virtual void Prev(StringView elem) = 0;

  virtual bool Valid() const = 0;

  virtual std::string Value() const = 0;

  virtual ~ListIterator() = default;
};

class HashIterator {
 public:
  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual bool Valid() const = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  virtual std::string Key() const = 0;

  virtual std::string Value() const = 0;

  virtual bool MatchKey(std::regex const& re) = 0;

  virtual ~HashIterator() = default;
};

}  // namespace KVDK_NAMESPACE