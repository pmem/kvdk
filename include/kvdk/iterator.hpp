/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "types.hpp"

namespace KVDK_NAMESPACE {

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
};

}  // namespace KVDK_NAMESPACE