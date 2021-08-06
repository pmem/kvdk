/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "namespace.hpp"

namespace KVDK_NAMESPACE {

class Iterator {
public:
  virtual void Seek(const std::string &key) = 0;

  virtual void SeekToFirst() = 0;

  virtual bool Valid() = 0;

  virtual bool Next() = 0;

  virtual bool Prev() = 0;

  virtual std::string Key() = 0;

  virtual std::string Value() = 0;
};

} // namespace KVDK_NAMESPACE