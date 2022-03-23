/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <ctype.h>

#include <string>

#include "../define.hpp"

namespace KVDK_NAMESPACE {

static std::string EncodeInt64(uint64_t value) {
  return std::string((char*)&value, sizeof(uint64_t));
}

static bool DecodeInt64(const StringView& src, uint64_t* value) {
  if (src.size() < sizeof(uint64_t)) {
    return false;
  }
  *value = *(uint64_t*)src.data();
  return true;
}

static void AppendInt64(std::string* dst, uint64_t value) {
  dst->append((char*)&value, sizeof(uint64_t));
}

static bool FetchInt64(StringView* src, uint64_t* value) {
  if (src->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = *(uint64_t*)src->data();
  *src = StringView(src->data() + sizeof(uint64_t),
                    src->size() - sizeof(uint64_t));
  return true;
}

static std::string EncodeInt32(uint32_t value) {
  return std::string((char*)&value, sizeof(uint32_t));
}

static bool DecodeInt32(const StringView& src, uint32_t* value) {
  if (src.size() < sizeof(uint32_t)) {
    return false;
  }
  *value = *(uint32_t*)src.data();
  return true;
}

static void AppendInt32(std::string* dst, uint32_t value) {
  dst->append((char*)&value, sizeof(uint32_t));
}

static bool FetchInt32(StringView* src, uint32_t* value) {
  if (src->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = *(uint32_t*)src->data();
  *src = StringView(src->data() + sizeof(uint32_t),
                    src->size() - sizeof(uint32_t));
  return true;
}

static void AppendFixedString(std::string* dst, const StringView& str) {
  AppendInt32(dst, str.size());
  dst->append(str.data(), str.size());
}

static bool FetchFixedString(StringView* src, std::string* value) {
  uint32_t size;
  bool ret = FetchInt32(src, &size) && src->size() >= size;
  if (ret) {
    value->assign(src->data(), size);
    *src = StringView(src->data() + size, src->size() - size);
  }
  return ret;
}

}  // namespace KVDK_NAMESPACE