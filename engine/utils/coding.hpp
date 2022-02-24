/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <ctype.h>

#include <string>

#include "../alias.hpp"

namespace KVDK_NAMESPACE {

static std::string EncodeInt64(uint64_t value) {
  return std::string((char*)&value, sizeof(uint64_t));
}

static uint64_t DecodeInt64(const StringView& src) {
  uint64_t ret = *(uint64_t*)src.data();
  return ret;
}

static void AppendInt64(std::string* dst, uint64_t value) {
  dst->append((char*)&value, sizeof(uint64_t));
}

static uint64_t FetchInt64(StringView* src) {
  uint64_t ret = *(uint64_t*)src->data();
  *src = StringView(src->data() + sizeof(uint64_t),
                    src->size() - sizeof(uint64_t));
  return ret;
}

static std::string EncodeInt32(uint32_t value) {
  return std::string((char*)&value, sizeof(uint32_t));
}

static uint32_t DecodeInt32(const StringView& src) {
  uint32_t ret = *(uint32_t*)src.data();
  return ret;
}

static void AppendInt32(std::string* dst, uint32_t value) {
  dst->append((char*)&value, sizeof(uint32_t));
}

static uint32_t FetchInt32(StringView* src) {
  uint32_t ret = *(uint32_t*)src->data();
  *src = StringView(src->data() + sizeof(uint32_t),
                    src->size() - sizeof(uint32_t));
  return ret;
}

static void AppendFixedString(std::string* dst, const StringView& str) {
  AppendInt32(dst, str.size());
  dst->append(str.data(), str.size());
}

static std::string FetchFixedString(StringView* src) {
  uint32_t size = FetchInt32(src);
  std::string ret(src->data(), size);
  *src = StringView(src->data() + size, src->size() - size);
  return ret;
}

}  // namespace KVDK_NAMESPACE