/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cinttypes>
#include <string>

#include "../alias.hpp"
#include "../macros.hpp"

namespace KVDK_NAMESPACE {

inline std::string EncodeUint64(uint64_t value) {
  return std::string((char*)&value, sizeof(uint64_t));
}

inline bool DecodeUint64(const StringView& src, uint64_t* value) {
  if (src.size() < sizeof(uint64_t)) {
    return false;
  }
  *value = *(uint64_t*)src.data();
  return true;
}

inline void AppendUint64(std::string* dst, uint64_t value) {
  dst->append((char*)&value, sizeof(uint64_t));
}

inline bool FetchUint64(StringView* src, uint64_t* value) {
  if (src->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = *(uint64_t*)src->data();
  *src = StringView(src->data() + sizeof(uint64_t),
                    src->size() - sizeof(uint64_t));
  return true;
}

inline std::string EncodeUint32(uint32_t value) {
  return std::string((char*)&value, sizeof(uint32_t));
}

inline bool DecodeUint32(const StringView& src, uint32_t* value) {
  if (src.size() < sizeof(uint32_t)) {
    return false;
  }
  *value = *(uint32_t*)src.data();
  return true;
}

inline void AppendUint32(std::string* dst, uint32_t value) {
  dst->append((char*)&value, sizeof(uint32_t));
}

inline bool FetchUint32(StringView* src, uint32_t* value) {
  if (src->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = *(uint32_t*)src->data();
  *src = StringView(src->data() + sizeof(uint32_t),
                    src->size() - sizeof(uint32_t));
  return true;
}

// POD stands for Plain-old-data.
// We don't serialize object which may have pointers to other objects.
template <typename T>
inline void AppendPOD(std::string* dst, T const& value) {
  dst->append(reinterpret_cast<char const*>(&value), sizeof(T));
}

template <typename T>
inline T FetchPOD(StringView* src) {
  kvdk_assert(src->size() >= sizeof(T), "");
  T ret = *reinterpret_cast<T const*>(src->data());
  *src = StringView{src->data() + sizeof(T), src->size() - sizeof(T)};
  return ret;
}

inline void AppendFixedString(std::string* dst, const StringView& str) {
  AppendUint32(dst, str.size());
  dst->append(str.data(), str.size());
}

inline bool FetchFixedString(StringView* src, std::string* value) {
  uint32_t size;
  bool ret = FetchUint32(src, &size) && src->size() >= size;
  if (ret) {
    value->assign(src->data(), size);
    *src = StringView(src->data() + size, src->size() - size);
  }
  return ret;
}

}  // namespace KVDK_NAMESPACE