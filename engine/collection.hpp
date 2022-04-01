/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cassert>
#include <cinttypes>
#include <cstring>
#include <string>

#include "alias.hpp"
#include "kvdk/status.hpp"

namespace KVDK_NAMESPACE {
/// TODO: (ziyan) add expire_time field to Collection.
class Collection {
 public:
  Collection(const std::string& name, CollectionIDType id)
      : collection_name_(name), collection_id_(id) {}
  // Return unique ID of the collection
  uint64_t ID() const { return collection_id_; }

  // Return name of the collection
  const std::string& Name() const { return collection_name_; }

  virtual ExpireTimeType GetExpireTime() const = 0;
  virtual bool HasExpired() const = 0;
  virtual Status SetExpireTime(ExpireTimeType) = 0;

  // Return internal representation of "key" in the collection
  // By default, we concat key with the collection id
  std::string InternalKey(const StringView& key) {
    return makeInternalKey(key, ID());
  }

  inline static std::string EncodeID(CollectionIDType id) {
    return EncodeUint64(id);
  }

  inline static CollectionIDType DecodeID(const StringView& string_id) {
    CollectionIDType id;
    bool ret = DecodeUint64(string_id, &id);
    kvdk_assert(ret, "size of string id does not match CollectionIDType size!");
    return id;
  }

  inline static StringView ExtractUserKey(const StringView& internal_key) {
    constexpr size_t sz_id = sizeof(CollectionIDType);
    kvdk_assert(sz_id <= internal_key.size(),
                "internal_key does not has space for key");
    return StringView(internal_key.data() + sz_id, internal_key.size() - sz_id);
  }

  inline static uint64_t ExtractID(const StringView& internal_key) {
    return DecodeID(internal_key);
  }

  inline static std::string ID2String(CollectionIDType id) {
    return std::string{reinterpret_cast<char*>(&id), sizeof(CollectionIDType)};
  }

 protected:
  inline static std::string makeInternalKey(const StringView& user_key,
                                            uint64_t list_id) {
    return ID2String(list_id).append(user_key.data(), user_key.size());
  }

  std::string collection_name_;
  CollectionIDType collection_id_;
};
}  // namespace KVDK_NAMESPACE