/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstring>
#include <string>

#include "../engine/alias.hpp"
#include "../engine/macros.hpp"
#include "../engine/utils/coding.hpp"
#include "../engine/utils/utils.hpp"
#include "kvdk/namespace.hpp"
#include "libpmemobj++/string_view.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;

// A collection of key-value pairs
class Collection {
 public:
  Collection(const std::string& name, CollectionIDType id,
             ExpiredTimeType t = ExpiredTimeType{})
      : collection_name_(name), collection_id_(id), expire_time{t} {}
  // Return unique ID of the collection
  uint64_t ID() const { return collection_id_; }

  // Return name of the collection
  const std::string& Name() const { return collection_name_; }

  ExpiredTimeType ExpireTime() const { return ExpireTime(); }
  bool HasExpired() const { return TimeUtils::CheckIsExpired(expire_time); }
  virtual void ExpireAt(ExpiredTimeType) = 0;

  // Return internal representation of "key" in the collection
  // By default, we concat key with the collection id
  std::string InternalKey(const StringView& key) {
    return makeInternalKey(key, ID());
  }

  inline static StringView ExtractUserKey(const StringView& internal_key) {
    constexpr size_t sz_id = sizeof(CollectionIDType);
    kvdk_assert(sz_id <= internal_key.size(),
                "internal_key does not has space for key");
    return StringView(internal_key.data() + sz_id, internal_key.size() - sz_id);
  }

  inline static CollectionIDType ExtractID(const StringView& internal_key) {
    kvdk_assert(sizeof(CollectionIDType) <= internal_key.size(),
                "internal_key does not has space for id");
    CollectionIDType id;
    memcpy(&id, internal_key.data(), sizeof(CollectionIDType));
    return id;
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
  // To simplify code base, expire_time field is added.
  // However, it's up to inherited class to maintain that field.
  // Always update this field in ExpireAt()!
  ExpiredTimeType expire_time;
};
}  // namespace KVDK_NAMESPACE