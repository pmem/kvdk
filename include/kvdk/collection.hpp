/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstring>
#include <string>

#include "kvdk/namespace.hpp"
#include "libpmemobj++/string_view.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using ExpiredTimeType = std::int64_t;
using CollectionIDType = std::uint64_t;
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

  ExpiredTimeType ExpireTime() const { return expire_time; }
  virtual bool HasExpired() const = 0;
  virtual void ExpireAt(ExpiredTimeType) = 0;

  // Return internal representation of "key" in the collection
  // By default, we concat key with the collection id
  std::string InternalKey(const StringView& key) {
    return makeInternalKey(key, ID());
  }

  inline static StringView ExtractUserKey(const StringView& internal_key) {
    constexpr size_t sz_id = sizeof(CollectionIDType);
    return StringView(internal_key.data() + sz_id, internal_key.size() - sz_id);
  }

  inline static CollectionIDType ExtractID(const StringView& internal_key) {
    CollectionIDType id;
    memcpy(&id, internal_key.data(), sizeof(CollectionIDType));
    return id;
  }

  inline static std::string ID2String(CollectionIDType id) {
    return std::string{reinterpret_cast<char*>(&id), sizeof(CollectionIDType)};
  }

 protected:
  inline static std::string makeInternalKey(const StringView& user_key,
                                            CollectionIDType list_id) {
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