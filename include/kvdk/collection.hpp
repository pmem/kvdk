/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <string>

#include "kvdk/namespace.hpp"
#include "../engine/macros.hpp"
#include "../engine/alias.hpp"

#include "libpmemobj++/string_view.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;

// A collection of key-value pairs
class Collection {
 public:
  Collection(const std::string& name, uint64_t id)
      : collection_name_(name), collection_id_(id) {}
  // Return unique ID of the collection
  uint64_t ID() const { return collection_id_; }

  // Return name of the collection
  const std::string& Name() const { return collection_name_; }

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

inline static uint64_t ExtractID(const StringView& internal_key) {
  CollectionIDType id;
  memcpy(&id, internal_key.data(), sizeof(CollectionIDType));
  return id;
}

inline static std::string ID2String(CollectionIDType id) {
  return std::string(reinterpret_cast<char*>(&id), sizeof(CollectionIDType));
}

inline static CollectionIDType string2ID(const StringView& string_id) {
  CollectionIDType id;
  kvdk_assert(sizeof(CollectionIDType) <= string_id.size(),
              "size of string id does not match CollectionIDType size!");
  memcpy(&id, string_id.data(), sizeof(CollectionIDType));
  return id;
}

 protected:
  inline static std::string makeInternalKey(const StringView& user_key,
                                            uint64_t list_id) {
    return std::string((char*)&list_id, 8)
        .append(user_key.data(), user_key.size());
  }

  std::string collection_name_;
  uint64_t collection_id_;
};
}  // namespace KVDK_NAMESPACE