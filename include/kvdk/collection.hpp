/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <string>

#include "kvdk/namespace.hpp"
#include "libpmemobj++/string_view.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;

// A collection of key-value pairs
class Collection {
 public:
  Collection(const std::string& name, uint64_t id, bool is_expired = false)
      : collection_name_(name), collection_id_(id), is_expired_(is_expired) {}
  // Return unique ID of the collection
  uint64_t ID() const { return collection_id_; }

  // Return name of the collection
  const std::string& Name() const { return collection_name_; }

  bool IsExpired() const { return is_expired_; }

  void UpdateExpiredStatus(bool is_expired) { is_expired_ = is_expired; }

  virtual int64_t GetExpiredTime() const = 0;

  // Return internal representation of "key" in the collection
  // By default, we concat key with the collection id
  std::string InternalKey(const StringView& key) {
    return makeInternalKey(key, ID());
  }

 protected:
  inline static std::string makeInternalKey(const StringView& user_key,
                                            uint64_t list_id) {
    return std::string((char*)&list_id, 8)
        .append(user_key.data(), user_key.size());
  }

  std::string collection_name_;
  uint64_t collection_id_;
  bool is_expired_;
};
}  // namespace KVDK_NAMESPACE