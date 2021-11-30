/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>
#include <malloc.h>

#include "kvdk/namespace.hpp"
#include "logger.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

// A pointer with additional information on high 16 bits
template <typename T> class PointerWithTag {
public:
  static constexpr uint64_t kPointerMask = (((uint64_t)1 << 48) - 1);

  // TODO: Maybe explicit
  PointerWithTag(T *pointer) : tagged_pointer((uint64_t)pointer) {}

  explicit PointerWithTag(T *pointer, uint16_t tag)
      : tagged_pointer((uint64_t)pointer | ((uint64_t)tag << 48)) {}

  PointerWithTag() : tagged_pointer(0) {}

  T *RawPointer() { return (T *)(tagged_pointer & kPointerMask); }

  const T *RawPointer() const {
    return (const T *)(tagged_pointer & kPointerMask);
  }

  bool Null() { return RawPointer() == nullptr; }

  uint16_t GetTag() { return tagged_pointer >> 48; }

  void ClearTag() { tagged_pointer &= kPointerMask; }

  void SetTag(uint16_t tag) { tagged_pointer |= ((uint64_t)tag << 48); }

  const T &operator*() const { return *RawPointer(); }

  T &operator*() { return *(RawPointer()); }

  const T *operator->() const { return RawPointer(); }

  T *operator->() { return RawPointer(); }

  bool operator==(const T *raw_pointer) { return RawPointer() == raw_pointer; }

  bool operator==(const T *raw_pointer) const {
    return RawPointer() == raw_pointer;
  }

private:
  uint64_t tagged_pointer;
};

struct PendingBatch {
  enum class Stage {
    Finish = 0,
    Processing = 1,
  };

  PendingBatch(Stage s, uint32_t nkv, TimeStampType ts)
      : stage(s), num_kv(nkv), timestamp(ts) {}

  void PersistProcessing(void *target,
                         const std::vector<uint64_t> &entry_offsets);

  void Restore(char *target, std::vector<uint64_t> *entry_offsets);

  void PersistStage(Stage s);

  bool Unfinished() { return stage == Stage::Processing; }

  Stage stage;
  uint32_t num_kv;
  TimeStampType timestamp;
};

// A collection of key-value pairs
class Collection {
public:
  Collection(const std::string &name, CollectionIDType id)
      : collection_name_(name), collection_id_(id) {}
  // Return unique ID of the collection
  virtual uint64_t ID() const { return collection_id_; }

  // Return name of the collection
  virtual const std::string &Name() const { return collection_name_; }

  // Return internal representation of "key" in the collection
  // By default, we concat key with the collection id
  virtual std::string InternalKey(const StringView &key) {
    return makeInternalKey(key, ID());
  }

  inline static StringView ExtractUserKey(const StringView &internal_key) {
    constexpr size_t sz_id = sizeof(CollectionIDType);
    kvdk_assert(sz_id <= internal_key.size(),
                "internal_key does not has space for key");
    return StringView(internal_key.data() + sz_id, internal_key.size() - sz_id);
  }

  inline static uint64_t ExtractID(const StringView &internal_key) {
    CollectionIDType id;
    memcpy(&id, internal_key.data(), sizeof(CollectionIDType));
    return id;
  }

protected:
  inline static std::string makeInternalKey(const StringView &user_key,
                                            uint64_t list_id) {
    return std::string((char *)&list_id, 8)
        .append(user_key.data(), user_key.size());
  }

  inline static std::string ID2String(CollectionIDType id) {
    return std::string(reinterpret_cast<char *>(&id), 8);
  }

  inline static CollectionIDType string2ID(const StringView &string_id) {
    CollectionIDType id;
    kvdk_assert(sizeof(CollectionIDType) == string_id.size(),
                "size of string id does not match CollectionIDType size!");
    memcpy(&id, string_id.data(), sizeof(CollectionIDType));
    return id;
  }

  std::string collection_name_;
  CollectionIDType collection_id_;
};
} // namespace KVDK_NAMESPACE
