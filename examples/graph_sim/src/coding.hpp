/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <string>

inline bool LittleEndian() {
  int i = 1;
  return *((char*)(&i));
}

extern void PutFixed32(std::string* dst, uint32_t value);
extern void PutFixed64(std::string* dst, uint64_t value);

extern bool GetFixed32(std::string* input, uint32_t* value);
extern bool GetFixed64(std::string* input, uint64_t* value);

extern void EncodeFixed32(char* buf, uint32_t value);
extern void EncodeFixed64(char* buf, uint64_t value);

inline void EncodeFixed32(char* buf, uint32_t value) {
  if (LittleEndian()) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

inline void EncodeFixed64(char* buf, uint64_t value) {
  if (LittleEndian()) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

inline void PutFixed64(std::string* dst, uint64_t value) {
  if (LittleEndian()) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                sizeof(value));
  } else {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
  }
}

inline uint32_t DecodeFixed32(const char* ptr) {
  if (LittleEndian()) {
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

inline uint64_t DecodeFixed64(const char* ptr) {
  if (LittleEndian()) {
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

inline bool GetFixed64(std::string* input, uint64_t* value) {
  if (input->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = DecodeFixed64(input->data());
  input->erase(0, sizeof(uint64_t));
  return true;
}

inline bool GetFixed32(std::string* input, uint32_t* value) {
  if (input->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = DecodeFixed32(input->data());
  input->erase(0, sizeof(uint32_t));
  return true;
}