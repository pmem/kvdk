/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#pragma once

#define GET_CPLUSPLUS_POINTER(_pointer) \
  static_cast<jlong>(reinterpret_cast<size_t>(_pointer))
