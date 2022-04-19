/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#define KVDK_MODIFY_WRITE 0
#define KVDK_MODIFY_DELETE 1
#define KVDK_MODIFY_ABORT 2

// Customized modify function used in KVDKModify, indicate how to modify
// existing value
//
// Below is args of the function, "input" is passed by KVDK engine, and the
// function is responsible to fill outputs in "output" args.
//
// *(input) old_val: existing value of key, or nullptr if key not exist
// *(input) old_val_len: length of "old_val"
// *(output) new_val: store new value after modifying. It's responsible of
// KVDKModifyFunc to allocate space for *new_val and fill modify result here if
// the function returns KVDK_MODIFY_WRITE.
// *(output) new_val_len: length of "new_val", It's responsible of
// KVDKModifyFunc to fill it
// * args: customer args
//
// return KVDK_MODIFY_WRITE indicates to update existing value to
// "new_value"
// return KVDK_MODIFY_DELETE indicates to delete the kv from engine
// return KVDK_MODIFY_ABORT indicates the existing kv should not be
// modified and abort the operation
typedef int (*KVDKModifyFunc)(const char* old_val, size_t old_val_len,
                              char** new_val, size_t* new_val_len, void* args);
// Used in KVDKModify, indicate how to free allocated space in KVDKModifyFunc
typedef void (*KVDKFreeFunc)(void*);