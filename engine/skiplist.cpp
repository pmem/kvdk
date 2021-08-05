/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "skiplist.hpp"

namespace PMEMDB_NAMESPACE {

void Skiplist::Seek(const Slice &key, Splice *splice) {
  Node *prev = header_;
  Node *tmp;
  // TODO: do not search from max height every time
  for (int i = kMaxHeight - 1; i >= 0; i--) {
    while (1) {
      tmp = prev->Next(i);
      if (tmp == nullptr) {
        splice->nexts[i] = nullptr;
        splice->prevs[i] = prev;
        break;
      }
      int cmp;
      if (i > 3) {
        cmp = Slice::compare(key, Slice(tmp->key, tmp->data_entry->k_size));
      } else {
        cmp = Slice::compare(
            key, Slice((char *)tmp->data_entry + sizeof(SortedDataEntry),
                       tmp->data_entry->k_size));
      }

      if (cmp > 0) {
        prev = tmp;
      } else {
        splice->nexts[i] = tmp;
        splice->prevs[i] = prev;
        break;
      }
    }
  }

  SortedDataEntry *prev_data_entry = prev->data_entry;
  int cmp_cnt = 0;
  while (1) {
    uint64_t next_data_entry_offset = prev_data_entry->next;
    if (next_data_entry_offset == NULL_PMEM_OFFSET) {
      splice->prev_data_entry = prev_data_entry;
      splice->next_data_entry = nullptr;
      break;
    }
    SortedDataEntry *next_data_entry =
        reinterpret_cast<SortedDataEntry *>(pmem_log_ + next_data_entry_offset);
    int cmp = Slice::compare(
        key, Slice((char *)next_data_entry + sizeof(SortedDataEntry),
                   next_data_entry->k_size));
    if (cmp >= 0) {
      prev_data_entry = next_data_entry;
    } else {
      splice->next_data_entry = next_data_entry;
      splice->prev_data_entry = prev_data_entry;
      break;
    }
  }
}
} // namespace PMEMDB_NAMESPACE