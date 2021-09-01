/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <assert.h>
#include <cstdint>

#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "structures.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {
    /// Data Structure that resides on DRAM for accessing PMem data
    class DLinkedList {
    private:
        /// PMem pointer(pmp) to head node on PMem
        DLDataEntry* _pmp_head_;
        /// PMem pointer(pmp) to tail node on PMem
        DLDataEntry* _pmp_tail_;
        /// Provides locking facility
        std::shared_ptr<HashTable> _sp_hash_table_;
        /// Allocator for allocating space for new nodes, 
        // as well as for deallocating space to delete nodes
        std::shared_ptr<PMEMAllocator> _sp_pmem_allocator_;

    public:
        DLinkedList(const std::shared_ptr<PMEMAllocator>& pmem_allocator,
            std::shared_ptr<HashTable> hash_table) :
            _pmp_head_{ nullptr },
            _pmp_tail_{ nullptr },
            _sp_pmem_allocator_{ pmem_allocator },
            _sp_hash_table_{ hash_table } 
        {
            Rebuild();
        }

        ~DLinkedList() {
            // _pmp_head_ and _pmp_tail_ points to persisted Record of Head and Tail on
            // PMem, no need to delete anything
        }

        uint64_t id() override { return id_; }

        const std::string& name() { return name_; }

        DLDataEntry* head() { return _pmp_head_; }

        DLDataEntry* tail() { return _pmp_tail_; }

        inline static Slice UserKey(const Slice& list_key) {
            // 8 bytes for id
            return Slice(list_key.data() + 8, list_key.size() - 8);
        }

        Status Rebuild();

        void* InsertDataEntry(Splice* insert_splice, DLDataEntry* inserting_entry,
            const Slice& inserting_key, SkiplistNode* node);

        void* InsertRecord(DLDataEntry* p_new_entry, DLDataEntry* pmp_prev);

        void DeleteDataEntry(Splice* delete_splice, const Slice& deleting_key,
            SkiplistNode* node);

    public:
        class Iterator : public kvdk::Iterator {
        private:
            std::shared_ptr<DLinkedList> sp_dlinked_list;
            DLDataEntry* current;

        public:
            Iterator(DLinkedList* p_list)
                : sp_dlinked_list(p_list), current(p_list->head()) {}

            virtual void SeekToFirst() override {
                current = sp_dlinked_list->head();
                Next();
            }

            bool Ok() {
                bool ok = true;
                ok = ok && (current != nullptr);
                ok = ok && (current->type & (DATA_ENTRY_TYPE::DLIST_DATA_RECORD |
                    DATA_ENTRY_TYPE::DLIST_DELETE_RECORD |
                    DATA_ENTRY_TYPE::DLIST_HEAD_RECORD |
                    DATA_ENTRY_TYPE::DLIST_TAIL_RECORD));
                return ok;
            }

            virtual bool Valid() override {
                return Ok() && (current->type == DATA_ENTRY_TYPE::DLIST_DATA_RECORD);
            }

            virtual bool Next() override {
                while (Ok()) {
                    current = reinterpret_cast<DLDataEntry*>(
                        sp_dlinked_list->_sp_pmem_allocator_->offset2addr(current->next));
                    if (current->type == DATA_ENTRY_TYPE::DLIST_DATA_RECORD) {
                        return true;
                    }
                    else if (current->type == DATA_ENTRY_TYPE::DLIST_DELETE_RECORD) {
                        continue;
                    }
                    else {
                        // Reaching tail
                        return false;
                    }
                }
            }

            virtual bool Prev() override {
                while (Ok()) {
                    current = reinterpret_cast<DLDataEntry*>(
                        sp_dlinked_list->_sp_pmem_allocator_->offset2addr(current->prev));
                    if (current->type == DATA_ENTRY_TYPE::DLIST_DATA_RECORD) {
                        return true;
                    }
                    else if (current->type == DATA_ENTRY_TYPE::DLIST_DELETE_RECORD) {
                        continue;
                    }
                    else {
                        // Reaching head
                        return false;
                    }
                }
            }

            virtual std::string Key() override {
                if (!Valid())
                    return "";
                return DLinkedList::UserKey(current->Key()).to_string();
            }

            virtual std::string Value() override {
                if (!Valid())
                    return "";
                return current->Value().to_string();
            }
        };
    };

} // namespace KVDK_NAMESPACE
