/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include <libpmemobj++/string_view.hpp>
#include <libpmem.h>


#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "structures.hpp"
#include "utils.hpp"

#include <list>

namespace KVDK_NAMESPACE
{
    /// Data Structure that resides on DRAM for accessing PMem data
    /// Caller is responsible for locking since caller may use lock elsewhere
    class DLinkedList 
    {
    private:
        /// PMem pointer(pmp) to head node on PMem
        DLDataEntry* _pmp_head_;
        /// PMem pointer(pmp) to tail node on PMem
        DLDataEntry* _pmp_tail_;
        /// Provides locking facility
        std::shared_ptr<HashTable> _sp_hash_table_;
        /// Allocator for allocating space for new nodes, 
        /// as well as for deallocating space to delete nodes
        std::shared_ptr<PMEMAllocator> _sp_pmem_allocator_;

        class Iterator;

    public:
        DLinkedList
        (
            DLDataEntry* pmp_head,
            DLDataEntry* pmp_tail,
            const std::shared_ptr<PMEMAllocator>& sp_pmem_allocator,
            std::shared_ptr<HashTable> sp_hash_table
        ) :
            _pmp_head_{ pmp_head },
            _pmp_tail_{ pmp_tail },
            _sp_pmem_allocator_{ sp_pmem_allocator },
            _sp_hash_table_{ sp_hash_table }
        {
        }

        ~DLinkedList() {
            // _pmp_head_ and _pmp_tail_ points to persisted Record of Head and Tail on PMem
            // 
            // No need to delete anything
        }

        /// Emplace right before iter.
        /// p_new_record points to data on DRAM prepared for writing to PMem
        Iterator Emplace
        (
            Iterator iter, 
            DLDataEntry const& p_new_record, 
            pmem::obj::string_view const key, 
            pmem::obj::string_view const value
        )
        {
            assert(iter.valid() && "Invalid iterator in dlinked_list!");

            // Insert happens between iter_prev and iter_next
            Iterator iter_next = iter;
            Iterator& iter_prev = --iter;

            

        }

        /// Deallocate the Record by pmem_allocator
        Iterator Erase(Iterator iter)
        {

        }

        /// In-place swap out an old record for new one
        /// Old record remains on PMem
        Iterator Swap(Iterator iter, DLDataEntry const& p_new_record)
        {

        }

    private:
        inline void KVEngine::PersistDataEntry
        (
            void* pmp, 
            DLDataEntry const& entry,
            pmem::obj::string_view const key,
            pmem::obj::string_view const value
        )
        {
            char* pmp_dest = pmp;
            auto entry_size = sizeof(DLDataEntry);
            memcpy(pmp_dest, data_entry, entry_size);
            memcpy(pmp_dest + entry_size, key.data(), key.size());
            memcpy(pmp_dest + entry_size + key.size(), value.data(), value.size());
            DLDataEntry* entry_with_data = ((DLDataEntry*)data_cpy_target);
            entry_with_data->header.checksum = entry_with_data->Checksum();
            pmem_flush(block_base, entry_size + key.size() + value.size());
            pmem_drain();
        }

    public:
        class Iterator
        {
        private:
            /// shared pointer to pin the dlinked_list
            /// as well as share the pmem_allocator to transform offset to PMem pointer
            /// shared pointer instead of normal pointer is used to pin the pmem_allocator
            std::shared_ptr<DLinkedList> _sp_dlinked_list_;

            /// PMem pointer to current Record
            DLDataEntry* _pmp_curr_;

        public:
        /// Constructors and Destructors
            Iterator(std::shared_ptr<DLinkedList> sp_list) :
                _sp_dlinked_list_(sp_list),
                _pmp_curr_(_sp_dlinked_list_->_pmp_head_)
            {
            }

            Iterator(Iterator const& other) :
                _sp_dlinked_list_(other._sp_dlinked_list_),
                _pmp_curr_(other._pmp_curr_)
            {
            }

        /// Conversion to bool
            /// Double check for the validity of dlinked_list object on PMem
            inline bool valid()
            {
                bool ok = true;
                ok = ok && (_pmp_curr_ != nullptr);
                ok = ok && (_pmp_curr_->type & (DATA_ENTRY_TYPE::DLIST_DATA_RECORD |
                    DATA_ENTRY_TYPE::DLIST_DELETE_RECORD |
                    DATA_ENTRY_TYPE::DLIST_HEAD_RECORD |
                    DATA_ENTRY_TYPE::DLIST_TAIL_RECORD));
                return ok;
            }

            inline operator bool()
            {
                return valid();
            }

        /// Increment and Decrement operators
            Iterator& operator++()
            {
                _pmp_curr_ = _get_pmp_next_(_pmp_curr_);
                return *this;
            }

            Iterator operator++(int)
            {
                Iterator old{ *this };
                this->operator++();
                return old;
            }

            Iterator& operator--()
            {
                _pmp_curr_ = _get_pmp_prev_(_pmp_curr_);
                return *this;
            }

            Iterator operator++(int)
            {
                Iterator old{ *this };
                this->operator--();
                return old;
            }

        /// Dereference and direct PMem pointer access
            DLDataEntry& operator*()
            {
                return *_pmp_curr_;
            }

        private:
            inline static DLDataEntry* _get_pmp_next_(DLDataEntry* pmp_curr) const
            {
                return reinterpret_cast<DLDataEntry*>(_sp_dlinked_list_->_sp_pmem_allocator_->offset2addr(pmp_curr->next));
            }

            inline static DLDataEntry* _get_pmp_prev_(DLDataEntry* pmp_curr) const
            {
                return reinterpret_cast<DLDataEntry*>(_sp_dlinked_list_->_sp_pmem_allocator_->offset2addr(pmp_curr->prev));
            }
        };
    };

} // namespace KVDK_NAMESPACE
