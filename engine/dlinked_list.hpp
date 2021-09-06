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
#include <libpmem/libpmem.h>


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
            DLDataEntry p_new_record,           // Contains header and meta, but not prev and next pointers
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
        inline void _persist_record_
        (
            void* pmp, 
            DLDataEntry const& entry,           // Complete DLDataEntry supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value
        )
        {
            size_t sz_entry = sizeof(DLDataEntry);
            char* pmp_dest = pmp;
            pmem_memcpy(pmp_dest, &entry, sz_entry, PMEM_F_MEM_NOFLUSH | PMEM_F_MEM_NONTEMPORAL);
            pmp_dest += sz_entry;
            pmem_memcpy(pmp_dest, key.data(), key.size(), PMEM_F_MEM_NOFLUSH | PMEM_F_MEM_NONTEMPORAL);
            pmp_dest += key.size();
            pmem_memcpy(pmp_dest, value.data(), value.size(), PMEM_F_MEM_NOFLUSH | PMEM_F_MEM_NONTEMPORAL);
            pmem_flush();
            pmem_drain();
            std::uint32_t checksum = static_cast<DLDataEntry*>(pmp)->Checksum();

            //memcpy(pmp_dest, data_entry, sz_entry);
            //memcpy(pmp_dest + sz_entry, key.data(), key.size());
            //memcpy(pmp_dest + sz_entry + key.size(), value.data(), value.size());
            //DLDataEntry* entry_with_data = ((DLDataEntry*)data_cpy_target);
            //entry_with_data->header.checksum = entry_with_data->Checksum();
            //pmem_flush(block_base, entry_size + key.size() + value.size());
            //pmem_drain();
        }

        inline static std::uint32_t _check_sum_
        (
            DLDataEntry const& entry,           // Complete DLDataEntry supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value
        )
        {
            std::uint32_t cs1 = get_checksum
            (
                reinterpret_cast<char*>(&entry) + sizeof(decltype(entry.header)),
                sizeof(DataEntry) - sizeof(decltype(entry.header))
            );
            std::uint32_t cs2 = get_checksum
            (
                
            );
            return get_checksum((char*)this + sizeof(DataHeader), sizeof(DataEntry) - sizeof(DataHeader)) +
                get_checksum(data, v_size + k_size);
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
                _pmp_curr_ = _get_pmp_next_();
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
                _pmp_curr_ = _get_pmp_prev_();
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
            inline DLDataEntry* _get_pmp_next_() const
            {
                return reinterpret_cast<DLDataEntry*>(_sp_dlinked_list_->_sp_pmem_allocator_->offset2addr(_pmp_curr_->next));
            }

            inline DLDataEntry* _get_pmp_prev_() const
            {
                return reinterpret_cast<DLDataEntry*>(_sp_dlinked_list_->_sp_pmem_allocator_->offset2addr(_pmp_curr_->prev));
            }

            inline std::uint64_t _get_offset_() const
            {
                return _sp_dlinked_list_->_sp_pmem_allocator_->addr2offset(_pmp_curr_);
            }
        };
    };

} // namespace KVDK_NAMESPACE
