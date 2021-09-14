/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <exception>

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
    /// DLinkedList guarantees that forward links are always valid
    /// Backward links are restored on recovery
    /// DLinkedList does not deallocate records. Deallocation is done by caller
    /// Locking is done by caller at HashTable
    class DLinkedList : public std::enable_shared_from_this<DLinkedList>
    {
    private:
        /// PMem pointer(pmp) to head node on PMem
        DLDataEntry* _pmp_head_;
        /// PMem pointer(pmp) to tail node on PMem
        DLDataEntry* _pmp_tail_;
        /// Allocator for allocating space for new nodes, 
        /// as well as for deallocating space to delete nodes
        std::shared_ptr<PMEMAllocator> _sp_pmem_allocator_;
        /// Shared pointer to pin
        std::shared_ptr<DLinkedList> _sp_dlinked_list_;

        class Iterator;

        friend class Iterator;
        friend class UnorderedCollection;
        friend class UnorderedIterator;

        static constexpr std::uint64_t _null_offset_ = kNullPmemOffset;

    public:
        /// Create DLinkedList and construct head and tail node on PMem.
        /// key and value are stored in head and tail nodes
        DLinkedList
        (
            std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
            std::uint64_t timestamp,
            pmem::obj::string_view const key,   
            pmem::obj::string_view const value
        ) 
        try:
            _pmp_head_{ nullptr },
            _pmp_tail_{ nullptr },
            _sp_pmem_allocator_{ sp_pmem_allocator }
        {
            // head and tail only holds DLDataEntry. No id or collection name needed.
            auto space_head = _sp_pmem_allocator_->Allocate(sizeof(DLDataEntry));
            if (space_head.size == 0)
            {
                throw std::bad_alloc{};
            }
            auto space_tail = _sp_pmem_allocator_->Allocate(sizeof(DLDataEntry));
            if (space_tail.size == 0)
            {
                _sp_pmem_allocator_->Free(space_head);
                throw std::bad_alloc{};
            }

            std::uint64_t offset_head = space_head.space_entry.offset;
            std::uint64_t offset_tail = space_tail.space_entry.offset;
            void* pmp_head = _sp_pmem_allocator_->offset2addr(offset_head);
            void* pmp_tail = _sp_pmem_allocator_->offset2addr(offset_tail);

            DLDataEntry entry_head;  // Set up entry with meta
            {
                entry_head.timestamp = timestamp;
                entry_head.type = DataEntryType::DlistHeadRecord;
                entry_head.k_size = 0;
                entry_head.v_size = 0;

                // checksum can only be calculated with complete meta
                entry_head.header.b_size = space_head.size;
                entry_head.header.checksum = _CheckSum_(entry_head, key, value);

                entry_head.prev = _null_offset_;
                entry_head.next = offset_tail;
            }

            DLDataEntry entry_tail;  // Set up entry with meta
            {
                entry_tail.timestamp = timestamp;
                entry_tail.type = DataEntryType::DlistTailRecord;
                entry_tail.k_size = 0;
                entry_tail.v_size = 0;

                // checksum can only be calculated with complete meta
                entry_tail.header.b_size = space_head.size;
                entry_tail.header.checksum = _CheckSum_(entry_tail, key, value);

                entry_tail.prev = offset_head;
                entry_tail.next = _null_offset_;
            }

            // Persist tail first then head
            // If only tail is persisted then it can be deallocated by caller at recovery
            _PersistRecord_(pmp_tail, entry_tail, key, value);
            _PersistRecord_(pmp_head, entry_head, key, value);
            _pmp_head_ = static_cast<DLDataEntry*>(pmp_head);
            _pmp_tail_ = static_cast<DLDataEntry*>(pmp_tail);
        }
        catch (std::bad_alloc const& ex)
        {
            std::cerr << ex.what() << std::endl;
            std::cerr << "Fail to create DLinkedList object!" << std::endl;
            throw;
        }

        /// Create DLinkedList from existing head and tail node. Used for recovery.
        DLinkedList
        (
            DLDataEntry* pmp_head,
            DLDataEntry* pmp_tail,
            std::shared_ptr<PMEMAllocator> sp_pmem_allocator
        ) :
            _pmp_head_{ pmp_head },
            _pmp_tail_{ pmp_tail },
            _sp_pmem_allocator_{ sp_pmem_allocator }
        {
            Iterator curr{ shared_from_this(), pmp_head };
            assert(pmp_head->type == DataEntryType::DlistHeadRecord);
            do
            {
                Iterator next{ curr }; ++next;
                // Check for backward link
                // Forward link is guaranteed to be valid
                if (next->prev != curr._GetOffset_())
                {
                    auto offset = curr._GetOffset_();
                    pmem_memcpy(&next->prev, &offset, sizeof(decltype(next->prev)), PMEM_F_MEM_NONTEMPORAL);
                }
                curr = next;
            } while (curr->type != DataEntryType::DlistTailRecord);
            assert(_pmp_tail_ == curr._pmp_curr_);
        }

        ~DLinkedList()
        {
            // _pmp_head_ and _pmp_tail_ points to persisted Record of Head and Tail on PMem
            // No need to delete anything
        }

        /// Emplace right before iter.
        /// When fail to Emplace, return iterator at head
        Iterator EmplaceBefore
        (
            Iterator iter,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value
        )
        {
            assert(iter.valid() && "Invalid iterator in dlinked_list!");

            // Insert happens between iter_prev and iter_next
            Iterator iter_prev{ iter }; --iter_prev;
            Iterator iter_next{ iter };

            return _EmplaceBetween_(iter_prev, iter_next, timestamp, key, value, DataEntryType::DlistDataRecord);
        }

        /// Emplace right after iter.
        /// When fail to Emplace, return invalid iterator.
        Iterator EmplaceAfter
        (
            Iterator iter,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value
        )
        {
            assert(iter.valid() && "Invalid iterator in dlinked_list!");

            // Insert happens between iter_prev and iter_next
            Iterator iter_prev{ iter };
            Iterator iter_next{ iter }; ++iter_next;

            return _EmplaceBetween_(iter_prev, iter_next, timestamp, key, value, DataEntryType::DlistDataRecord);
        }

        /// In-place swap out an old record for new one
        /// Old record should be freed by caller after calling this function
        /// Timestamp should be checked by caller
        Iterator SwapEmplace
        (
            Iterator iter,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type
        )
        {
            assert(iter.valid() && "Invalid iterator in dlinked_list!");
            assert(type == DataEntryType::DlistDataRecord || type == DataEntryType::DlistDeleteRecord);

            // Swap happens between iter_prev and iter_next
            Iterator iter_prev{ iter }; --iter_prev;
            Iterator iter_next{ iter }; ++iter_prev;

            return _EmplaceBetween_(iter_prev, iter_next, timestamp, key, value, type);
        }

        Iterator First()
        {
            Iterator ret{ shared_from_this(), _pmp_head_ };
            assert(ret.valid());
            ++ret;
            return ret;
        }

        Iterator Last()
        {
            Iterator ret{ shared_from_this(), _pmp_tail_ };
            assert(ret.valid());
            --ret;
            return ret;
        }

        Iterator Head()
        {
            return Iterator{ shared_from_this(), _pmp_head_ };
        }

        Iterator Tail()
        {
            return Iterator{ shared_from_this(), _pmp_tail_ };
        }

        /// Helper function to deallocate Record, called only by caller
        inline static void Deallocate(Iterator iter)
        {
            iter._sp_dlinked_list_->_sp_pmem_allocator_->Free
            (
                SizedSpaceEntry{ iter._GetOffset_(), iter->header.b_size, iter->timestamp }
            );
        }

    private:
        inline static void _PersistRecord_
        (
            void* pmp,
            DLDataEntry const& entry,           // Complete DLDataEntry supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value
        )
        {
            // Persist key and value
            size_t sz_entry = sizeof(DLDataEntry);
            char* pmp_dest = static_cast<char*>(pmp);
            pmp_dest += sz_entry;
            pmem_memcpy(pmp_dest, key.data(), key.size(), PMEM_F_MEM_NOFLUSH | PMEM_F_MEM_NONTEMPORAL);
            pmp_dest += key.size();
            pmem_memcpy(pmp_dest, value.data(), value.size(), PMEM_F_MEM_NOFLUSH | PMEM_F_MEM_NONTEMPORAL);
            pmem_flush(pmp, key.size() + value.size());
            pmem_drain();

            // Persist DLDataEntry last
            pmem_memcpy(pmp, &entry, sz_entry, PMEM_F_MEM_NONTEMPORAL);
        }

        inline static std::uint32_t _CheckSum_
        (
            DLDataEntry const& entry,           // Incomplete DLDataEntry, only meta is valid
            pmem::obj::string_view const key,
            pmem::obj::string_view const value
        )
        {
            std::uint32_t cs1 = get_checksum
            (
                reinterpret_cast<char const*>(&entry) + sizeof(decltype(entry.header)),
                sizeof(DataEntry) - sizeof(decltype(entry.header))
            );
            std::uint32_t cs2 = get_checksum(key.data(), key.size());
            std::uint32_t cs3 = get_checksum(value.data(), value.size());
            return cs1 + cs2 + cs3;
        }

        /// Emplace between iter_prev and iter_next
        /// When fail to Emplace, throw bad_alloc
        /// If system fails, it is guaranteed the dlinked_list is in one of the following state:
        ///     1) Nothing emplaced
        ///     2) entry emplaced but not linked
        ///     3) entry emplaced and linked in the forward direction
        ///     4) entry emplaced and linked in both directions
        /// When recovering, just recover head and tail, then iterating through to repair
        inline Iterator _EmplaceBetween_
        (
            Iterator iter_prev,
            Iterator iter_next,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type
        )
        try
        {
            assert(iter_prev.valid() && "Invalid iterator in dlinked_list!");
            assert(iter_next.valid() && "Invalid iterator in dlinked_list!");

            auto space = _sp_pmem_allocator_->Allocate(sizeof(DLDataEntry) + key.size() + value.size());
            if (space.size == 0)
            {
                throw std::bad_alloc{};
            }
            std::uint64_t offset = space.space_entry.offset;
            void* pmp = _sp_pmem_allocator_->offset2addr(offset);

            DLDataEntry entry;  // Set up entry with meta
            {
                entry.timestamp = timestamp;
                entry.type = type;
                entry.k_size = key.size();
                entry.v_size = value.size();

                // checksum can only be calculated with complete meta
                entry.header.b_size = space.size;
                entry.header.checksum = _CheckSum_(entry, key, value);

                entry.prev = iter_prev._GetOffset_();
                entry.next = iter_next._GetOffset_();
                // entry is now complete
            }

            _PersistRecord_(pmp, entry, key, value);
            pmem_memcpy(&iter_prev->next, &offset, sizeof(offset), PMEM_F_MEM_NONTEMPORAL);
            pmem_memcpy(&iter_next->prev, &offset, sizeof(offset), PMEM_F_MEM_NONTEMPORAL);

            return Iterator{ shared_from_this(), static_cast<DLDataEntry*>(pmp) };
        }
        catch (std::bad_alloc const& ex)
        {
            std::cerr << ex.what() << std::endl;
            std::cerr << "Fail to create DLinkedList object!" << std::endl;
            throw;
        }

    public:
        class Iterator
        {
        private:
            friend class DLinkedList;
            friend class UnorderedCollection;

        private:
            /// shared pointer to pin the dlinked_list
            /// as well as share the pmem_allocator to transform offset to PMem pointer
            /// shared pointer instead of normal pointer is used to pin the pmem_allocator
            std::shared_ptr<DLinkedList> _sp_dlinked_list_;

            /// PMem pointer to current Record
            DLDataEntry* _pmp_curr_;

        public:
            /// Constructors and Destructors
            explicit Iterator() = delete;

            explicit Iterator(std::shared_ptr<DLinkedList> sp_list) :
                _sp_dlinked_list_{ sp_list },
                _pmp_curr_{ _sp_dlinked_list_->_pmp_head_ }
            {
            }

            explicit Iterator(std::shared_ptr<DLinkedList> sp_list, DLDataEntry* curr) :
                _sp_dlinked_list_{ sp_list },
                _pmp_curr_{ curr }
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
                if (!_sp_dlinked_list_ || !_pmp_curr_)
                {
                    return false;
                }
                DataEntryType type_curr = static_cast<DataEntryType>(_pmp_curr_->type);
                bool ok =
                    (
                        type_curr == DataEntryType::DlistHeadRecord ||
                        type_curr == DataEntryType::DlistTailRecord ||
                        type_curr == DataEntryType::DlistDataRecord ||
                        type_curr == DataEntryType::DlistDeleteRecord
                    );
                return ok;
            }

            inline operator bool()
            {
                return valid();
            }

            /// Increment and Decrement operators
            Iterator& operator++()
            {
                _pmp_curr_ = _GetPmpNext_();
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
                _pmp_curr_ = _GetPmpPrev_();
                return *this;
            }

            Iterator operator--(int)
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

            DLDataEntry* operator->()
            {
                return _pmp_curr_;
            }

        private:
            inline DLDataEntry* _GetPmpNext_() const
            {
                return reinterpret_cast<DLDataEntry*>(_sp_dlinked_list_->_sp_pmem_allocator_->offset2addr(_pmp_curr_->next));
            }

            inline DLDataEntry* _GetPmpPrev_() const
            {
                return reinterpret_cast<DLDataEntry*>(_sp_dlinked_list_->_sp_pmem_allocator_->offset2addr(_pmp_curr_->prev));
            }

            inline std::uint64_t _GetOffset_() const
            {
                return _sp_dlinked_list_->_sp_pmem_allocator_->addr2offset(_pmp_curr_);
            }
        };
    };

} // namespace KVDK_NAMESPACE
