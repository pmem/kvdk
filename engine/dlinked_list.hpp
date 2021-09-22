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
#include <iostream>
#include <iomanip>
#include <bitset>

#include <libpmemobj++/string_view.hpp>
#include <libpmem.h>


#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "structures.hpp"
#include "utils.hpp"

#include <list>

#define hex_print(x) std::hex << std::setfill ('0') << std::setw(sizeof(decltype(x))*2) << x

namespace KVDK_NAMESPACE
{
    /// DlistIterator does not pin DlinkedList
    /// It's up to caller to ensure that any
    /// DlistIterator constructed belongs to the
    /// right DlinkedList and to ensure that
    /// DlinkedList is valid when DlistIterator
    /// accesses data on it.
    class DlistIterator
    {
    private:
        friend class DLinkedList;
        friend class UnorderedCollection;

    private:

        /// PMem pointer to current Record
        std::shared_ptr<PMEMAllocator> _sp_pmem_allocator_;
        DLDataEntry* _pmp_curr_;

    public:
        /// It's up to caller to provide correct PMem pointer 
        /// and PMemAllocator to construct a DlistIterator
        explicit DlistIterator(std::shared_ptr<PMEMAllocator> sp_pmem_allocator, DLDataEntry* curr) :
            _sp_pmem_allocator_{ sp_pmem_allocator },
            _pmp_curr_{ curr }
        {
        }

        DlistIterator(DlistIterator const& other) :
            _sp_pmem_allocator_{ other._sp_pmem_allocator_ },
            _pmp_curr_(other._pmp_curr_)
        {
        }

        /// Conversion to bool
        /// Returns true if the iterator is on some DlinkedList
        inline bool valid() const
        {
            if (!_pmp_curr_)
            {
                return false;
            }
            switch (static_cast<DataEntryType>(_pmp_curr_->type))
            {
                case DataEntryType::DlistDataRecord:
                case DataEntryType::DlistDeleteRecord:
                case DataEntryType::DlistHeadRecord:
                case DataEntryType::DlistTailRecord:
                {
                    return true;
                }
                case DataEntryType::DlistRecord:
                default:
                {
                    return false;
                }
            }
        }

        inline operator bool() const
        {
            return valid();
        }

        /// Increment and Decrement operators
        DlistIterator& operator++()
        {
            _pmp_curr_ = _GetPmpNext_();
            return *this;
        }

        DlistIterator operator++(int)
        {
            DlistIterator old{ *this };
            this->operator++();
            return old;
        }

        DlistIterator& operator--()
        {
            _pmp_curr_ = _GetPmpPrev_();
            return *this;
        }

        DlistIterator operator--(int)
        {
            DlistIterator old{ *this };
            this->operator--();
            return old;
        }

        DLDataEntry& operator*()
        {
            return *_pmp_curr_;
        }

        DLDataEntry* operator->()
        {
            return _pmp_curr_;
        }

        friend bool operator==(DlistIterator lhs, DlistIterator rhs)
        {
            assert(lhs._sp_pmem_allocator_ == rhs._sp_pmem_allocator_);
            return lhs._pmp_curr_ == rhs._pmp_curr_;
        }

        friend bool operator!=(DlistIterator lhs, DlistIterator rhs)
        {
            return !(lhs == rhs);
        }

    private:
        inline DLDataEntry* _GetPmpNext_() const
        {
            return reinterpret_cast<DLDataEntry*>(_sp_pmem_allocator_->offset2addr(_pmp_curr_->next));
        }

        inline DLDataEntry* _GetPmpPrev_() const
        {
            return reinterpret_cast<DLDataEntry*>(_sp_pmem_allocator_->offset2addr(_pmp_curr_->prev));
        }

    public:
        inline std::uint64_t _GetOffset_() const
        {
            return _sp_pmem_allocator_->addr2offset(_pmp_curr_);
        }

    };
}

namespace KVDK_NAMESPACE
{
    /// DLinkedList is a helper class to access PMem
    /// DLinkedList guarantees that forward links are always valid
    /// Backward links are restored on recovery
    /// DLinkedList does not deallocate records. Deallocation is done by caller
    /// Locking is done by caller at HashTable
    class DLinkedList
    {
    private:
        /// Allocator for allocating space for new nodes, 
        /// as well as for deallocating space to delete nodes
        std::shared_ptr<PMEMAllocator> _sp_pmem_allocator_;
        /// PMem pointer(pmp) to head node on PMem
        DLDataEntry* _pmp_head_;
        /// PMem pointer(pmp) to tail node on PMem
        DLDataEntry* _pmp_tail_;

        friend class DlistIterator;
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
            _sp_pmem_allocator_{ sp_pmem_allocator },
            _pmp_head_{ nullptr },
            _pmp_tail_{ nullptr }
        {
        {
            // head and tail only holds DLDataEntry. No id or collection name needed.
            auto space_head = _sp_pmem_allocator_->Allocate(sizeof(DLDataEntry)+key.size()+value.size());
            if (space_head.size == 0)
            {
                throw std::bad_alloc{};
            }
            auto space_tail = _sp_pmem_allocator_->Allocate(sizeof(DLDataEntry)+key.size()+value.size());
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
                entry_head.k_size = key.size();
                entry_head.v_size = value.size();

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
                entry_tail.k_size = key.size();
                entry_tail.v_size = value.size();

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
        }
        catch (std::bad_alloc const& ex)
        {
            std::cerr << ex.what() << std::endl;
            std::cerr << "Fail to create DLinkedList object!" << std::endl;
            throw ex;
        }

        /// Create DLinkedList from existing head and tail node. Used for recovery.
        /// If from head to tail node is not forward linked, throw.
        DLinkedList
        (
            std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
            DLDataEntry* pmp_head,
            DLDataEntry* pmp_tail
        ) :
            _sp_pmem_allocator_{ sp_pmem_allocator },
            _pmp_head_{ pmp_head },
            _pmp_tail_{ pmp_tail }
        {
        {
            if (pmp_head->type != DataEntryType::DlistHeadRecord)
            {
                assert(false && "Cannot rebuild a DlinkedList from given PMem pointer not pointing to a DlistHeadRecord!");
                throw std::runtime_error{ "Cannot rebuild a DlinkedList from given PMem pointer not pointing to a DlistHeadRecord!" };
            }
            DlistIterator curr{ _sp_pmem_allocator_,  pmp_head }; 
            ++curr;

            while (true)
            {
                switch (static_cast<DataEntryType>(curr->type))
                {
                case DataEntryType::DlistDataRecord:
                case DataEntryType::DlistDeleteRecord:
                {
                    DlistIterator next{curr}; ++next;
                    std::uint64_t offset_curr = curr._GetOffset_();
                    if (next->prev != offset_curr)
                    {
                        throw std::runtime_error{"Found broken linkage when rebuilding DLinkedList!"};
                        // pmem_memcpy(&next->prev, &offset_curr, sizeof(decltype(offset_curr)), PMEM_F_MEM_NONTEMPORAL);
                    }
                    ++curr;
                    continue;
                }
                case DataEntryType::DlistTailRecord:
                {
                    if (curr._pmp_curr_ == pmp_tail)
                    {
                        return;
                    }       
                    else
                    {
                        throw std::runtime_error{"Unmatched head and tail when rebuilding a DlinkedList!"};
                    }
                }
                case DataEntryType::DlistHeadRecord:
                case DataEntryType::DlistRecord:
                default:
                {
                    assert(false);
                    throw std::runtime_error{"Invalid Record met when rebuilding a DlinkedList!"};
                }
                }
            }            
        }
        }

        // _pmp_head_ and _pmp_tail_ points to persisted Record of Head and Tail on PMem
        // No need to delete anything
        ~DLinkedList() = default;

        /// Emplace right before iter.
        DlistIterator EmplaceBefore
        (
            DlistIterator iter,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type
        )
        {
            if (!iter.valid())
            {
                throw std::runtime_error{"Invalid iterator in dlinked_list!"};
            }
            
            // Insert happens between iter_prev and iter_next
            DlistIterator iter_prev{ iter }; --iter_prev;
            DlistIterator iter_next{ iter };

            return _EmplaceBetween_(iter_prev, iter_next, timestamp, key, value, type);
        }

        /// Emplace right after iter.
        DlistIterator EmplaceAfter
        (
            DlistIterator iter,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type
        )
        {
            if (!iter.valid())
            {
                throw std::runtime_error{"Invalid iterator in dlinked_list!"};
            }

            // Insert happens between iter_prev and iter_next
            DlistIterator iter_prev{ iter };
            DlistIterator iter_next{ iter }; ++iter_next;

            return _EmplaceBetween_(iter_prev, iter_next, timestamp, key, value, type);
        }

        /// Emplace right after Head().
        DlistIterator EmplaceFront
        (
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type
        )
        {
            // Insert happens between iter_prev and iter_next
            DlistIterator iter_prev{ Head() };
            DlistIterator iter_next{ Head() }; ++iter_next;

            return _EmplaceBetween_(iter_prev, iter_next, timestamp, key, value, type);
        }

        /// Emplace right before Tail().
        DlistIterator EmplaceBack
        (
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type
        )
        {
            // Insert happens between iter_prev and iter_next
            DlistIterator iter_prev{ Tail() }; --iter_prev;
            DlistIterator iter_next{ Tail() };

            return _EmplaceBetween_(iter_prev, iter_next, timestamp, key, value, type);
        }

        /// In-place swap out an old record for new one
        /// Old record should be freed by caller after calling this function
        /// Timestamp should be checked by caller
        DlistIterator SwapEmplace
        (
            DlistIterator iter,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type
        )
        {
            if (!iter.valid())
            {
                throw std::runtime_error{"Invalid iterator in dlinked_list!"};
            }

            // Swap happens between iter_prev and iter_next
            DlistIterator iter_prev{ iter }; --iter_prev;
            DlistIterator iter_next{ iter }; ++iter_prev;

            return _EmplaceBetween_(iter_prev, iter_next, timestamp, key, value, type);
        }

        // Not checked yet
        DlistIterator First()
        {
            DlistIterator ret{ _sp_pmem_allocator_, _pmp_head_ };
            assert(ret.valid());
            ++ret;
            return ret;
        }

        // Not checked yet
        DlistIterator Last()
        {
            DlistIterator ret{ _sp_pmem_allocator_, _pmp_tail_ };
            assert(ret.valid());
            --ret;
            return ret;
        }

        DlistIterator Head()
        {
            return DlistIterator{ _sp_pmem_allocator_, _pmp_head_ };
        }

        DlistIterator Tail()
        {
            return DlistIterator{ _sp_pmem_allocator_, _pmp_tail_ };
        }

        /// Helper function to deallocate Record, called only by caller
        inline static void Deallocate(DlistIterator iter)
        {
            iter._sp_pmem_allocator_->Free
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

        /// Emplace between iter_prev and iter_next, linkage not checked
        /// When fail to Emplace, throw bad_alloc
        /// If system fails, it is guaranteed the dlinked_list is in one of the following state:
        ///     1) Nothing emplaced
        ///     2) entry emplaced but not linked
        ///     3) entry emplaced and linked in the forward direction
        ///     4) entry emplaced and linked in both directions
        /// When recovering, just recover head and tail, then iterating through to repair
        inline DlistIterator _EmplaceBetween_
        (
            DlistIterator iter_prev,
            DlistIterator iter_next,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type
        )
        try
        {
            if (type != DataEntryType::DlistDataRecord && type != DataEntryType::DlistDeleteRecord)
            {
                throw std::runtime_error{"Trying to emplace invalid Record!"};
            }
            if (!iter_prev || !iter_next)
            {
                throw std::runtime_error{"Invalid iterator in dlinked_list!"};
            }
            
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

            return DlistIterator{ _sp_pmem_allocator_, static_cast<DLDataEntry*>(pmp) };
        }
        catch (std::bad_alloc const& ex)
        {
            std::cerr << ex.what() << std::endl;
            std::cerr << "Fail to create DLinkedList object!" << std::endl;
            throw;
        }

        inline static pmem::obj::string_view _ExtractKey_(pmem::obj::string_view internal_key)
        {
            constexpr size_t sz_id = 8;
            assert(sz_id <= internal_key.size() && "internal_key does not has space for key");
            return pmem::obj::string_view(internal_key.data() + sz_id, internal_key.size() - sz_id);
        }

        inline static std::uint64_t _ExtractID_(pmem::obj::string_view internal_key)
        {
            std::uint64_t id;
            assert(sizeof(decltype(id)) <= internal_key.size() && "internal_key is smaller than the size of an id!");
            memcpy(&id, internal_key.data(), sizeof(decltype(id)));
            return id;
        }

        friend std::ostream& operator<<(std::ostream& out, DLinkedList const& dlist)
        {
            out << "Contents of DlinkedList:\n";
            DlistIterator iter = DlistIterator{dlist._sp_pmem_allocator_, dlist._pmp_head_};
            while (iter != DlistIterator{dlist._sp_pmem_allocator_, dlist._pmp_tail_})
            {
                auto internal_key = iter->Key();
                out << "Type: " << hex_print(iter->type)    <<"\t"
                    << "Offset: " << hex_print(iter._GetOffset_())  << "\t"
                    << "Prev: " << hex_print(iter->prev)    <<"\t"
                    << "Next: " << hex_print(iter->next)    <<"\t"
                    << "Key: "  << hex_print(_ExtractID_(internal_key))<<_ExtractKey_(internal_key)<<"\t"
                    << "Value: "<< iter->Value()<<"\n";
                ++iter;
            }
            auto internal_key = iter->Key();
            out << "Type: " << hex_print(iter->type)    <<"\t"
                << "Offset: " << hex_print(iter._GetOffset_())  << "\t"
                << "Prev: " << hex_print(iter->prev)    <<"\t"
                << "Next: " << hex_print(iter->next)    <<"\t"
                << "Key: "  << hex_print(_ExtractID_(internal_key))<<_ExtractKey_(internal_key)<<"\t"
                << "Value: "<< iter->Value()<<"\n";
            return out;
        }
    };

} // namespace KVDK_NAMESPACE

