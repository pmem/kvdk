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
#include "dlinked_list.hpp"

namespace KVDK_NAMESPACE 
{
    /// UnorderedCollection is stored on DRAM, indexed by HashTable
    class UnorderedCollection : public PersistentList 
    {
    private:
        std::shared_ptr<PMEMAllocator> _sp_pmem_allocator_;
        std::shared_ptr<HashTable> _sp_hash_table_;
        kvdk::DLinkedList _dlinked_list_;

        inline void _deallocate_record_(kvdk::DLinkedList::Iterator iter)
        {
            _sp_pmem_allocator_->Free(SizedSpaceEntry{ iter._get_offset_(), iter->header.b_size, iter->timestamp });
        }


    public:
        UnorderedCollection
        (
            std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
            std::shared_ptr<HashTable> sp_hash_table
        ):
            _sp_pmem_allocator_{sp_pmem_allocator},
            _sp_hash_table_{ sp_hash_table }
        {
        }



        uint64_t id() override { return id_; }

        const std::string& name() { return name_; }

        SkiplistNode* header() { return header_; }

        static int RandomHeight() {
            int height = 0;
            while (height < kMaxHeight && fast_random() & 1) {
                height++;
            }

            return height;
        }

        inline static pmem::obj::string_view
            UserKey(const pmem::obj::string_view& skiplist_key) {
            return pmem::obj::string_view(skiplist_key.data() + 8,
                skiplist_key.size() - 8);
        }

        void Seek(const pmem::obj::string_view& key, Splice* splice);

        Status Rebuild();

        bool FindAndLockWritePos(Splice* splice,
            const pmem::obj::string_view& insert_key,
            const HashTable::KeyHashHint& hint,
            std::vector<SpinMutex*>& spins,
            DLDataEntry* updated_data_entry);

        void* InsertDataEntry(Splice* insert_splice, DLDataEntry* inserting_entry,
            const pmem::obj::string_view& inserting_key,
            SkiplistNode* node);

        void DeleteDataEntry(Splice* delete_splice,
            const pmem::obj::string_view& deleting_key,
            SkiplistNode* node);

    private:
        std::string name_;
        uint64_t id_;
        std::shared_ptr<HashTable> hash_table_;
        std::shared_ptr<PMEMAllocator> pmem_allocator_;
    };

    class SortedIterator : public Iterator {
    public:
        SortedIterator(Skiplist* skiplist,
            const std::shared_ptr<PMEMAllocator>& pmem_allocator)
            : skiplist_(skiplist), pmem_allocator_(pmem_allocator), current(nullptr) {
        }

        virtual void Seek(const std::string& key) override {
            assert(skiplist_);
            Skiplist::Splice splice;
            skiplist_->Seek(key, &splice);
            current = splice.next_data_entry;
            while (current->type == SORTED_DELETE_RECORD) {
                current = (DLDataEntry*)(pmem_allocator_->offset2addr(current->next));
            }
        }

        virtual void SeekToFirst() override {
            uint64_t first = skiplist_->header()->data_entry->next;
            current = (DLDataEntry*)pmem_allocator_->offset2addr(first);
            while (current && current->type == SORTED_DELETE_RECORD) {
                current = (DLDataEntry*)pmem_allocator_->offset2addr(current->next);
            }
        }

        virtual bool Valid() override {
            return (current != nullptr) && (current->type != SORTED_DELETE_RECORD);
        }

        virtual bool Next() override {
            if (!Valid()) {
                return false;
            }
            do {
                current = (DLDataEntry*)pmem_allocator_->offset2addr(current->next);
            } while (current && current->type == SORTED_DELETE_RECORD);
            return current != nullptr;
        }

        virtual bool Prev() override {
            if (!Valid()) {
                return false;
            }

            do {
                current = (DLDataEntry*)(pmem_allocator_->offset2addr(current->prev));
            } while (current->type == SORTED_DELETE_RECORD);

            if (current == skiplist_->header()->data_entry) {
                current = nullptr;
                return false;
            }

            return true;
        }

        virtual std::string Key() override {
            if (!Valid())
                return "";
            pmem::obj::string_view key = Skiplist::UserKey(current->Key());
            return std::string(key.data(), key.size());
        }

        virtual std::string Value() override {
            if (!Valid())
                return "";
            pmem::obj::string_view value = current->Value();
            return std::string(value.data(), value.size());
        }

    private:
        Skiplist* skiplist_;
        std::shared_ptr<PMEMAllocator> pmem_allocator_;
        DLDataEntry* current;
    };
} // namespace KVDK_NAMESPACE
