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

namespace KVDK_NAMESPACE
{

    class DLinkedList : public PersistentList
    {
    private:
        DLDataEntry* p_head_;
        DLDataEntry* p_tail_;
        std::string name_;
        uint64_t id_;
        std::shared_ptr<HashTable> hash_table_;
        std::shared_ptr<PMEMAllocator> pmem_allocator_;

    public:
        DLinkedList(DLDataEntry* p_head, DLDataEntry* p_tail, const std::string& name, uint64_t id,
            const std::shared_ptr<PMEMAllocator>& pmem_allocator, std::shared_ptr<HashTable> hash_table) : 
            name_{ name },
            id_{ id },
            pmem_allocator_{ pmem_allocator },
            hash_table_{ hash_table },
            p_head_{ p_head },
            p_tail_{p_tail}
        {
        }

        ~Skiplist()
        {
            if (header_)
            {
                SkiplistNode* to_delete = header_;
                while (to_delete)
                {
                    SkiplistNode* next = to_delete->Next(1);
                    SkiplistNode::DeleteNode(to_delete);
                    to_delete = next;
                }
            }
        }

        uint64_t id() override
        {
            return id_;
        }

        const std::string& name()
        {
            return name_;
        }

        DLDataEntry* head()
        {
            return p_head_;
        }

        DLDataEntry* tail()
        {
            return p_tail_;
        }

        inline static Slice UserKey(const Slice& list_key)
        {
            // 8 bytes for id
            return Slice(list_key.data() + 8, list_key.size() - 8);
        }

        void Seek(const Slice& key, Splice* splice);

        Status Rebuild();

        void* InsertDataEntry(Splice* insert_splice, DLDataEntry* inserting_entry, const Slice& inserting_key,
            SkiplistNode* node);

        void DeleteDataEntry(Splice* delete_splice, const Slice& deleting_key, SkiplistNode* node);

    public:
        class UnsortedIterator : public Iterator
        {
        private:
            std::shared_ptr<DLinkedList> dlinked_list;
            DLDataEntry* current;

        public:
            UnsortedIterator(DLinkedList* p_list) : 
                dlinked_list(p_list), 
                current(p_list->head())
            {
            }

            virtual void SeekToFirst() override
            {
                current = dlinked_list->head();
                Next();
            }

            bool Ok()
            {
                bool ok = true;
                ok = ok && (current != nullptr);
                ok = ok &&
                    (
                        current->type
                        &
                        (
                            DATA_ENTRY_TYPE::DLIST_DATA_RECORD |
                            DATA_ENTRY_TYPE::DLIST_DELETE_RECORD |
                            DATA_ENTRY_TYPE::DLIST_HEAD_RECORD |
                            DATA_ENTRY_TYPE::DLIST_TAIL_RECORD
                        )
                    );
                return ok;
            }

            virtual bool Valid() override
            {
                return Ok() && (current->type==)
            }

            virtual bool Next() override
            {
                while (Valid())
                {
                    current = reinterpret_cast<DLDataEntry*>(dlinked_list->pmem_allocator_->offset2addr(current->next));
                    if (current->type == DATA_ENTRY_TYPE::DLIST_DATA_RECORD)
                    {
                        return true;
                    }
                    else if (current->type == DATA_ENTRY_TYPE::DLIST_DELETE_RECORD)
                    {
                        continue;
                    }
                    else
                    {
                        // Reaching tail
                        return false;
                    }
                }
            }

            virtual bool Prev() override
            {
                while (Valid())
                {
                    current = reinterpret_cast<DLDataEntry*>(dlinked_list->pmem_allocator_->offset2addr(current->prev));
                    if (current->type == DATA_ENTRY_TYPE::DLIST_DATA_RECORD)
                    {
                        return true;
                    }
                    else if (current->type == DATA_ENTRY_TYPE::DLIST_DELETE_RECORD)
                    {
                        continue;
                    }
                    else
                    {
                        // Reaching head
                        return false;
                    }
                }
            }

            virtual std::string Key() override
            {
                if (!Valid())
                    return "";
                return DLinkedList::UserKey(current->Key()).to_string();
            }

            virtual std::string Value() override
            {
                if (!Valid())
                    return "";
                return current->Value().to_string();
            }
        };

    };

} // namespace KVDK_NAMESPACE
