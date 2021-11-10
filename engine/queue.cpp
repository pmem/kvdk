#include "queue.hpp"

namespace KVDK_NAMESPACE {
Queue::Queue(PMEMAllocator *pmem_allocator_ptr, std::string const name,
             CollectionIDType id, TimeStampType timestamp) 
    : collection_record_ptr_{nullptr},
      dlinked_list_{pmem_allocator_ptr, timestamp, id2View(id), StringView{""}},
      collection_name_{name}, collection_id_{id}, timestamp_{timestamp} {
  {
    auto list_record_space = dlinked_list_.pmem_allocator_ptr_->Allocate(
        sizeof(DLRecord) + collection_name_.size() + sizeof(CollectionIDType));
    if (list_record_space.size == 0) {
      dlinked_list_.purgeAndFree(dlinked_list_.Head().GetCurrentAddress());
      dlinked_list_.purgeAndFree(dlinked_list_.Tail().GetCurrentAddress());
      dlinked_list_.head_pmmptr_ = nullptr;
      dlinked_list_.tail_pmmptr_ = nullptr;
      throw std::bad_alloc{};
    }
    PMemOffsetType offset_list_record = list_record_space.space_entry.offset;
    collection_record_ptr_ = DLRecord::PersistDLRecord(
        dlinked_list_.pmem_allocator_ptr_->offset2addr_checked(
            offset_list_record),
        list_record_space.size, timestamp, RecordType::QueueRecord,
        dlinked_list_.Head().GetCurrentOffset(),
        dlinked_list_.Tail().GetCurrentOffset(), collection_name_,
        id2View(collection_id_));
  }
}

Queue::Queue(PMEMAllocator *pmem_allocator_ptr, DLRecord *collection_record)
    : collection_record_ptr_{collection_record},
      dlinked_list_{
          pmem_allocator_ptr,
          pmem_allocator_ptr->offset2addr_checked<DLRecord>(
              collection_record->prev),
          pmem_allocator_ptr->offset2addr_checked<DLRecord>(
              collection_record->next),
      },
      collection_name_{string_view_2_string(collection_record->Key())},
      collection_id_{view2ID(collection_record->Value())},
      timestamp_{collection_record->entry.meta.timestamp} {}

void Queue::LPush(TimeStampType timestamp,
                  StringView const value) {
    LockType lock_queue{queue_lock_};
    dlinked_list_.EmplaceFront(timestamp, makeInternalKey(""), value);
    ++sz_;
}

void Queue::RPush(TimeStampType timestamp,
                  StringView const value) {
    LockType lock_queue{queue_lock_};
    dlinked_list_.EmplaceBack(timestamp, makeInternalKey(""), value);
    ++sz_;
}

bool Queue::LPop(std::string *value_got) {
    LockType lock_queue{queue_lock_};
    if (sz_ > 0)
    {
      --sz_;
      DLRecord* old_front = dlinked_list_.First().GetCurrentAddress();
      auto val = old_front->Value();
      kvdk_assert(extractID(old_front->Key()) == ID(), "");
      value_got->assign(val.data(), val.size());
      dlinked_list_.PopFront();
      return true;
    }
    else
    {
      return false;
    }
}

bool Queue::RPop(std::string *value_got) {
    LockType lock_queue{queue_lock_};
    if (sz_ > 0)
    {
      --sz_;
      DLRecord* old_back = dlinked_list_.Last().GetCurrentAddress();
      auto val = old_back->Value();
      kvdk_assert(extractID(old_front->Key()) == ID(), "");
      value_got->assign(val.data(), val.size());
      dlinked_list_.PopBack();
      return true;
    }
    else
    {
      return false;
    }  
}

} // namespace KVDK_NAMESPACE