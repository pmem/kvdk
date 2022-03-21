#include "queue.hpp"

namespace KVDK_NAMESPACE {
Queue::Queue(PMEMAllocator* pmem_allocator_ptr, std::string const name,
             CollectionIDType id, TimeStampType timestamp, bool is_expired)
    : Collection{name, id, is_expired},
      collection_record_ptr_{nullptr},
      dlinked_list_{pmem_allocator_ptr, timestamp,
                    CollectionUtils::EncodeID(id), StringView{""}},
      timestamp_{timestamp} {
  {
    auto list_record_space = dlinked_list_.pmem_allocator_ptr_->Allocate(
        sizeof(DLRecord) + Name().size() + sizeof(CollectionIDType));
    if (list_record_space.size == 0) {
      dlinked_list_.purgeAndFree(dlinked_list_.Head().GetCurrentAddress());
      dlinked_list_.purgeAndFree(dlinked_list_.Tail().GetCurrentAddress());
      dlinked_list_.head_pmmptr_ = nullptr;
      dlinked_list_.tail_pmmptr_ = nullptr;
      throw std::bad_alloc{};
    }
    PMemOffsetType offset_list_record = list_record_space.offset;
    collection_record_ptr_ = DLRecord::PersistDLRecord(
        dlinked_list_.pmem_allocator_ptr_->offset2addr_checked(
            offset_list_record),
        list_record_space.size, timestamp, RecordType::QueueRecord,
        kNullPMemOffset, dlinked_list_.Head().GetCurrentOffset(),
        dlinked_list_.Tail().GetCurrentOffset(), Name(),
        CollectionUtils::EncodeID(ID()));
  }
}

Queue::Queue(PMEMAllocator* pmem_allocator_ptr, DLRecord* collection_record)
    : Collection{string_view_2_string(collection_record->Key()),
                 CollectionUtils::DecodeID(collection_record->Value())},
      collection_record_ptr_{collection_record},
      dlinked_list_{
          pmem_allocator_ptr,
          pmem_allocator_ptr->offset2addr_checked<DLRecord>(
              collection_record->prev),
          pmem_allocator_ptr->offset2addr_checked<DLRecord>(
              collection_record->next),
      },
      timestamp_{collection_record->entry.meta.timestamp} {
  sz_ = 0;
  for (iterator iter = dlinked_list_.First(); iter != dlinked_list_.Tail();
       ++iter)
    ++sz_;
}

void Queue::PushFront(TimeStampType timestamp, StringView const value) {
  LockType lock_queue{queue_lock_};
  dlinked_list_.EmplaceFront(timestamp, InternalKey(""), value);
  ++sz_;
}

void Queue::PushBack(TimeStampType timestamp, StringView const value) {
  LockType lock_queue{queue_lock_};
  dlinked_list_.EmplaceBack(timestamp, InternalKey(""), value);
  ++sz_;
}

bool Queue::PopFront(std::string* value_got) {
  LockType lock_queue{queue_lock_};
  if (sz_ > 0) {
    --sz_;
    DLRecord* old_front = dlinked_list_.First().GetCurrentAddress();
    auto val = old_front->Value();
    kvdk_assert(CollectionUtils::ExtractID(old_front->Key()) == ID(), "");
    value_got->assign(val.data(), val.size());
    dlinked_list_.PopFront();

    lock_queue.unlock();

    dlinked_list_.purgeAndFree(old_front);
    return true;
  } else {
    return false;
  }
}

bool Queue::PopBack(std::string* value_got) {
  LockType lock_queue{queue_lock_};
  if (sz_ > 0) {
    --sz_;
    DLRecord* old_back = dlinked_list_.Last().GetCurrentAddress();
    auto val = old_back->Value();
    kvdk_assert(CollectionUtils::ExtractID(old_back->Key()) == ID(), "");
    value_got->assign(val.data(), val.size());
    dlinked_list_.PopBack();

    lock_queue.unlock();

    dlinked_list_.purgeAndFree(old_back);
    return true;
  } else {
    return false;
  }
}

}  // namespace KVDK_NAMESPACE