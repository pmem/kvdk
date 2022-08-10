#include "dl_list.hpp"

namespace KVDK_NAMESPACE {
std::unique_ptr<DLListRecordIterator> DLList::GetRecordIterator() {
  return std::unique_ptr<DLListRecordIterator>(
      new DLListRecordIterator(this, pmem_allocator_));
}
}  // namespace KVDK_NAMESPACE