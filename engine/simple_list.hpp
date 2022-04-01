#pragma once

#include "generic_list.hpp"

namespace KVDK_NAMESPACE {
using List = GenericList<RecordType::ListRecord, RecordType::ListElem>;
using ListBuilder =
    GenericListBuilder<RecordType::ListRecord, RecordType::ListElem>;
}  // namespace KVDK_NAMESPACE
