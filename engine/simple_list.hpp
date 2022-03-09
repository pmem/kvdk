#pragma once
#ifndef SIMPLE_LIST_HPP
#define SIMPLE_LIST_HPP

#include "generic_list.hpp"

namespace KVDK_NAMESPACE {
using List = GenericList<RecordType::ListRecord, RecordType::ListElem>;
using ListBuilder =
    GenericListBuilder<RecordType::ListRecord, RecordType::ListElem>;
}  // namespace KVDK_NAMESPACE

#endif  // SIMPLE_LIST_HPP