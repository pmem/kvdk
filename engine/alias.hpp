#include <cinttypes>

#include "../libpmemobj++/string_view.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using PMemOffsetType = std::uint64_t;
using TimeStampType = std::uint64_t;
using CollectionIDType = std::uint64_t;
using KeyHashType = std::uint64_t;
} // namespace KVDK_NAMESPACE
