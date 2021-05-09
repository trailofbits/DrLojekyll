// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include "Util.h"

#include <cassert>
#include <climits>
#include <cstring>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <tuple>
#include <type_traits>
#include <utility>

#include "Column.h"
#include "Index.h"
#include "Table.h"

namespace hyde {
namespace rt {

using index_t = size_t;

// DrLojekyll supported types
using UTF8 = std::string;
using Any = void;
using Bytes = std::basic_string<uint8_t>;

enum class TupleState : uint8_t {
  kAbsent,
  kPresent,
  kUnknown,
};

template <typename... Ts>
struct TypeList;

// An append-only and iterable container for serialized data
template <typename BackingStore, typename... Columns>
class SerializedVector;

template <typename BackingStore, typename... Columns>
class Vector;

template <typename StorageT, typename TableId, typename... Columns>
class Table;

template <typename StorageT, typename TableId, const unsigned kIndexId,
          typename... Columns>
class Index;

// A vector-like object that holds a reference to a serialized view of data and
// and hands back SerialRefs
template <typename BackingStore, typename... Ts>
class ReadOnlySerializedVector;

}  // namespace rt
}  // namespace hyde

