// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <cassert>
#include <climits>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "Column.h"
#include "Index.h"
#include "Table.h"
#include "Util.h"

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

// An append-only and iterable container for serialized data
template <typename StorageT, typename... Columns>
class SerializedVector;

template <typename StorageT, typename... Columns>
class Vector;

template <typename StorageT, const unsigned  kTableId>
class Table;

template <typename StorageT, const unsigned kIndexId>
class Index;

// A vector-like object that holds a reference to a serialized view of data and
// and hands back SerialRefs
template <typename StorageT, typename... Ts>
class ReadOnlySerializedVector;

}  // namespace rt
}  // namespace hyde
