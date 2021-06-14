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
#include "Int.h"
#include "Table.h"
#include "Util.h"

namespace hyde {
namespace rt {

using index_t = size_t;

// DrLojekyll supported types
using ASCII = std::string;
using UTF8 = std::string;
using Any = void;
using Bytes = std::vector<uint8_t>;
using UUID = uint128_t;

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

template <unsigned kIndexId>
struct IndexTag {};

template <unsigned kTableId>
struct TableTag {};

template <typename StorageT, const unsigned  kTableId>
class Table;

template <typename StorageT, const unsigned kIndexId>
class Index;

template <typename StorageT, typename IndexOrTableTag>
class Scan;

template <typename StorageT, unsigned kNumPivots, typename... IndexOrTableTags>
class Join;

// A vector-like object that holds a reference to a serialized view of data and
// and hands back SerialRefs
template <typename StorageT, typename... Ts>
class ReadOnlySerializedVector;

}  // namespace rt
}  // namespace hyde
