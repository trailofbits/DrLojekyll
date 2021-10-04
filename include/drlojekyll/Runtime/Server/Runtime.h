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

#include <drlojekyll/Runtime/Endian.h>
#include <drlojekyll/Runtime/Int.h>

#include "Column.h"
#include "Index.h"
#include "Reference.h"
#include "Table.h"
#include "Util.h"

namespace hyde {
namespace rt {

using index_t = size_t;

// DrLojekyll supported types
using Any = void;
using Bytes = std::vector<uint8_t>;

template <typename T, typename Traits, typename Alloc>
static std::vector<uint8_t> BytesFromString(
    const std::basic_string<T, Traits, Alloc> &str) {
  std::vector<uint8_t> ret;
  ret.reserve(str.size() * sizeof(T));
  for (auto c : str) {
    if constexpr (sizeof(T) == 1u) {
      ret.push_back(static_cast<uint8_t>(c));

    // Little endian.
    } else if HYDE_RT_CONSTEXPR_ENDIAN (HYDE_RT_LITTLE_ENDIAN) {
      if constexpr (sizeof(T) == 2u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 0));
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 8));
      } else if constexpr (sizeof(T) == 4u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 0));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 16));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 24));
      } else {
        static_assert(sizeof(T) == 1u || sizeof(T) == 2u || sizeof(T) == 4u);
      }

    // Big endian.
    } else {
      if constexpr (sizeof(T) == 2u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 0));
      } else if constexpr (sizeof(T) == 4u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 24));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 16));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 0));
      } else {
        static_assert(sizeof(T) == 1u || sizeof(T) == 2u || sizeof(T) == 4u);
      }
    }
  }
  return ret;
}

template <typename T>
static std::vector<uint8_t> BytesFromString(std::basic_string_view<T> str) {
  std::vector<uint8_t> ret;
  ret.reserve(str.size() * sizeof(T));
  for (auto c : str) {
    if constexpr (sizeof(T) == 1u) {
      ret.push_back(static_cast<uint8_t>(c));

    // Little endian.
    } else if HYDE_RT_CONSTEXPR_ENDIAN (HYDE_RT_LITTLE_ENDIAN) {
      if constexpr (sizeof(T) == 2u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 0));
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 8));
      } else if constexpr (sizeof(T) == 4u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 0));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 16));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 24));
      } else {
        static_assert(sizeof(T) == 1u || sizeof(T) == 2u || sizeof(T) == 4u);
      }

    // Big endian.
    } else {
      if constexpr (sizeof(T) == 2u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 0));
      } else if constexpr (sizeof(T) == 4u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 24));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 16));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 0));
      } else {
        static_assert(sizeof(T) == 1u || sizeof(T) == 2u || sizeof(T) == 4u);
      }
    }
  }
  return ret;
}

enum class TupleState : uint8_t {
  kAbsent,
  kPresent,
  kUnknown,
};

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
