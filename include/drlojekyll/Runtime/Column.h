// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include "Serializer.h"

namespace hyde {
namespace rt {

// Wrapper around a key column in an index. We list out all table columns as
// `Key`-wrapped or `Value`-wrapped. These classes are never defined. They exist
// to signal that some columns are keys and some are values, then we can find
// the types of those columns via the column descriptors.
template <unsigned kColId>
class KeyColumn;

template <unsigned kColId>
class ValueColumn;

// Specialized by code generation to contain a variety of information about
// specific columns. For example:
//
//    template <>
//    struct ColumnDescriptor<48> {
//      static constexpr bool kIsNamed = false;
//      static constexpr unsigned kId = 48;
//      static constexpr unsigned kTableId = 47;
//      static constexpr unsigned kOffset = 0
//      using Type = uint64_t;
//    };
template <unsigned kColId>
struct ColumnDescriptor;

template <unsigned kColId>
struct ValueType<KeyColumn<kColId>>
    : public ValueType<typename ColumnDescriptor<kColId>::Type> {};

template <unsigned kColId>
struct ValueType<ValueColumn<kColId>>
    : public ValueType<typename ColumnDescriptor<kColId>::Type> {};

template <unsigned kColId>
struct ValueType<ColumnDescriptor<kColId>>
    : public ValueType<typename ColumnDescriptor<kColId>::Type> {};

// A column serializer is a thin wrapper around a normal `Serializer`. It exists
// to be able to selectively serialize just the `Key`-wrapped columns, or just
// the `Value`-wrapped columns.
template <typename Writer, typename... Columns>
struct ColumnSerializer;

template <typename Writer, unsigned kColId>
struct ColumnSerializer<Writer, ColumnDescriptor<kColId>> {
 public:
  using ColumnType = typename ValueType<ColumnDescriptor<kColId>>::Type;

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void WriteKey(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::Write(writer, val);
  }

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void WriteValue(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::Write(writer, val);
  }
};

// Serialize a single `Key`-wrapped column.
template <typename Writer, unsigned kColId>
struct ColumnSerializer<Writer, KeyColumn<kColId>> {
 public:
  using ColumnType = typename ValueType<KeyColumn<kColId>>::Type;

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void WriteKey(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::Write(writer, val);
  }

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void WriteValue(Writer &, const ColumnType &val) {}
};

// Serialize a single `Value`-wrapped column.
template <typename Writer, unsigned kColId>
struct ColumnSerializer<Writer, ValueColumn<kColId>> {
 public:
  using ColumnType = typename ValueType<ValueColumn<kColId>>::Type;

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void WriteKey(Writer &writer, const ColumnType &val) {}

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void WriteValue(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::Write(writer, val);
  }
};

// Unpack template pack and write the Key/Value columns as they appear
template <typename Writer, typename Column, typename... Columns>
struct ColumnSerializer<Writer, Column, Columns...> {
 public:
  using ColumnType = typename ValueType<Column>::Type;

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void WriteKey(Writer &writer, const ColumnType &val,
                       const typename ValueType<Columns>::Type &...rest) {
    ColumnSerializer<Writer, Column>::WriteKey(writer, val);
    if constexpr (0u < sizeof...(Columns)) {
      ColumnSerializer<Writer, Columns...>::WriteKey(writer, rest...);
    }
  }

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void WriteValue(Writer &writer, const ColumnType &val,
                         const typename ValueType<Columns>::Type &...rest) {
    ColumnSerializer<Writer, Column>::WriteValue(writer, val);
    if constexpr (0u < sizeof...(Columns)) {
      ColumnSerializer<Writer, Columns...>::WriteValue(writer, rest...);
    }
  }
};

}  // namespace rt
}  // namespace hyde
