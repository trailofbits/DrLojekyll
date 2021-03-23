// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <string>
#include <tuple>
#include <type_traits>

namespace hyde {
namespace rt {

template <class T>
class Key;

template <typename T>
class Value;

template <typename KVColDesc>
struct ValueType;

template <typename ColDesc>
struct ValueType<Key<ColDesc>> {
  using type = typename ColDesc::type;
};

template <typename ColDesc>
struct ValueType<Value<ColDesc>> {
  using type = typename ColDesc::type;
};

template <typename T>
struct IsKey : std::false_type {};

template <typename T>
struct IsKey<Key<T>> : std::true_type {};

template <typename T>
struct IsValue : std::false_type {};

template <typename T>
struct IsValue<Value<T>> : std::true_type {};

// Template parameter pack filtering to collect all Keys/Values
template <template <typename> class F, typename T>
struct filter {
  using type =
      std::conditional_t<F<T>::value, std::tuple<typename ValueType<T>::type>,
                         std::tuple<>>;
};

// Concatenate multiple tuples into one (removing empty tuples)
template <typename... T>
using tuple_cat_t = decltype(std::tuple_cat(std::declval<T>()...));

template <template <typename> class F, typename... Ts>
struct filtered {
  using type = tuple_cat_t<typename filter<F, Ts>::type...>;
};

// Methods to overload for writing data
template <typename Writer, typename DataT>
struct WriteData {
  static inline void AppendKeySort(Writer &writer, const DataT &data);
  static inline void AppendKeyUnique(Writer &writer, const DataT &data);
  static inline void AppendKeyData(Writer &writer, const DataT &data);
  static inline void AppendValue(Writer &writer, const DataT &data);
};

// Writing keys and values with a pack
template <typename Writer, typename... Columns>
struct KeyValueWriter;

// Write a single key column
template <typename Writer, typename Column>
struct KeyValueWriter<Writer, Key<Column>> {
 public:
  using ColumnType = typename Column::type;
  static inline void WriteKeySort(Writer &writer, const ColumnType &val) {
    WriteData<Writer, ColumnType>::AppendKeySort(writer, val);
  }
  static inline void WriteKeyUnique(Writer &writer, const ColumnType &val) {
    WriteData<Writer, ColumnType>::AppendKeyUnique(writer, val);
  }
  static inline void WriteKeyData(Writer &writer, const ColumnType &val) {
    WriteData<Writer, ColumnType>::AppendKeyData(writer, val);
  }
  static inline void WriteValue(Writer &, const ColumnType &val) {}
};

// Write a single value column
template <typename Writer, typename Column>
struct KeyValueWriter<Writer, Value<Column>> {
 public:
  using ColumnType = typename Column::type;
  static inline void WriteKeySort(Writer &writer, const ColumnType &val) {}
  static inline void WriteKeyUnique(Writer &writer, const ColumnType &val) {}
  static inline void WriteKeyData(Writer &writer, const ColumnType &val) {}
  static inline void WriteValue(Writer &writer, const ColumnType &val) {
    WriteData<Writer, ColumnType>::AppendValue(writer, val);
  }
};

// Don't do anything if nothing to write
template <typename Writer>
struct KeyValueWriter<Writer> {
 public:
  static inline void WriteKeySort(Writer &) {}
  static inline void WriteKeyUnique(Writer &) {}
  static inline void WriteKeyData(Writer &) {}
  static inline void WriteValue(Writer &) {}
};

// Unpack template pack and write the Key/Value columns as they appear
template <typename Writer, typename Column, typename... Columns>
struct KeyValueWriter<Writer, Column, Columns...> {
 public:
  using ColumnType = typename ValueType<Column>::type;

  static inline void
  WriteKeySort(Writer &writer, const ColumnType &val,
               const typename ValueType<Columns>::type &...rest) {
    KeyValueWriter<Writer, Column>::WriteKeySort(writer, val);
    KeyValueWriter<Writer, Columns...>::WriteKeySort(writer, rest...);
  }
  static inline void
  WriteKeyUnique(Writer &writer, const ColumnType &val,
                 const typename ValueType<Columns>::type &...rest) {
    KeyValueWriter<Writer, Column>::WriteKeyUnique(writer, val);
    KeyValueWriter<Writer, Columns...>::WriteKeyUnique(writer, rest...);
  }
  static inline void
  WriteKeyData(Writer &writer, const ColumnType &val,
               const typename ValueType<Columns>::type &...rest) {
    KeyValueWriter<Writer, Column>::WriteKeyData(writer, val);
    KeyValueWriter<Writer, Columns...>::WriteKeyData(writer, rest...);
  }
  static inline void
  WriteValue(Writer &writer, const ColumnType &val,
             const typename ValueType<Columns>::type &...rest) {
    KeyValueWriter<Writer, Column>::WriteValue(writer, val);
    KeyValueWriter<Writer, Columns...>::WriteValue(writer, rest...);
  }
};

template <typename StorageT, typename TableId, typename... Columns>
class Table;

template <typename StorageT, typename TableId, const unsigned kIndexId,
          typename... Columns>
class Index;

// Method on an index for AddToIndex that takes in all columns and does the
// right thing for updating based on Key, Value of column Ids


using UTF8 = std::string_view;
using Any = void;

/* **************************************** */
/* START https://stackoverflow.com/a/264088 */

// Templated function <T, Sign> named 'name' that checks whether the type `T`
// has a member function named 'func' with signature `Sign`.
// See stackoverflow link for usage.
#define HAS_MEM_FUNC(func, name) \
  template <typename T, typename Sign> \
  struct name { \
    typedef char yes[1]; \
    typedef char no[2]; \
    template <typename U, U> \
    struct type_check; \
    template <typename _1> \
    static yes &chk(type_check<Sign, &_1::func> *); \
    template <typename> \
    static no &chk(...); \
    static bool const value = sizeof(chk<T>(0)) == sizeof(yes); \
  }

template <bool C, typename T = void>
struct enable_if {
  typedef T type;
};

template <typename T>
struct enable_if<false, T> {};

HAS_MEM_FUNC(merge_into, has_merge_into);

/* END  https://stackoverflow.com/a/264088 */
/* *************************************** */

}  // namespace rt
}  // namespace hyde
