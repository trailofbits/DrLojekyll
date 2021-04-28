// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <cassert>
#include <cstring>
#include <string>
#include <tuple>
#include <type_traits>

namespace hyde {
namespace rt {

// Used in Tabe::TransitionState to determine how to check and modify an entry's state value
enum class TupleState : unsigned {
  kPresent,
  kAbsent,
  kUnknown,
  kAbsentOrUnknown
};

// Constants for the different state values
static constexpr auto kStateMask = 0x3u;
static constexpr auto kStatePresentBit = 0x4u;
static constexpr auto kStateAbsent = 0u;
static constexpr auto kStatePresent = 1u;
static constexpr auto kStateUnknown = 2u;

template <class T>
struct TypeIdentity {
  using type = T;
};

template <typename... Ts>
struct TypeList;

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
struct filtered_tuple {
  using type = tuple_cat_t<typename filter<F, Ts>::type...>;
};

// A vector-like object that holds a reference to a serialized view of data and
// and hands back SerialRefs
template <typename BackingStore, class T>
class VectorRef;

// Reference to a BackingStoreT container that holds a contiguous serialized form
// of type T at an offset. The object T is reified from BackingStoreT using
// the starting memory address at the offset until the size of the object
// instance (where the size should be encoded in the serialized form if the type
// is not a fundamental type).
template <typename BackingStoreT, typename T>
struct SerialRef {
  explicit SerialRef(const BackingStoreT &store_, size_t offset_)
      : store(store_),
        offset(offset_) {}

  using Type = T;

  // Size of the element T for its contiguous serialized form
  size_t ElementSize() const {
    if constexpr (std::is_fundamental_v<T>) {
      return sizeof(T);
    } else {
      assert(0 && "ElementSize is only supported for fundamental types.");
    }
  }

  const T Reify() const {
    if constexpr (std::is_fundamental_v<T>) {
      return ReifyFundamental();
    } else {
      assert(0 && "Reify is only supported for fundamental types.");
    }
  }

  const auto begin() const {
    return &store[offset];
  }

  const auto end() const {
    return &store[ElementSize()];
  }

 private:
  // Fundamental types are returned by value
  const T ReifyFundamental() const {
    T tmp;
    std::memcpy(&tmp, &(store.at(offset)), ElementSize());
    return tmp;
  }

  const BackingStoreT &store;
  const size_t offset;
};

// A read-only reference to a group of objects at a particular offset within a
// serialized view (VectorRef)
template <typename BackingStore, class T>
class SerializedTupleRef;

// Methods to overload for serializing data
template <typename Writer, typename DataT>
struct Serializer {
  static inline void AppendKeySort(Writer &writer, const DataT &data) = delete;
  static inline void AppendKeyUnique(Writer &writer,
                                     const DataT &data) = delete;
  static inline void AppendKeyData(Writer &writer, const DataT &data) = delete;
  static inline void AppendValue(Writer &writer, const DataT &data) = delete;
};

// An append-only and iterable container for serialized data
template <typename BackingStore, typename DataT>
class SerializedVector;

// Writing keys and values with a pack
template <typename Writer, typename... Columns>
struct KeyValueWriter;

// Write a single key column
template <typename Writer, typename Column>
struct KeyValueWriter<Writer, Key<Column>> {
 public:
  using ColumnType = typename Column::type;
  static inline void WriteKeySort(Writer &writer, const ColumnType &val) {
    Serializer<Writer, ColumnType>::AppendKeySort(writer, val);
  }
  static inline void WriteKeyUnique(Writer &writer, const ColumnType &val) {
    Serializer<Writer, ColumnType>::AppendKeyUnique(writer, val);
  }
  static inline void WriteKeyData(Writer &writer, const ColumnType &val) {
    Serializer<Writer, ColumnType>::AppendKeyData(writer, val);
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
    Serializer<Writer, ColumnType>::AppendValue(writer, val);
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

using UTF8 = std::string_view;
using Any = void;

/* **************************************** */
/* START https://stackoverflow.com/a/264088 */

// Templated function <T, Sign> named 'name' that checks whether the type `T`
// has a member function named 'func' with signature `Sign`.
// See stackoverflow link for usage.
#define HAS_MEMBER_FUNC(func, name) \
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

HAS_MEMBER_FUNC(merge_into, has_merge_into);

/* END  https://stackoverflow.com/a/264088 */
/* *************************************** */

}  // namespace rt
}  // namespace hyde
