// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <cassert>
#include <cstring>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <tuple>
#include <type_traits>
#include <utility>

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

#define DRLOJEKYLL_HYDE_RT_NAMESPACE_BEGIN
#define DRLOJEKYLL_HYDE_RT_NAMESPACE_END
#define DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(type, cast_type, method_suffix) \
    DRLOJEKYLL_HYDE_RT_NAMESPACE_BEGIN \
    template <typename Writer> \
    struct Serializer<Writer, type> { \
      static inline void AppendKeySort(Writer &writer, type data) { \
        writer.Append ## method_suffix (static_cast<cast_type>(data)); \
      } \
      static inline void AppendKeyUnique(Writer &writer, type data) {} \
      static inline void AppendKeyData(Writer &writer, type data) {} \
      static inline void AppendValue(Writer &writer, type data) { \
        writer.Append ## method_suffix(static_cast<cast_type>(data)); \
      } \
    }; \
    DRLOJEKYLL_HYDE_RT_NAMESPACE_END


DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(char, uint8_t, U8)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(uint8_t, uint8_t, U8)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(int8_t, uint8_t, U8)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(uint16_t, uint16_t, U16)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(int16_t, uint16_t, U16)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(uint32_t, uint32_t, U32)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(int32_t, uint32_t, U32)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(uint64_t, uint64_t, U64)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(int64_t, uint64_t, U64)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(float, float, F32)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(double, double, F64)

#undef DRLOJEKYLL_HYDE_RT_NAMESPACE_BEGIN
#undef DRLOJEKYLL_HYDE_RT_NAMESPACE_END

template <typename Writer, typename ContainerType, typename ElementType>
struct LinearContainerSerializer {

  static inline void AppendKeySort(Writer &writer,
                                   const ContainerType &data) {
    const auto len = static_cast<uint32_t>(data.size());
    writer.AppendU32(len);
  }

  static inline void AppendKeyUnique(Writer &writer,
                                     const ContainerType &data) {
    for (const ElementType &val : data) {
      Serializer<Writer, ElementType>::AppendValue(writer, val);
    }
  }

  static inline void AppendKeyData(Writer &writer,
                                   const ContainerType &data) {}

  static inline void AppendValue(Writer &writer, const ContainerType &data) {
    const auto len = static_cast<uint32_t>(data.size());
    writer.AppendU32(len);
    for (const ElementType &val : data) {
      Serializer<Writer, ElementType>::AppendValue(writer, val);
    }
  }
};

template <typename Writer, typename T>
struct Serializer<Writer, std::vector<T>>
    : public LinearContainerSerializer<Writer, std::vector<T>, T> {};

template <typename Writer>
struct Serializer<Writer, std::string>
    : public LinearContainerSerializer<Writer, std::string, char> {};

template <typename Writer>
struct Serializer<Writer, std::string_view>
    : public LinearContainerSerializer<Writer, std::string_view, char> {};

template <typename Writer, typename Val, size_t i, typename First, typename... Rest>
struct IndexedSerializer {
  static inline void AppendKeySort(Writer &writer, const Val &data) {
    Serializer<Writer, First>::AppendKeySort(writer, std::get<i>(data));
    if constexpr (sizeof...(Rest)) {
      IndexedSerializer<Writer, Val, i + 1u, Rest...>::AppendKeySort(writer, data);
    }
  }

  static inline void AppendKeyUnique(Writer &writer, const Val &data) {
    Serializer<Writer, First>::AppendKeyUnique(writer, std::get<i>(data));
    if constexpr (sizeof...(Rest)) {
      IndexedSerializer<Writer, Val, i + 1u, Rest...>::AppendKeyUnique(writer, data);
    }
  }

  static inline void AppendKeyData(Writer &writer, const Val &data) {
    Serializer<Writer, First>::AppendKeyData(writer, std::get<i>(data));
    if constexpr (sizeof...(Rest)) {
      IndexedSerializer<Writer, Val, i + 1u, Rest...>::AppendKeyData(writer, data);
    }
  }

  static inline void AppendValue(Writer &writer, const Val &data) {
    Serializer<Writer, First>::AppendValue(writer, std::get<i>(data));
    if constexpr (sizeof...(Rest)) {
      IndexedSerializer<Writer, Val, i + 1u, Rest...>::AppendValue(writer, data);
    }
  }
};

template <typename Writer, typename A, typename B>
struct Serializer<Writer, std::pair<A, B>>
    : public IndexedSerializer<Writer, std::tuple<A, B>, 0, A, B> {};

template <typename Writer, typename... Elems>
struct Serializer<Writer, std::tuple<Elems...>>
    : public IndexedSerializer<Writer, std::tuple<Elems...>, 0, Elems...> {};


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

// DrLojekyll supported types
using Bytes = std::vector<std::uint8_t>;

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

#define DRLOJEKYLL_HYDE_RT_NAMESPACE_BEGIN namespace hyde::rt {
#define DRLOJEKYLL_HYDE_RT_NAMESPACE_END }

