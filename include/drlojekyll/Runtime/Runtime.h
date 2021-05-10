// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace hyde {
namespace rt {

using index_t = size_t;

// DrLojekyll supported types
using UTF8 = std::string_view;
using Any = void;
struct Bytes : public std::vector<uint8_t> {
 public:
  inline Bytes(void) {}

  inline Bytes(const uint8_t *begin_, const uint8_t *end_)
      : std::vector<uint8_t>(begin_, end_) {}

  inline Bytes(const std::vector<uint8_t> &that) : std::vector<uint8_t>(that) {}


  inline Bytes(std::vector<uint8_t> &&that) noexcept
      : std::vector<uint8_t>(that) {}

  Bytes(std::string_view that)
      : Bytes(reinterpret_cast<const uint8_t *>(that.data()),
              reinterpret_cast<const uint8_t *>(that.data() + that.size())) {}

  Bytes(const std::string &that)
      : Bytes(reinterpret_cast<const uint8_t *>(that.data()),
              reinterpret_cast<const uint8_t *>(that.data() + that.size())) {}
};

enum class TupleState : uint8_t {
  kAbsent,
  kPresent,
  kUnknown,
};

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

template <typename ColDesc>
struct ValueType {
  using type = typename ColDesc::type;
};

template <typename ColDesc>
struct ValueType<Key<ColDesc>> {
  using type = typename ColDesc::type;
};

template <typename ColDesc>
struct ValueType<Value<ColDesc>> {
  using type = typename ColDesc::type;
};

struct ByteCountingWriter {
 public:
  inline void WriteF64(double d) {
    count += 8;
  }

  inline void WriteF32(float d) {
    count += 4;
  }

  inline void WriteU64(uint64_t d) {
    count += 8;
  }

  inline void WriteU32(uint32_t d) {
    count += 4;
  }

  inline void WriteU16(uint16_t h) {
    count += 2;
  }

  inline void WriteU8(uint8_t b) {
    count += 1;
  }

  size_t count{0};
};

struct NullReader {
 public:
  inline double ReadF64(void) {
    return {};
  }
  inline float ReadF32(void) {
    return {};
  }
  inline uint64_t ReadU64(uint64_t) {
    return {};
  }
  inline uint64_t ReadU32(uint32_t) {
    return {};
  }
  inline uint64_t ReadU16(uint16_t) {
    return {};
  }
  inline uint64_t ReadU8(uint8_t) {
    return {};
  }
};

struct NullWriter {
 public:
  inline void WriteF64(double) {}
  inline void WriteF32(float) {}
  inline void WriteU64(uint64_t) {}
  inline void WriteU32(uint32_t) {}
  inline void WriteU16(uint16_t) {}
  inline void WriteU8(uint8_t) {}
};

// Methods to overload for serializing data
template <typename Reader, typename Writer, typename DataT>
struct Serializer;

#define DRLOJEKYLL_HYDE_RT_NAMESPACE_BEGIN
#define DRLOJEKYLL_HYDE_RT_NAMESPACE_END
#define DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(type, cast_type, method_suffix) \
  DRLOJEKYLL_HYDE_RT_NAMESPACE_BEGIN \
  template <typename Reader, typename Writer> \
  struct Serializer<Reader, Writer, type> { \
    static inline void WriteKeySort(Writer &writer, type data) { \
      writer.Write##method_suffix(static_cast<cast_type>(data)); \
    } \
    static inline void WriteKeyUnique(Writer &writer, type data) {} \
    static inline void WriteKeyData(Writer &writer, type data) {} \
    static inline void WriteValue(Writer &writer, type data) { \
      writer.Write##method_suffix(static_cast<cast_type>(data)); \
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

template <typename Reader, typename Writer, typename ContainerType,
          typename ElementType>
struct LinearContainerSerializer {

  static inline void WriteKeySort(Writer &writer, const ContainerType &data) {
    const auto len = static_cast<uint32_t>(data.size());
    writer.WriteU32(len);
  }

  static inline void WriteKeyUnique(Writer &writer, const ContainerType &data) {
    if constexpr (std::is_fundamental_v<ElementType> ||
                  std::is_enum_v<ElementType>) {
      for (ElementType val : data) {
        Serializer<Reader, Writer, ElementType>::WriteValue(writer, val);
      }
    } else {
      for (const ElementType &val : data) {
        Serializer<Reader, Writer, ElementType>::WriteValue(writer, val);
      }
    }
  }

  static inline void WriteKeyData(Writer &writer, const ContainerType &data) {}

  static inline void WriteValue(Writer &writer, const ContainerType &data) {
    WriteKeySort(writer, data);
    WriteKeyUnique(writer, data);
  }
};

template <typename Reader, typename Writer, typename T>
struct Serializer<Reader, Writer, std::vector<T>>
    : public LinearContainerSerializer<Reader, Writer, std::vector<T>, T> {};

template <typename Reader, typename Writer>
struct Serializer<Reader, Writer, std::string>
    : public LinearContainerSerializer<Reader, Writer, std::string, char> {};

template <typename Reader, typename Writer>
struct Serializer<Reader, Writer, std::string_view>
    : public LinearContainerSerializer<Reader, Writer, std::string_view, char> {
};

template <typename Reader, typename Writer, typename Val, size_t i,
          typename First, typename... Rest>
struct IndexedSerializer {
  static inline void WriteKeySort(Writer &writer, const Val &data) {
    Serializer<Reader, Writer, First>::WriteKeySort(writer, std::get<i>(data));
    if constexpr (0u < sizeof...(Rest)) {
      IndexedSerializer<Reader, Writer, Val, i + 1u, Rest...>::WriteKeySort(
          writer, data);
    }
  }

  static inline void WriteKeyUnique(Writer &writer, const Val &data) {
    Serializer<Reader, Writer, First>::WriteKeyUnique(writer,
                                                      std::get<i>(data));
    if constexpr (0u < sizeof...(Rest)) {
      IndexedSerializer<Reader, Writer, Val, i + 1u, Rest...>::WriteKeyUnique(
          writer, data);
    }
  }

  static inline void WriteKeyData(Writer &writer, const Val &data) {
    Serializer<Reader, Writer, First>::WriteKeyData(writer, std::get<i>(data));
    if constexpr (0u < sizeof...(Rest)) {
      IndexedSerializer<Reader, Writer, Val, i + 1u, Rest...>::WriteKeyData(
          writer, data);
    }
  }

  static inline void WriteValue(Writer &writer, const Val &data) {
    Serializer<Reader, Writer, First>::WriteValue(writer, std::get<i>(data));
    if constexpr (0u < sizeof...(Rest)) {
      IndexedSerializer<Reader, Writer, Val, i + 1u, Rest...>::WriteValue(
          writer, data);
    }
  }
};

template <typename Reader, typename Writer, typename A, typename B>
struct Serializer<Reader, Writer, std::pair<A, B>>
    : public IndexedSerializer<Reader, Writer, std::pair<A, B>, 0, A, B> {};

template <typename Reader, typename Writer, typename... Elems>
struct Serializer<Reader, Writer, std::tuple<Elems...>>
    : public IndexedSerializer<Reader, Writer, std::tuple<Elems...>, 0,
                               Elems...> {};

template <typename Reader, typename Writer>
struct Serializer<Reader, Writer, Bytes>
    : public LinearContainerSerializer<Reader, Writer, Bytes, uint8_t> {};

// An append-only and iterable container for serialized data
template <typename BackingStore, typename... Columns>
class SerializedVector;

template <typename BackingStore, typename... Columns>
class Vector;

// Writing keys and values with a pack
template <typename Writer, typename... Columns>
struct KeyValueWriter;

template <typename Writer, typename Column>
struct KeyValueColumnWriter {
 public:
  using ColumnType = typename Column::type;
  static inline void WriteKeySort(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::WriteKeySort(writer, val);
  }
  static inline void WriteKeyUnique(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::WriteKeyUnique(writer, val);
  }
  static inline void WriteKeyData(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::WriteKeyData(writer, val);
  }
  static inline void WriteValue(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::WriteValue(writer, val);
  }
};

// Write a single key column
template <typename Writer, typename Column>
struct KeyValueColumnWriter<Writer, Key<Column>> {
 public:
  using ColumnType = typename Column::type;
  static inline void WriteKeySort(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::WriteKeySort(writer, val);
  }
  static inline void WriteKeyUnique(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::WriteKeyUnique(writer, val);
  }
  static inline void WriteKeyData(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::WriteKeyData(writer, val);
  }
  static inline void WriteValue(Writer &, const ColumnType &val) {}
};

// Write a single value column
template <typename Writer, typename Column>
struct KeyValueColumnWriter<Writer, Value<Column>> {
 public:
  using ColumnType = typename Column::type;
  static inline void WriteKeySort(Writer &writer, const ColumnType &val) {}
  static inline void WriteKeyUnique(Writer &writer, const ColumnType &val) {}
  static inline void WriteKeyData(Writer &writer, const ColumnType &val) {}
  static inline void WriteValue(Writer &writer, const ColumnType &val) {
    Serializer<NullReader, Writer, ColumnType>::WriteValue(writer, val);
  }
};

//
//// Don't do anything if nothing to write
//template <typename Writer>
//struct KeyValueWriter<Writer> {
// public:
//  static inline void WriteKeySort(Writer &) {}
//  static inline void WriteKeyUnique(Writer &) {}
//  static inline void WriteKeyData(Writer &) {}
//  static inline void WriteValue(Writer &) {}
//};

// Unpack template pack and write the Key/Value columns as they appear
template <typename Writer, typename Column, typename... Columns>
struct KeyValueWriter<Writer, Column, Columns...> {
 public:
  using ColumnType = typename ValueType<Column>::type;

  static inline void
  WriteKeySort(Writer &writer, const ColumnType &val,
               const typename ValueType<Columns>::type &...rest) {
    KeyValueColumnWriter<Writer, Column>::WriteKeySort(writer, val);
    if constexpr (0u < sizeof...(Columns)) {
      KeyValueWriter<Writer, Columns...>::WriteKeySort(writer, rest...);
    }
  }
  static inline void
  WriteKeyUnique(Writer &writer, const ColumnType &val,
                 const typename ValueType<Columns>::type &...rest) {
    KeyValueColumnWriter<Writer, Column>::WriteKeyUnique(writer, val);
    if constexpr (0u < sizeof...(Columns)) {
      KeyValueWriter<Writer, Columns...>::WriteKeyUnique(writer, rest...);
    }
  }
  static inline void
  WriteKeyData(Writer &writer, const ColumnType &val,
               const typename ValueType<Columns>::type &...rest) {
    KeyValueColumnWriter<Writer, Column>::WriteKeyData(writer, val);
    if constexpr (0u < sizeof...(Columns)) {
      KeyValueWriter<Writer, Columns...>::WriteKeyData(writer, rest...);
    }
  }
  static inline void
  WriteValue(Writer &writer, const ColumnType &val,
             const typename ValueType<Columns>::type &...rest) {
    KeyValueColumnWriter<Writer, Column>::WriteValue(writer, val);
    if constexpr (0u < sizeof...(Columns)) {
      KeyValueWriter<Writer, Columns...>::WriteValue(writer, rest...);
    }
  }
};

template <typename StorageT, typename TableId, typename... Columns>
class Table;

template <typename StorageT, typename TableId, const unsigned kIndexId,
          typename... Columns>
class Index;

// A vector-like object that holds a reference to a serialized view of data and
// and hands back SerialRefs
template <typename BackingStore, typename... Ts>
class ReadOnlySerializedVector;

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
  size_t ElementSize(void) const noexcept {
    if constexpr (std::is_fundamental_v<T> || std::is_enum_v<T>) {
      return sizeof(T);
    } else {
      assert(0 && "ElementSize is only supported for fundamental types.");
    }
  }

  std::pair<T, index_t> Reify(void) const {
    if constexpr (std::is_fundamental_v<T> || std::is_enum_v<T>) {

      // TODO(pag,ekilmer): This assumes that writes into the backing store
      // are little-endian, and that memory loads on the host architecture are
      // also little-endian.
      T tmp;
      std::memcpy(&tmp, &(store.at(offset)), ElementSize());
      return {tmp, offset + sizeof(T)};

    } else {
      assert(false && "TODO: Reify is only supported for fundamental types.");
    }
  }

  const auto begin() const {
    return &(store[offset]);
  }

  const auto end() const {
    return &(store[ElementSize()]);
  }

 private:
  const BackingStoreT &store;
  const size_t offset;
};

// A read-only reference to a group of objects at a particular offset within a
// serialized view (VectorRef)
template <typename BackingStore, typename... Ts>
class SerializedTupleRef;

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
