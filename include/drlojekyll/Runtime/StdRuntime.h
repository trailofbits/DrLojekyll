// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Runtime.h"

#pragma once

#include <cassert>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <iomanip>
#include <ios>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

namespace hyde {
namespace rt {

// Tag type for usage of standard containers
struct std_containers {};

// Alias for a serialized buffer type
using StdSerialBuffer = std::basic_string<uint8_t>;

// Basic buffered data writer for writing fundamental types into a byte buffer.
struct BufferedWriter {
 public:
  explicit BufferedWriter(StdSerialBuffer &key_storage_)
      : key_storage(key_storage_) {}

  void WriteF64(double d);
  void WriteF32(float d);
  void WriteU64(uint64_t d);
  void WriteU32(uint32_t d);
  void WriteU16(uint16_t h);
  void WriteU8(uint8_t b);

 private:
  StdSerialBuffer &key_storage;
};

// Deserialize a value from a serial byte store.
template <typename T>
inline static T ReifyValue(const StdSerialBuffer &backing_store,
                           index_t &offset) {
  if constexpr (std::is_fundamental_v<T> || std::is_enum_v<T>) {

    // TODO(pag,ekilmer): This assumes that writes into the backing store
    // are little-endian, and that memory loads on the host architecture are
    // also little-endian.
    T tmp;
    std::memcpy(&tmp, &(backing_store[offset]), sizeof(T));
    offset += static_cast<index_t>(sizeof(T));
    return tmp;

  } else {
    assert(false && "TODO: Reify is only supported for fundamental types.");
    return {};
  }
}

// Deserialize a series of values form a serial byte store.
template <typename E, typename... Es>
inline static std::tuple<E, Es...> ReifyTuple(
    const StdSerialBuffer &backing_store, index_t &offset) {
  auto val = ReifyValue<E>(backing_store, offset);
  if constexpr (0u < sizeof...(Es)) {
    std::tuple<Es...> next_vals = ReifyTuple<Es...>(backing_store, offset);
    return std::tuple_cat(std::make_tuple(val), next_vals);
  } else {
    return {val};
  }
}

class SerializedVectorIteratorEnd {};

template <typename... Ts>
class SerializedVectorIterator {
 public:
  inline SerializedVectorIterator(const StdSerialBuffer &backing_store_)
      : backing_store(backing_store_),
        max_offset(backing_store.size()) {}

  using SelfType = SerializedVectorIterator<Ts...>;

  inline bool operator!=(SerializedVectorIteratorEnd that) const {
    return offset < max_offset;
  }

  SelfType &operator++(void) {
    offset = next_offset;
    if (next_offset == max_offset) {
      max_offset = backing_store.size();
    }
    return *this;
  }

  std::tuple<Ts...> operator*(void) const {
    next_offset = offset;
    return ReifyTuple<Ts...>(backing_store, next_offset);
  }

 private:
  const StdSerialBuffer &backing_store;
  index_t offset{0};
  index_t max_offset;
  mutable index_t next_offset{0};
};

template <typename... Ts>
class Vector<std_containers, Ts...> {
 private:
  std::vector<std::tuple<Ts...>> backing_store;

 public:
  void Add(Ts... args) {
    backing_store.emplace_back(std::make_tuple(std::move(args)...));
  }

  index_t Size(void) const noexcept {
    return static_cast<index_t>(backing_store.size());
  }

  auto begin(void) const -> decltype(this->backing_store.begin()) {
    return backing_store.begin();
  }

  auto end(void) const -> decltype(this->backing_store.end()) {
    return backing_store.end();
  }

  void Clear(void) {
    backing_store.clear();
  }

  void Swap(Vector<std_containers, Ts...> &that) {
    backing_store.swap(that.backing_store);
  }

  void SortAndUnique(void) {
    std::sort(backing_store.begin(), backing_store.end());
    auto it = std::unique(backing_store.begin(), backing_store.end());
    backing_store.erase(it, backing_store.end());
  }
};

// A ReadOnlySerializedVector is a vector that holds a collection of
// references to a tuple of types that can be reified.
template <typename... Ts>
class ReadOnlySerializedVector<std_containers, Ts...> {
 private:
  const StdSerialBuffer &backing_store;

 public:
  explicit ReadOnlySerializedVector(const StdSerialBuffer &backing_store_)
      : backing_store(backing_store_) {}

  inline auto begin(void) const {
    return SerializedVectorIterator<Ts...>(backing_store);
  }

  inline SerializedVectorIteratorEnd end(void) const {
    return {};
  }
};

// A SerializedVector owns its own backing store, unlike a
// ReadOnlySerializedVector that only references another backing store. Both
// hold serialized data.
template <typename... Ts>
class SerializedVector<std_containers, Ts...> {
 private:
  StdSerialBuffer backing_store;

 public:

  // Add a single serialized element
  void Add(Ts... ts) {
    BufferedWriter writer(backing_store);
    Serializer<NullWriter, BufferedWriter, std::tuple<Ts...>>::WriteValue(
        writer, std::make_tuple<Ts...>(std::move(ts)...));
  }

  void Clear(void) {
    backing_store.clear();
  }

  void Swap(SerializedVector<std_containers, Ts...> &store) {
    backing_store.swap(store.backing_store);
  }

  inline auto begin(void) const {
    return SerializedVectorIterator<Ts...>(backing_store);
  }

  inline SerializedVectorIteratorEnd end(void) const {
    return {};
  }
};

// Database index class using standard containers. `Columns` will be a list
// of `Key<T>` or `Val<T>`, where each `T` is a column descriptor, and
// `KeyColumns` and `ValColumns` will be lists of column descriptors.
template <typename TableId, unsigned kIndexId, typename... Columns,
          typename... KeyColumns, typename... ValColumns>
class Index<std_containers, TableId, kIndexId, TypeList<Columns...>,
            TypeList<KeyColumns...>, TypeList<ValColumns...>> {
 public:
  explicit Index(std_containers &) : backing_store(){};

  void Add(const typename ValueType<Columns>::type &...cols) {
    key_data.clear();
    BufferedWriter key_writer(key_data);
    KeyValueWriter<BufferedWriter, Columns...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, Columns...>::WriteKeyUnique(
        key_writer, cols...);

    BufferedWriter data_writer(backing_store[key_data]);
    KeyValueWriter<BufferedWriter, Columns...>::WriteValue(
        data_writer, cols...);
  }

  using ReadOnlySerializedVecType = ReadOnlySerializedVector<
      std_containers, typename ValueType<ValColumns>::type...>;

  const ReadOnlySerializedVecType
  Get(const typename ValueType<KeyColumns>::type &...cols) const {
    key_data.clear();
    BufferedWriter key_writer(key_data);
    KeyValueWriter<BufferedWriter, KeyColumns...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, KeyColumns...>::WriteKeyUnique(
        key_writer, cols...);

    auto it = backing_store.find(key_data);
    if (it == backing_store.end()) {
      return kEmptyVecRef;
    } else {
      return ReadOnlySerializedVecType(it->second);
    }
  }

  bool Contains(const typename ValueType<KeyColumns>::type &...cols) const {
    key_data.clear();
    BufferedWriter key_writer(key_data);
    KeyValueWriter<BufferedWriter, KeyColumns...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, KeyColumns...>::WriteKeyUnique(
        key_writer, cols...);
    return  backing_store.find(key_data) != backing_store.end();
  }

 private:

  // Working buffer for writing key data when doing lookups.
  mutable StdSerialBuffer key_data;

  // Stores serialized Key/Value objects
  std::map<StdSerialBuffer, StdSerialBuffer> backing_store;

  static const ReadOnlySerializedVector<std_containers,
                                        typename ValueType<ValColumns>::type...>
      kEmptyVecRef;
};

extern const StdSerialBuffer kEmptyIndexBackingBuffer;

template <typename TableId, unsigned kIndexId, typename... Columns,
          typename... KeyColumns, typename... ValColumns>
const ReadOnlySerializedVector<std_containers,
                               typename ValueType<ValColumns>::type...>
Index<std_containers, TableId, kIndexId, TypeList<Columns...>,
      TypeList<KeyColumns...>, TypeList<ValColumns...>>::kEmptyVecRef(
          kEmptyIndexBackingBuffer);

// Backing implementation of a table.
struct TableImpl {
 protected:
  std::pair<bool, bool> TryChangeStateFromAbsentOrUnknownToPresent(void);
  std::pair<bool, bool> TryChangeStateFromAbsentToPresent(void);
  bool TryChangeStateFromPresentToUnknown(void);
  bool TryChangeStateFromUnknownToAbsent(void);
  bool KeyExists(void) const;
  TupleState GetState(void) const;

  StdSerialBuffer all_key_data;
  mutable StdSerialBuffer key_data;
  std::map<StdSerialBuffer, TupleState> backing_store;
};

template <typename kTableId, typename... Indices, typename... Columns>
class Table<std_containers, kTableId, TypeList<Indices...>,
            TypeList<Columns...>> : public TableImpl {
 public:
  Table(std_containers &, Indices &...indices_)
      : indices(indices_...) {}

  // For use when indices are aliased to the Table. Gets the state
  inline TupleState GetState(
      const typename ValueType<Columns>::type &... cols) const {
    SerializeKey(cols...);
    return TableImpl::GetState();
  }

  inline bool TryChangeStateFromAbsentOrUnknownToPresent(
      const typename ValueType<Columns>::type &... cols) {
    SerializeKey(cols...);
    const auto [transitioned, added] =
        TableImpl::TryChangeStateFromAbsentOrUnknownToPresent();
    if (added) {
      UpdateIndices(cols...);
    }
    return transitioned;
  }

  inline bool TryChangeStateFromAbsentToPresent(
      const typename ValueType<Columns>::type &... cols) {
    SerializeKey(cols...);
    const auto [transitioned, added] =
        TableImpl::TryChangeStateFromAbsentToPresent();
    if (added) {
      UpdateIndices(cols...);
    }
    return transitioned;
  }

  inline bool TryChangeStateFromPresentToUnknown(
      const typename ValueType<Columns>::type &... cols) {
    SerializeKey(cols...);
    return TableImpl::TryChangeStateFromPresentToUnknown();
  }

  inline bool TryChangeStateFromUnknownToAbsent(
      const typename ValueType<Columns>::type &... cols) {
    SerializeKey(cols...);
    return TableImpl::TryChangeStateFromUnknownToAbsent();
  }

  using ReadOnlySerializedVectorType = ReadOnlySerializedVector<
      std_containers, typename ValueType<Columns>::type...>;

  ReadOnlySerializedVectorType Keys(void) const {
    return ReadOnlySerializedVectorType(all_key_data);
  }

 private:
  std::tuple<std::reference_wrapper<Indices>...> indices;

  // Serialize columns into a Key that can be used to look up the value in our backing_store
  void SerializeKey(const typename ValueType<Columns>::type &... cols) const {
    key_data.clear();
    BufferedWriter key_writer(key_data);
    KeyValueWriter<BufferedWriter, Key<Columns>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, Key<Columns>...>::WriteKeyUnique(
        key_writer, cols...);
  }

  template <index_t I = 0>
  void UpdateIndices(const typename ValueType<Columns>::type &... cols) {

    // If we have iterated through all elements
    if constexpr (I < sizeof...(Indices)) {
      // Add to this index
      std::get<I>(indices).get().Add(cols...);

      // Then go for next
      UpdateIndices<I + 1>(cols...);
    }
  }
};

}  // namespace rt
}  // namespace hyde
