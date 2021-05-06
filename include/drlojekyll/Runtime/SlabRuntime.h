// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Runtime.h"

#pragma once

#include <atomic>
#include <cassert>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <iomanip>
#include <ios>
#include <iterator>
#include <memory>
#include <new>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

namespace hyde {
namespace rt {

class Slab;
class SlabList;
class SlabListWriter;
class SlabListReader;
class SlabStorage;
class SlabStorage;
class SlabReference;

//template <typename... Ts>
//class Vector<SlabStorage, Ts...> {
// private:
//  std::vector<std::tuple<Ts...>> backing_store;
//
// public:
//  void Add(Ts... args) {
//    backing_store.emplace_back(std::make_tuple(std::move(args)...));
//  }
//
//  index_t Size(void) const noexcept {
//    return static_cast<index_t>(backing_store.size());
//  }
//
//  auto begin(void) const -> decltype(this->backing_store.begin()) {
//    return backing_store.begin();
//  }
//
//  auto end(void) const -> decltype(this->backing_store.end()) {
//    return backing_store.end();
//  }
//
//  void Clear(void) {
//    backing_store.clear();
//  }
//
//  void Swap(Vector<SlabStorage, Ts...> &that) {
//    backing_store.swap(that.backing_store);
//  }
//
//  void SortAndUnique(void) {
//    std::sort(backing_store.begin(), backing_store.end());
//    auto it = std::unique(backing_store.begin(), backing_store.end());
//    backing_store.erase(it, backing_store.end());
//  }
//};
//
//// A ReadOnlySerializedVector is a vector that holds a collection of
//// references to a tuple of types that can be reified.
//template <typename... Ts>
//class ReadOnlySerializedVector<SlabStorage, Ts...> {
// public:
//  explicit ReadOnlySerializedVector(SlabStorage &, const SlabList &slab_list_)
//      : slab_list(slab_list_) {}
//
//  inline auto begin(void) const {
//    return SerializedVectorIterator<Ts...>(slab_list);
//  }
//
//  inline SerializedVectorIteratorEnd end(void) const {
//    return {};
//  }
//
// private:
//  SlabList slab_list;
//};
//
//
//
//// A SerializedVector owns its own backing store, unlike a
//// ReadOnlySerializedVector that only references another backing store. Both
//// hold serialized data.
//template <typename... Ts>
//class SerializedVector<SlabStorage, Ts...> : public SlabListVector {
// public:
//  using SlabListVector::SlabListVector;
//
//
//
//  inline auto begin(void) const {
//    return SerializedVectorIterator<Ts...>(slab_list);
//  }
//
// private:
//  SerializedVector(void) = delete;
//};
//
//
//template <typename... Ts>
//class Vector<SlabStorage, Ts...>
//    : public SerializedVector<SlabStorage, Ts...> {};
//
//// Database index class using standard containers. `Columns` will be a list
//// of `Key<T>` or `Val<T>`, where each `T` is a column descriptor, and
//// `KeyColumns` and `ValColumns` will be lists of column descriptors.
//template <typename TableId, unsigned kIndexId, typename... Columns,
//          typename... KeyColumns, typename... ValColumns>
//class Index<SlabStorage, TableId, kIndexId, TypeList<Columns...>,
//            TypeList<KeyColumns...>, TypeList<ValColumns...>> {
// public:
//  explicit Index(SlabStorage &) : backing_store(){};
//
//  void Add(const typename ValueType<Columns>::type &...cols) {
//    key_data.clear();
//    BufferedWriter key_writer(key_data);
//    ColumnSerializer<BufferedWriter, Columns...>::WriteKeySort(
//        key_writer, cols...);
//    ColumnSerializer<BufferedWriter, Columns...>::WriteKeyUnique(
//        key_writer, cols...);
//
//    BufferedWriter data_writer(backing_store[key_data]);
//    ColumnSerializer<BufferedWriter, Columns...>::WriteValue(
//        data_writer, cols...);
//  }
//
//  using ReadOnlySerializedVecType = ReadOnlySerializedVector<
//      SlabStorage, typename ValueType<ValColumns>::type...>;
//
//  const ReadOnlySerializedVecType
//  Get(const typename ValueType<KeyColumns>::type &...cols) const {
//    key_data.clear();
//    BufferedWriter key_writer(key_data);
//    ColumnSerializer<BufferedWriter, KeyColumns...>::WriteKeySort(
//        key_writer, cols...);
//    ColumnSerializer<BufferedWriter, KeyColumns...>::WriteKeyUnique(
//        key_writer, cols...);
//
//    auto it = backing_store.find(key_data);
//    if (it == backing_store.end()) {
//      return kEmptyVecRef;
//    } else {
//      return ReadOnlySerializedVecType(it->second);
//    }
//  }
//
//  bool Contains(const typename ValueType<KeyColumns>::type &...cols) const {
//    key_data.clear();
//    BufferedWriter key_writer(key_data);
//    ColumnSerializer<BufferedWriter, KeyColumns...>::WriteKeySort(
//        key_writer, cols...);
//    ColumnSerializer<BufferedWriter, KeyColumns...>::WriteKeyUnique(
//        key_writer, cols...);
//    return  backing_store.find(key_data) != backing_store.end();
//  }
//
// private:
//
//  // Working buffer for writing key data when doing lookups.
//  mutable StdSerialBuffer key_data;
//
//  // Stores serialized Key/Value objects
//  std::map<StdSerialBuffer, StdSerialBuffer> backing_store;
//
//  static const ReadOnlySerializedVector<SlabStorage,
//                                        typename ValueType<ValColumns>::type...>
//      kEmptyVecRef;
//};
//
//extern const StdSerialBuffer kEmptyIndexBackingBuffer;
//
//template <typename TableId, unsigned kIndexId, typename... Columns,
//          typename... KeyColumns, typename... ValColumns>
//const ReadOnlySerializedVector<SlabStorage,
//                               typename ValueType<ValColumns>::type...>
//Index<SlabStorage, TableId, kIndexId, TypeList<Columns...>,
//      TypeList<KeyColumns...>, TypeList<ValColumns...>>::kEmptyVecRef(
//          kEmptyIndexBackingBuffer);
//
//// Backing implementation of a table.
//class TableImpl;
//class TableBase {
// protected:
//  TableBase(void);
//
//  std::pair<bool, bool> TryChangeStateFromAbsentOrUnknownToPresent(void);
//  std::pair<bool, bool> TryChangeStateFromAbsentToPresent(void);
//  bool TryChangeStateFromPresentToUnknown(void);
//  bool TryChangeStateFromUnknownToAbsent(void);
//  bool KeyExists(void) const;
//  TupleState GetState(void) const;
//
//
//  StdSerialBuffer all_key_data;
//  mutable StdSerialBuffer key_data;
//
// private:
//  std::unique_ptr<TableImpl> impl;
//  std::map<StdSerialBuffer, TupleState> backing_store;
//
//};
//
//template <typename kTableId, typename... Indices, typename... Columns>
//class Table<SlabStorage, kTableId, TypeList<Indices...>,
//            TypeList<Columns...>> : public TableBase {
// public:
//  Table(SlabStorage &, Indices &...indices_)
//      : indices(indices_...) {}
//
//  // For use when indices are aliased to the Table. Gets the state
//  inline TupleState GetState(
//      const typename ValueType<Columns>::type &... cols) const {
//    SerializeKey(cols...);
//    return TableImpl::GetState();
//  }
//
//  inline bool TryChangeStateFromAbsentOrUnknownToPresent(
//      const typename ValueType<Columns>::type &... cols) {
//    SerializeKey(cols...);
//    const auto [transitioned, added] =
//        TableImpl::TryChangeStateFromAbsentOrUnknownToPresent();
//    if (added) {
//      UpdateIndices(cols...);
//    }
//    return transitioned;
//  }
//
//  inline bool TryChangeStateFromAbsentToPresent(
//      const typename ValueType<Columns>::type &... cols) {
//    SerializeKey(cols...);
//    const auto [transitioned, added] =
//        TableImpl::TryChangeStateFromAbsentToPresent();
//    if (added) {
//      UpdateIndices(cols...);
//    }
//    return transitioned;
//  }
//
//  inline bool TryChangeStateFromPresentToUnknown(
//      const typename ValueType<Columns>::type &... cols) {
//    SerializeKey(cols...);
//    return TableImpl::TryChangeStateFromPresentToUnknown();
//  }
//
//  inline bool TryChangeStateFromUnknownToAbsent(
//      const typename ValueType<Columns>::type &... cols) {
//    SerializeKey(cols...);
//    return TableImpl::TryChangeStateFromUnknownToAbsent();
//  }
//
//  using ReadOnlySerializedVectorType = ReadOnlySerializedVector<
//      SlabStorage, typename ValueType<Columns>::type...>;
//
//  ReadOnlySerializedVectorType Keys(void) const {
//    return ReadOnlySerializedVectorType(all_key_data);
//  }
//
// private:
//  std::tuple<std::reference_wrapper<Indices>...> indices;
//
//  // Serialize columns into a Key that can be used to look up the value in our backing_store
//  void SerializeKey(const typename ValueType<Columns>::type &... cols) const {
//    key_data.clear();
//    BufferedWriter key_writer(key_data);
//    ColumnSerializer<BufferedWriter, Key<Columns>...>::WriteKeySort(
//        key_writer, cols...);
//    ColumnSerializer<BufferedWriter, Key<Columns>...>::WriteKeyUnique(
//        key_writer, cols...);
//  }
//
//  template <index_t I = 0>
//  void UpdateIndices(const typename ValueType<Columns>::type &... cols) {
//
//    // If we have iterated through all elements
//    if constexpr (I < sizeof...(Indices)) {
//      // Add to this index
//      std::get<I>(indices).get().Add(cols...);
//
//      // Then go for next
//      UpdateIndices<I + 1>(cols...);
//    }
//  }
//};

}  // namespace rt
}  // namespace hyde
