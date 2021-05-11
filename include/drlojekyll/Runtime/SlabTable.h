// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <cassert>
#include <vector>
#include <tuple>
#include <utility>

#include "Table.h"
#include "Runtime.h"
#include "SlabStorage.h"
#include "SlabVector.h"

namespace hyde {
namespace rt {

//// Base class for a slab table.
//class SlabTableBase {
// public:
//  SlabTableBase(SlabStorage &storage_, unsigned table_id);
//
// private:
//  SlabStorage &storage;
//
//  SlabTableBase(void) = delete;
//  SlabTableBase(const SlabTableBase &) = delete;
//  SlabTableBase(SlabTableBase &&) noexcept = delete;
//  SlabTableBase &operator=(const SlabTableBase &) = delete;
//  SlabTableBase &operator=(SlabTableBase &&) noexcept = delete;
//
// protected:
//  std::pair<bool, bool> TryChangeStateFromAbsentOrUnknownToPresent(void);
//  std::pair<bool, bool> TryChangeStateFromAbsentToPresent(void);
//  bool TryChangeStateFromPresentToUnknown(void);
//  bool TryChangeStateFromUnknownToAbsent(void);
//  bool KeyExists(void) const;
//  TupleState GetState(void) const;
//};

// A helper to construct typed data structures given only integer
// identifiers for entities.
template <typename T>
struct SlabTableHelper;

// Given a table ID, find the vector and map types for the backing data of that
// table.
template <unsigned kTableId>
struct SlabTableHelper<TableDescriptor<kTableId>>
    : public SlabTableHelper<
          typename TableDescriptor<kTableId>::ColumnIds> {};

// Given a list of column IDs, find the backing data of that table.
template <unsigned... kColumnIds>
struct SlabTableHelper<IdList<kColumnIds...>> {
 public:
  using VectorType = PersistentTypedSlabVector<
      std::tuple<typename ColumnDescriptor<kColumnIds>::Type...>,
      Mutable<TupleState>>;

  using MapKeyType = TypedSlabReference<
      std::tuple<typename ColumnDescriptor<kColumnIds>::Type...>>;
  using MapValueType = TypedSlabReference<Mutable<TupleState>>;
  using MapType = std::vector<std::pair<MapKeyType, MapValueType>>;

  using TupleType = std::tuple<typename ColumnDescriptor<kColumnIds>::Type...>;
};

// Implements a table, which is backed by a persistent list of slabs.
template <unsigned kTableId>
class SlabTable {
 public:
  explicit SlabTable(SlabStorage &storage_)
      : storage(storage_),
        data(*(storage.manager), storage.GetTableSlabs(kTableId), 0u) {

    // Revive the persistent data, if any.
    for (auto [key, val] : data) {
      assoc_data.emplace_back(std::move(key), std::move(val));
    }
  }

  template <typename... Ts>
  HYDE_RT_FLATTEN
  bool TryChangeStateFromAbsentOrUnknownToPresent(const Ts&... cols) {
    const TupleType col_vals(cols...);
    auto it = Find(col_vals);
    if (it == assoc_data.end()) {
      auto [tuple, state] = data.ReturnAddedTuple(
          col_vals, TupleState::kPresent);
      assoc_data.emplace_back(std::move(tuple), std::move(state));
      return true;

    } else if (it->second != TupleState::kPresent) {

      return true;

    } else {
      return false;
    }
  }

//  template <typename... Ts>
//  HYDE_RT_FLATTEN
//  bool TryChangeStateFromAbsentToPresent(const Ts&... cols) {
//    const TupleType col_vals(cols...);
//    auto it = Find(col_vals);
//    if (it == assoc_data.end()) {
//      auto [tuple, state] = data.ReturnAddedTuple(
//          col_vals, TupleState::kPresent);
//      auto [it, added] = assoc_data.emplace(std::move(tuple), std::move(state));
//      assert(added);
//      (void) it;
//      (void) added;
//    }
//  }

  ~SlabTable(void) {
    storage.PutTableSlabs(kTableId, data);
  }

 private:
  using TableDesc = TableDescriptor<kTableId>;
  using Helper = SlabTableHelper<TableDesc>;
  using VectorType = typename Helper::VectorType;
  using MapKeyType = typename Helper::MapKeyType;
  using MapValueType = typename Helper::MapValueType;
  using MapType = typename Helper::MapType;
  using TupleType = typename Helper::TupleType;

  typename MapType::iterator Find(const TupleType &col_vals) {
    auto it = std::lower_bound(
        assoc_data.begin(), assoc_data.end(), col_vals,
        +[] (const std::pair<MapKeyType, MapValueType> &elem_type,
             const TupleType &col_vals_) {
          return elem_type.first < col_vals_;
        });
    if (it != assoc_data.end() && it->first != col_vals) {
      return assoc_data.end();
    } else {
      return it;
    }
  }

  SlabStorage &storage;
  VectorType data;
  MapType assoc_data;
};

template <unsigned kTableId>
class Table<SlabStorage, kTableId> : public SlabTable<kTableId> {};

}  // namespace rt
}  // namespace hyde
