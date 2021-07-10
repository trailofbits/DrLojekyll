// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <vector>
#include <tuple>
#include <unordered_map>
#include <utility>

#include <drlojekyll/Runtime/Runtime.h>

#include "SlabStorage.h"
#include "SlabVector.h"

namespace hyde {
namespace rt {

// A helper to construct typed data structures given only integer
// identifiers for entities.
template <typename T>
struct SlabTableHelper;

template <typename T>
struct SlabIndexHelper;

template <unsigned kIndexId>
struct IndexDescriptor;

template <unsigned kTableId>
struct TableDescriptor;

template <unsigned kColumnId>
struct ColumnDescriptor;

template <unsigned kIndexId>
using NextTupleInIndexType = NextTuplePointer;

// Given a list of column IDs, find the backing data of that table.
template <unsigned kTableId, unsigned... kColumnIds, unsigned... kIndexIds>
struct SlabTableHelper<TypeList<TableDescriptor<kTableId>,
                                IdList<kColumnIds...>,
                                IdList<kIndexIds...>>> {
 public:
  static constexpr auto kNumColumns = sizeof...(kColumnIds);
  static constexpr auto kNumIndexes = sizeof...(kIndexIds);

//  // The offset of the first byte of data in `TupleType`, relative to where
//  // this tuple is serialized.
//  constexpr uint32_t kTupleOffset =
//      1u + static_cast<uint32_t>(kNumIndexes * 8u);
  //  using IndexTupleType = std::tuple<NextTupleInIndexType<kIndexIds>...>;

  using TupleType = std::tuple<typename ColumnDescriptor<kColumnIds>::Type...>;
  using TupleBuilderType = TupleBuilder<Mutable<TupleState>, TupleType>;
  using VectorType = PersistentTypedSlabVector<Mutable<TupleState>, TupleType>;
};

// Given a table ID, find the vector and map types for the backing data of that
// table.
template <unsigned kTableId>
struct SlabTableHelper<TableDescriptor<kTableId>>
    : public SlabTableHelper<TypeList<
          TableDescriptor<kTableId>,
          typename TableDescriptor<kTableId>::ColumnIds,
          typename TableDescriptor<kTableId>::IndexIds>> {};

template <typename IndexIdList>
class SlabTableIndices;

using SlabIndexMapType = std::unordered_multimap<uint64_t, uint8_t *>;

// Base class for a slab table. We separate this out so that we can have a
// few things implemented by common code when the types all match up. In
// general, it's pretty common for types to match up, so we don't want the
// name mangling based on table IDs for slab bases to get in the way of reducing
// code sizes.
class SlabTableBase {
 public:
  explicit SlabTableBase(SlabStorage &storage_, unsigned table_id) noexcept
      : storage(storage_),
        table_info(storage.GetTableSlabs(table_id)) {}

  // Compute the size (in bytes) of the serialized form of `tuple`, and compute
  // a hash of the contents of `tuple`.
  //
  // NOTE(pag): This hash is NOT safe to store persistently, as it hashes
  //            pointers.
  template <typename TupleType>
  HYDE_RT_INLINE static std::pair<uint64_t, uint32_t> HashAndSizeColumns(
      const TupleType &tuple) noexcept {
    using Writer = ByteCountingWriterProxy<HashingWriter>;
    Writer writer;
    Serializer<NullReader, Writer, TupleType>::Write(writer, tuple);
    return {writer.Digest(), writer.num_bytes};
  }

  // Try to change the state of a tuple. If it's not present, then add the
  // tuple.
  template <typename TupleType, typename TupleBuilderType>
  HYDE_RT_FLATTEN HYDE_RT_NEVER_INLINE
  TupleState GetStateImpl(const TupleType &tuple) noexcept {
    const auto [hash, num_bytes] = HashAndSizeColumns(tuple);
    for (auto [it, end] = assoc_data[0].equal_range(hash);
         it != end; ++it) {
      TupleBuilderType builder(it->second, num_bytes  /* estimation */);
      if (auto [state_ref, tuple_ref] = builder.Build(); tuple_ref == tuple) {
        return static_cast<TupleState>(state_ref);
      }
    }

    return TupleState::kAbsent;
  }

  // Try to change the state of a tuple. If it's not present, then add the
  // tuple.
  template <typename TupleType, typename TupleBuilderType>
  HYDE_RT_FLATTEN HYDE_RT_NEVER_INLINE
  std::tuple<bool, uint64_t, bool> TryChangeTuple(
      const TupleType &tuple, TupleState a_state, TupleState b_state) noexcept {
    const auto [hash, num_bytes] = HashAndSizeColumns(tuple);
    for (auto [it, end] = assoc_data[0].equal_range(hash);
         it != end; ++it) {

      TupleBuilderType builder(it->second, num_bytes  /* estimation */);
      if (auto [state_ref, tuple_ref] = builder.Build(); tuple_ref == tuple) {
        const auto curr_state = static_cast<TupleState>(state_ref);
        if (curr_state == a_state || curr_state == b_state) {
          state_ref = TupleState::kPresent;
          return {true, hash, false};

        } else {
          return {false, hash, false};
        }
      }
    }

    return {true, hash, true};
  }

  // Try to change the state of a tuple which should already be present.
  template <typename TupleType, typename TupleBuilderType>
  HYDE_RT_FLATTEN HYDE_RT_NEVER_INLINE
  bool ChangeTuple(const TupleType &tuple, TupleState from_state,
                   TupleState to_state) noexcept {
    const auto [hash, num_bytes] = HashAndSizeColumns(tuple);
    for (auto [it, end] = assoc_data[0].equal_range(hash);
         it != end; ++it) {

      TupleBuilderType builder(it->second, num_bytes  /* estimation */);
      if (auto [state_ref, tuple_ref] = builder.Build();
          static_cast<TupleState>(state_ref) == from_state) {
        if (tuple_ref == tuple) {
          state_ref = to_state;
          return true;
        }
      }
    }

    return false;
  }

  SlabStorage &storage;
  std::pair<SlabList, uint64_t> table_info;
  std::array<SlabIndexMapType, 2> assoc_data;

 private:
  SlabTableBase(void) = delete;
  SlabTableBase(const SlabTableBase &) = delete;
  SlabTableBase(SlabTableBase &&) noexcept = delete;
};

// Base case when we get to the end of indices: inherit from `SlabTableBase`,
// giving all of our index scanners access to `assoc_data`.
template <>
class SlabTableIndices<IdList<>> : public SlabTableBase {
 private:
  class Impossible {
   private:
    Impossible(void) = delete;
  };
 public:

  using SlabTableBase::SlabTableBase;

  static constexpr unsigned kNumIndices = 0u;

  template <typename TupleType>
  HYDE_RT_INLINE void AddToIndices(const TupleType &, uint8_t *data) {}

  void Scan(Impossible &) {}
};

// Inductive case: each index corresponds with a base class, and we figure out
// and offset in `assoc_data` (from `SlabTableBase`, our topmost base class)
// for this index.
template <unsigned kIndexId, unsigned... kIndexIds>
class SlabTableIndices<IdList<kIndexId, kIndexIds...>>
    : public SlabTableIndices<IdList<kIndexIds...>> {
 public:

  using Parent = SlabTableIndices<IdList<kIndexIds...>>;
  using Parent::Parent;

  using IndexDesc = IndexDescriptor<kIndexId>;
  using KeyColumnIds = typename IndexDesc::KeyColumnIds;
  using ValueColumnIds = typename IndexDesc::ValueColumnIds;
  using Columns = typename IndexDesc::Columns;

  static constexpr bool kIndexIsDifferentFromBaseTable
      = !std::is_same_v<ValueColumnIds, IdList<>>;

  static constexpr unsigned kAssocDataIndex
      = kIndexIsDifferentFromBaseTable ? 1u : 0u;

  static constexpr unsigned kNumIndices =
      Parent::kNumIndices + (kIndexIsDifferentFromBaseTable ? 1u : 0u);

  using TableDesc = TableDescriptor<IndexDesc::kTableId>;
  static constexpr unsigned kNumColumns = TableDesc::kNumColumns;

  using ColumnHelper = SlabTableHelper<TableDesc>;
  using VectorType = typename ColumnHelper::VectorType;
  using TupleType = typename ColumnHelper::TupleType;
  using TupleBuilderType = typename ColumnHelper::TupleBuilderType;

  using Parent::Scan;

  // Scan this index.
  template <typename T, typename... Ts>
  HYDE_RT_FLATTEN std::vector<TupleType> Scan(
      const T &col, const Ts&... cols, IndexTag<kIndexId>) {
    std::vector<TupleType> tuples;
    HashingWriter writer;

    HashKeyColumns<T, Ts...>(col, cols..., writer, KeyColumnIds{});
    const uint64_t hash = writer.Digest();
    std::vector<uint8_t *> rows;

    for (auto [it, end] = this->assoc_data[kAssocDataIndex].equal_range(hash);
         it != end; ++it) {

      // Omit tuples that are absent. We only care about present or unknown
      // tuples in a scan.
      TupleBuilderType builder(it->second, 1  /* estimation */);
      if (auto [state_ref, tuple_ref] = builder.Build();
          static_cast<TupleState>(state_ref) != TupleState::kAbsent) {
        tuples.emplace_back(tuple_ref);
      }
    }

    return tuples;
  }

  // Add `tuple` to this index, and then to the next one down.
  template <typename TupleType>
  HYDE_RT_INLINE void AddToIndices(const TupleType &tuple, uint8_t *data) {
    if constexpr (kIndexIsDifferentFromBaseTable) {
      HashingWriter writer;
      HashIndexedColumns<TupleType, 0u>(tuple, data, writer, Columns{});
      const uint64_t hash = writer.Digest();
      this->assoc_data[kAssocDataIndex].emplace(hash, data);
    }
    this->Parent::AddToIndices(tuple, data);
  }

 private:
  // Accumulate a hash of the `ColType`s which are `KeyColumn<id>`s, and ignore
  // the `ColType`s which are `ValueColumn<id>`.
  template <typename TupleType, unsigned kColOffset, typename ColType,
            typename... ColTypes>
  HYDE_RT_INLINE void HashIndexedColumns(
      const TupleType &tuple, uint8_t *data, HashingWriter &writer,
      TypeList<ColType, ColTypes...>) {

    ColumnSerializer<HashingWriter, ColType>::WriteKey(
        writer, std::get<kColOffset>(tuple));

    if constexpr ((kColOffset + 1u) != kNumColumns) {
      HashIndexedColumns<TupleType, kColOffset + 1u>(
          tuple, data, writer, TypeList<ColTypes...>{});
    }
  }

  template <typename ColumnType, typename... ColumnTypes,
            unsigned kColumnId, unsigned... kColumnIds>
  HYDE_RT_INLINE void HashKeyColumns(
      const ColumnType &col, ColumnTypes&... cols,
      HashingWriter &writer, IdList<kColumnId, kColumnIds...>) {
    using ColumnDesc = ColumnDescriptor<kColumnId>;
    using DesiredValueType = typename ValueType<ColumnDesc>::Type;
    using ColumnValueType = typename ValueType<ColumnType>::Type;
    static_assert(std::is_same_v<DesiredValueType, ColumnValueType>);

    Serializer<NullReader, HashingWriter, DesiredValueType>::Write(
        writer, col);
  }
};

// Implements a table, which is backed by a persistent list of slabs.
template <unsigned kTableId>
class SlabTable
    : public SlabTableIndices<typename TableDescriptor<kTableId>::IndexIds> {
 public:

  using TableDesc = TableDescriptor<kTableId>;
  using Parent = SlabTableIndices<typename TableDesc::IndexIds>;

  using ColumnHelper = SlabTableHelper<TableDesc>;
  using VectorType = typename ColumnHelper::VectorType;
  using TupleType = typename ColumnHelper::TupleType;
  using TupleBuilderType = typename ColumnHelper::TupleBuilderType;

  explicit SlabTable(SlabStorage &storage_)
      : Parent(storage_, kTableId),
        data(*(storage_.manager), std::move(this->table_info.first), 0u) {

    // Reserve enough storage for the number of rows in the table.
    this->assoc_data[0].reserve(this->table_info.second);
    this->assoc_data[1].reserve(this->table_info.second * Parent::kNumIndices);

    // Revive the persistent data, if any.
    for (auto [state_ref, tuple_ref] : data) {
      const auto data_ptr = state_ref.Data();
      this->assoc_data[0].emplace(tuple_ref.Hash(), data_ptr);
      this->Parent::template AddToIndices<TupleType>(tuple_ref, data_ptr);
    }

    // Consistency check on row count.
#ifndef NDEBUG
    for (const auto &index : this->assoc_data) {
      assert(index.size() == this->table_info.second);
    }
#endif
  }

  ~SlabTable(void) noexcept {
    this->storage.PutTableSlabs(kTableId, data, this->assoc_data.size());
  }

  using Parent::Scan;

  // Return the number of rows in the table.
  uint64_t Size(void) const noexcept {
    return this->assoc_data[0].size();
  }

  // Scan the entire table.
  HYDE_RT_FLATTEN std::vector<TupleType> Scan(TableTag<kTableId>) {
    std::vector<TupleType> tuples;
    for (auto [state_ref, tuple_ref] : data) {
      if (static_cast<TupleState>(state_ref) != TupleState::kAbsent) {
        tuples.emplace_back(tuple_ref);
      }
    }
    return tuples;
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  TupleState GetState(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    return this->template GetStateImpl<TupleType, TupleBuilderType>(tuple);
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  bool TryChangeTupleFromAbsentToPresent(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    const auto [ret, hash, add] =
        this->template TryChangeTuple<TupleType, TupleBuilderType>(
            tuple, TupleState::kAbsent, TupleState::kAbsent);
    if (add) {
      return AddTuple(tuple, hash);
    }
    return ret;
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  bool TryChangeTupleFromAbsentOrUnknownToPresent(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    const auto [ret, hash, add] =
        this->template TryChangeTuple<TupleType, TupleBuilderType>(
            tuple, TupleState::kAbsent, TupleState::kUnknown);
    if (add) {
      return AddTuple(tuple, hash);
    }
    return ret;
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  bool TryChangeTupleFromPresentToUnknown(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    return this->template ChangeTuple<TupleType, TupleBuilderType>(
        tuple, TupleState::kPresent, TupleState::kUnknown);
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  bool TryChangeTupleFromUnknownToAbsent(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    return this->template ChangeTuple<TupleType, TupleBuilderType>(
        tuple, TupleState::kUnknown, TupleState::kAbsent);
  }

 private:

//  using ScanBuilderType = typename ColumnHelper::ScanBuilderType;
//  using ScanTupleType = typename ColumnHelper::ScanTupleType;

  // Add `tuple` with `hash` to the table for persistent storage.
  HYDE_RT_INLINE bool AddTuple(const TupleType &tuple, uint64_t hash) noexcept {
    auto ret = data.ReturnAddedTuple(TupleState::kPresent, tuple);
    auto data = ret.Data();
    this->assoc_data[0].emplace(hash, data);
    this->Parent::AddToIndices(tuple, data);
    return true;
  }

  VectorType data;
};

template <unsigned kTableId>
class Table<SlabStorage, kTableId> : public SlabTable<kTableId> {};

}  // namespace rt
}  // namespace hyde
