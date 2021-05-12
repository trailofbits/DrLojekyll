// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <cassert>
#include <vector>
#include <tuple>
#include <unordered_map>
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
struct SlabColumnHelper;

template <typename T>
struct SlabIndexHelper;

template <unsigned kIndexId>
struct IndexDescriptor;

template <unsigned kTableId>
struct TableDescriptor;

template <unsigned kColumnId>
struct ColumnDescriptor;

// Given a table ID, find the vector and map types for the backing data of that
// table.
template <unsigned kTableId>
struct SlabColumnHelper<TableDescriptor<kTableId>>
    : public SlabColumnHelper<
          typename TableDescriptor<kTableId>::ColumnIds> {};

// Given a list of column IDs, find the backing data of that table.
template <unsigned... kColumnIds>
struct SlabColumnHelper<IdList<kColumnIds...>> {
 public:
  using TupleType = std::tuple<typename ColumnDescriptor<kColumnIds>::Type...>;
  using TupleBuilderType = TupleBuilder<Mutable<TupleState>, TupleType>;
  using VectorType = PersistentTypedSlabVector<Mutable<TupleState>, TupleType>;
};

template <unsigned kOffset, typename IndexIdList>
class SlabTableIndices;

template <unsigned kIndexId>
struct IndexTag {};

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
  std::tuple<bool, uint64_t, bool> TryChangeState(
      const TupleType &tuple, TupleState a_state, TupleState b_state) noexcept {
    const auto [hash, num_bytes] = HashAndSizeColumns(tuple);
    for (auto [it, end] = assoc_data[0].equal_range(hash);
         it != end; ++it) {

      TupleBuilderType builder(it->second, num_bytes  /* estimation */);
      if (auto [state_ref, tuple_ref] = builder.Build(); tuple_ref == tuple) {
        const auto curr_state = static_cast<TupleState>(state_ref);
        if (curr_state == a_state | curr_state == b_state) {
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
  bool ChangeState(const TupleType &tuple, TupleState from_state,
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
  std::vector<std::unordered_multimap<uint64_t, uint8_t *>> assoc_data;

 private:
  SlabTableBase(void) = delete;
  SlabTableBase(const SlabTableBase &) = delete;
  SlabTableBase(SlabTableBase &&) noexcept = delete;
};

// Base case when we get to the end of indices: inherit from `SlabTableBase`,
// giving all of our index scanners access to `assoc_data`.
template <unsigned kOffset>
class SlabTableIndices<kOffset, IdList<>> : public SlabTableBase {
 public:

  static constexpr unsigned kNumIndices = 0u;

  template <typename TupleType>
  HYDE_RT_INLINE void AddToIndices(const TupleType &, uint8_t *data) {}
};

// Inductive case: each index corresponds with a base class, and we figure out
// and offset in `assoc_data` (from `SlabTableBase`, our topmost base class)
// for this index.
template <unsigned kOffset, unsigned kIndexId, unsigned... kIndexIds>
class SlabTableIndices<kOffset, IdList<kIndexId, kIndexIds...>>
    : public SlabTableIndices<kOffset + 1u, IdList<kIndexIds...>> {
 public:

  using Parent = SlabTableIndices<kOffset + 1u, IdList<kIndexIds...>>;
  using KeyColumnIds = typename IndexDescriptor<kIndexId>::KeyColumnIds;
  using ValueColumnIds = typename IndexDescriptor<kIndexId>::ValueColumnIds;

  static constexpr bool kIsNeeded = !std::is_same_v<ValueColumnIds, IdList<>>;

  // What is the actual offset of `assoc_data` to use?
  static constexpr unsigned kActualOffset = !kIsNeeded ? 0u : kOffset;


  static constexpr unsigned kNumIndices =
      Parent::kNumIndices + (kIsNeeded ? 1u : 0u);

  // Scan this index.
  template <typename... Ts>
  void Scan(const Ts&... cols, IndexTag<kIndexId>) {

  }

  template <typename TupleType>
  HYDE_RT_INLINE void AddToIndices(const TupleType &tuple, uint8_t *data) {
    if constexpr (kActualOffset != 0u) {

    }
    this->Parent::AddToIndices(tuple);
  }
};

// Implements a table, which is backed by a persistent list of slabs.
template <unsigned kTableId>
class SlabTable
    : public SlabTableIndices<
          0u, typename TableDescriptor<kTableId>::IndexIds> {
 public:

  using TableDesc = TableDescriptor<kTableId>;
  using Parent = SlabTableIndices<0u, typename TableDesc::IndexIds>;

  explicit SlabTable(SlabStorage &storage_)
      : Parent(storage_, kTableId),
        data(*(storage_.manager), std::move(this->table_info.first), 0u) {

    // Allocate space for all indices, and the table. If there's a full
    // row index then we'll remap it back to `assoc_data[0]`.
    this->assoc_data.resize(Parent::kNumIndices + 1u);

    // Reserve enough storage for the number of rows in the table.
    this->assoc_data[0].reserve(table_info.second);

    // Revive the persistent data, if any.
    for (auto [state_ref, tuple_ref] : data) {
      this->assoc_data[0].emplace(tuple_ref.Hash(), state_ref.Data());
    }

    // Consistency check on row count.
    assert(this->assoc_data.size() == table_info.second);
  }

  ~SlabTable(void) noexcept {
    this->storage.PutTableSlabs(kTableId, data, assoc_data.size());
  }

  // Return the number of rows in the table.
  uint64_t Size(void) const noexcept {
    return this->assoc_data.size();
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  TupleState GetState(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    return this->SlabTableBase::GetStateImpl<TupleType, TupleBuilderType>(tuple);
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  bool TryChangeStateFromAbsentToPresent(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    const auto [ret, hash, add] =
        this->SlabTableBase::TryChangeState<TupleType, TupleBuilderType>(
            tuple, TupleState::kAbsent, TupleState::kAbsent);
    if (add) {
      return AddTuple(tuple, hash);
    }
    return ret;
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  bool TryChangeStateFromAbsentOrUnknownToPresent(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    const auto [ret, hash, add] =
        this->SlabTableBase::TryChangeState<TupleType, TupleBuilderType>(
            tuple, TupleState::kAbsent, TupleState::kUnknown);
    if (add) {
      return AddTuple(tuple, hash);
    }
    return ret;
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  bool TryChangeStateFromPresentToUnknown(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    return this->SlabTableBase::ChangeState<TupleType, TupleBuilderType>(
        tuple, TupleState::kPresent, TupleState::kUnknown);
  }

  template <typename... Ts>
  HYDE_RT_ALWAYS_INLINE
  bool TryChangeStateFromUnknownToAbsent(const Ts&... cols) noexcept {
    const TupleType tuple(cols...);
    return this->SlabTableBase::ChangeState<TupleType, TupleBuilderType>(
        tuple, TupleState::kUnknown, TupleState::kAbsent);
  }

 private:

  using ColumnHelper = SlabColumnHelper<TableDesc>;
  using VectorType = typename ColumnHelper::VectorType;
  using TupleType = typename ColumnHelper::TupleType;
  using TupleBuilderType = typename ColumnHelper::TupleBuilderType;

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
