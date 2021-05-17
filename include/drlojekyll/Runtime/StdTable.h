// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <memory>
#include <deque>
#include <tuple>
#include <utility>
#include <unordered_map>

#include <drlojekyll/Runtime/StdStorage.h>

namespace hyde {
namespace rt {

template <unsigned>
class StdTableScan;

template <unsigned>
class StdIndexScan;

// A helper to construct typed data structures given only integer
// identifiers for entities.
template <typename T>
struct StdTableHelper;

// Given a list of column IDs, find the backing data of that table.
template <unsigned kTableId, unsigned... kColumnIds, unsigned... kIndexIds>
struct StdTableHelper<TypeList<TableDescriptor<kTableId>,
                               IdList<kColumnIds...>,
                               IdList<kIndexIds...>>> {
 public:

  static constexpr auto kNumColumns = sizeof...(kColumnIds);
  static constexpr auto kNumIndexes = sizeof...(kIndexIds);

  using TupleType = std::tuple<
      typename ColumnDescriptor<kColumnIds>::Type...>;

  // A base record is the tuple data, along with its state.
  using BaseRecordType = std::pair<TupleState, TupleType>;

  using BackPointerArrayType = std::array<void *, kNumIndexes + 1u>;

  // A complete record is a base record, with `kNumIndexes + 1u` back pointers.
  // The first back pointer chains this record to the most recently added record
  // in this table. The `kNumIndexes` pointers chain the record back to other
  // records with identical hashes for their corresponding indexes.
  using RecordType = std::pair<BaseRecordType, BackPointerArrayType>;

  using IndexIdList = IdList<kIndexIds...>;
};

// Given a table ID, find the vector and map types for the backing data of that
// table.
template <unsigned kTableId>
struct StdTableHelper<TableDescriptor<kTableId>>
    : public StdTableHelper<TypeList<
          TableDescriptor<kTableId>,
          typename TableDescriptor<kTableId>::ColumnIds,
          typename TableDescriptor<kTableId>::IndexIds>> {};



// Try to change the state of a tuple. If it's not present, then add the
// tuple.
HYDE_RT_INLINE static bool TryChangeStateToPresent(
    TupleState *state, TupleState a_state, TupleState b_state) noexcept {
  const auto curr_state = *state;
  if (curr_state == a_state | curr_state == b_state) {
    *state = TupleState::kPresent;
    return true;

  } else {
    return false;
  }
}

// Try to change the state of a tuple which should already be present.
HYDE_RT_INLINE static bool ChangeState(TupleState *state,
                                       TupleState from_state,
                                       TupleState to_state) noexcept {
  const auto curr_state = *state;
  if (*state == from_state) {
    *state = to_state;
    return true;
  } else {
    return false;
  }
}

// Common things slit off into a base class. We anticipate that many tables
// will have the same shapes, and thus have the same `TupleType`s. None will
// have the same table IDs, however, so splitting things off lets us convince
// the compiler to merge common code.
template <typename TupleType>
class StdTableBase {
 public:
  using BaseRecordType = std::pair<TupleState, TupleType>;

 protected:

  // Hash a complete tuple.
  HYDE_RT_ALWAYS_INLINE static uint64_t HashTuple(
      const TupleType &tuple) noexcept {
    HashingWriter writer;
    Serializer<NullReader, HashingWriter, TupleType>::Write(writer, tuple);
    return writer.Digest();
  }

  // Hash an empty list of columns.
  HYDE_RT_INLINE static void HashColumnsByOffets(
      const TupleType &, HashingWriter &, IdList<>) {}

  // Hash a specific list of columns known to be inside of a tuple.
  template <unsigned kColumnOffset, unsigned... kColumnOffsets>
  HYDE_RT_INLINE static void HashColumnsByOffets(
      const TupleType &tuple, HashingWriter &writer,
      IdList<kColumnOffset, kColumnOffsets...>) {

    const auto &col = std::get<kColumnOffset>(tuple);
    using ColType = std::remove_const_t<std::remove_reference_t<decltype(col)>>;
    Serializer<NullReader, HashingWriter, ColType>::Write(writer, col);
    if constexpr (0u < sizeof...(kColumnOffsets)) {
      HashColumnsByOffets(tuple, writer, IdList<kColumnOffsets...>{});
    }
  }
};

// The `kTableId` is used to reference an auto-generated table descriptor, which
// looks roughly like this:
//
//      template <>
//      struct TableDescriptor<7> {
//        using ColumnIds = IdList<8, 9>;
//        using IndexIds = IdList<149>;
//      };
template <unsigned kTableId>
class StdTable
    : public StdTableBase<
          typename StdTableHelper<TableDescriptor<kTableId>>::TupleType> {
 public:
  static constexpr auto kCacheSize = 1024u;

  using TableDesc = TableDescriptor<kTableId>;
  using TableHelper = StdTableHelper<TableDesc>;
  using TupleType = typename TableHelper::TupleType;
  using BaseRecordType = typename TableHelper::BaseRecordType;
  using BackPointerArrayType = typename TableHelper::BackPointerArrayType;
  using RecordType = typename TableHelper::RecordType;
  using IndexIdList = typename TableHelper::IndexIdList;

  using Parent = StdTableBase<TupleType>;

  static constexpr unsigned kNumColumns = TableHelper::kNumColumns;
  static constexpr unsigned kNumIndexes = TableHelper::kNumIndexes;

  using Parent::Parent;

  template <typename... Ts>
  TupleState GetState(Ts&&... cols) const noexcept {
    const TupleType tuple(std::forward<Ts>(cols)...);
    const uint64_t hash = this->HashTuple(tuple);
    if (RecordType *record = FindRecord(tuple, hash); record) {
      return record->first.first;
    } else {
      return TupleState::kAbsent;
    }
  }

  template <typename... Ts>
  bool TryChangeStateFromPresentToUnknown(Ts&&... cols) const noexcept {
    const TupleType tuple(std::forward<Ts>(cols)...);
    const auto hash = this->HashTuple(tuple);
    if (const auto record = FindRecord(tuple, hash); record) {
      return ChangeState(&(record->first.first), TupleState::kPresent,
                         TupleState::kUnknown);
    } else {
      return false;
    }
  }

  template <typename... Ts>
  bool TryChangeStateFromUnknownToAbsent(Ts&&... cols) const noexcept {
    const TupleType tuple(std::forward<Ts>(cols)...);
    const auto hash = this->HashTuple(tuple);
    if (const auto record = FindRecord(tuple, hash); record) {
      return ChangeState(&(record->first.first), TupleState::kUnknown,
                         TupleState::kAbsent);
    } else {
      return false;
    }
  }

  template <typename... Ts>
  bool TryChangeStateFromAbsentToPresent(Ts&&... cols) noexcept {
    TupleType tuple(std::forward<Ts>(cols)...);
    const auto hash = this->HashTuple(tuple);
    if (const auto record = FindRecord(tuple, hash); record) {
      return TryChangeStateToPresent(&(record->first.first),
                                     TupleState::kAbsent, TupleState::kAbsent);
    } else {
      auto &record_ref = records.emplace_back(
          std::make_pair<TupleState, TupleType>(TupleState::kPresent,
                                                std::move(tuple)),
          BackPointerArrayType{});
      LinkNewRecord(&record_ref, hash);
      return true;
    }
  }

  template <typename... Ts>
  bool TryChangeStateFromAbsentOrUnknownToPresent(Ts&&... cols) noexcept {
    TupleType tuple(std::forward<Ts>(cols)...);
    const auto hash = this->HashTuple(tuple);
    if (const auto record = FindRecord(tuple, hash); record) {
      return TryChangeStateToPresent(&(record->first.first),
                                     TupleState::kAbsent, TupleState::kUnknown);
    } else {
      auto &record_ref = records.emplace_back(
          std::make_pair<TupleState, TupleType>(TupleState::kPresent,
                                                std::move(tuple)),
          BackPointerArrayType{});
      LinkNewRecord(&record_ref, hash);
      return true;
    }
  }

 private:

  template <unsigned>
  friend class StdTableScan;

  template <unsigned>
  friend class StdIndexScan;

  // Find the base record associated with a tuple.
  RecordType *FindRecord(const TupleType &tuple, uint64_t hash) const noexcept {

    // We have a single element cache that scans update.
    if (RecordType *scan_record =
            last_scanned_record.load(std::memory_order_acquire)) {
      if (scan_record->first.second == tuple) {
        return scan_record;
      }
    }

    // We have a `kCacheSize`-sized cache that `FindRecord` populates, as we
    // expect finding a record to be associated with later state changes.
    auto &cache = last_accessed_record[hash % kCacheSize];
    if (RecordType *cached_record = cache;
        cached_record && cached_record->first.second == tuple) {
      return cached_record;
    }

    // We missed in the cache, so go look for the record. This requires finding
    // the first tuple that hashed to `hash`, then traversing its linked list
    // to other records that have the same hash.
    auto it = hash_to_record.find(hash);
    if (it == hash_to_record.end()) {
      return nullptr;
    }

    RecordType *record = it->second;
    assert(record != nullptr);

    // We've got a tuple for this hash, go traverse the linked list.
    while (record) {

      // The tuple matches what we're looking for.
      if (record->first.second == tuple) {

        // We'll update the most recently touched record here on the assumption
        // that a subsequent operation near in time will try to change the
        // state of this tuple.
        cache = record;

        // We found the record we're looking for, return it.
        return record;
      }

      // Go to the next record with the same hash.
      const auto record_addr = reinterpret_cast<uintptr_t>(record->second[0]);

      // The next record has a different hash, or it is null.
      if (!(record_addr >> 1u)) {
        return nullptr;

      } else {
        record = reinterpret_cast<RecordType *>(record_addr - 1u);
      }
    }
    return nullptr;
  }

  void LinkNewRecord(RecordType *record, uint64_t hash) {

    // Make sure all record addresses are evenly aligned; we rely on this
    // for figuring out when we've gotten to the end of a linked list in a full
    // table for a given hash.
    assert(!(reinterpret_cast<uintptr_t>(record) & 1u));

    auto &prev_record = hash_to_record[hash];

    // Add it to our cache.
    last_accessed_record[hash % kCacheSize] = record;

    // We have a prior record for this hash. This prior record might be linked
    // in to another record somewhere else, so we can't do anything about that.
    // We want to inject our new node in-between the prior node and its next
    // node for the whole table.
    if (prev_record) {
      record->second[0] = record;
      std::swap(record->second[0], prev_record->second[0]);

    // We don't have a previous record associated with this hash, so we'll add
    // it in as the first record for the hash, and then we'll link this record
    // to the "last first" record, and mark this record as the last record.
    //
    // We make sure that the last record has its low bit marked as `1`, to tell
    // us that its hash doesn't match with `last_record`.
    } else {
      record->second[0] = reinterpret_cast<void *>(
          reinterpret_cast<uintptr_t>(record) + 1u);
      std::swap(record->second[0], last_record);
      prev_record = record;
    }

    if constexpr (0u < kNumIndexes) {
      AddToIndexes(record, IndexIdList{});
    }
  }

  HYDE_RT_INLINE static void AddToIndexes(RecordType *, IdList<>) {}

  template <unsigned kIndexId, unsigned... kIndexIds>
  HYDE_RT_INLINE
  void AddToIndexes(RecordType *record, IdList<kIndexId, kIndexIds...>) {
    using IndexDesc = IndexDescriptor<kIndexId>;
    using KeyColumnOffsets = typename IndexDesc::KeyColumnOffsets;

    static constexpr unsigned kOffset = IndexDesc::kOffset;
    auto &index = indexes[kOffset];

    const TupleType &tuple = record->first.second;

    HashingWriter writer;
    this->HashColumnsByOffets(tuple, writer, KeyColumnOffsets{});
    const uint64_t hash = writer.Digest();

    // The index maps a hash to the most recently added tuple with that hash.
    // The record for the just-added tuple (this one) includes pointers back to
    // those prior tuples.
    auto &record_ptr = index[hash];
    auto &prev_record_ptr = std::get<kOffset + 1u>(record->second);
    prev_record_ptr = record_ptr;
    record_ptr = record;

    // Recursively add to the next level of indices.
    if constexpr (0u < sizeof...(kIndexIds)) {
      AddToIndexes(record, IdList<kIndexIds...>{});
    }
  }

  std::deque<RecordType> records;
  std::unordered_map<uint64_t, RecordType *> hash_to_record;
  std::array<std::unordered_map<uint64_t, RecordType *>, kNumIndexes> indexes;
  void * last_record{nullptr};

  // TODO(pag): Eventually make this be an array, with one per worker.
  std::atomic<RecordType *> last_scanned_record;
  mutable std::array<RecordType *, kCacheSize> last_accessed_record;
};

template <unsigned kTableId>
class Table<StdStorage, kTableId> : public StdTable<kTableId> {
 public:
  Table(StdStorage &) {}
};

}  // namespace rt
}  // namespace hyde