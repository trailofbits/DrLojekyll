// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <array>
#include <atomic>
#include <bitset>
#include <cassert>
#include <memory>
#include <deque>
#include <tuple>
#include <utility>
#include <unordered_map>

#include <drlojekyll/Runtime/Server/Std/Storage.h>

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

  using BackPointerArrayType = std::array<void *, kNumIndexes>;

  // A complete record is a base record, with `kNumIndexes + 1u` back pointers.
  // The first back pointer chains this record to the most recently added record
  // in this table. The `kNumIndexes` pointers chain the record back to other
  // records with identical hashes for their corresponding indexes.
  using RecordType = std::tuple<TupleState, TupleType, BackPointerArrayType>;

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
HYDE_RT_ALWAYS_INLINE static bool TryChangeTupleToPresent(
    TupleState *state, TupleState a_state, TupleState b_state) noexcept {
  const auto curr_state = *state;
  if (curr_state == a_state || curr_state == b_state) {
    *state = TupleState::kPresent;
    return true;

  } else {
    return false;
  }
}

// Try to change the state of a tuple which should already be present.
HYDE_RT_ALWAYS_INLINE static bool ChangeState(TupleState *state,
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
 protected:

  // Hash a complete tuple.
  HYDE_RT_ALWAYS_INLINE static uint64_t HashTuple(
      const TupleType &tuple) noexcept {
    HashingWriter writer;
    Serializer<NullReader, HashingWriter, TupleType>::Write(writer, tuple);
    return writer.Digest();
  }

  // Hash an empty list of columns.
  HYDE_RT_ALWAYS_INLINE static void HashColumnsByOffets(
      const TupleType &, HashingWriter &, IdList<>) {}

  // Hash a specific list of columns known to be inside of a tuple.
  template <unsigned kColumnOffset, unsigned... kColumnOffsets>
  HYDE_RT_ALWAYS_INLINE static void HashColumnsByOffets(
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
  // Size of the inline cache of recently accessed tuples.
  static constexpr auto kCacheSize = 1024u;

  // Number of bloom filters.
  static constexpr auto kNumBloomFilters = 2u;

  using TableDesc = TableDescriptor<kTableId>;
  using TableHelper = StdTableHelper<TableDesc>;
  using TupleType = typename TableHelper::TupleType;
  using BackPointerArrayType = typename TableHelper::BackPointerArrayType;
  using RecordType = typename TableHelper::RecordType;
  using IndexIdList = typename TableHelper::IndexIdList;

  using Parent = StdTableBase<TupleType>;

  static constexpr unsigned kNumColumns = TableHelper::kNumColumns;
  static constexpr unsigned kNumIndexes = TableHelper::kNumIndexes;

  static_assert(0u < kNumIndexes);

  static constexpr size_t kStateIndex = 0u;
  static constexpr size_t kTupleIndex = 1u;
  static constexpr size_t kBackLinksIndex = 2u;

  using Parent::Parent;

  template <typename... Ts>
  HYDE_RT_NEVER_INLINE
  TupleState GetState(Ts... cols) const noexcept {
    const TupleType tuple(std::move(cols)...);
    const uint64_t hash = this->HashTuple(tuple);
    if (RecordType *record = FindRecord(tuple, hash); record) {
      return std::get<kStateIndex>(*record);
    } else {
      return TupleState::kAbsent;
    }
  }

  template <typename... Ts>
  HYDE_RT_NEVER_INLINE
  bool TryChangeTupleFromPresentToUnknown(Ts... cols) const noexcept {
    const TupleType tuple(std::move(cols)...);
    const auto hash = this->HashTuple(tuple);
    if (const auto record = FindRecord(tuple, hash); record) {
      return ChangeState(&std::get<kStateIndex>(*record), TupleState::kPresent,
                         TupleState::kUnknown);
    } else {
      return false;
    }
  }

  template <typename... Ts>
  HYDE_RT_NEVER_INLINE
  bool TryChangeTupleFromUnknownToAbsent(Ts... cols) const noexcept {
    const TupleType tuple(std::move(cols)...);
    const auto hash = this->HashTuple(tuple);
    if (const auto record = FindRecord(tuple, hash); record) {
      return ChangeState(&std::get<kStateIndex>(*record), TupleState::kUnknown,
                         TupleState::kAbsent);
    } else {
      return false;
    }
  }

  template <typename... Ts>
  HYDE_RT_NEVER_INLINE
  bool TryChangeTupleFromAbsentToPresent(Ts... cols) noexcept {
    TupleType tuple(std::move(cols)...);
    const auto hash = this->HashTuple(tuple);
    if (const auto record = FindRecord(tuple, hash); record) {
      return TryChangeTupleToPresent(&std::get<kStateIndex>(*record),
                                     TupleState::kAbsent, TupleState::kAbsent);
    } else {
      auto &record_ref = records.emplace_back(
          TupleState::kPresent, std::move(tuple), BackPointerArrayType{});
      LinkNewRecord(&record_ref, hash);
      return true;
    }
  }

  template <typename... Ts>
  HYDE_RT_NEVER_INLINE
  bool TryChangeTupleFromAbsentOrUnknownToPresent(Ts... cols) noexcept {
    TupleType tuple(std::move(cols)...);
    const auto hash = this->HashTuple(tuple);
    if (const auto record = FindRecord(tuple, hash); record) {
      return TryChangeTupleToPresent(&std::get<kStateIndex>(*record),
                                     TupleState::kAbsent, TupleState::kUnknown);
    } else {
      auto &record_ref = records.emplace_back(
          TupleState::kPresent, std::move(tuple), BackPointerArrayType{});
      LinkNewRecord(&record_ref, hash);
      return true;
    }
  }

  // Return the number of records in the table.
  uint64_t Size(void) const noexcept {
    return num_records;
  }

 private:

  template <unsigned>
  friend class StdTableScan;

  template <unsigned>
  friend class StdIndexScan;

  // Find the base record associated with a tuple.
  HYDE_RT_ALWAYS_INLINE RecordType *FindRecord(
      const TupleType &tuple, uint64_t hash) const noexcept {

    // Check for the record in our bloom filter.
    uint64_t filter_index = hash;
    for (const auto &filter : bloom_filter) {
      if (!filter.test(static_cast<uint16_t>(filter_index))) {
        return nullptr;
      } else {
        filter_index >>= 16u;
      }
    }

    // We have a single element cache that scans update.
    if (RecordType *scan_record =
            last_scanned_record.load(std::memory_order_acquire)) {
      if (std::get<kTupleIndex>(*scan_record) == tuple) {
        return scan_record;
      }
    }

    // We have a `kCacheSize`-sized cache that `FindRecord` populates, as we
    // expect finding a record to be associated with later state changes.
    if (RecordType *cached_record = last_accessed_record[hash % kCacheSize];
        cached_record && std::get<kTupleIndex>(*cached_record) == tuple) {
      return cached_record;
    }

    // The first index covers all columns, so we can use `hash`.
    if constexpr (TableDesc::kHasCoveringIndex) {
      return FindRecordInFirstIndex(tuple, hash);

    // The first index operates on a subset of the columns, so we need to
    // send along the tuple and hash a subset of those columns.
    } else {
      using FirstIndexDesc = IndexDescriptor<TableDesc::kFirstIndexId>;
      using FirstIndexKeyColumnOffsets =
          typename FirstIndexDesc::KeyColumnOffsets;
      return FindRecordInIndex<FirstIndexKeyColumnOffsets>(tuple);
    }
  }

  // We want to find `tuple` in an index, but none of the indexes cover all of
  // the columns, so we need to compute a new hash that summarizes a subset of
  // the columns inside of `tuple`, and used that as a lookup hash for the
  // first index.
  template <typename KeyColumnOffsets>
  HYDE_RT_ALWAYS_INLINE RecordType *FindRecordInIndex(
      const TupleType &tuple) const noexcept {
    uint64_t hash = 0;
    {
      HashingWriter writer;
      this->HashColumnsByOffets(tuple, writer, KeyColumnOffsets{});
      hash = writer.Digest();
    }
    return FindRecordInFirstIndex(tuple, hash);
  }

  // If we have a covering index, i.e. an index over all columns, then the
  // code generator will have arranged for that to be the first index in our
  // index ID list for this table. We can thus rely on `hash` to get us into
  // the map for the index.
  HYDE_RT_NEVER_INLINE RecordType *FindRecordInFirstIndex(
      const TupleType &tuple, uint64_t hash) const noexcept {

    auto it = indexes[0].find(hash);
    if (it == indexes[0].end()) {
      return nullptr;
    }

    // NOTE(pag): Records can still be null, as our index scans eagerly
    //            create hash table entries so that they can observe the
    //            updates, if any.

    // We've got a tuple for this hash, go traverse the linked list.
    for (RecordType *record = it->second; record; ) {

      // The tuple matches what we're looking for.
      if (std::get<kTupleIndex>(*record) == tuple) {

        // We'll update the most recently touched record here on the assumption
        // that a subsequent operation near in time will try to change the
        // state of this tuple.
        assert(!(reinterpret_cast<uintptr_t>(record) & 1u));
        last_accessed_record[hash % kCacheSize] = record;

        // We found the record we're looking for, return it.
        return record;
      }

      // Go to the next record with the same hash.
      const auto &back_links = std::get<kBackLinksIndex>(*record);
      const auto record_addr = reinterpret_cast<uintptr_t>(
          back_links[0]);

      // The next record has a different hash, or it is null.
      const auto shifted_record_addr = record_addr >> 1u;
      if (!shifted_record_addr) {
        return nullptr;

      } else {
        record = reinterpret_cast<RecordType *>(shifted_record_addr << 1u);
      }
    }
    return nullptr;
  }

  HYDE_RT_NEVER_INLINE HYDE_RT_FLATTEN
  void LinkNewRecord(RecordType *record, uint64_t hash) {

    // Increment the total number of records in our table.
    ++num_records;

    // Make sure all record addresses are evenly aligned; we rely on this
    // for figuring out when we've gotten to the end of a linked list in a full
    // table for a given hash.
    assert(!(reinterpret_cast<uintptr_t>(record) & 1u));

    // Add the record to our bloom filter.
    uint64_t filter_index = hash;
    for (auto &filter : bloom_filter) {
      filter.set(static_cast<uint16_t>(filter_index), true);
      filter_index >>= 16u;
    }

    // Add it to our cache.
    last_accessed_record[hash % kCacheSize] = record;

    AddToIndexes(record, IndexIdList{});
  }

  HYDE_RT_INLINE static void AddToIndexes(RecordType *, IdList<>) {}

  template <unsigned kIndexId, unsigned... kIndexIds>
  HYDE_RT_ALWAYS_INLINE
  void AddToIndexes(RecordType *record, IdList<kIndexId, kIndexIds...>) {
    using IndexDesc = IndexDescriptor<kIndexId>;
    using KeyColumnOffsets = typename IndexDesc::KeyColumnOffsets;

    uint64_t hash = 0;
    {
      HashingWriter writer;
      const TupleType &tuple = std::get<kTupleIndex>(*record);
      this->HashColumnsByOffets(tuple, writer, KeyColumnOffsets{});
      hash = writer.Digest();
    }

    static constexpr unsigned kIndexOffset = IndexDesc::kOffset;
    auto &prev_record = indexes[kIndexOffset][hash];
    auto &index_link = std::get<kIndexOffset>(
        std::get<kBackLinksIndex>(*record));

    // We have a prior record for this hash. This prior record might be linked
    // in to another record somewhere else, so we can't do anything about that.
    // We want to inject our new node in-between the prior node and its next
    // node for the whole table.
    if (prev_record) {
      auto &prev_index_link = std::get<kIndexOffset>(
          std::get<kBackLinksIndex>(*prev_record));

      index_link = prev_index_link;
      prev_index_link = record;

    // We don't have a previous record associated with this hash, so we'll add
    // it in as the first record for the hash, and then we'll link this record
    // to the "last first" record, and mark this record as the last record.
    //
    // We make sure that the last record has its low bit marked as `1`, to tell
    // us that its hash doesn't match with `last_record`.
    } else {
      index_link = reinterpret_cast<void *>(
          reinterpret_cast<uintptr_t>(last_record) | 1u);
      prev_record = record;

      if constexpr (std::is_same_v<IdList<kIndexId, kIndexIds...>,
                    IndexIdList>) {
        last_record = record;
      }
    }

    // Recursively add to the next level of indices.
    if constexpr (0u < sizeof...(kIndexIds)) {
      AddToIndexes(record, IdList<kIndexIds...>{});
    }
  }

  // Backing storage for all records.
  std::deque<RecordType> records;

  // The bloom filter that tells us if a record is definitely not in our
  // table.
  std::array<std::bitset<65536>, kNumBloomFilters> bloom_filter;

  // List of hash-mapped linked lists
  std::array<std::unordered_map<uint64_t, RecordType *>, kNumIndexes> indexes;

  // Last record added whose hash didn't collide with another pre-existing
  // record. This is basically the head of the linked list of all records. In
  // the case of a record whose hash is already used, we link that record in
  // after the pre-existing record.
  void * last_record{nullptr};

  // The most recent record produced from an index or table scan.
  //
  // TODO(pag): Eventually make this be an array, with one per worker.
  std::atomic<RecordType *> last_scanned_record{};

  // Cache of recently accessed records.
  mutable std::array<RecordType *, kCacheSize> last_accessed_record = {};

  uint64_t num_records{0};
};

template <unsigned kTableId>
class Table<StdStorage, kTableId> : public StdTable<kTableId> {
 public:
  Table(StdStorage &) {}
};

}  // namespace rt
}  // namespace hyde
