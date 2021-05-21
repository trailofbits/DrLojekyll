// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <memory>

#include <drlojekyll/Runtime/StdTable.h>

namespace hyde {
namespace rt {

// An iterator that scans through a linked list of records, where the next
// pointer of the record is stored at `std::get<2>(record)[kBackLink]`.
// A `kBackLink` value of `0` means we're traversing through the table,
// and of `N + 1` means we're traversing through the table's `N`th index.
template <typename RecordType, unsigned kBackLink, bool kIsTableScan>
class StdScanIterator {
 private:
  RecordType *ptr{nullptr};
  std::atomic<RecordType *> *scanned_ptr{nullptr};

 public:
  using Self = StdScanIterator<RecordType, kBackLink, kIsTableScan>;

  static constexpr size_t kStateIndex = 0u;
  static constexpr size_t kTupleIndex = 1u;
  static constexpr size_t kBackLinksIndex = 2u;

  HYDE_RT_ALWAYS_INLINE StdScanIterator(void) = default;

  HYDE_RT_ALWAYS_INLINE explicit StdScanIterator(
      RecordType *ptr_, std::atomic<RecordType *> *scanned_ptr_) noexcept
      : ptr(ptr_),
        scanned_ptr(scanned_ptr_) {}

  HYDE_RT_ALWAYS_INLINE StdScanIterator(const Self &that) noexcept
      : ptr(that.ptr),
        scanned_ptr(that.scanned_ptr) {}

  HYDE_RT_ALWAYS_INLINE StdScanIterator(Self &&that) noexcept
      : ptr(that.ptr),
        scanned_ptr(that.scanned_ptr) {}

  HYDE_RT_ALWAYS_INLINE void operator=(const Self &that) noexcept {
    ptr = that.ptr;
    scanned_ptr = that.scanned_ptr;
  }

  HYDE_RT_ALWAYS_INLINE void operator=(Self &&that) noexcept {
    ptr = that.ptr;
    scanned_ptr = that.scanned_ptr;
  }

  HYDE_RT_ALWAYS_INLINE bool operator==(Self that) const noexcept {
    return ptr == that.ptr;
  }

  HYDE_RT_ALWAYS_INLINE bool operator!=(Self that) const noexcept {
    return ptr != that.ptr;
  }

  // Return the tuple pointed to by the scan's pointer, and store it back into
  // the table as our most recently scanned tuple.
  HYDE_RT_ALWAYS_INLINE auto operator*(void) const noexcept
      -> decltype(std::get<kTupleIndex>(*this->ptr)) {
    scanned_ptr->store(ptr, std::memory_order_release);
    return std::get<kTupleIndex>(*ptr);
  }

  // The full table records are of the form:
  //
  //    pair<pair<TupleState, TupleType>, std::array<void *, kNumIndices + 1u>>
  //
  // The first pointer connects together every tuple in the table. The remaining
  // pointers connect together tuples with identical hashes in the indices.
  HYDE_RT_ALWAYS_INLINE void operator++(void) noexcept {
    const auto addr = reinterpret_cast<uintptr_t>(
        std::get<kBackLink>(std::get<kBackLinksIndex>(*ptr)));

    // If it's an index scan, then we want to treat a pointer to tuple with
    // a different hash as a null pointer.
    if constexpr (!kIsTableScan) {
      static constexpr auto kShift = (sizeof(void *) * 8u) - 1u;
      auto mask = (~static_cast<uintptr_t>(
          static_cast<intptr_t>(addr << kShift) >> kShift)) << 1u;
      ptr = reinterpret_cast<RecordType *>(addr & mask);

    // If it's a table scan, then we want to follow all pointers, even if they
    // cross to a different hash.
    } else {
      ptr = reinterpret_cast<RecordType *>((addr >> 1u) << 1u);
    }
  }
};

// A scanner for iterating through all records in a table.
template <unsigned kTableId>
class StdTableScan {
 private:
  using TableDesc = TableDescriptor<kTableId>;
  using Table = StdTable<kTableId>;
  using RecordType = typename Table::RecordType;

  std::atomic<RecordType *> * const last_scanned_record;
  RecordType *first{nullptr};

 public:

  // The iterator for a full table scan uses the offset `0` in the embedded
  // `std::array` of a table, representing
  using Iterator = StdScanIterator<RecordType, 0u, true>;

  HYDE_RT_ALWAYS_INLINE StdTableScan(StdStorage &, Table &table) noexcept
      : last_scanned_record(&(table.last_scanned_record)) {
    const auto record_addr = reinterpret_cast<uintptr_t>(table.last_record);
    first = reinterpret_cast<RecordType *>((record_addr >> 1u) << 1u);
  }

  HYDE_RT_ALWAYS_INLINE Iterator begin(void) const noexcept {
    return Iterator(first, last_scanned_record);
  }

  HYDE_RT_ALWAYS_INLINE Iterator end(void) const noexcept {
    return Iterator();
  }
};

// A scanner for iterating through all records in a particular index. This will
// actually scan through a superset of what's in the index -- it scans through
// everything that has the same hash.
template <unsigned kIndexId>
class StdIndexScan {
 private:
  using IndexDesc = IndexDescriptor<kIndexId>;
  static constexpr unsigned kOffset = IndexDesc::kOffset;
  static constexpr unsigned kTableId = IndexDesc::kTableId;

  using Table = StdTable<kTableId>;
  using RecordType = typename Table::RecordType;

  std::atomic<RecordType *> * const last_scanned_record;
  RecordType *first{nullptr};

 public:

  using Iterator = StdScanIterator<RecordType, kOffset, false>;

  template <typename... Ts>
  StdIndexScan(StdStorage &, Table &table, Ts&&... cols) noexcept
      : last_scanned_record(&(table.last_scanned_record)){

    using TupleType = std::tuple<Ts...>;
    TupleType tuple(std::forward<Ts>(cols)...);
    HashingWriter writer;
    Serializer<NullReader, HashingWriter, TupleType>::Write(writer, tuple);
    auto hash = writer.Digest();
    auto &index = table.indexes[kOffset];
    if (auto it = index.find(hash); it != index.end()) {
      first = it->second;
    }
  }

  HYDE_RT_ALWAYS_INLINE Iterator begin(void) const noexcept {
    return Iterator(first, last_scanned_record);
  }

  HYDE_RT_ALWAYS_INLINE Iterator end(void) const noexcept {
    return Iterator();
  }
};

template <unsigned kTableId>
class Scan<StdStorage, TableTag<kTableId>> : public StdTableScan<kTableId> {
 public:
  using StdTableScan<kTableId>::StdTableScan;
};

template <unsigned kIndexId>
class Scan<StdStorage, IndexTag<kIndexId>> : public StdIndexScan<kIndexId> {
 public:
  using StdIndexScan<kIndexId>::StdIndexScan;
};

}  // namespace rt
}  // namespace hyde
