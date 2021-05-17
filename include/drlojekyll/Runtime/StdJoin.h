//// Copyright 2021, Trail of Bits, Inc. All rights reserved.
//
//#pragma once
//
//#include <memory>
//
//#include <drlojekyll/Runtime/StdScan.h>
//
//namespace hyde {
//namespace rt {
//
//template <typename T>
//struct StdScannerFor;
//
//template <unsigned kTableId>
//struct StdScannerFor<TableTag<kTableId>> {
//  using Table = StdTable<kTableId>;
//  using RecordType = typename Table::RecordType;
//  static constexpr unsigned kOffset = 0u;
//};
//
//template <unsigned kIndexId>
//struct StdScannerFor<IndexTag<kIndexId>> {
//  using IndexDesc = IndexDescriptor<kIndexId>;
//  static constexpr auto kTableId = IndexDesc::kTableId;
//  using Table = StdTable<kTableId>;
//  using RecordType = typename Table::RecordType;
//  static constexpr unsigned kOffset = IndexDesc::kOffset + 1u;
//};
//
//template <unsigned kNumPivots, typename... IndexOrTableTags>
//class StdJoin {
//
//};
//
//template <typename StorageT, unsigned kNumPivots, typename... IndexOrTableTags>
//class Join<StdStorage, kNumPivots, IndexOrTableTags...>
//    : public StdJoin<kNumPivots, IndexOrTableTags...> {};
//
//}  // namespace rt
//}  // namespace hyde
