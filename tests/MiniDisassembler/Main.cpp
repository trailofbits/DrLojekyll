// Copyright 2021, Trail of Bits. All rights reserved.

#include <gtest/gtest.h>

#include <drlojekyll/Runtime/StdRuntime.h>
#include <algorithm>
#include <vector>
#include <iomanip>
#include <iostream>
#include <set>
#include <cstdio>
#include <cinttypes>

#if 1
#include "database.db.h"
#else
#include <algorithm>
#include <cstdio>
#include <cinttypes>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <vector>


namespace database {

}  // namespace database

namespace hyde::rt {
template <>
struct ColumnDescriptor<8> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 8;
  static constexpr unsigned kTableId = 7;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<9> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 9;
  static constexpr unsigned kTableId = 7;
  static constexpr unsigned kOffset = 1;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<10> {
  static constexpr unsigned kId = 10;
  static constexpr unsigned kTableId = 7;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 2;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<8>, KeyColumn<9>>;
  using KeyColumnIds = IdList<8, 9>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct IndexDescriptor<148> {
  static constexpr unsigned kId = 148;
  static constexpr unsigned kTableId = 7;
  static constexpr unsigned kOffset = 1;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 1;
  static constexpr bool kCoversAllColumns = false;
  using Columns = TypeList<ValueColumn<8>, KeyColumn<9>>;
  using KeyColumnIds = IdList<9>;
  using ValueColumnIds = IdList<8>;
  using KeyColumnOffsets = IdList<1>;
  using ValueColumnOffsets = IdList<0>;
};
template <>
struct IndexDescriptor<164> {
  static constexpr unsigned kId = 164;
  static constexpr unsigned kTableId = 7;
  static constexpr unsigned kOffset = 2;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 1;
  static constexpr bool kCoversAllColumns = false;
  using Columns = TypeList<KeyColumn<8>, ValueColumn<9>>;
  using KeyColumnIds = IdList<8>;
  using ValueColumnIds = IdList<9>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<1>;
};
template <>
struct TableDescriptor<7> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<8, 9>;
  using IndexIds = IdList<10, 148, 164>;
  static constexpr unsigned kFirstIndexId = 10;
  static constexpr unsigned kNumColumns = 2;
};

template <>
struct ColumnDescriptor<12> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 12;
  static constexpr unsigned kTableId = 11;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<13> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 13;
  static constexpr unsigned kTableId = 11;
  static constexpr unsigned kOffset = 1;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<14> {
  static constexpr unsigned kId = 14;
  static constexpr unsigned kTableId = 11;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 2;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<12>, KeyColumn<13>>;
  using KeyColumnIds = IdList<12, 13>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct IndexDescriptor<140> {
  static constexpr unsigned kId = 140;
  static constexpr unsigned kTableId = 11;
  static constexpr unsigned kOffset = 1;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 1;
  static constexpr bool kCoversAllColumns = false;
  using Columns = TypeList<KeyColumn<12>, ValueColumn<13>>;
  using KeyColumnIds = IdList<12>;
  using ValueColumnIds = IdList<13>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<1>;
};
template <>
struct TableDescriptor<11> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<12, 13>;
  using IndexIds = IdList<14, 140>;
  static constexpr unsigned kFirstIndexId = 14;
  static constexpr unsigned kNumColumns = 2;
};

template <>
struct ColumnDescriptor<16> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 16;
  static constexpr unsigned kTableId = 15;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<17> {
  static constexpr unsigned kId = 17;
  static constexpr unsigned kTableId = 15;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<16>>;
  using KeyColumnIds = IdList<16>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<15> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<16>;
  using IndexIds = IdList<17>;
  static constexpr unsigned kFirstIndexId = 17;
  static constexpr unsigned kNumColumns = 1;
};

template <>
struct ColumnDescriptor<19> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 19;
  static constexpr unsigned kTableId = 18;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<20> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 20;
  static constexpr unsigned kTableId = 18;
  static constexpr unsigned kOffset = 1;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<21> {
  static constexpr unsigned kId = 21;
  static constexpr unsigned kTableId = 18;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 2;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<19>, KeyColumn<20>>;
  using KeyColumnIds = IdList<19, 20>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<18> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<19, 20>;
  using IndexIds = IdList<21>;
  static constexpr unsigned kFirstIndexId = 21;
  static constexpr unsigned kNumColumns = 2;
};

template <>
struct ColumnDescriptor<23> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 23;
  static constexpr unsigned kTableId = 22;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<24> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 24;
  static constexpr unsigned kTableId = 22;
  static constexpr unsigned kOffset = 1;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<25> {
  static constexpr unsigned kId = 25;
  static constexpr unsigned kTableId = 22;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 2;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<23>, KeyColumn<24>>;
  using KeyColumnIds = IdList<23, 24>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct IndexDescriptor<66> {
  static constexpr unsigned kId = 66;
  static constexpr unsigned kTableId = 22;
  static constexpr unsigned kOffset = 1;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 1;
  static constexpr bool kCoversAllColumns = false;
  using Columns = TypeList<KeyColumn<23>, ValueColumn<24>>;
  using KeyColumnIds = IdList<23>;
  using ValueColumnIds = IdList<24>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<1>;
};
template <>
struct TableDescriptor<22> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<23, 24>;
  using IndexIds = IdList<25, 66>;
  static constexpr unsigned kFirstIndexId = 25;
  static constexpr unsigned kNumColumns = 2;
};

template <>
struct ColumnDescriptor<27> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 27;
  static constexpr unsigned kTableId = 26;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<28> {
  static constexpr unsigned kId = 28;
  static constexpr unsigned kTableId = 26;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<27>>;
  using KeyColumnIds = IdList<27>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<26> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<27>;
  using IndexIds = IdList<28>;
  static constexpr unsigned kFirstIndexId = 28;
  static constexpr unsigned kNumColumns = 1;
};

template <>
struct ColumnDescriptor<30> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 30;
  static constexpr unsigned kTableId = 29;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<31> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 31;
  static constexpr unsigned kTableId = 29;
  static constexpr unsigned kOffset = 1;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<32> {
  static constexpr unsigned kId = 32;
  static constexpr unsigned kTableId = 29;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 2;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<30>, KeyColumn<31>>;
  using KeyColumnIds = IdList<30, 31>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct IndexDescriptor<95> {
  static constexpr unsigned kId = 95;
  static constexpr unsigned kTableId = 29;
  static constexpr unsigned kOffset = 1;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 1;
  static constexpr bool kCoversAllColumns = false;
  using Columns = TypeList<KeyColumn<30>, ValueColumn<31>>;
  using KeyColumnIds = IdList<30>;
  using ValueColumnIds = IdList<31>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<1>;
};
template <>
struct TableDescriptor<29> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<30, 31>;
  using IndexIds = IdList<32, 95>;
  static constexpr unsigned kFirstIndexId = 32;
  static constexpr unsigned kNumColumns = 2;
};

template <>
struct ColumnDescriptor<34> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 34;
  static constexpr unsigned kTableId = 33;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<35> {
  static constexpr unsigned kId = 35;
  static constexpr unsigned kTableId = 33;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<34>>;
  using KeyColumnIds = IdList<34>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<33> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<34>;
  using IndexIds = IdList<35>;
  static constexpr unsigned kFirstIndexId = 35;
  static constexpr unsigned kNumColumns = 1;
};

template <>
struct ColumnDescriptor<37> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 37;
  static constexpr unsigned kTableId = 36;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<38> {
  static constexpr unsigned kId = 38;
  static constexpr unsigned kTableId = 36;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<37>>;
  using KeyColumnIds = IdList<37>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<36> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<37>;
  using IndexIds = IdList<38>;
  static constexpr unsigned kFirstIndexId = 38;
  static constexpr unsigned kNumColumns = 1;
};

template <>
struct ColumnDescriptor<40> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 40;
  static constexpr unsigned kTableId = 39;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<41> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 41;
  static constexpr unsigned kTableId = 39;
  static constexpr unsigned kOffset = 1;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<42> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 42;
  static constexpr unsigned kTableId = 39;
  static constexpr unsigned kOffset = 2;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<43> {
  static constexpr unsigned kId = 43;
  static constexpr unsigned kTableId = 39;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 3;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<40>, KeyColumn<41>, KeyColumn<42>>;
  using KeyColumnIds = IdList<40, 41, 42>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1, 2>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct IndexDescriptor<232> {
  static constexpr unsigned kId = 232;
  static constexpr unsigned kTableId = 39;
  static constexpr unsigned kOffset = 1;
  static constexpr unsigned kNumKeyColumns = 2;
  static constexpr unsigned kNumValueColumns = 1;
  static constexpr bool kCoversAllColumns = false;
  using Columns = TypeList<ValueColumn<40>, KeyColumn<41>, KeyColumn<42>>;
  using KeyColumnIds = IdList<41, 42>;
  using ValueColumnIds = IdList<40>;
  using KeyColumnOffsets = IdList<1, 2>;
  using ValueColumnOffsets = IdList<0>;
};
template <>
struct TableDescriptor<39> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<40, 41, 42>;
  using IndexIds = IdList<43, 232>;
  static constexpr unsigned kFirstIndexId = 43;
  static constexpr unsigned kNumColumns = 3;
};

template <>
struct ColumnDescriptor<45> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 45;
  static constexpr unsigned kTableId = 44;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<46> {
  static constexpr unsigned kId = 46;
  static constexpr unsigned kTableId = 44;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<45>>;
  using KeyColumnIds = IdList<45>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<44> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<45>;
  using IndexIds = IdList<46>;
  static constexpr unsigned kFirstIndexId = 46;
  static constexpr unsigned kNumColumns = 1;
};

}  // namepace hyde::rt

namespace database {
class DatabaseFunctors {
  public:
};

class DatabaseLog {
  public:
};

template <typename StorageT, typename LogT=DatabaseLog, typename FunctorsT=DatabaseFunctors>
class Database {
  public:
    StorageT &storage;
    LogT &log;
    FunctorsT &functors;

    ::hyde::rt::Table<StorageT, 7> table_7;
    ::hyde::rt::Table<StorageT, 11> table_11;
    ::hyde::rt::Table<StorageT, 15> table_15;
    ::hyde::rt::Table<StorageT, 18> table_18;
    ::hyde::rt::Table<StorageT, 22> table_22;
    ::hyde::rt::Table<StorageT, 26> table_26;
    ::hyde::rt::Table<StorageT, 29> table_29;
    ::hyde::rt::Table<StorageT, 33> table_33;
    ::hyde::rt::Table<StorageT, 36> table_36;
    ::hyde::rt::Table<StorageT, 39> table_39;
    ::hyde::rt::Table<StorageT, 44> table_44;
    uint64_t var_48;

    static constexpr uint8_t var_5 = {1};
    static constexpr uint8_t var_6 = {0};

    explicit Database(StorageT &s, LogT &l, FunctorsT &f)
      : storage(s),
        log(l),
        functors(f),
        table_7(s),
        table_11(s),
        table_15(s),
        table_18(s),
        table_22(s),
        table_26(s),
        table_29(s),
        table_33(s),
        table_36(s),
        table_39(s),
        table_44(s),
        var_48{0} {
      init_4_();
    }

    template <typename _Generator>
    ::hyde::rt::index_t function_instructions_bf(uint64_t param_0, _Generator _generator) {
      ::hyde::rt::index_t num_generated = 0;
      ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<164>> scan(storage, table_7, param_0);
      for (auto [shadow_param_0, param_1] : scan) {
        if (std::make_tuple(param_0) != std::make_tuple(shadow_param_0)) {
          continue;
        }
        if (!find_118_(param_0, param_1)) {
          continue;
        }
        num_generated += 1u;
        if (!_generator(param_0, param_1)) {
          return num_generated;
        }
      }
      return num_generated;
    }

    bool raw_transfer_3(::hyde::rt::Vector<StorageT, uint64_t, uint64_t, uint8_t> vec_154) {
      ::hyde::rt::Vector<StorageT, uint64_t> vec_156(storage, 156u);
      if (proc_47_(std::move(vec_154), std::move(vec_156))) {
      }
      return true;
      assert(false);
      return false;
    }

    bool instruction_1(::hyde::rt::Vector<StorageT, uint64_t> vec_158) {
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t, uint8_t> vec_160(storage, 160u);
      if (proc_47_(std::move(vec_160), std::move(vec_158))) {
      }
      return true;
      assert(false);
      return false;
    }


    template <typename Printer>
    void DumpSizes(Printer _print) const {
      _print(7, table_7.Size());
      _print(11, table_11.Size());
      _print(15, table_15.Size());
      _print(18, table_18.Size());
      _print(22, table_22.Size());
      _print(26, table_26.Size());
      _print(29, table_29.Size());
      _print(33, table_33.Size());
      _print(36, table_36.Size());
      _print(39, table_39.Size());
      _print(44, table_44.Size());
    }

    void DumpStats(void) const {
      if constexpr (false) {
        return;  /* change to false to enable */
      }
      static FILE *tables = nullptr;
      if (!tables) {
        tables = fopen("/tmp/tables.csv", "w");
        fprintf(tables, "table 7,table 11,table 15,table 18,table 22,table 26,table 29,table 33,table 36,table 39,table 44\n");
      }
      fprintf(tables, "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n", table_7.Size(), table_11.Size(), table_15.Size(), table_18.Size(), table_22.Size(), table_26.Size(), table_29.Size(), table_33.Size(), table_36.Size(), table_39.Size(), table_44.Size());
    }

    bool init_4_() {
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t, uint8_t> vec_162(storage, 162u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_163(storage, 163u);
      if (proc_47_(std::move(vec_162), std::move(vec_163))) {
      }
      return false;
      assert(false);
      return false;
    }

    bool proc_47_(::hyde::rt::Vector<StorageT, uint64_t, uint64_t, uint8_t> vec_49, ::hyde::rt::Vector<StorageT, uint64_t> vec_80) {
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_58(storage, 58u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_61(storage, 61u);
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_63(storage, 63u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_78(storage, 78u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_92(storage, 92u);
      for (auto [var_51, var_52, var_53] : vec_49) {
        if (table_44.TryChangeTupleFromAbsentToPresent(var_52)) {
          if (find_54_(var_52)) {
            if (table_26.TryChangeTupleFromPresentToUnknown(var_52)) {
              if (table_18.TryChangeTupleFromPresentToUnknown(var_52, var_52)) {
                if (table_7.TryChangeTupleFromPresentToUnknown(var_52, var_52)) {
                  vec_58.Add(var_52, var_52);
                  printf("Adding var_52=%" PRIu64 " var_52 to vec_58\n", var_52);
                }
              }
              {
                ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<66>> scan_67(storage, table_22, var_52);
                for (auto [var_68, var_69] : scan_67) {
                  if (std::make_tuple(var_68) == std::make_tuple(var_52)) {
                    if (find_70_(var_68, var_69)) {
                      if (find_74_(var_68)) {
                      } else {
                        vec_63.Add(var_68, var_69);
                      }
                    }
                  }
                }
              }
            }
          }
        }
        if (std::make_tuple(var_5) == std::make_tuple(var_53)) {
          if (table_29.TryChangeTupleFromAbsentToPresent(var_51, var_52)) {
            vec_78.Add(var_51);
          }
        }
        if (std::make_tuple(var_6) == std::make_tuple(var_53)) {
          if (table_11.TryChangeTupleFromAbsentToPresent(var_51, var_52)) {
            vec_61.Add(var_51);
          }
        }
      }
      for (auto [var_82] : vec_80) {
        printf("# var_82=%" PRIu64 "\n", var_82);
        if (table_33.TryChangeTupleFromAbsentToPresent(var_82)) {
          printf("\t# var_82=%" PRIu64 "\n", var_82);
          if (find_83_(var_82)) {
          } else {
            if (table_26.TryChangeTupleFromAbsentOrUnknownToPresent(var_82)) {
              if (table_18.TryChangeTupleFromAbsentOrUnknownToPresent(var_82, var_82)) {
                if (table_7.TryChangeTupleFromAbsentOrUnknownToPresent(var_82, var_82)) {
                  vec_58.Add(var_82, var_82);
                  printf("Adding var_82=%" PRIu64 " var_82 to vec_58\n", var_82);
                }
              }
              {
                ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<66>> scan_87(storage, table_22, var_82);
                for (auto [var_88, var_89] : scan_87) {
                  if (std::make_tuple(var_88) == std::make_tuple(var_82)) {
                    if (find_70_(var_88, var_89)) {
                      vec_63.Add(var_88, var_89);
                    }
                  }
                }
              }
            }
          }
          vec_78.Add(var_82);
          vec_92.Add(var_82);
        }
      }
      vec_49.Clear();
      vec_80.Clear();
      if (flow_262_(std::move(vec_58), std::move(vec_61), std::move(vec_63), std::move(vec_78), std::move(vec_92))) {
      }
      return false;
      assert(false);
      return false;
    }

    bool find_54_(uint64_t var_55) {
      switch (table_33.GetState(var_55)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          return false;
          break;
        }
      }
      assert(false);
      return false;
    }

    bool find_70_(uint64_t var_71, uint64_t var_72) {
      switch (table_22.GetState(var_71, var_72)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_22.TryChangeTupleFromUnknownToAbsent(var_71, var_72)) {
            if (find_223_(var_71, var_72)) {
              if (table_22.TryChangeTupleFromAbsentToPresent(var_71, var_72)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_70_(var_71, var_72)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_74_(uint64_t var_75) {
      switch (table_26.GetState(var_75)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_26.TryChangeTupleFromUnknownToAbsent(var_75)) {
            if (find_218_(var_75)) {
              if (table_26.TryChangeTupleFromAbsentToPresent(var_75)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_74_(var_75)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_83_(uint64_t var_84) {
      switch (table_44.GetState(var_84)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          return false;
          break;
        }
      }
      assert(false);
      return false;
    }

    bool find_118_(uint64_t var_119, uint64_t var_120) {
      switch (table_7.GetState(var_119, var_120)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_7.TryChangeTupleFromUnknownToAbsent(var_119, var_120)) {
            if (find_167_(var_119, var_120)) {
              if (table_7.TryChangeTupleFromAbsentToPresent(var_119, var_120)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_118_(var_119, var_120)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_128_(uint64_t var_129, uint64_t var_130) {
      if (find_70_(var_129, var_130)) {
        if (find_74_(var_129)) {
          return false;
        } else {
          return true;
        }
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_167_(uint64_t var_168, uint64_t var_169) {
      if (find_172_(var_168, var_169)) {
        return true;
      }
      if (find_176_(var_168, var_169)) {
        return true;
      }
      return false;
      assert(false);
      return false;
    }

    bool find_172_(uint64_t var_173, uint64_t var_174) {
      switch (table_18.GetState(var_173, var_174)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_18.TryChangeTupleFromUnknownToAbsent(var_173, var_174)) {
            if (find_186_(var_173, var_174)) {
              if (table_18.TryChangeTupleFromAbsentToPresent(var_173, var_174)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_172_(var_173, var_174)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_176_(uint64_t var_177, uint64_t var_178) {
      if (find_128_(var_178, var_177)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_186_(uint64_t var_187, uint64_t var_188) {
      // Ensuring downward equality of projection
      if (std::make_tuple(var_188) == std::make_tuple(var_187)) {
        if (find_196_(var_187)) {
          return true;
        } else {
          return false;
        }
      }
      return false;
      assert(false);
      return false;
    }

    bool find_196_(uint64_t var_197) {
      switch (table_26.GetState(var_197)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_26.TryChangeTupleFromUnknownToAbsent(var_197)) {
            if (find_199_(var_197)) {
              if (table_26.TryChangeTupleFromAbsentToPresent(var_197)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_196_(var_197)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_199_(uint64_t var_200) {
      if (find_207_(var_200)) {
        return true;
      }
      if (find_210_(var_200)) {
        return true;
      }
      return false;
      assert(false);
      return false;
    }

    bool find_207_(uint64_t var_208) {
      switch (table_15.GetState(var_208)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          return false;
          break;
        }
      }
      assert(false);
      return false;
    }

    bool find_210_(uint64_t var_211) {
      if (find_213_(var_211)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_213_(uint64_t var_214) {
      if (find_54_(var_214)) {
        if (find_83_(var_214)) {
          return false;
        } else {
          return true;
        }
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_218_(uint64_t var_219) {
      if (find_199_(var_219)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_223_(uint64_t var_224, uint64_t var_225) {
      if (find_228_(var_225, var_224)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_228_(uint64_t var_229, uint64_t var_230) {
      {
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<232>> scan_233(storage, table_39, var_229, var_230);
        for (auto [var_234, var_235, var_236] : scan_233) {
          if (std::make_tuple(var_229, var_230) == std::make_tuple(var_235, var_236)) {
            if (find_237_(var_234, var_235, var_236)) {
              return true;
            }
          }
        }
      }
      return false;
      assert(false);
      return false;
    }

    bool find_237_(uint64_t var_238, uint64_t var_239, uint64_t var_240) {
      switch (table_39.GetState(var_238, var_239, var_240)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_39.TryChangeTupleFromUnknownToAbsent(var_238, var_239, var_240)) {
            if (find_242_(var_238, var_239, var_240)) {
              if (table_39.TryChangeTupleFromAbsentToPresent(var_238, var_239, var_240)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_237_(var_238, var_239, var_240)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_242_(uint64_t var_243, uint64_t var_244, uint64_t var_245) {
      if (find_248_(var_243, var_245)) {
        if (find_252_(var_244, var_243)) {
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_248_(uint64_t var_249, uint64_t var_250) {
      switch (table_11.GetState(var_249, var_250)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          return false;
          break;
        }
      }
      assert(false);
      return false;
    }

    bool find_252_(uint64_t var_253, uint64_t var_254) {
      switch (table_7.GetState(var_253, var_254)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_7.TryChangeTupleFromUnknownToAbsent(var_253, var_254)) {
            if (find_256_(var_253, var_254)) {
              if (table_7.TryChangeTupleFromAbsentToPresent(var_253, var_254)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_118_(var_253, var_254)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_256_(uint64_t var_257, uint64_t var_258) {
      if (find_167_(var_257, var_258)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool flow_262_(::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_58, ::hyde::rt::Vector<StorageT, uint64_t> vec_61, ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_63, ::hyde::rt::Vector<StorageT, uint64_t> vec_78, ::hyde::rt::Vector<StorageT, uint64_t> vec_92) {
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_59(storage, 59u);
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_60(storage, 60u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_62(storage, 62u);
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_64(storage, 64u);
      var_48 += 1;
      vec_78.SortAndUnique();
      for (auto [var_94] : vec_78) {
        printf("var_94 = %" PRIu64 "\n", var_94);
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<35>> scan_93_0(storage, table_33, var_94);
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<95>> scan_93_1(storage, table_29, var_94);
        for (auto [var_97] : scan_93_0) {
          for (auto [var_96, var_98] : scan_93_1) {
            if (std::make_tuple(var_94, var_94) == std::make_tuple(var_96, var_97)) {
              if (table_36.TryChangeTupleFromAbsentToPresent(var_98)) {
                vec_92.Add(var_98);
              }
            }
          }
        }
      }
      vec_78.Clear();
      vec_92.SortAndUnique();
      printf("? vec_92 = %lu\n", vec_92.Size());
      for (auto [var_100] : vec_92) {
        printf("var_100=%" PRIu64 "\n", var_100);
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<38>> scan_99_0(storage, table_36, var_100);
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<35>> scan_99_1(storage, table_33, var_100);
        for (auto [var_102] : scan_99_0) {
          printf("var_102=%" PRIu64 "\n", var_102);
          for (auto [var_101] : scan_99_1) {
            printf("var_101=%" PRIu64 "\n", var_101);
            if (std::make_tuple(var_100, var_100) == std::make_tuple(var_101, var_102)) {
              if (table_15.TryChangeTupleFromAbsentToPresent(var_102)) {
                if (table_26.TryChangeTupleFromAbsentOrUnknownToPresent(var_102)) {
                  if (table_18.TryChangeTupleFromAbsentOrUnknownToPresent(var_102, var_102)) {
                    if (table_7.TryChangeTupleFromAbsentOrUnknownToPresent(var_102, var_102)) {
                      vec_58.Add(var_102, var_102);

                      printf("Adding var_102=%" PRIu64 " var_102 to vec_58\n", var_102);
                    }
                  }
                  {
                    ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<66>> scan_104(storage, table_22, var_102);
                    for (auto [var_105, var_106] : scan_104) {
                      if (std::make_tuple(var_105) == std::make_tuple(var_102)) {
                        if (find_70_(var_105, var_106)) {
                          vec_63.Add(var_105, var_106);
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      vec_92.Clear();
      // set 0 depth 1
      // set 0 depth 1
      for (auto changed_57 = true; changed_57; changed_57 = !!(vec_58.Size() | vec_61.Size() | vec_63.Size())) {
        DumpStats();
        if constexpr (true) {
          fprintf(stderr, "vec_58 = %" PRIu64 " vec_61 = %" PRIu64 " vec_63 = %" PRIu64 "\n", vec_58.Size(), vec_61.Size(), vec_63.Size());
        }

        vec_59.Clear();
        vec_58.SortAndUnique();
        vec_58.Swap(vec_59);
        for (auto [var_110, var_111] : vec_59) {
          switch (table_7.GetState(var_110, var_111)) {
            case ::hyde::rt::TupleState::kAbsent: break;
            case ::hyde::rt::TupleState::kPresent: {
              vec_60.Add(var_110, var_111);
              vec_61.Add(var_111);
              break;
            }
            case ::hyde::rt::TupleState::kUnknown: {
              vec_60.Add(var_110, var_111);
              {
                ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<140>> scan_141(storage, table_11, var_111);
                for (auto [var_142, var_143] : scan_141) {
                  if (std::make_tuple(var_142) == std::make_tuple(var_111)) {
                    if (table_39.TryChangeTupleFromPresentToUnknown(var_142, var_110, var_143)) {
                      if (table_22.TryChangeTupleFromPresentToUnknown(var_143, var_110)) {
                        vec_63.Add(var_143, var_110);
                      }
                    }
                  }
                }
              }
              break;
            }
          }
        }
        vec_62.Clear();
        vec_61.SortAndUnique();
        vec_61.Swap(vec_62);
        for (auto [var_147] : vec_62) {
          ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<140>> scan_146_0(storage, table_11, var_147);
          ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<148>> scan_146_1(storage, table_7, var_147);
          for (auto [var_150, var_152] : scan_146_0) {
            for (auto [var_151, var_149] : scan_146_1) {
              if (std::make_tuple(var_147, var_147) == std::make_tuple(var_149, var_150)) {
                switch (table_7.GetState(var_151, var_150)) {
                  case ::hyde::rt::TupleState::kAbsent: break;
                  case ::hyde::rt::TupleState::kPresent: {
                    if (table_39.TryChangeTupleFromAbsentOrUnknownToPresent(var_150, var_151, var_152)) {
                      if (table_22.TryChangeTupleFromAbsentOrUnknownToPresent(var_152, var_151)) {
                        if (find_74_(var_152)) {
                        } else {
                          vec_63.Add(var_152, var_151);
                        }
                      }
                    }
                    break;
                  }
                  case ::hyde::rt::TupleState::kUnknown: break;
                }
              }
            }
          }
        }
        vec_62.Clear();
        vec_64.Clear();
        vec_63.SortAndUnique();
        vec_63.Swap(vec_64);
        for (auto [var_126, var_127] : vec_64) {
          if (find_128_(var_126, var_127)) {
            if (table_7.TryChangeTupleFromAbsentOrUnknownToPresent(var_127, var_126)) {
              vec_58.Add(var_127, var_126);
            }
          } else {
            if (table_7.TryChangeTupleFromPresentToUnknown(var_127, var_126)) {
              vec_58.Add(var_127, var_126);
            }
          }
        }
      }
      // set 0 depth 1
      vec_58.Clear();
      vec_59.Clear();
      vec_60.SortAndUnique();
      for (auto [var_116, var_117] : vec_60) {
        if (find_118_(var_116, var_117)) {
        }
      }
      vec_61.Clear();
      vec_62.Clear();
      vec_63.Clear();
      vec_64.Clear();
      vec_60.Clear();
      return true;
      assert(false);
      return false;
    }

};

}  // namespace database

#endif


template <typename DB>
void dump(DB &db) {
  std::cout << "Dump:\n";

  for (auto func_ea = 0; func_ea < 50; func_ea++) {
    db.function_instructions_bf(func_ea, [] (uint64_t func_ea_, uint64_t inst_ea) {
      std::cout << "  FuncEA=" << func_ea_ << " InstEA=" << inst_ea << "\n";
      return true;
    });
  }

  std::cout << "\n";
}

template <typename DB>
size_t NumFunctionInstructions(DB &db, uint64_t func_ea) {
  std::vector<uint64_t> eas;
  db.function_instructions_bf(func_ea, [&eas] (uint64_t, uint64_t inst_ea) {
    eas.push_back(inst_ea);
    return true;
  });
  std::sort(eas.begin(), eas.end());
  auto it = std::unique(eas.begin(), eas.end());
  eas.erase(it, eas.end());
  return eas.size();
}

using DatabaseStorage = hyde::rt::StdStorage;
using DatabaseFunctors = database::DatabaseFunctors;
using DatabaseLog = database::DatabaseLog;
using Database = database::Database<DatabaseStorage, DatabaseLog, DatabaseFunctors>;

template <typename... Args>
using Vector = hyde::rt::Vector<DatabaseStorage, Args...>;

// A simple Google Test example
TEST(MiniDisassembler, DifferentialUpdatesWork) {

  DatabaseFunctors functors;
  DatabaseLog log;
  DatabaseStorage storage;
  Database db(storage, log, functors);

  constexpr uint8_t FALL_THROUGH = 0;
  constexpr uint8_t CALL = 1;

  // Start with a few instructions, with no control-flow between them.
  Vector<uint64_t> instructions(storage, 0);
  instructions.Add(10);
  instructions.Add(11);
  instructions.Add(12);
  instructions.Add(13);
  instructions.Add(14);
  instructions.Add(15);
  db.instruction_1(std::move(instructions));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 1u);

  // Now we add the fall-through edges, and 10 is the only instruction with
  // no predecessor, so its the function head.
  Vector<uint64_t, uint64_t, uint8_t> transfers(storage, 0);

  transfers.Add(10, 11, FALL_THROUGH);
  transfers.Add(11, 12, FALL_THROUGH);
  transfers.Add(12, 13, FALL_THROUGH);
  transfers.Add(13, 14, FALL_THROUGH);
  transfers.Add(14, 15, FALL_THROUGH);
  db.raw_transfer_3(std::move(transfers));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 6u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);

  // Now add the instruction 9. It will show up as a function head, because
  // it has no predecessors. The rest will stay the same because there is
  // no changes to control-flow.
  Vector<uint64_t> instructions2(storage, 0);
  instructions2.Add(9);
  db.instruction_1(std::move(instructions2));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 6u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);

  // Now add a fall-through between 9 and 10. 10 now has a successor, so it's
  // not a function head anymore, so all of the function instructions transfer
  // over to function 9.
  Vector<uint64_t, uint64_t, uint8_t> transfers2(storage, 0);
  transfers2.Add(9, 10, FALL_THROUGH);
  db.raw_transfer_3(std::move(transfers2));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 7u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);

  // Now add a function call between 10 and 14. That makes 14 look like
  // a function head, and so now that 14 is a function head, it's no longer
  // part of function 9.
  Vector<uint64_t, uint64_t, uint8_t> transfers3(storage, 0);
  transfers3.Add(10, 14, CALL);
  db.raw_transfer_3(std::move(transfers3));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 5u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 2u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);
}

