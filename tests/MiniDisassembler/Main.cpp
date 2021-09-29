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
struct IndexDescriptor<153> {
  static constexpr unsigned kId = 153;
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
struct IndexDescriptor<169> {
  static constexpr unsigned kId = 169;
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
  using IndexIds = IdList<10, 153, 169>;
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
struct IndexDescriptor<145> {
  static constexpr unsigned kId = 145;
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
  using IndexIds = IdList<14, 145>;
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
struct IndexDescriptor<71> {
  static constexpr unsigned kId = 71;
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
  using IndexIds = IdList<25, 71>;
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
struct ColumnDescriptor<32> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 32;
  static constexpr unsigned kTableId = 29;
  static constexpr unsigned kOffset = 2;
  using Type = uint8_t;
};
template <>
struct IndexDescriptor<33> {
  static constexpr unsigned kId = 33;
  static constexpr unsigned kTableId = 29;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 3;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<30>, KeyColumn<31>, KeyColumn<32>>;
  using KeyColumnIds = IdList<30, 31, 32>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1, 2>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<29> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<30, 31, 32>;
  using IndexIds = IdList<33>;
  static constexpr unsigned kFirstIndexId = 33;
  static constexpr unsigned kNumColumns = 3;
};

template <>
struct ColumnDescriptor<35> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 35;
  static constexpr unsigned kTableId = 34;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<36> {
  static constexpr unsigned kId = 36;
  static constexpr unsigned kTableId = 34;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<35>>;
  using KeyColumnIds = IdList<35>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<34> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<35>;
  using IndexIds = IdList<36>;
  static constexpr unsigned kFirstIndexId = 36;
  static constexpr unsigned kNumColumns = 1;
};

template <>
struct ColumnDescriptor<38> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 38;
  static constexpr unsigned kTableId = 37;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<39> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 39;
  static constexpr unsigned kTableId = 37;
  static constexpr unsigned kOffset = 1;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<40> {
  static constexpr unsigned kId = 40;
  static constexpr unsigned kTableId = 37;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 2;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<38>, KeyColumn<39>>;
  using KeyColumnIds = IdList<38, 39>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct IndexDescriptor<100> {
  static constexpr unsigned kId = 100;
  static constexpr unsigned kTableId = 37;
  static constexpr unsigned kOffset = 1;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 1;
  static constexpr bool kCoversAllColumns = false;
  using Columns = TypeList<KeyColumn<38>, ValueColumn<39>>;
  using KeyColumnIds = IdList<38>;
  using ValueColumnIds = IdList<39>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<1>;
};
template <>
struct TableDescriptor<37> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<38, 39>;
  using IndexIds = IdList<40, 100>;
  static constexpr unsigned kFirstIndexId = 40;
  static constexpr unsigned kNumColumns = 2;
};

template <>
struct ColumnDescriptor<42> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 42;
  static constexpr unsigned kTableId = 41;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<43> {
  static constexpr unsigned kId = 43;
  static constexpr unsigned kTableId = 41;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<42>>;
  using KeyColumnIds = IdList<42>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<41> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<42>;
  using IndexIds = IdList<43>;
  static constexpr unsigned kFirstIndexId = 43;
  static constexpr unsigned kNumColumns = 1;
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
struct ColumnDescriptor<46> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 46;
  static constexpr unsigned kTableId = 44;
  static constexpr unsigned kOffset = 1;
  using Type = uint64_t;
};
template <>
struct ColumnDescriptor<47> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 47;
  static constexpr unsigned kTableId = 44;
  static constexpr unsigned kOffset = 2;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<48> {
  static constexpr unsigned kId = 48;
  static constexpr unsigned kTableId = 44;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 3;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<45>, KeyColumn<46>, KeyColumn<47>>;
  using KeyColumnIds = IdList<45, 46, 47>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0, 1, 2>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct IndexDescriptor<237> {
  static constexpr unsigned kId = 237;
  static constexpr unsigned kTableId = 44;
  static constexpr unsigned kOffset = 1;
  static constexpr unsigned kNumKeyColumns = 2;
  static constexpr unsigned kNumValueColumns = 1;
  static constexpr bool kCoversAllColumns = false;
  using Columns = TypeList<ValueColumn<45>, KeyColumn<46>, KeyColumn<47>>;
  using KeyColumnIds = IdList<46, 47>;
  using ValueColumnIds = IdList<45>;
  using KeyColumnOffsets = IdList<1, 2>;
  using ValueColumnOffsets = IdList<0>;
};
template <>
struct TableDescriptor<44> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<45, 46, 47>;
  using IndexIds = IdList<48, 237>;
  static constexpr unsigned kFirstIndexId = 48;
  static constexpr unsigned kNumColumns = 3;
};

template <>
struct ColumnDescriptor<50> {
  static constexpr bool kIsNamed = false;
  static constexpr unsigned kId = 50;
  static constexpr unsigned kTableId = 49;
  static constexpr unsigned kOffset = 0;
  using Type = uint64_t;
};
template <>
struct IndexDescriptor<51> {
  static constexpr unsigned kId = 51;
  static constexpr unsigned kTableId = 49;
  static constexpr unsigned kOffset = 0;
  static constexpr unsigned kNumKeyColumns = 1;
  static constexpr unsigned kNumValueColumns = 0;
  static constexpr bool kCoversAllColumns = true;
  using Columns = TypeList<KeyColumn<50>>;
  using KeyColumnIds = IdList<50>;
  using ValueColumnIds = IdList<>;
  using KeyColumnOffsets = IdList<0>;
  using ValueColumnOffsets = IdList<>;
};
template <>
struct TableDescriptor<49> {
  static constexpr bool kHasCoveringIndex = true;
  using ColumnIds = IdList<50>;
  using IndexIds = IdList<51>;
  static constexpr unsigned kFirstIndexId = 51;
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
    ::hyde::rt::Table<StorageT, 34> table_34;
    ::hyde::rt::Table<StorageT, 37> table_37;
    ::hyde::rt::Table<StorageT, 41> table_41;
    ::hyde::rt::Table<StorageT, 44> table_44;
    ::hyde::rt::Table<StorageT, 49> table_49;
    uint64_t var_53;

    static constexpr uint8_t var_5 = {0};
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
        table_34(s),
        table_37(s),
        table_41(s),
        table_44(s),
        table_49(s),
        var_53{0} {
      init_4_();
    }

    template <typename _Generator>
    ::hyde::rt::index_t function_instructions_bf(uint64_t param_0, _Generator _generator) {
      ::hyde::rt::index_t num_generated = 0;
      ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<169>> scan(storage, table_7, param_0);
      for (auto [shadow_param_0, param_1] : scan) {
        if (std::make_tuple(param_0) != std::make_tuple(shadow_param_0)) {
          continue;
        }
        if (!find_123_(param_0, param_1)) {
          continue;
        }
        num_generated += 1u;
        if (!_generator(param_0, param_1)) {
          return num_generated;
        }
      }
      return num_generated;
    }

    bool raw_transfer_3(::hyde::rt::Vector<StorageT, uint64_t, uint64_t, uint8_t> vec_159) {
      ::hyde::rt::Vector<StorageT, uint64_t> vec_161(storage, 161u);
      if (proc_52_(std::move(vec_159), std::move(vec_161))) {
      }
      return true;
      assert(false);
      return false;
    }

    bool instruction_1(::hyde::rt::Vector<StorageT, uint64_t> vec_163) {
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t, uint8_t> vec_165(storage, 165u);
      if (proc_52_(std::move(vec_165), std::move(vec_163))) {
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
      _print(34, table_34.Size());
      _print(37, table_37.Size());
      _print(41, table_41.Size());
      _print(44, table_44.Size());
      _print(49, table_49.Size());
    }

    void DumpStats(void) const {
      if constexpr (false) {
        return;  /* change to false to enable */
      }
      static FILE *tables = nullptr;
      if (!tables) {
        tables = fopen("/tmp/tables.csv", "w");
        fprintf(tables, "table 7,table 11,table 15,table 18,table 22,table 26,table 29,table 34,table 37,table 41,table 44,table 49\n");
      }
      fprintf(tables, "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n", table_7.Size(), table_11.Size(), table_15.Size(), table_18.Size(), table_22.Size(), table_26.Size(), table_29.Size(), table_34.Size(), table_37.Size(), table_41.Size(), table_44.Size(), table_49.Size());
    }

    bool init_4_() {
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t, uint8_t> vec_167(storage, 167u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_168(storage, 168u);
      if (proc_52_(std::move(vec_167), std::move(vec_168))) {
      }
      return false;
      assert(false);
      return false;
    }

    bool proc_52_(::hyde::rt::Vector<StorageT, uint64_t, uint64_t, uint8_t> vec_54, ::hyde::rt::Vector<StorageT, uint64_t> vec_85) {
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_63(storage, 63u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_66(storage, 66u);
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_68(storage, 68u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_83(storage, 83u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_97(storage, 97u);
      for (auto [var_56, var_57, var_58] : vec_54) {
        if (table_29.TryChangeTupleFromAbsentToPresent(var_56, var_57, var_58)) {
          if (table_49.TryChangeTupleFromAbsentToPresent(var_57)) {
            if (find_59_(var_57)) {
              if (table_26.TryChangeTupleFromPresentToUnknown(var_57)) {
                if (table_18.TryChangeTupleFromPresentToUnknown(var_57, var_57)) {
                  if (table_7.TryChangeTupleFromPresentToUnknown(var_57, var_57)) {
                    vec_63.Add(var_57, var_57);
                  }
                }
                {
                  ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<71>> scan_72(storage, table_22, var_57);
                  for (auto [var_73, var_74] : scan_72) {
                    if (std::make_tuple(var_73) == std::make_tuple(var_57)) {
                      if (find_75_(var_73, var_74)) {
                        if (find_79_(var_73)) {
                        } else {
                          vec_68.Add(var_73, var_74);
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          if (std::make_tuple(var_5) == std::make_tuple(var_58)) {
            if (table_37.TryChangeTupleFromAbsentToPresent(var_56, var_57)) {
              vec_83.Add(var_56);
            }
          }
          if (std::make_tuple(var_6) == std::make_tuple(var_58)) {
            if (table_11.TryChangeTupleFromAbsentToPresent(var_56, var_57)) {
              vec_66.Add(var_56);
            }
          }
        }
      }
      for (auto [var_87] : vec_85) {
        if (table_34.TryChangeTupleFromAbsentToPresent(var_87)) {
          if (find_88_(var_87)) {
          } else {
            if (table_26.TryChangeTupleFromAbsentOrUnknownToPresent(var_87)) {
              if (table_18.TryChangeTupleFromAbsentOrUnknownToPresent(var_87, var_87)) {
                if (table_7.TryChangeTupleFromAbsentOrUnknownToPresent(var_87, var_87)) {
                  vec_63.Add(var_87, var_87);
                }
              }
              {
                ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<71>> scan_92(storage, table_22, var_87);
                for (auto [var_93, var_94] : scan_92) {
                  if (std::make_tuple(var_93) == std::make_tuple(var_87)) {
                    if (find_75_(var_93, var_94)) {
                      vec_68.Add(var_93, var_94);
                    }
                  }
                }
              }
            }
          }
          vec_83.Add(var_87);
          vec_97.Add(var_87);
        }
      }
      vec_54.Clear();
      vec_85.Clear();
      std::cerr << "induction_in:63:\n";
      for (auto [a, b] : vec_63) {
        std::cerr << "\t" << a << " " << b << "\n";
      }
      std::cerr << "\ninduction_pivots:66:\n";
      for (auto [a] : vec_66) {
        std::cerr << "\t" << a << "\n";
      }
      std::cerr << "\ninduction_in:68:\n";
      for (auto [a, b] : vec_68) {
        std::cerr << "\t" << a << " " << b <<"\n";
      }
      std::cerr << "\npivots:83:\n";
      for (auto [a] : vec_83) {
        std::cerr << "\t" << a << "\n";
      }
      std::cerr << "\npivots:97:\n";
      for (auto [a] : vec_97) {
        std::cerr << "\t" << a << "\n";
      }
      if (flow_267_(std::move(vec_63), std::move(vec_66), std::move(vec_68), std::move(vec_83), std::move(vec_97))) {
      }
      return false;
      assert(false);
      return false;
    }

    bool find_59_(uint64_t var_60) {
      switch (table_34.GetState(var_60)) {
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

    bool find_75_(uint64_t var_76, uint64_t var_77) {
      switch (table_22.GetState(var_76, var_77)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_22.TryChangeTupleFromUnknownToAbsent(var_76, var_77)) {
            if (find_228_(var_76, var_77)) {
              if (table_22.TryChangeTupleFromAbsentToPresent(var_76, var_77)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_75_(var_76, var_77)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_79_(uint64_t var_80) {
      switch (table_26.GetState(var_80)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_26.TryChangeTupleFromUnknownToAbsent(var_80)) {
            if (find_223_(var_80)) {
              if (table_26.TryChangeTupleFromAbsentToPresent(var_80)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_79_(var_80)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_88_(uint64_t var_89) {
      switch (table_49.GetState(var_89)) {
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

    bool find_123_(uint64_t var_124, uint64_t var_125) {
      switch (table_7.GetState(var_124, var_125)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_7.TryChangeTupleFromUnknownToAbsent(var_124, var_125)) {
            if (find_172_(var_124, var_125)) {
              if (table_7.TryChangeTupleFromAbsentToPresent(var_124, var_125)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_123_(var_124, var_125)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_133_(uint64_t var_134, uint64_t var_135) {
      if (find_75_(var_134, var_135)) {
        if (find_79_(var_134)) {
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

    bool find_172_(uint64_t var_173, uint64_t var_174) {
      if (find_177_(var_173, var_174)) {
        return true;
      }
      if (find_181_(var_173, var_174)) {
        return true;
      }
      return false;
      assert(false);
      return false;
    }

    bool find_177_(uint64_t var_178, uint64_t var_179) {
      switch (table_18.GetState(var_178, var_179)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_18.TryChangeTupleFromUnknownToAbsent(var_178, var_179)) {
            if (find_191_(var_178, var_179)) {
              if (table_18.TryChangeTupleFromAbsentToPresent(var_178, var_179)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_177_(var_178, var_179)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_181_(uint64_t var_182, uint64_t var_183) {
      if (find_133_(var_183, var_182)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_191_(uint64_t var_192, uint64_t var_193) {
      // Ensuring downward equality of projection
      if (std::make_tuple(var_193) == std::make_tuple(var_192)) {
        if (find_201_(var_192)) {
          return true;
        } else {
          return false;
        }
      }
      return false;
      assert(false);
      return false;
    }

    bool find_201_(uint64_t var_202) {
      switch (table_26.GetState(var_202)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_26.TryChangeTupleFromUnknownToAbsent(var_202)) {
            if (find_204_(var_202)) {
              if (table_26.TryChangeTupleFromAbsentToPresent(var_202)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_201_(var_202)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_204_(uint64_t var_205) {
      if (find_212_(var_205)) {
        return true;
      }
      if (find_215_(var_205)) {
        return true;
      }
      return false;
      assert(false);
      return false;
    }

    bool find_212_(uint64_t var_213) {
      switch (table_15.GetState(var_213)) {
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

    bool find_215_(uint64_t var_216) {
      if (find_218_(var_216)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_218_(uint64_t var_219) {
      if (find_59_(var_219)) {
        if (find_88_(var_219)) {
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

    bool find_223_(uint64_t var_224) {
      if (find_204_(var_224)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_228_(uint64_t var_229, uint64_t var_230) {
      if (find_233_(var_230, var_229)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool find_233_(uint64_t var_234, uint64_t var_235) {
      {
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<237>> scan_238(storage, table_44, var_234, var_235);
        for (auto [var_239, var_240, var_241] : scan_238) {
          if (std::make_tuple(var_234, var_235) == std::make_tuple(var_240, var_241)) {
            if (find_242_(var_239, var_240, var_241)) {
              return true;
            }
          }
        }
      }
      return false;
      assert(false);
      return false;
    }

    bool find_242_(uint64_t var_243, uint64_t var_244, uint64_t var_245) {
      switch (table_44.GetState(var_243, var_244, var_245)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_44.TryChangeTupleFromUnknownToAbsent(var_243, var_244, var_245)) {
            if (find_247_(var_243, var_244, var_245)) {
              if (table_44.TryChangeTupleFromAbsentToPresent(var_243, var_244, var_245)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_242_(var_243, var_244, var_245)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_247_(uint64_t var_248, uint64_t var_249, uint64_t var_250) {
      if (find_253_(var_248, var_250)) {
        if (find_257_(var_249, var_248)) {
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

    bool find_253_(uint64_t var_254, uint64_t var_255) {
      switch (table_11.GetState(var_254, var_255)) {
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

    bool find_257_(uint64_t var_258, uint64_t var_259) {
      switch (table_7.GetState(var_258, var_259)) {
        case ::hyde::rt::TupleState::kAbsent: {
          return false;
          break;
        }
        case ::hyde::rt::TupleState::kPresent: {
          return true;
          break;
        }
        case ::hyde::rt::TupleState::kUnknown: {
          if (table_7.TryChangeTupleFromUnknownToAbsent(var_258, var_259)) {
            if (find_261_(var_258, var_259)) {
              if (table_7.TryChangeTupleFromAbsentToPresent(var_258, var_259)) {
                return true;
              }
            } else {
              return false;
            }
          }
          break;
        }
      }
      if (find_123_(var_258, var_259)) {
        return true;
      } else {
        return true;
      }
      assert(false);
      return false;
    }

    bool find_261_(uint64_t var_262, uint64_t var_263) {
      if (find_172_(var_262, var_263)) {
        return true;
      } else {
        return false;
      }
      assert(false);
      return false;
    }

    bool flow_267_(::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_63, ::hyde::rt::Vector<StorageT, uint64_t> vec_66, ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_68, ::hyde::rt::Vector<StorageT, uint64_t> vec_83, ::hyde::rt::Vector<StorageT, uint64_t> vec_97) {
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_64(storage, 64u);
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_65(storage, 65u);
      ::hyde::rt::Vector<StorageT, uint64_t> vec_67(storage, 67u);
      ::hyde::rt::Vector<StorageT, uint64_t, uint64_t> vec_69(storage, 69u);
      var_53 += 1;
      vec_83.SortAndUnique();
      for (auto [var_99] : vec_83) {
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<36>> scan_98_0(storage, table_34, var_99);
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<100>> scan_98_1(storage, table_37, var_99);
        for (auto [var_102] : scan_98_0) {
          for (auto [var_101, var_103] : scan_98_1) {
            if (std::make_tuple(var_99, var_99) == std::make_tuple(var_101, var_102)) {
              if (table_41.TryChangeTupleFromAbsentToPresent(var_103)) {
                vec_97.Add(var_103);
              }
            }
          }
        }
      }
      vec_83.Clear();
      vec_97.SortAndUnique();
      for (auto [var_105] : vec_97) {
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<43>> scan_104_0(storage, table_41, var_105);
        ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<36>> scan_104_1(storage, table_34, var_105);
        for (auto [var_107] : scan_104_0) {
          for (auto [var_106] : scan_104_1) {
            if (std::make_tuple(var_105, var_105) == std::make_tuple(var_106, var_107)) {
              if (table_15.TryChangeTupleFromAbsentToPresent(var_107)) {
                if (table_26.TryChangeTupleFromAbsentOrUnknownToPresent(var_107)) {
                  if (table_18.TryChangeTupleFromAbsentOrUnknownToPresent(var_107, var_107)) {
                    if (table_7.TryChangeTupleFromAbsentOrUnknownToPresent(var_107, var_107)) {
                      vec_63.Add(var_107, var_107);
                    }
                  }
                  {
                    ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<71>> scan_109(storage, table_22, var_107);
                    for (auto [var_110, var_111] : scan_109) {
                      if (std::make_tuple(var_110) == std::make_tuple(var_107)) {
                        if (find_75_(var_110, var_111)) {
                          vec_68.Add(var_110, var_111);
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
      vec_97.Clear();
      // set 0 depth 1
      // set 0 depth 1
      for (auto changed_62 = true; changed_62; changed_62 = !!(vec_63.Size() | vec_66.Size() | vec_68.Size())) {
        DumpStats();
        if constexpr (true) {
          fprintf(stderr, "vec_63 = %" PRIu64 " vec_66 = %" PRIu64 " vec_68 = %" PRIu64 "\n", vec_63.Size(), vec_66.Size(), vec_68.Size());
        }

        vec_64.Clear();
        vec_63.SortAndUnique();
        vec_63.Swap(vec_64);
        for (auto [var_115, var_116] : vec_64) {
          switch (table_7.GetState(var_115, var_116)) {
            case ::hyde::rt::TupleState::kAbsent: break;
            case ::hyde::rt::TupleState::kPresent: {
              vec_65.Add(var_115, var_116);
              vec_66.Add(var_116);
              break;
            }
            case ::hyde::rt::TupleState::kUnknown: {
              vec_65.Add(var_115, var_116);
              {
                ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<145>> scan_146(storage, table_11, var_116);
                for (auto [var_147, var_148] : scan_146) {
                  if (std::make_tuple(var_147) == std::make_tuple(var_116)) {
                    if (table_44.TryChangeTupleFromPresentToUnknown(var_147, var_115, var_148)) {
                      if (table_22.TryChangeTupleFromPresentToUnknown(var_148, var_115)) {
                        vec_68.Add(var_148, var_115);
                      }
                    }
                  }
                }
              }
              break;
            }
          }
        }
        vec_67.Clear();
        vec_66.SortAndUnique();
        vec_66.Swap(vec_67);
        for (auto [var_152] : vec_67) {
          ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<145>> scan_151_0(storage, table_11, var_152);
          ::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<153>> scan_151_1(storage, table_7, var_152);
          for (auto [var_155, var_157] : scan_151_0) {
            for (auto [var_156, var_154] : scan_151_1) {
              if (std::make_tuple(var_152, var_152) == std::make_tuple(var_154, var_155)) {
                switch (table_7.GetState(var_156, var_155)) {
                  case ::hyde::rt::TupleState::kAbsent: break;
                  case ::hyde::rt::TupleState::kPresent: {
                    if (table_44.TryChangeTupleFromAbsentOrUnknownToPresent(var_155, var_156, var_157)) {
                      if (table_22.TryChangeTupleFromAbsentOrUnknownToPresent(var_157, var_156)) {
                        if (find_79_(var_157)) {
                        } else {
                          vec_68.Add(var_157, var_156);
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
        vec_67.Clear();
        vec_69.Clear();
        vec_68.SortAndUnique();
        vec_68.Swap(vec_69);
        for (auto [var_131, var_132] : vec_69) {
          if (find_133_(var_131, var_132)) {
            if (table_7.TryChangeTupleFromAbsentOrUnknownToPresent(var_132, var_131)) {
              vec_63.Add(var_132, var_131);
            }
          } else {
            if (table_7.TryChangeTupleFromPresentToUnknown(var_132, var_131)) {
              vec_63.Add(var_132, var_131);
            }
          }
        }
      }
      // set 0 depth 1
      vec_63.Clear();
      vec_64.Clear();
      vec_65.SortAndUnique();
      for (auto [var_121, var_122] : vec_65) {
        if (find_123_(var_121, var_122)) {
        }
      }
      vec_66.Clear();
      vec_67.Clear();
      vec_68.Clear();
      vec_69.Clear();
      vec_65.Clear();
      return true;
      assert(false);
      return false;
    }

};

}  // namespace database

#ifndef __DRLOJEKYLL_EPILOGUE_CODE_Database
#  define __DRLOJEKYLL_EPILOGUE_CODE_Database
#endif  // __DRLOJEKYLL_EPILOGUE_CODE_Database
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

