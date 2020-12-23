// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace drlojekyll {

struct Token {
  const char *display_name;
  unsigned line;
  unsigned column;
  const char *token;
};

template <typename...>
struct Columns;

template <typename, typename>
struct KeyValues;

template <typename...>
struct Indices;

class Table;
class Index;

extern "C" {

extern bool __hyde_init(int argc, char *argv[], unsigned num_tables,
                        unsigned num_indices, unsigned num_rx_messages,
                        unsigned num_tx_messages);

extern int __hyde_run(bool (*init_proc)(void));

extern Table *__hyde_create_table(unsigned id, unsigned num_indices,
                                  unsigned num_columns, unsigned tuple_size);

}  // extern "C"
namespace {

// Initialize the runtime, telling it about the command-line arguments
// (`argc` and `argv`), the number of tables and indices, and the number
// of received and transmitted message types.
inline static void Init(int argc, char *argv[], unsigned num_tables,
                        unsigned num_indices, unsigned num_rx_messages,
                        unsigned num_tx_messages) {
  __hyde_init(argc, argv, num_tables, num_indices, num_rx_messages,
              num_tx_messages);
}

// Run the database, passing in the initialization procedure.
inline static int Run(bool (*init_proc)(void)) {
  return __hyde_run(init_proc);
}

// Create an index on a table.
template <typename T>
inline static void CreateIndex(Table *table, unsigned slot) {
  T::gStorage = __hyde_create_index(table, T::gId, T::gSlot);
}

// Unroll the indices specification into calls that create the indices.
template <typename... Rest>
struct IndexMaker;

template <typename... Rest>
struct IndexMaker<Indices<Rest...>> : public IndexMaker<Rest...> {};

template <typename T, typename... Rest>
struct IndexMaker<T, Rest...> {
  inline static void Create(Table *table) {
    CreateIndex<T>(table);
    IndexMaker<Rest...>::Create(table);
  }
};

template <>
struct IndexMaker<> {
  inline static void Create(Table *) {}
};

// Create a table described by the type `T`.
template <typename T>
inline static void CreateTable(void) {
  const auto table = __hyde_create_table(T::gId, T::kNumIndices, T::kNumColumns,
                                         T::kTupleSize);
  IndexMaker<typename T::IndexSpec>::Create(table);
  T::gStorage = table;
}

}  // namespace
}  // namespace drlojekyll
