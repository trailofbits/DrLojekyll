// Copyright 2020, Trail of Bits. All rights reserved.

#include <algorithm>
#include <sstream>

#include "Program.h"

namespace hyde {
namespace {

void SortAndUnique(std::vector<unsigned> &col_ids) {
  std::sort(col_ids.begin(), col_ids.end());
  auto it = std::unique(col_ids.begin(), col_ids.end());
  col_ids.erase(it, col_ids.end());
}

static std::string ColumnSpec(std::vector<unsigned> &col_ids) {
  auto sep = "";
  std::stringstream ss;
  for (auto col_id : col_ids) {
    ss << sep << col_id;
    sep = ":";
  }

  return ss.str();
}

}  // namespace

Node<DataView>::~Node(void) {}
Node<DataIndex>::~Node(void) {}
Node<DataTable>::~Node(void) {}

Node<DataView>::Node(Node<DataTable> *table_, QueryView view_tag_)
    : Def<Node<DataView>>(this),
      User(this),
      view_tag(view_tag_),
      viewed_table(this, table_) {}

Node<DataIndex>::Node(Node<DataTable> *table_, std::string column_spec_)
    : Def<Node<DataIndex>>(this),
      User(this),
      column_spec(column_spec_),
      indexed_table(this, table_) {}

Node<DataTable>::Node(std::vector<unsigned> column_ids_)
    : Def<Node<DataTable>>(this),
      User(this),
      column_ids(std::move(column_ids_)),
      views(this),
      indices(this) {}

// Get or create a table in the program.
Node<DataView> *Node<DataTable>::GetOrCreate(
    ProgramImpl *program, DefinedNodeRange<QueryColumn> cols, QueryView tag) {

  std::vector<unsigned> col_ids;
  for (auto col : cols) {
    col_ids.push_back(col.Id());
  }

  SortAndUnique(col_ids);

  auto col_spec = ColumnSpec(col_ids);
  auto &table = program->col_spec_to_table[col_spec];
  if (!table) {
    table = program->tables.Create(std::move(col_ids));
  }

  return table;
}

Node<DataView> *Node<DataTable>::GetOrCreate(ProgramImpl *program,
                                             UsedNodeRange<QueryColumn> cols,
                                             QueryView tag) {

  std::vector<unsigned> col_ids;
  for (auto col : cols) {
    col_ids.push_back(col.Id());
  }

  SortAndUnique(col_ids);

  auto col_spec = ColumnSpec(col_ids);
  auto &table = program->col_spec_to_table[col_spec];
  if (!table) {
    table = program->tables.Create(std::move(col_ids));
  }

  return table;
}

// Get or create a table in a procedure.
Node<DataTable> *Node<DataTable>::Create(Node<ProgramRegion> *region,
                                         DefinedNodeRange<QueryColumn> cols) {
  std::vector<unsigned> col_ids;
  for (auto col : cols) {
    col_ids.push_back(col.Id());
  }
  SortAndUnique(col_ids);
  return region->containing_procedure->tables.Create(std::move(col_ids));
}

// Get or create a table in a procedure.
Node<DataTable> *Node<DataTable>::Create(Node<ProgramRegion> *region,
                                         UsedNodeRange<QueryColumn> cols) {
  std::vector<unsigned> col_ids;
  for (auto col : cols) {
    col_ids.push_back(col.Id());
  }
  SortAndUnique(col_ids);
  return region->containing_procedure->tables.Create(std::move(col_ids));
}

// Get or create an index on the table.
Node<DataIndex> *
Node<DataView>::GetOrCreateIndex(std::vector<QueryColumn> cols) {
  return viewed_table->GetOrCreateIndex(std::move(cols));
}

// Get or create an index on the table.
Node<DataIndex> *
Node<DataTable>::GetOrCreateIndex(std::vector<QueryColumn> cols) {

  std::vector<unsigned> col_ids;
  for (auto col : cols) {
    col_ids.push_back(col.Id());
  }

  auto col_spec = ColumnSpec(col_ids);
  for (auto index : indices) {
    if (index->column_spec == col_spec) {
      return index;
    }
  }

  return indices.Create(this, std::move(col_spec));
}

}  // namespace hyde
