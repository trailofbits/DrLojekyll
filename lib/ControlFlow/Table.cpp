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

Node<DataView>::Node(Node<DataTable> *table_, QueryView view_tag_,
                     std::string col_spec_)
    : Def<Node<DataView>>(this),
      User(this),
      view_tag(view_tag_),
      col_spec(std::move(col_spec_)),
      viewed_table(this, table_) {}

Node<DataIndex>::Node(Node<DataTable> *table_, std::string column_spec_)
    : Def<Node<DataIndex>>(this),
      User(this),
      column_spec(column_spec_),
      indexed_table(this, table_) {}

Node<DataTable>::Node(TableKind kind_)
    : Def<Node<DataTable>>(this),
      User(this),
      kind(kind_),
      views(this),
      indices(this) {}

// Get or create a table in the program.
Node<DataView> *Node<DataTable>::GetOrCreate(
    ProgramImpl *program, DefinedNodeRange<QueryColumn> cols, QueryView tag) {

  // TODO(pag): Make it so that the view has the right columns in the right
  //            order, even if the table itself deduplicates some of them.

  std::vector<unsigned> col_ids;
  DataModel *model = nullptr;
  for (auto col : cols) {
    auto view = QueryView::Containing(col);
    auto view_model = program->view_to_model[view]->FindAs<DataModel>();
    if (!model) {
      model = view_model;
    } else {
      assert(model == view_model);
    }
    col_ids.push_back(col.Id());
  }

  if (!model->table) {
    model->table = program->tables.Create(TableKind::kPersistent);
  }

  const auto table = model->table;
  auto &columns = table->columns;
  for (auto col : cols) {
    columns.push_back(col);
  }

  std::sort(columns.begin(), columns.end());
  auto it = std::unique(columns.begin(), columns.end());
  columns.erase(it, columns.end());

  SortAndUnique(col_ids);
  auto col_spec = ColumnSpec(col_ids);

  for (auto view : table->views) {
    if (view->view_tag == tag && view->col_spec == col_spec) {
      return view;
    }
  }

  return table->views.Create(table, tag, std::move(col_spec));
}

Node<DataView> *Node<DataTable>::GetOrCreate(ProgramImpl *program,
                                             UsedNodeRange<QueryColumn> cols,
                                             QueryView tag) {
  assert(!cols.empty());
  const auto first_view = QueryView::Containing(cols[0]);
#ifndef NDEBUG
  for (auto i = 1u; i < cols.size(); ++i) {
    assert(first_view == QueryView::Containing(cols[i]));
  }
#endif
  return Node<DataTable>::GetOrCreate(program, first_view.Columns(), tag);
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
