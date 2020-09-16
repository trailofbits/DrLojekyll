// Copyright 2020, Trail of Bits. All rights reserved.

#include <algorithm>
#include <sstream>

#include "Program.h"

namespace hyde {
namespace {

template <typename List>
void SortAndUnique(List &col_ids) {
  std::sort(col_ids.begin(), col_ids.end());
  auto it = std::unique(col_ids.begin(), col_ids.end());
  col_ids.erase(it, col_ids.end());
}

static std::string ColumnSpec(const std::vector<unsigned> &col_ids) {
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

Node<DataView>::Node(Node<DataTable> *table_, unsigned id_, QueryView view_tag_,
                     const std::vector<unsigned> col_ids_,
                     std::string col_spec_)
    : Def<Node<DataView>>(this),
      User(this),
      id(id_),
      view_tag(view_tag_),
      col_spec(std::move(col_spec_)),
      col_ids(std::move(col_ids_)),
      viewed_table(this, table_) {}

Node<DataIndex>::Node(Node<DataTable> *table_, std::vector<unsigned> col_ids_,
                      std::string column_spec_)
    : Def<Node<DataIndex>>(this),
      User(this),
      column_spec(column_spec_),
      col_ids(std::move(col_ids_)),
      indexed_table(this, table_) {}

// Get or create a table in the program.
Node<DataView> *Node<DataTable>::GetOrCreateImpl(
    ProgramImpl *impl, const std::vector<QueryColumn> &cols, QueryView tag) {

  // TODO(pag): Make it so that the view has the right columns in the right
  //            order, even if the table itself de-duplicates some of them.

  std::vector<unsigned> col_ids;
  DataModel *model = nullptr;
  for (auto col : cols) {
    auto view = QueryView::Containing(col);
    auto view_model = impl->view_to_model[view]->FindAs<DataModel>();
    if (!model) {
      model = view_model;
    } else {
      assert(model == view_model);
    }
    col_ids.push_back(col.Id());
  }

  // TODO(pag): Could this happen with all `cols` being constant?
  assert(model != nullptr);

  if (!model->table) {
    model->table = impl->tables.Create(impl->next_id++);
  }

  const auto table = model->table;
  auto &columns = table->columns;
  for (auto col : cols) {
    columns.push_back(col);
  }

  std::sort(columns.begin(), columns.end(), [] (QueryColumn a, QueryColumn b) {
    return a.Id() < b.Id();
  });
  auto it = std::unique(columns.begin(), columns.end(),
                        [] (QueryColumn a, QueryColumn b) {
                          return a.Id() == b.Id();
                        });
  columns.erase(it, columns.end());

  SortAndUnique(col_ids);

  auto col_spec = ColumnSpec(col_ids);
  for (auto view : table->views) {
    if (view->view_tag == tag && view->col_spec == col_spec) {
      return view;
    }
  }

  return table->views.Create(table, impl->next_id++, tag, std::move(col_ids),
                             std::move(col_spec));
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

  SortAndUnique(col_ids);
  auto col_spec = ColumnSpec(col_ids);
  for (auto index : indices) {
    if (index->column_spec == col_spec) {
      return index;
    }
  }

  return indices.Create(this, std::move(col_ids), std::move(col_spec));
}

}  // namespace hyde
