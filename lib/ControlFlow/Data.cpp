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

Node<DataColumn>::~Node(void) {}
Node<DataIndex>::~Node(void) {}
Node<DataTable>::~Node(void) {}

Node<DataColumn>::Node(unsigned id_, TypeKind type_, Node<DataTable> *table_)
    : Def<Node<DataColumn>>(this),
      User(this),
      id(id_),
      index(table_->columns.Size()),
      type(type_),
      table(this, table_) {}

Node<DataIndex>::Node(unsigned id_, Node<DataTable> *table_,
                      std::string column_spec_)
    : Def<Node<DataIndex>>(this),
      User(this),
      id(id_),
      column_spec(column_spec_),
      columns(this),
      mapped_columns(this),
      table(this, table_) {}

// Get or create a table in the program.
Node<DataTable> *Node<DataTable>::GetOrCreate(ProgramImpl *impl,
                                              QueryView view) {

  std::vector<QueryColumn> cols;
  if (view.IsInsert()) {
    for (auto col : QueryInsert::From(view).InputColumns()) {
      cols.push_back(col);
    }
  } else {
    for (auto col : view.Columns()) {
      cols.push_back(col);
    }
  }

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (!model->table) {
    model->table = impl->tables.Create(impl->next_id++);

    for (auto col : cols) {
      (void) model->table->columns.Create(impl->next_id++, col.Type().Kind(),
                                          model->table);
    }
  }

  // Add additional names to the columns; this is helpful in debugging
  // output.
  unsigned i = 0u;
  for (auto col : cols) {
    auto table_col = model->table->columns[i++];
    auto name = col.Variable().Name();
    switch (name.Lexeme()) {
      case Lexeme::kIdentifierVariable:
      case Lexeme::kIdentifierAtom: {
        table_col->names.push_back(name);
        std::sort(table_col->names.begin(), table_col->names.end(),
                  [](Token a, Token b) {
                    return a.IdentifierId() < b.IdentifierId();
                  });
        auto it = std::unique(table_col->names.begin(), table_col->names.end(),
                              [](Token a, Token b) {
                                return a.IdentifierId() == b.IdentifierId();
                              });
        table_col->names.erase(it, table_col->names.end());
        break;
      }
      default: break;
    }
  }

  return model->table;
}

// Get or create an index on the table.
TABLEINDEX *
Node<DataTable>::GetOrCreateIndex(ProgramImpl *impl,
                                  std::vector<unsigned> col_indexes) {
  SortAndUnique(col_indexes);
  auto col_spec = ColumnSpec(col_indexes);
  for (auto index : indices) {
    if (index->column_spec == col_spec) {
      return index;
    }
  }

  const auto index = indices.Create(impl->next_id++, this, std::move(col_spec));
  for (auto col_index : col_indexes) {
    index->columns.AddUse(columns[col_index]);
  }

  auto i = 0u;
  for (auto col_index : col_indexes) {
    for (; i < col_index; ++i) {
      index->mapped_columns.AddUse(columns[i]);
    }
    i = col_index + 1u;
  }
  for (auto max_i = columns.Size(); i < max_i; ++i) {
    index->mapped_columns.AddUse(columns[i]);
  }

  return index;
}

bool Node<DataVector>::IsRead(void) const {
  auto is_used = false;
  ForEachUse<OP>([&](OP *op, VECTOR *) {
    if (dynamic_cast<VECTORLOOP *>(op) || dynamic_cast<TABLEJOIN *>(op) ||
        dynamic_cast<INDUCTION *>(op) || dynamic_cast<CALL *>(op)) {
      is_used = true;
    }
  });
  return is_used;
}

}  // namespace hyde
