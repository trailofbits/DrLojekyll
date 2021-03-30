// Copyright 2020, Trail of Bits. All rights reserved.

#include <algorithm>
#include <sstream>

#include "Program.h"
#include "Build/Build.h"

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

TypeLoc Node<DataVariable>::Type(void) const noexcept {
  switch (role) {
    case VariableRole::kConditionRefCount:
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
      return TypeKind::kUnsigned64;
    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue:
      return TypeKind::kBoolean;
    case VariableRole::kConstant:
      if (query_const) {
        return query_const->Literal().Type().Kind();
      }
      [[clang::fallthrough]];
    default:
      if (query_column) {
        return query_column->Type().Kind();
      }
  }
  assert(false);
  return TypeKind::kInvalid;
}

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
                                              Context &context,
                                              QueryView view) {

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();

  std::vector<QueryColumn> cols;
  if (view.IsInsert()) {
    for (auto col : QueryInsert::From(view).InputColumns()) {
      cols.push_back(col);
    }

    // TODO(pag): Eventually revisit this idea. It needs corresponding support
    //            in Build.cpp, `BuildDataModel::is_diff_map`.
    //
//  // This is a bit hidden away here, but in general, we want to avoid trying
//  // to save too much stuff in output tables produced from maps, because we
//  // can recompute that data. That has the effect of possibly pessimizing
//  // our data modeling, though.
//  } else if (view.IsMap() && (view.CanReceiveDeletions() ||
//                              !!view.SetCondition())) {
//    const auto map = QueryMap::From(view);
//    const auto functor = map.Functor();
//
//    // If a functor is pure, then we won't store the outputs for the output
//    // data because we can re-compute it.
//    if (functor.IsPure()) {
//      for (auto col : map.MappedBoundColumns()) {
//        cols.push_back(col);
//      }
//      for (auto col : map.CopiedColumns()) {
//        cols.push_back(col);
//      }
//
//    // It's an impure functor, so we need to be able to observe the old and
//    // new outputs, so we store all data.
//    } else {
//      for (auto col : map.Columns()) {
//        cols.push_back(col);
//      }
//    }

  } else {
    for (auto col : view.Columns()) {
      cols.push_back(col);
    }
  }

  if (!model->table) {
    model->table = impl->tables.Create(impl->next_id++);

    for (auto col : cols) {
      (void) model->table->columns.Create(impl->next_id++, col.Type().Kind(),
                                          model->table);
    }
  }

  const auto old_size = model->table->views.size();
  model->table->views.push_back(view);

  // Sort the views associated with this model so that the first view is
  // the deepest inductive union associated with the table. This is super
  // important to know when we're doing top-down checkers, because if we invoke
  // a top-down checker of a predecessor of a (possibly inductive) union,
  // and if our invocation is responsible for doing the assertion of absence
  // prior to trying to re-prove the tuple, then that assertion and top-down
  // check could unilaterally make a decision about the absence of a tuple
  // without consulting whether or not the other sources feeding the union
  // might have provided the data.
  std::sort(model->table->views.begin(), model->table->views.end(),
            [&context] (QueryView a, QueryView b) -> bool {

              // Order merges first.
              if (a.IsMerge() && !b.IsMerge()) {
                return true;

              } else if (!a.IsMerge() && b.IsMerge()) {
                return false;

              // Order inductive merges first.
              } else if (context.inductive_successors.count(a) &&
                         !context.inductive_successors.count(b)) {
                return true;

              } else if (!context.inductive_successors.count(a) &&
                         context.inductive_successors.count(b)) {
                return false;

              // Order deepest first.
              } else  {
                return a.Depth() > b.Depth();
              }
            });

  auto it = std::unique(model->table->views.begin(), model->table->views.end());
  model->table->views.erase(it, model->table->views.end());

  // Add additional names to the columns; this is helpful in debugging
  // output.
  if (model->table->views.size() > old_size) {

    // Breaks abstraction layers but is super nifty for debugging.
    view.SetTableId(model->table->id);

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
        dynamic_cast<TABLEPRODUCT *>(op) || dynamic_cast<INDUCTION *>(op) ||
        dynamic_cast<CALL *>(op) || dynamic_cast<VECTORSWAP *>(op)) {
      is_used = true;
    }
  });
  return is_used;
}

}  // namespace hyde
