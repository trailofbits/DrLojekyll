// Copyright 2020, Trail of Bits. All rights reserved.

#include <algorithm>
#include <sstream>

#include "Build/Build.h"
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

DataRecordCaseImpl::DataRecordCaseImpl(unsigned id_)
    : Def<DataRecordCaseImpl>(this),
      User(this),
      id(id_),
      derived_from(this) {}

DataRecordImpl::DataRecordImpl(unsigned id_, TABLE *table_)
    : Def<DataRecordImpl>(this),
      User(this),
      id(id_),
      cases(this),
      table(this, table_) {}

DataVariableImpl::DataVariableImpl(unsigned id_, VariableRole role_)
    : Def<DataVariableImpl>(this),
      role(role_),
      id(id_) {}

TypeLoc DataVariableImpl::Type(void) const noexcept {
  switch (role) {
    case VariableRole::kConditionRefCount:
    case VariableRole::kInitGuard:
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
    case VariableRole::kWorkerId: return TypeKind::kUnsigned64;

    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue: return TypeKind::kBoolean;

    case VariableRole::kConstantTag: return TypeKind::kUnsigned16;

    case VariableRole::kConstant:
      if (query_const) {
        return query_const->Type();
      }
      [[clang::fallthrough]];
    default:
      if (query_column) {
        return query_column->Type();
      }
      if (query_const) {
        return query_const->Type();
      }
      if (parsed_param) {
        return parsed_param->Type();
      }
      break;
  }
  assert(false);
  return TypeKind::kInvalid;
}


bool DataVariableImpl::IsGlobal(void) const noexcept {
  switch (role) {
    case VariableRole::kConditionRefCount:
    case VariableRole::kInitGuard:
    case VariableRole::kConstant:
    case VariableRole::kConstantTag:
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue: return true;
    default: return false;
  }
}

bool DataVariableImpl::IsConstant(void) const noexcept {
  switch (role) {
    case VariableRole::kConstant:
    case VariableRole::kConstantTag:
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue: return true;
    case VariableRole::kConditionRefCount:
    case VariableRole::kRecordElement: return false;
    default:
      if (query_const.has_value()) {
        return true;
      } else if (query_column.has_value() &&
                 query_column->IsConstantOrConstantRef()) {
        return true;
      } else {
        return false;
      }
  }
}

DataColumnImpl::~DataColumnImpl(void) {}
DataIndexImpl::~DataIndexImpl(void) {}
DataTableImpl::~DataTableImpl(void) {}

DataTableImpl::DataTableImpl(unsigned id_)
    : Def<DataTableImpl>(this),
      User(this),
      id(id_),
      columns(this),
      indices(this),
      records(this) {}

DataColumnImpl::DataColumnImpl(unsigned id_, const TypeLoc &type_,
                               DataTableImpl *table_)
    : Def<DataColumnImpl>(this),
      User(this),
      id(id_),
      index(table_->columns.Size()),
      type(type_),
      table(this, table_) {}

DataIndexImpl::DataIndexImpl(unsigned id_, DataTableImpl *table_,
                             std::string column_spec_)
    : Def<DataIndexImpl>(this),
      User(this),
      id(id_),
      column_spec(column_spec_),
      columns(this),
      mapped_columns(this),
      table(this, table_) {}

// Get or create a table in the program.
DataTableImpl *DataTableImpl::GetOrCreate(ProgramImpl *impl, Context &,
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

    std::vector<unsigned> offsets;
    unsigned col_index = 0;
    for (auto col : cols) {
      offsets.push_back(col_index++);
      (void) model->table->columns.Create(impl->next_id++, col.Type(),
                                          model->table);
    }

    // Always create an index over every column.
    (void) model->table->GetOrCreateIndex(impl, std::move(offsets));
  }

  const auto old_size = model->table->views.size();
  model->table->views.push_back(view);

  view.SetTableId(model->table->id);

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
            [](QueryView a, QueryView b) -> bool {
              if (!a.IsMerge() && !b.IsMerge()) {
                return a.Depth() > b.Depth();

              // Order merges first.
              } else if (a.IsMerge() && !b.IsMerge()) {
                return true;

              } else if (!a.IsMerge() && b.IsMerge()) {
                return false;
              }

              auto a_inductive = a.InductionGroupId().has_value();
              auto b_inductive = b.InductionGroupId().has_value();

              // If both are inductive, then order by the deepest.
              if (a_inductive && b_inductive) {
                auto a_order = *(a.InductionDepth());
                auto b_order = *(a.InductionDepth());

                if (a_order != b_order) {
                  assert(false);  // Shouldn't be possible.
                  return a_order > b_order;

                } else {
                  return a.Depth() > b.Depth();
                }

              // Order inductive merges first.
              } else if (a_inductive && !b_inductive) {
                return true;

              } else if (!a_inductive && b_inductive) {
                return false;

              // Order deepest first.
              } else {
                return a.Depth() > b.Depth();
              }
            });

  auto it = std::unique(model->table->views.begin(), model->table->views.end());
  model->table->views.erase(it, model->table->views.end());

  // Add additional names to the columns; this is helpful in debugging
  // output.
  if (model->table->views.size() > old_size) {

    // Breaks abstraction layers but is super nifty for debugging.
    // view.SetTableId(model->table->id);

    unsigned i = 0u;
    for (auto col : cols) {
      auto table_col = model->table->columns[i++];
      if (auto var = col.Variable(); var.has_value()) {
        auto name = var->Name();
        switch (name.Lexeme()) {
          case Lexeme::kIdentifierVariable:
          case Lexeme::kIdentifierAtom: {
            table_col->names.push_back(name);
            std::sort(table_col->names.begin(), table_col->names.end(),
                      [](Token a, Token b) {
                        return a.IdentifierId() < b.IdentifierId();
                      });
            auto it = std::unique(table_col->names.begin(),
                                  table_col->names.end(), [](Token a, Token b) {
                                    return a.IdentifierId() == b.IdentifierId();
                                  });
            table_col->names.erase(it, table_col->names.end());
            break;
          }
          default: break;
        }
      }
    }
  }

  return model->table;
}

// Get or create an index on the table.
TABLEINDEX *
DataTableImpl::GetOrCreateIndex(ProgramImpl *impl,
                                std::vector<unsigned> col_indexes) {
  SortAndUnique(col_indexes);

//  // The index covers all columns, i.e. we don't want/need it.
//  if (col_indexes.size() == this->columns.Size()) {
//    return nullptr;
//  }

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

bool DataVectorImpl::IsRead(void) const {
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
