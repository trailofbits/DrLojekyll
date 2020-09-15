// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

}  // namespace

// Build an eager region for publishing data, or inserting it. This might end
// up passing things through if this isn't actually a message publication.
void BuildEagerInsertRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryInsert insert, Context &context, OP *parent) {
  const auto view = QueryView(insert);
  const auto cols = view.Columns();

  if (insert.IsStream()) {
    assert(!"TODO");

  // Inserting into a relation.
  } else if (insert.IsRelation()) {
    const auto insert = impl->operation_regions.CreateDerived<VIEWINSERT>(parent);
    for (auto col : cols) {
      const auto var = parent->VariableFor(impl, col);
      insert->col_values.AddUse(var);
    }
    insert->col_values.Unique();

    for (auto var : insert->col_values) {
      insert->col_ids.push_back(var->id);
    }

    // TODO(pag): Think about eliminating `view` as a tag if there is only
    //            one inserter into VIEW.
    const auto table_view = TABLE::GetOrCreate(impl, cols, view);

    UseRef<VIEW>(insert, table_view).Swap(insert->view);
    UseRef<REGION>(parent, insert).Swap(parent->body);

    if (const auto succs = view.Successors(); succs.size()) {
      BuildEagerSuccessorRegions(impl, view, context, insert, succs);
    }

  } else {
    assert(false);
  }
}

}  // namespace hyde
