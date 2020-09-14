// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

}  // namespace

// Build an eager region for publishing data, or inserting it. This might end
// up passing things through if this isn't actually a message publication.
void BuildEagerInsertRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryInsert insert, Context &context, OP *parent) {
  const auto proc = parent->containing_procedure;
  const auto view = QueryView(insert);
  const auto cols = view.Columns();

  if (insert.IsStream()) {
    assert(!"TODO");

  // Inserting into a relation.
  } else if (insert.IsRelation()) {
    OP * const insert = impl->operation_regions.CreateDerived<VIEWINSERT>(parent);
    for (auto col : cols) {
      const auto var = proc->VariableFor(col);
      insert->variables.AddUse(var);
    }

    // TODO(pag): Think about eliminating `view` as a tag if there is only
    //            one inserter into VIEW.

    insert->views.AddUse(TABLE::GetOrCreate(impl, cols, view));
    insert->variables.Unique();
    UseRef<REGION>(parent, insert).Swap(parent->body);

    if (const auto succs = view.Successors(); succs.size()) {
      BuildEagerSuccessorRegions(impl, view, context, insert, succs);
    }

  } else {
    assert(false);
  }
}

}  // namespace hyde
