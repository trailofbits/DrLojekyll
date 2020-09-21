// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {}  // namespace

// Build an eager region for publishing data, or inserting it. This might end
// up passing things through if this isn't actually a message publication.
void BuildEagerInsertRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryInsert insert, Context &context, OP *parent,
                            TABLE *last_model) {
  const auto view = QueryView(insert);
  const auto cols = insert.InputColumns();

  if (insert.IsStream()) {
    assert(false && "TODO");

  // Inserting into a relation.
  } else if (insert.IsRelation()) {
    const auto table = TABLE::GetOrCreate(impl, view);
    if (table != last_model) {
      const auto insert =
          impl->operation_regions.CreateDerived<TABLEINSERT>(parent);
      for (auto col : cols) {
        const auto var = parent->VariableFor(impl, col);
        insert->col_values.AddUse(var);
      }

      UseRef<TABLE>(insert, table).Swap(insert->table);
      UseRef<REGION>(parent, insert).Swap(parent->body);
      parent = insert;
    }

    if (const auto succs = view.Successors(); succs.size()) {
      BuildEagerSuccessorRegions(impl, view, context, parent, succs, table);
    }

  } else {
    assert(false);
  }
}

}  // namespace hyde
