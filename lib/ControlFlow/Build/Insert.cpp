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

    // If we're setting a condition then we also need to see if any constant
    // tuples depend on that condition.
    if (auto set_cond = view.SetCondition(); set_cond) {
      const auto seq = impl->series_regions.Create(parent);
      UseRef<REGION>(parent, seq).Swap(parent->body);

      // Now that we know that the data has been dealt with, we increment the
      // condition variable.
      const auto set = impl->operation_regions.CreateDerived<ASSERT>(
          seq, insert.IsDelete() ? ProgramOperation::kDecrementAll
                                 : ProgramOperation::kIncrementAll);
      set->cond_vars.AddUse(ConditionVariable(impl, *set_cond));
      set->ExecuteAfter(impl, seq);

      if (insert.IsDelete()) {
        assert(false && "TODO");

      // If anything non-dataflow dependent depends on this condition, then
      // it is implicitly captured in the init-procedure, and so we can call
      // the init procedure here.
      } else {
        auto call = impl->operation_regions.CreateDerived<CALL>(
            seq, impl->procedure_regions[0]);
        call->ExecuteAfter(impl, seq);
      }

      // Create a dummy/empty LET binding so that we have an `OP *` as a parent
      // going forward.
      parent = impl->operation_regions.CreateDerived<LET>(seq);
      parent->ExecuteAfter(impl, seq);
    }

    if (const auto succs = view.Successors(); succs.size()) {
      BuildEagerSuccessorRegions(impl, view, context, parent, succs, table);
    }

  } else {
    assert(false);
  }
}

}  // namespace hyde
