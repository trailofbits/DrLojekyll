// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for a `QueryMerge` that is NOT part of an inductive
// loop, and thus passes on its data to the next thing down as long as that
// data is unique.
void BuildEagerUnionRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryMerge view, Context &context, OP *parent,
                           TABLE *last_model) {

  // If we can receive deletions, and if we're in a path where we haven't
  // actually inserted into a view, then we need to go and do a differential
  // insert/update/check.
  if (QueryView(view).CanReceiveDeletions()) {
    if (const auto table = TABLE::GetOrCreate(impl, view);
        table != last_model) {
      parent = BuildInsertCheck(impl, view, context, parent, table,
                                true, view.Columns());
      last_model = table;
    }
  }

  BuildEagerSuccessorRegions(impl, view, context, parent,
                             QueryView(view).Successors(), last_model);

  // TODO(pag): Think about whether or not we need to actually de-duplicate
  //            anything. It could be that we only need to dedup if we're on
  //            the edge between eager/lazy, or if we're in lazy.
}

// Build a top-down checker on a union. This applies to non-differential
// unions.
void BuildTopDownUnionChecker(ProgramImpl *impl, Context &context,
                              PROC *proc, QueryMerge view) {
  if (QueryView(view).CanReceiveDeletions()) {
    BuildTopDownInductionChecker(impl, context, proc, view);
    return;
  }

  const auto par = impl->parallel_regions.Create(proc);
  par->ExecuteAfter(impl, proc);

  for (auto pred : view.MergedViews()) {
    const auto rec_check = impl->operation_regions.CreateDerived<CALL>(
        par, GetOrCreateTopDownChecker(impl, context, pred),
        ProgramOperation::kCallProcedureCheckTrue);

    for (auto col : view.Columns()) {
      const auto var = proc->VariableFor(impl, col);
      rec_check->arg_vars.AddUse(var);
    }

    rec_check->ExecuteAlongside(impl, par);

    // If the tuple is present, then return `true`.
    const auto rec_present = impl->operation_regions.CreateDerived<RETURN>(
        rec_check, ProgramOperation::kReturnTrueFromProcedure);
    UseRef<REGION>(rec_check, rec_present).Swap(rec_check->OP::body);
  }
}

}  // namespace hyde
