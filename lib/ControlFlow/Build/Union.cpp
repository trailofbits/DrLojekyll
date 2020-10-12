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
  if (MayNeedToBePersisted(view)) {
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
                              PROC *proc, QueryView succ_view,
                              QueryMerge merge) {
  QueryView view(merge);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();

  // Call the predecessors.
  auto call_pred =
      [=, &context] (REGION *parent, QueryView pred_view) -> REGION * {
        const auto check = impl->operation_regions.CreateDerived<CALL>(
            parent, GetOrCreateTopDownChecker(impl, context, view, pred_view),
            ProgramOperation::kCallProcedureCheckTrue);

        const auto inout_cols = GetColumnMap(view, pred_view);
        for (auto [pred_col, view_col] : inout_cols) {
          const auto in_var = parent->VariableFor(impl, view_col);
          check->arg_vars.AddUse(in_var);
        }

        const auto ret_true = BuildStateCheckCaseReturnTrue(impl, check);
        UseRef<REGION>(check, ret_true).Swap(check->body);

        return check;
      };

  // This union has persistent backing; go check it, and then check the
  // predecessors.
  if (model->table) {
    const auto region = BuildMaybeScanPartial(
        impl, succ_view, view, model->table, proc,
        [&] (REGION *parent) -> REGION * {
          return BuildTopDownCheckerStateCheck(
              impl, parent, model->table, view.Columns(),
              BuildStateCheckCaseReturnTrue,
              BuildStateCheckCaseNothing,
              [&] (ProgramImpl *, REGION *parent) -> REGION * {
                return BuildTopDownCheckerResetAndProve(
                    impl, model->table, parent, view.Columns(),
                    [&] (PARALLEL *par) {
                      for (QueryView pred_view : view.Predecessors()) {
                        call_pred(par, pred_view)->ExecuteAlongside(impl, par);
                      }
                    });
              });
        });

    UseRef<REGION>(proc, region).Swap(proc->body);

  // This unions doesn't have persistent backing.
  } else {

  }
//
//
//  if (QueryView(view).CanReceiveDeletions()) {
//    BuildTopDownInductionChecker(impl, context, proc, succ_view, view);
//    return;
//  }
//
//  const auto par = impl->parallel_regions.Create(proc);
//  par->ExecuteAfter(impl, proc);
//
//  for (auto pred_view : view.MergedViews()) {
//    const auto rec_check = impl->operation_regions.CreateDerived<CALL>(
//        par, GetOrCreateTopDownChecker(impl, context, view, pred_view),
//        ProgramOperation::kCallProcedureCheckTrue);
//
//    for (auto col : view.Columns()) {
//      const auto var = proc->VariableFor(impl, col);
//      rec_check->arg_vars.AddUse(var);
//    }
//
//    rec_check->ExecuteAlongside(impl, par);
//
//    // If the tuple is present, then return `true`.
//    const auto rec_present = impl->operation_regions.CreateDerived<RETURN>(
//        rec_check, ProgramOperation::kReturnTrueFromProcedure);
//    UseRef<REGION>(rec_check, rec_present).Swap(rec_check->OP::body);
//  }
}

}  // namespace hyde
