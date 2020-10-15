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
void BuildTopDownUnionChecker(
    ProgramImpl *impl, Context &context, PROC *proc, QueryMerge merge,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  QueryView view(merge);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();

  // This union has persistent backing; go check it, and then check the
  // predecessors.
  if (model->table) {

    // The caller has done a state transition on `model->table` and it will
    // do the marking of present.
    if (already_checked == model->table) {
      assert(view_cols.size() == view.Columns().size());

      auto par = impl->parallel_regions.Create(proc);
      UseRef<REGION>(proc, par).Swap(proc->body);

      for (QueryView pred_view : view.Predecessors()) {
        if (pred_view.IsInsert() &&
            QueryInsert::From(pred_view).IsDelete()) {
          continue;
        }

        const auto check = ReturnTrueIfPredecessorCallSucceeds(
            impl, context, par, view, view_cols, pred_view, already_checked);
        check->ExecuteAlongside(impl, par);
      }

    // Our caller didn't check this union, so we need to go do it.
    } else {

      // Call all of the predecessors.
      auto call_preds = [&] (PARALLEL *par) {
        for (QueryView pred_view : view.Predecessors()) {
          if (pred_view.IsInsert() &&
              QueryInsert::From(pred_view).IsDelete()) {
            continue;
          }

          const auto check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
              impl, context, par, view, view_cols, model->table, pred_view);
          check->ExecuteAlongside(impl, par);
        }
      };

      auto do_unknown = [&] (ProgramImpl *, REGION *parent) -> REGION * {
        return BuildTopDownCheckerResetAndProve(
            impl, model->table, parent, view.Columns(),
            [&] (PARALLEL *par) {
              call_preds(par);
            });
      };

      const auto region = BuildMaybeScanPartial(
          impl, view, view_cols, model->table, proc,
          [&] (REGION *parent) -> REGION * {
            return BuildTopDownCheckerStateCheck(
                impl, parent, model->table, view.Columns(),
                BuildStateCheckCaseReturnTrue,
                BuildStateCheckCaseNothing,
                do_unknown);
          });

      UseRef<REGION>(proc, region).Swap(proc->body);
    }

  // This union doesn't have persistent backing, so we have to call down to
  // each predecessor. If any of them returns true then we can return true.
  } else {
    auto par = impl->parallel_regions.Create(proc);
    UseRef<REGION>(proc, par).Swap(proc->body);

    for (QueryView pred_view : view.Predecessors()) {
      if (pred_view.IsInsert() &&
          QueryInsert::From(pred_view).IsDelete()) {
        continue;
      }

      const auto check = ReturnTrueIfPredecessorCallSucceeds(
          impl, context, par, view, view_cols, pred_view, nullptr);
      check->ExecuteAlongside(impl, par);
    }
  }
}

}  // namespace hyde
