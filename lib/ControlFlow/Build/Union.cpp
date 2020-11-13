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

    // Call all of the predecessors.
    auto call_preds = [&] (PARALLEL *par) {
      for (QueryView pred_view : view.Predecessors()) {
        if (pred_view.IsInsert() &&
            QueryInsert::From(pred_view).IsDelete()) {
          continue;
        }
        const auto check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
            impl, context, par, view, view_cols, model->table, pred_view,
            already_checked);
        check->ExecuteAlongside(impl, par);
      }
    };

    const auto region = BuildMaybeScanPartial(
        impl, view, view_cols, model->table, proc,
        [&] (REGION *parent) -> REGION * {
          if (already_checked != model->table) {
            already_checked = model->table;

            // TODO(pag): We should be able to optimize
            //            `BuildTopDownTryMarkAbsent` to not actually
            //            have to check during its state change, but oh well.
            return BuildTopDownCheckerStateCheck(
                impl, parent, model->table, view.Columns(),
                BuildStateCheckCaseReturnTrue,
                BuildStateCheckCaseNothing,
                [&] (ProgramImpl *, REGION *parent) -> REGION * {
                  return BuildTopDownTryMarkAbsent(
                      impl, model->table, parent, view.Columns(),
                      [&] (PARALLEL *par) {
                        call_preds(par);
                      });
                });

          } else {
            const auto par = impl->parallel_regions.Create(parent);
            call_preds(par);
            return parent;
          }
        });

    UseRef<REGION>(proc, region).Swap(proc->body);

  // This union doesn't have persistent backing, so we have to call down to
  // each predecessor. If any of them returns true then we can return true.
  } else {
    auto par = impl->parallel_regions.Create(proc);
    UseRef<REGION>(proc, par).Swap(proc->body);

    for (QueryView pred_view : view.Predecessors()) {

      // NOTE(pag): We don't need to handle the `DELETE` (really, and insert)
      //            case, as otherwise this union would have persistent backing.

      const auto check = CallTopDownChecker(
          impl, context, par, view, view_cols, pred_view,
          ProgramOperation::kCallProcedureCheckTrue, nullptr);
      check->ExecuteAlongside(impl, par);
    }
  }


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

void CreateBottomUpUnionRemover(ProgramImpl *impl, Context &context,
                                QueryView view, PROC *proc,
                                TABLE *already_checked) {

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  PARALLEL *parent = nullptr;

  if (model->table) {

    // We've already transitioned for this table, so our job is just to pass
    // the buck along, and then eventually we'll temrinate recursion.
    if (already_checked == model->table) {

      parent = impl->parallel_regions.Create(proc);
      UseRef<REGION>(proc, parent).Swap(proc->body);

    // The caller didn't already do a state transition, so we cn do it.
    } else {
      auto remove = BuildBottomUpTryMarkUnknown(
          impl, model->table, proc, view.Columns(),
          [&] (PARALLEL *par) {
            parent = par;
          });

      UseRef<REGION>(proc, remove).Swap(proc->body);

      already_checked = model->table;
    }

  // This merge isn't associated with any persistent storage.
  } else {
    already_checked = nullptr;
    parent = impl->parallel_regions.Create(proc);
    UseRef<REGION>(proc, parent).Swap(proc->body);
  }

  for (auto succ_view : view.Successors()) {
    assert(!succ_view.IsMerge());

    const auto call = impl->operation_regions.CreateDerived<CALL>(
        parent, GetOrCreateBottomUpRemover(impl, context, view, succ_view,
                                           already_checked));

    for (auto col : view.Columns()) {
      const auto var = proc->VariableFor(impl, col);
      assert(var != nullptr);
      call->arg_vars.AddUse(var);
    }

    parent->regions.AddUse(call);
  }

  auto ret = impl->operation_regions.CreateDerived<RETURN>(
      proc, ProgramOperation::kReturnFalseFromProcedure);
  ret->ExecuteAfter(impl, parent);
}

}  // namespace hyde
