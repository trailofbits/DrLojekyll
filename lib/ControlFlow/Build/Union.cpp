// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for a `QueryMerge` that is NOT part of an inductive
// loop, and thus passes on its data to the next thing down as long as that
// data is unique.
//
// NOTE(pag): These merges could actually be part of an induction set, but
//            really the induction loop belongs to another merge which dominates
//            this merge.
void BuildEagerUnionRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryMerge merge, Context &context, OP *parent,
                           TABLE *last_table) {
  const QueryView view(merge);
  DataModel *const model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;

  // If we can receive deletions, and if we're in a path where we haven't
  // actually inserted into a view, then we need to go and do a differential
  // insert/update/check.
  if (table) {
    if (table != last_table) {
      parent = BuildInsertCheck(impl, view, context, parent, table,
                                view.CanReceiveDeletions(), view.Columns());
      last_table = table;
    }

  // We can't pass the table through.
  } else {
    last_table = nullptr;
  }

#ifndef NDEBUG
  if (1u < view.Predecessors().size()) {
    for (auto col : view.Columns()) {
      assert(!col.IsConstantRef());
    }
  }
#endif

  BuildEagerSuccessorRegions(impl, view, context, parent,
                             view.Successors(), last_table);
}

// Build a top-down checker on a union. This applies to non-differential
// unions.
void BuildTopDownUnionChecker(ProgramImpl *impl, Context &context, PROC *proc,
                              QueryMerge merge,
                              std::vector<QueryColumn> &view_cols,
                              TABLE *already_checked) {

  QueryView view(merge);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();

  // This union has persistent backing; go check it, and then check the
  // predecessors.
  if (model->table) {

    TABLE *table_to_update = model->table;

    // Call all of the predecessors.
    auto call_preds = [&](PARALLEL *par) {

      // TODO(pag): Find a way to not bother re-appearing non-inductive
      //            successors?
      for (QueryView pred_view : view.Predecessors()) {

        // Deletes have no backing data; they signal to their successors that
        // data should be deleted from their successor models.
        if (pred_view.IsDelete()) {
          continue;
        }

        const auto check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
            impl, context, par, view, view_cols, table_to_update, pred_view,
            already_checked);
        COMMENT( check->comment = __FILE__ ": BuildTopDownUnionChecker::call_preds"; )
        par->AddRegion(check);
      }
    };

    const auto region = BuildMaybeScanPartial(
        impl, view, view_cols, model->table, proc,
        [&](REGION *parent, bool) -> REGION * {
          if (already_checked != model->table) {
            already_checked = model->table;

            // TODO(pag): We should be able to optimize
            //            `BuildTopDownTryMarkAbsent` to not actually
            //            have to check during its state change, but oh well.

            if (view.CanProduceDeletions()) {
              return BuildTopDownCheckerStateCheck(
                   impl, parent, model->table, view.Columns(),
                   BuildStateCheckCaseReturnTrue, BuildStateCheckCaseNothing,
                   [&](ProgramImpl *, REGION *parent) -> REGION * {
                     return BuildTopDownTryMarkAbsent(
                         impl, model->table, parent, view.Columns(),
                         [&](PARALLEL *par) { call_preds(par); });
                   });
            } else {
              return BuildTopDownCheckerStateCheck(
                  impl, parent, model->table, view.Columns(),
                  BuildStateCheckCaseReturnTrue,
                  BuildStateCheckCaseNothing,
                  BuildStateCheckCaseNothing);
            }

          } else {
            table_to_update = nullptr;
            const auto par = impl->parallel_regions.Create(parent);
            if (view.CanProduceDeletions()) {
              call_preds(par);
            }
            return par;
          }
        });

    assert(region != proc);
    proc->body.Emplace(proc, region);

  // This union doesn't have persistent backing, so we have to call down to
  // each predecessor. If any of them returns true then we can return true.
  } else {
    auto par = impl->parallel_regions.Create(proc);
    proc->body.Emplace(proc, par);

    for (QueryView pred_view : view.Predecessors()) {

      // `DELETE`s will always return `false`, so we don't dispatch down
      // to them.
      if (pred_view.IsDelete()) {
        continue;
      }

      par->AddRegion(ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, par, view, view_cols, nullptr, pred_view, nullptr));
    }
  }
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
      proc->body.Emplace(proc, parent);

    // The caller didn't already do a state transition, so we cn do it.
    } else {
      auto remove =
          BuildBottomUpTryMarkUnknown(impl, model->table, proc, view.Columns(),
                                      [&](PARALLEL *par) { parent = par; });

      proc->body.Emplace(proc, remove);

      already_checked = model->table;
    }

  // This merge isn't associated with any persistent storage.
  } else {
    already_checked = nullptr;
    parent = impl->parallel_regions.Create(proc);
    proc->body.Emplace(proc, parent);
  }

  // By this point, we know that we have a data model, and that we or our caller
  // has marked this tuple as being unknown. If we're actually in an induction,
  // then we want to be really sure about calling the successors, which may go
  // and do lots and lots of loops (via recursion) and remove tons of stuff, but
  // maybe we can avoid that by finding an alternate proof for our tuple (via
  // this exact induction) so we want to avoid pushing forward a delete.
  //
  // NOTE(pag): Some inductive unions are actually handled by the normal union
  //            code if all paths out of those apparently inductive unions are
  //            post-dominated by another co-inductive union.
  if (model->table && context.inductive_successors.count(view)) {

    std::vector<QueryColumn> check_cols;
    for (auto col : view.Columns()) {
      check_cols.push_back(col);
    }

    const auto checker_proc = GetOrCreateTopDownChecker(
        impl, context, view, check_cols, model->table);

    // Now call the checker procedure. Unlike in normal checkers, we're doing
    // a check on `false`.
    const auto check = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, parent, checker_proc,
        ProgramOperation::kCallProcedureCheckFalse);

    COMMENT( check->comment = __FILE__ ": CreateBottomUpUnionRemover"; )

    auto i = 0u;
    for (auto col : check_cols) {
      const auto var = parent->VariableFor(impl, col);
      assert(var != nullptr);
      check->arg_vars.AddUse(var);

      const auto param = checker_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }

    // Re-parent into the body of the check.
    parent->AddRegion(check);
    parent = impl->parallel_regions.Create(check);
    check->body.Emplace(check, parent);
  }

  // Okay, by this point, we've either marked the tuple as unknown
  // (non-inductive) and we are proceeding to speculatively delete it in
  // the successors, or we've proven its absence, and are proceeding to
  // speculatively delete it in the successors.
  for (auto succ_view : view.Successors()) {

    // TODO(pag): I don't recall why this is important, but I have enforced it
    //            in the data flow side.
    assert(!succ_view.IsMerge());

    const auto checker_proc = GetOrCreateBottomUpRemover(
        impl, context, view, succ_view, already_checked);
    const auto call = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, parent, checker_proc);

    auto i = 0u;
    for (auto col : view.Columns()) {
      const auto var = proc->VariableFor(impl, col);
      assert(var != nullptr);
      call->arg_vars.AddUse(var);

      const auto param = checker_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }

    parent->AddRegion(call);
  }
}

}  // namespace hyde
