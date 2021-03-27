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

  BuildEagerInsertionRegions(impl, view, context, parent,
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
        [&](REGION *parent, bool in_loop) -> REGION * {
          if (already_checked != model->table) {
            already_checked = model->table;

            // TODO(pag): We should be able to optimize
            //            `BuildTopDownTryMarkAbsent` to not actually
            //            have to check during its state change, but oh well.

            auto continue_or_return = in_loop ? BuildStateCheckCaseNothing :
                                      BuildStateCheckCaseReturnFalse;

            if (view.CanProduceDeletions()) {
              return BuildTopDownCheckerStateCheck(
                   impl, parent, model->table, view.Columns(),
                   BuildStateCheckCaseReturnTrue, continue_or_return,
                   [&](ProgramImpl *, REGION *parent) -> REGION * {
                     return BuildTopDownTryMarkAbsent(
                         impl, model->table, parent, view.Columns(),
                         [&](PARALLEL *par) { call_preds(par); });
                   });
            } else {
              return BuildTopDownCheckerStateCheck(
                  impl, parent, model->table, view.Columns(),
                  BuildStateCheckCaseReturnTrue,
                  continue_or_return,
                  continue_or_return);
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
                                QueryView view, OP *parent_,
                                TABLE *already_removed_) {
  auto [parent, table, already_removed] = InTryMarkUnknown(
      impl, view, parent_, already_removed_);
  BuildEagerRemovalRegions(impl, view, context, parent,
                           view.Successors(), already_removed);
}

}  // namespace hyde
