// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build a top-down checker on a select.
void BuildTopDownSelectChecker(ProgramImpl *impl, Context &context, PROC *proc,
                               QuerySelect select,
                               std::vector<QueryColumn> &view_cols,
                               TABLE *already_checked) {

  const QueryView view(select);
  const auto pred_views = view.Predecessors();
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();

  // The base case is that we get to a SELECT from a stream. We treat
  // data received as ephemeral, and so there is no way to actually check
  // if the tuple exists, and so we treat it as not existing.
  if (!model->table) {
    assert(select.IsStream());
    proc->body.Emplace(proc, BuildStateCheckCaseReturnFalse(impl, proc));
    return;
  }

  TABLE *table_to_update = model->table;

  // The predecessors of a `SELECT` are inserts. `SELECT`s don't have input
  // nodes, and `INSERT`s don't have output nodes. The top-down checkers for
  auto call_pred = [&](REGION *parent) -> REGION * {
    assert(pred_views.size() == 1u);
    assert(pred_views[0].IsInsert());

    const auto insert = QueryInsert::From(pred_views[0]);

    std::vector<QueryColumn> insert_cols;

    for (auto col : select.Columns()) {
      const QueryColumn in_col = insert.InputColumns()[*(col.Index())];
      insert_cols.push_back(in_col);
      parent->col_id_to_var[in_col.Id()] =
          parent->VariableFor(impl, col);
    }

    const auto check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
        impl, context, parent, insert, insert_cols, table_to_update, insert,
        already_checked);

    COMMENT( check->comment = __FILE__ ": BuildTopDownSelectChecker::call_pred"; )

    return check;
  };

  // Mark the tuple as absent and return false.
  auto remove = [&] (REGION *parent) -> REGION * {
    const auto seq = impl->series_regions.Create(parent);
    seq->AddRegion(BuildChangeState(
        impl, model->table, seq, view.Columns(), TupleState::kUnknown,
        TupleState::kAbsent));
    seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));
    return seq;
  };

  const auto region = BuildMaybeScanPartial(
      impl, view, view_cols, model->table, proc,
      [&](REGION *parent, bool in_scan) -> REGION * {
        if (already_checked != model->table) {
          already_checked = model->table;

          if (view.CanProduceDeletions()) {
            return BuildTopDownCheckerStateCheck(
                impl, parent, model->table, view.Columns(),
                BuildStateCheckCaseReturnTrue,
                BuildStateCheckCaseReturnFalse,
                [&](ProgramImpl *, REGION *parent) -> REGION * {

                  // No predecessors, and the tuple is marked as unknown, so
                  // change it to absent and return `false` to our caller.
                  if (pred_views.empty()) {
                    return remove(parent);

                  // Predecessors, so mark the tuple as absent, then try to
                  // prove it in terms of its own absence.
                  } else {
                    return BuildTopDownTryMarkAbsent(
                        impl, model->table, parent, view.Columns(),
                        [&](PARALLEL *par) {
                          call_pred(par)->ExecuteAlongside(impl, par);
                        });
                  }
                });
          } else {
            return BuildTopDownCheckerStateCheck(
                impl, parent, model->table, view.Columns(),
                BuildStateCheckCaseReturnTrue,
                BuildStateCheckCaseReturnFalse,
                BuildStateCheckCaseReturnFalse);
          }

        // No predecessors, not our job to change states; return true to the
        // caller so they can change states.
        } else if (pred_views.empty()) {

          // We're in a scan, i.e. we've gone and selected a tuple and found it.
          //
          // TODO(pag): This should imply that `already_checked` doesn't match
          //            the parent...
          if (in_scan) {

            // TODO(pag): Finding it in a scan with no predecessors ought to
            //            mean that there is no way for that data to be deleted.
            //            However, it could be that the SELECT has some
            //            conditions which would make it behave differentially,
            //            so complain if we see that.
            assert(view.PositiveConditions().empty());
            assert(view.NegativeConditions().empty());

            return BuildStateCheckCaseReturnTrue(impl, parent);

          // We aren't actually in a scan, thus we were called with all the
          // data we need. The predecessor did the check, and presumably called
          // us because the data wasn't available, and our model is the same
          // as the predecessor's, so we have nothing to add, so return false.
          //
          // NOTE(pag): This generally comes up for `select.IsStream()`, i.e.
          //            a RECEIVE of a message.
          } else {
            auto ret = BuildStateCheckCaseReturnFalse(impl, parent);
            COMMENT( ret->comment = __FILE__ ": BuildTopDownSelectChecker, not in scan, no preds, already checked"; )
            return ret;
          }

        // There's a predecessor, and it will do the state changing
        } else {
          table_to_update = nullptr;
          return call_pred(parent);
        }
      });

  proc->body.Emplace(proc, region);
}

}  // namespace hyde
