// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for tuple. If the tuple can receive differential
// updates then its data needs to be saved.
void BuildEagerTupleRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryTuple tuple, Context &context, OP *parent,
                           TABLE *last_model) {
  const QueryView view(tuple);

  // If this tuple may be the base case for a top-down recursive check (that
  // supports differential updates), then we need to make sure that the input
  // data provided to this tuple is persisted. At first glance, one might think
  // that we need to persist the tuple's output data; however, this is not
  // quite right because the tuple might narrow it's input data, keeping only
  // a few columns, or it may widen it, i.e. duplicated some of the columns,
  // or introducing constants. We don't maintain precise enough refcounts to be
  // able to know the number of ways in which a tuple might have produced some
  // data, and so we need to be able to look upon that data at a later time to
  // recover the ways.
  if (MayNeedToBePersisted(view) &&
      !CanDeferPersistingToPredecessor(impl, context, view, pred_view)) {

    // NOTE(pag): See comment above, use of `pred_view` in getting the table
    //            is deliberate.
    if (const auto table = TABLE::GetOrCreate(impl, pred_view);
        table != last_model) {
      parent = BuildInsertCheck(impl, pred_view, context, parent, table,
                                true, pred_view.Columns());
      last_model = table;
    }
  }

  BuildEagerSuccessorRegions(impl, view, context, parent, view.Successors(),
                             last_model);
}

// Build a top-down checker on a tuple. This possibly widens the tuple, i.e.
// recovering "lost" columns, and possibly re-orders arguments before calling
// down to the tuple's predecessor's checker.
void BuildTopDownTupleChecker(
    ProgramImpl *impl, Context &context, PROC *proc, QueryTuple tuple,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  const QueryView view(tuple);
  const QueryView pred_view = view.Predecessors()[0];
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();

  // TODO(pag): We don't handle the case where `succ_view` is passing us a
  //            subset of the columns of `view`.

  // This tuple was persisted, thus we can check it.
  if (model->table) {

    // If the predecessor persists the same data then we'll call the
    // predecessor's checker.
    if (model->table == pred_model->table) {
      const auto check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, proc, view, view_cols, model->table, pred_view,
          already_checked);

      UseRef<REGION>(proc, check).Swap(proc->body);

    // The predecessor persists different data, so we'll check in the tuple,
    // and if it's not present, /then/ we'll call the predecessor handler.
    } else {
      auto call_pred = [&] (REGION *parent) -> REGION * {
        return ReturnTrueWithUpdateIfPredecessorCallSucceeds(
            impl, context, parent, view, view_cols,
            model->table, pred_view, already_checked);
      };

      const auto region = BuildMaybeScanPartial(
          impl, view, view_cols, model->table, proc,
          [&] (REGION *parent) -> REGION * {
            if (already_checked != model->table) {
              already_checked = model->table;
              return BuildTopDownCheckerStateCheck(
                  impl, parent, model->table, view.Columns(),
                  BuildStateCheckCaseReturnTrue,
                  BuildStateCheckCaseNothing,
                  [&] (ProgramImpl *, REGION *parent) -> REGION * {
                    return BuildTopDownCheckerResetAndProve(
                        impl, model->table, parent, view.Columns(),
                        [&] (PARALLEL *par) {
                          call_pred(par)->ExecuteAlongside(impl, par);
                        });
                  });

            } else {
              return call_pred(parent);
            }
          });

      UseRef<REGION>(proc, region).Swap(proc->body);
    }

  // Out best option at this point is to just call the predecessor; this tuple's
  // data is not persisted.
  } else {
    const auto check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, proc, view, view_cols, nullptr, pred_view,
          nullptr);
    UseRef<REGION>(proc, check).Swap(proc->body);
  }
}

}  // namespace hyde
