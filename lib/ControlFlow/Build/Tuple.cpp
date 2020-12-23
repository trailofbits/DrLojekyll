// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

// We're inside a partial scan need to call a bottom-up remover on our negation.
// We're not going to call the actual bottom-up remover as it does a double
// check on the negated view, which is the tuple we're being called from, and
// we already know the result of that check.
static OP *RemoveFromNegatedView(ProgramImpl *impl, Context &context,
                                 REGION *parent, QueryNegate negate,
                                 std::vector<QueryColumn> &view_cols,
                                 TABLE *table) {

//  const auto called_proc = GetOrCreateBottomUpRemover(
//      impl, context, negate, negate, nullptr);
//  const auto call = impl->operation_regions.CreateDerived<CALL>(
//      impl->next_id++, parent, called_proc);
//
//  auto i = 0u;
//  for (auto col : view_cols) {
//    const auto var = parent->VariableFor(impl, col);
//    assert(var != nullptr);
//    call->arg_vars.AddUse(var);
//
//    const auto param = called_proc->input_vars[i++];
//    assert(var->Type() == param->Type());
//    (void) param;
//  }
//
//  return call;

  // Change the tuple's state to mark it as deleted so that we can't use it
  // as its own base case.
  const auto table_remove = BuildChangeState(
      impl, table, parent, view_cols, TupleState::kPresent, TupleState::kAbsent);

  const auto par = impl->parallel_regions.Create(table_remove);
  table_remove->body.Emplace(table_remove, par);

  for (auto succ_view : QueryView(negate).Successors()) {

    const auto remover_proc = GetOrCreateBottomUpRemover(
        impl, context, negate, succ_view, table);
    const auto call = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, par, remover_proc);

    auto i = 0u;
    for (auto col : negate.Columns()) {
      const auto var = par->VariableFor(impl, col);
      assert(var != nullptr);
      call->arg_vars.AddUse(var);

      const auto param = remover_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }

    par->regions.AddUse(call);
  }

  return table_remove;
}

// We want to try to re-add an entry to a negated view that might have
// previously been deleted, thus we need to double check if the data from our
// negation's predecessor is present, and if so, try to add the negation in.
static OP *ReAddToNegatedView(ProgramImpl *impl, Context &context,
                              REGION *parent, QueryNegate negate,
                              std::vector<QueryColumn> &view_cols,
                              TABLE *table) {

  const QueryView view(negate);
  const auto pred_view = view.Predecessors()[0];

  std::vector<QueryColumn> pred_cols;
  for (auto col : negate.InputColumns()) {
    pred_cols.push_back(col);
  }

  // NOTE(pag): Passing `nullptr` because a negation can't share the data
  //            model of its predecessor, because it represents a subset of
  //            that data.
  const auto checker_proc = GetOrCreateTopDownChecker(
      impl, context, pred_view, pred_cols, nullptr);

  // Now call the checker procedure for our predecessor. If it returns `true`
  // then it means that the columns are available in our predecessor, the
  // columns are not in the negated view, and thus we have proved the presence
  // of this tuple and can stop.
  const auto check = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, parent, checker_proc,
      ProgramOperation::kCallProcedureCheckTrue);

  auto i = 0u;
  for (auto col : pred_cols) {
    const auto var = parent->VariableFor(impl, col);
    assert(var != nullptr);
    check->arg_vars.AddUse(var);

    const auto param = checker_proc->input_vars[i++];
    assert(var->Type() == param->Type());
    (void) param;
  }

  // We've proven the presence of this tuple by checking the predecessor of
  // the negate, and by virtue of being called in the context on the check of
  // the absence of some data in the negated view.
  const auto insert = BuildChangeState(
      impl, table, check, view_cols, TupleState::kAbsentOrUnknown,
      TupleState::kPresent);
  check->body.Emplace(check, insert);

  // Now that we have everything transitioned we can call an eager region on
  // this tuple to re-insert stuff.
  BuildEagerSuccessorRegions(impl, view, context, insert, view.Successors(),
                             table);

  return check;
}

// We've proven that we've deleted a tuple, which might need to trigger the
// re-addition of several rows to a negated view.
static void ReAddToNegatedViews(ProgramImpl *impl, Context &context,
                                PARALLEL *parent, QueryView view) {

  view.ForEachNegation([&] (QueryNegate negate) {
    DataModel * const negated_model = \
        impl->view_to_model[negate]->FindAs<DataModel>();
    TABLE * const negated_table = negated_model->table;
    auto col_index = 0u;

    std::vector<QueryColumn> negate_cols;

    for (auto col : view.Columns()) {
      auto in_var = parent->VariableFor(impl, col);
      auto neg_out_col = negate.Columns()[col_index];
      auto neg_in_col = negate.InputColumns()[col_index];
      parent->col_id_to_var[neg_in_col.Id()] = in_var;
      parent->col_id_to_var[neg_out_col.Id()] = in_var;
      ++col_index;
      negate_cols.push_back(neg_out_col);
    }

    // For each thing that we find in the index scan, we will try to push
    // through a re-addition.
    parent->regions.AddUse(BuildMaybeScanPartial(
      impl, negate, negate_cols, negated_table, parent,
      [&](REGION *in_scan) -> REGION * {

      negate.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                            std::optional<QueryColumn> out_col) {
          if (out_col) {
            in_scan->col_id_to_var[in_col.Id()] =
                in_scan->VariableFor(impl, *out_col);
          }
        });

        return ReAddToNegatedView(impl, context, in_scan, negate, negate_cols,
                                  negated_table);
      }));
  });
}

}  // namespace

// Build an eager region for tuple. If the tuple can receive differential
// updates then its data needs to be saved.
void BuildEagerTupleRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryTuple tuple, Context &context, OP *parent,
                           TABLE *last_table) {
  const QueryView view(tuple);

  DataModel * const model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;
  if (table) {
    if (table != last_table) {
      parent = BuildInsertCheck(
          impl, view, context, parent, table, view.CanReceiveDeletions(),
          view.Columns());
      last_table = table;
    }
  } else {
    last_table = nullptr;
  }

  // If this view is used by a negation then we need to go and see if we should
  // do a delete in the negation, then call a bunch of other deletion stuff.
  if (view.IsUsedByNegation()) {
    const auto seq = impl->series_regions.Create(parent);
    parent->body.Emplace(parent, seq);

    const auto par = impl->parallel_regions.Create(seq);
    seq->regions.AddUse(par);

    view.ForEachNegation([&] (QueryNegate negate) {
      DataModel * const negate_model = \
          impl->view_to_model[negate]->FindAs<DataModel>();
      TABLE * const negate_table = negate_model->table;
      auto col_index = 0u;

      std::vector<QueryColumn> negate_cols;

      for (auto col : view.Columns()) {
        auto in_var = par->VariableFor(impl, col);
        auto neg_out_col = negate.Columns()[col_index];
        auto neg_in_col = negate.InputColumns()[col_index];
        par->col_id_to_var[neg_in_col.Id()] = in_var;
        par->col_id_to_var[neg_out_col.Id()] = in_var;
        ++col_index;
        negate_cols.push_back(neg_out_col);
      }

      // For each thing that we find in the index scan, we will push through
      // a removal.
      par->regions.AddUse(BuildMaybeScanPartial(
        impl, negate, negate_cols, negate_table, par,
        [&](REGION *in_scan) -> REGION * {
          return RemoveFromNegatedView(impl, context, in_scan, negate,
                                       negate_cols, negate_table);
        }));
    });

    parent = impl->operation_regions.CreateDerived<LET>(seq);
    seq->regions.AddUse(parent);
  }

  BuildEagerSuccessorRegions(impl, view, context, parent, view.Successors(),
                             last_table);
}

// Build a top-down checker on a tuple. This possibly widens the tuple, i.e.
// recovering "lost" columns, and possibly re-orders arguments before calling
// down to the tuple's predecessor's checker.
void BuildTopDownTupleChecker(ProgramImpl *impl, Context &context, PROC *proc,
                              QueryTuple tuple,
                              std::vector<QueryColumn> &view_cols,
                              TABLE *already_checked) {

  const QueryView view(tuple);
  const auto pred_views = view.Predecessors();

  // All inputs are constants so this tuple is trivially true iff the input
  // data matches the constants of this tuple. We need to be careful, however,
  // that we compare the input data and not the constant which are referred to
  // by `view_cols`.
  //
  // NOTE(pag): Tuples are the only views allowed to have all constant inputs.
  //            Thus, all other views have at least one predecessor.
  if (pred_views.empty()) {
    auto cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
        proc, ComparisonOperator::kEqual);
    for (auto col : view_cols) {
      assert(QueryView::Containing(col) == view);
      const auto col_index = *(col.Index());
      auto param_var = proc->input_vars[col_index];
      assert(param_var->query_column == col);

      auto input_col = tuple.InputColumns()[col_index];
      assert(input_col.IsConstant());

      cmp->lhs_vars.AddUse(param_var);
      cmp->rhs_vars.AddUse(proc->VariableFor(impl, input_col));
    }

    cmp->body.Emplace(cmp, BuildStateCheckCaseReturnTrue(impl, cmp));
    proc->body.Emplace(proc, cmp);
    return;
  }

  const QueryView pred_view = pred_views[0];
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();


  // TODO(pag): We don't handle the case where `succ_view` is passing us a
  //            subset of the columns of `view`.

  // This tuple was persisted, thus we can check it.
  if (model->table) {
    TABLE *table_to_update = model->table;

    auto call_pred = [&](REGION *parent) -> REGION * {
      return ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, parent, view, view_cols, table_to_update, pred_view,
          already_checked);
    };

    // If the predecessor persists the same data then we'll call the
    // predecessor's checker.
    if (model->table == pred_model->table) {
      table_to_update = nullptr;  // Let the predecessor do the state change.

      proc->body.Emplace(
          proc, BuildMaybeScanPartial(impl, view, view_cols, model->table, proc,
                                      call_pred));

    // The predecessor persists different data, so we'll check in the tuple,
    // and if it's not present, /then/ we'll call the predecessor handler.
    } else {
      const auto region = BuildMaybeScanPartial(
          impl, view, view_cols, model->table, proc,
          [&](REGION *parent) -> REGION * {
            if (already_checked != model->table) {
              already_checked = model->table;
              return BuildTopDownCheckerStateCheck(
                  impl, parent, model->table, view.Columns(),
                  BuildStateCheckCaseReturnTrue, BuildStateCheckCaseNothing,
                  [&](ProgramImpl *, REGION *parent) -> REGION * {
                    return BuildTopDownTryMarkAbsent(
                        impl, model->table, parent, view.Columns(),
                        [&](PARALLEL *par) {
                          call_pred(par)->ExecuteAlongside(impl, par);
                        });
                  });

            } else {
              table_to_update = nullptr;
              return call_pred(parent);
            }
          });

      proc->body.Emplace(proc, region);
    }

  // Our best option at this point is to just call the predecessor; this tuple's
  // data is not persisted.
  } else {
    const auto check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
        impl, context, proc, view, view_cols, nullptr, pred_view, nullptr);
    proc->body.Emplace(proc, check);
  }
}

void CreateBottomUpTupleRemover(ProgramImpl *impl, Context &context,
                                QueryView view, PROC *proc,
                                TABLE *already_checked) {

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto caller_did_check = already_checked == model->table;
  PARALLEL *parent = nullptr;

  view.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                      std::optional<QueryColumn> out_col) {
    proc->col_id_to_var[out_col->Id()] = proc->VariableFor(impl, in_col);
  });

  if (model->table) {

    // We've already transitioned for this table, so our job is just to pass
    // the buck along, and then eventually we'll terminate recursion.
    if (caller_did_check) {
      parent = impl->parallel_regions.Create(proc);
      proc->body.Emplace(proc, parent);

    // The caller didn't already do a state transition, so we can do it.
    } else {
      auto remove =
          BuildBottomUpTryMarkUnknown(impl, model->table, proc, view.Columns(),
                                      [&](PARALLEL *par) { parent = par; });

      proc->body.Emplace(proc, remove);
      already_checked = model->table;
    }

  // This tuple isn't associated with any persistent storage.
  } else {
    assert(!view.IsUsedByNegation());

    already_checked = nullptr;
    parent = impl->parallel_regions.Create(proc);
    proc->body.Emplace(proc, parent);
  }

  // If this view is used by a negation then we need to go and see if we should
  // do a delete in the negation. This means first double-checking that this is
  // a true delete and not just a speculative one.
  if (view.IsUsedByNegation()) {
    std::vector<QueryColumn> cols;
    for (auto col : view.Columns()) {
      cols.push_back(col);
    }

    const auto checker_proc = GetOrCreateTopDownChecker(
        impl, context, view, cols, already_checked);

    const auto check = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, parent, checker_proc,
        ProgramOperation::kCallProcedureCheckFalse);
    parent->regions.AddUse(check);

    auto i = 0u;
    for (auto col : cols) {
      const auto var = parent->VariableFor(impl, col);
      assert(var != nullptr);
      check->arg_vars.AddUse(var);

      const auto param = checker_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }

    // The call to the top-down checker will have changed the state to
    // absent.
    if (caller_did_check) {
      parent = impl->parallel_regions.Create(check);
      check->body.Emplace(check, parent);

    // Change the tuple's state to mark it as deleted now that we've proven it
    // as such. The above `GetOrCreateTopDownChecker`.
    } else {
      const auto table_remove = BuildChangeState(
          impl, model->table, check, cols, TupleState::kUnknown,
          TupleState::kAbsent);
      check->body.Emplace(check, table_remove);

      parent = impl->parallel_regions.Create(table_remove);
      table_remove->body.Emplace(table_remove, parent);
    }

    // By this point, we know the tuple is gone, and so now we need to tell
    // the negation about the deleted tuple.
    ReAddToNegatedViews(impl, context, parent, view);
  }

  for (auto succ_view : view.Successors()) {

    const auto checker_proc = GetOrCreateBottomUpRemover(
        impl, context, view, succ_view, already_checked);
    const auto call = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, parent, checker_proc);

    auto i = 0u;
    for (auto col : view.Columns()) {
      const auto var = parent->VariableFor(impl, col);
      assert(var != nullptr);
      call->arg_vars.AddUse(var);

      const auto param = checker_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }

    parent->regions.AddUse(call);
  }

  auto ret = impl->operation_regions.CreateDerived<RETURN>(
      proc, ProgramOperation::kReturnFalseFromProcedure);
  ret->ExecuteAfter(impl, parent);
}

}  // namespace hyde
