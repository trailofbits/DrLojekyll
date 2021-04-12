// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for testing the absence of some data in another view.
void BuildEagerNegateRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryNegate negate, Context &context, OP *parent_,
                            TABLE *last_table_) {

  // NOTE(pag): NEGATEs are like simple JOINs, but instead of matching in
  //            another table, we don't want to match in another table. Thus,
  //            data must be present in both sides of the negation, similar to
  //            what is needed for it being required in both sides of a JOIN.
  //
  // TODO(pag): We can probably relax this constraint in some cases, e.g. if
  //            we have a tower of negations. That type of check could get
  //            tricky, though, due to cycles in the data flow graph.
  auto [parent, pred_table, last_table] =
      InTryInsert(impl, context, pred_view, parent_, last_table_);

  const QueryTuple negated_view = negate.NegatedView();
  std::vector<QueryColumn> negated_view_cols;
  for (QueryColumn out_col : negate.NegatedColumns()) {
    const auto i = *(out_col.Index());
    const auto neg_col = negated_view.NthColumn(i);
    VAR * out_col_var = parent->VariableFor(impl, out_col);
    assert(out_col_var != nullptr);
    parent->col_id_to_var[neg_col.Id()] = out_col_var;
    negated_view_cols.push_back(neg_col);
  }

  // Call the top-down checker for the negated view.
  const auto [neg_check, neg_check_call] = CallTopDownChecker(
      impl, context, parent, negated_view, negated_view_cols,
      negated_view, nullptr);
  parent->body.Emplace(parent, neg_check);

  // If the data isn't there, then keep going.
  OP *let = impl->operation_regions.CreateDerived<LET>(neg_check_call);
  neg_check_call->false_body.Emplace(neg_check_call, let);

  // NOTE(pag): A negation can never share the same data model as its
  //            predecessor, as it might not pass through all of its
  //            predecessor's data.
  const QueryView view(negate);
  return BuildEagerInsertionRegions(
      impl, view, context, let, view.Successors(), nullptr  /* last_table */);
}

// Build a top-down checker on a negation.
REGION *BuildTopDownNegationChecker(
    ProgramImpl *impl, Context &context, REGION *proc, QueryNegate negate,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  const QueryView view(negate);
  const QueryTuple negated_view = negate.NegatedView();
  const auto pred_views = view.Predecessors();
  assert(pred_views.size() == 1u);
  const QueryView pred_view = pred_views[0];

  // Negations aren't guaranteed to be persisted, but their inputs (on both
  // sides) are. So, if we don't have all of the columns that we need, then
  // go and find them.
  if (view_cols.size() != view.Columns().size()) {

    // If `already_checked` were not `nullptr`, then it means we have a table
    // for the negation, and that the top-down checker builder should have
    // filled up `view_cols`.
    assert(!already_checked);

    // Map outputs to the inputs we have, and build up a list of inputs we
    // have in `pred_view_cols` so that we can do a table scan of `pred_view`.
    std::vector<QueryColumn> pred_view_cols;
    negate.ForEachUse([&] (QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> out_col) {
      if (out_col && InputColumnRole::kCopied == role &&
          std::find(view_cols.begin(), view_cols.end(), *out_col) !=
              view_cols.end()) {
        VAR * const out_var = proc->VariableFor(impl, *out_col);
        assert(out_var != nullptr);

        proc->col_id_to_var[in_col.Id()] = out_var;
        pred_view_cols.push_back(in_col);
      }
    });

    const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();
    const auto pred_table = pred_model->table;
    assert(pred_table != nullptr);

    // Clear out and re-fill `view_cols`; we'll pass it to a recursive finder
    // function now that we have values for all of the columns (from the scan
    // of `pred_view`).
    view_cols.clear();
    for (auto col : view.Columns()) {
      view_cols.push_back(col);
    }

    SERIES * const seq = impl->series_regions.Create(proc);
    BuildMaybeScanPartial(
        impl, pred_view, pred_view_cols, pred_table, seq,
        [&](REGION *in_scan, bool in_loop) -> REGION * {

          assert(in_loop);

          // Make sure to make the variables for the negation's output columns
          // available to our recursive call.
          negate.ForEachUse([&] (QueryColumn in_col, InputColumnRole role,
                                 std::optional<QueryColumn> out_col) {
            if (out_col && InputColumnRole::kCopied == role) {
              VAR * const in_var = in_scan->VariableFor(impl, in_col);
              in_scan->col_id_to_var[out_col->Id()] = in_var;
            }
          });

          // Recursively call ourselves with all view columns.
          const auto [rec_check, rec_check_call] = CallTopDownChecker(
              impl, context, in_scan, negate, view_cols, negate, nullptr);

          // If any recursive call succeeded, then return true.
          rec_check_call->body.Emplace(
              rec_check_call,
              BuildStateCheckCaseReturnTrue(impl, rec_check_call));

          return rec_check;
        });

    // If nothing in the scan returned true, then return false.
    seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));

    return seq;
  }

  // First, check in the non-negated view.
  const auto [check, check_call] = CallTopDownChecker(
      impl, context, proc, negate, view_cols, pred_view, already_checked);

  std::vector<QueryColumn> negated_view_cols;
  for (QueryColumn out_col : negate.NegatedColumns()) {
    const auto i = *(out_col.Index());
    const auto in_col = negate.NthInputColumn(i);
    const auto neg_col = negated_view.NthColumn(i);
    VAR * out_col_var = check_call->VariableFor(impl, out_col);
    assert(out_col_var != nullptr);
    check_call->col_id_to_var[in_col.Id()] = out_col_var;
    check_call->col_id_to_var[neg_col.Id()] = out_col_var;

    negated_view_cols.push_back(neg_col);
  }

  // If it's there, then we need to make sure it's not in the negated view.
  const auto [neg_check, neg_check_call] = CallTopDownChecker(
      impl, context, check_call, negated_view, negated_view_cols,
      negated_view, nullptr);

  check_call->body.Emplace(check_call, neg_check);

  // If it's in `view` and in `negated_view`, then return false.
  neg_check_call->body.Emplace(
      neg_check_call,
      BuildStateCheckCaseReturnFalse(impl, neg_check_call));

  // If it's in `view` but not in `negated_view`, then return true.
  neg_check_call->false_body.Emplace(
      neg_check_call,
      BuildStateCheckCaseReturnTrue(impl, neg_check_call));

  // If it's not in `view`, then it doesn't matter if it is or isn't in
  // `negated_view`, because we only care about stuff that has previously
  // flowed through the data flow.
  check_call->false_body.Emplace(
      check_call, BuildStateCheckCaseReturnFalse(impl, check_call));

  return check;
}

//static PROC *CreateCheckInNegatedViewProc(ProgramImpl *impl, Context &context,
//                                          QueryNegate negate) {
//  const auto negated_view = negate.NegatedView();
//
//  const auto proc = impl->procedure_regions.Create(
//      impl->next_id++, ProcedureKind::kTupleFinder);
//
//  // Add the parameters to the negated checker procedure, and map all the
//  // variable IDs so that things work nicely.
//  for (auto col : negate.NegatedColumns()) {
//    auto param_var = proc->input_vars.Create(
//        impl->next_id++, VariableRole::kParameter);
//    param_var->query_column = col;
//    proc->col_id_to_var[col.Id()] = param_var;
//
//    const auto col_index = *(col.Index());
//    const auto input_col = negate.NthInputColumn(col_index);
//    proc->col_id_to_var[input_col.Id()] = param_var;
//
//    if (input_col.IsConstantOrConstantRef()) {
//      param_var->query_const = QueryConstant::From(input_col);
//    }
//
//    const auto negated_col = negated_view.Columns()[col_index];
//    proc->col_id_to_var[negated_col.Id()] = param_var;
//  }
//
//
//  std::vector<QueryColumn> view_cols;
//  for (auto col : negated_view.Columns()) {
//    view_cols.push_back(col);
//  }
//
//  // Call the top-down checker on the tuple. If it returns `false` then it
//  // means that we've not found the tuple in the negated view, and so we can
//  // proceed.
//  const auto check = CallTopDownChecker(
//      impl, context, proc, negated_view, view_cols, negated_view, nullptr);
//
//  proc->body.Emplace(proc, check);
//
//  auto ret = CheckInNegatedView(
//    impl, negate, context, proc, true  /* checker return value */,
//    [&] (REGION *if_present) {
//      auto seq = impl->series_regions.Create(if_present);
//      auto change = BuildChangeState(
//          impl, table, seq, view.Columns(),
//          TupleState::kUnknown, TupleState::kAbsent);
//      seq->AddRegion(change);
//
//      auto par = impl->parallel_regions.Create(change);
//      change->body.Emplace(change, par);
//
//      handle_sucesssors(par);
//
//      // Return early after marking the successors.
//      seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));
//
//      return seq;
//    });
//
//  return proc;
//}

void CreateBottomUpNegationRemover(ProgramImpl *impl, Context &context,
                                   QueryView view, OP *parent_,
                                   TABLE *already_removed_) {

  // NOTE(pag): NEGATEs are like simple JOINs, but instead of matching in
  //            another table, we don't want to match in another table. Thus,
  //            data must be present in both sides of the negation, similar to
  //            what is needed for it being required in both sides of a JOIN.
  auto pred_view = view.Predecessors()[0];
  auto [parent, pred_table, already_removed] = InTryMarkUnknown(
        impl, context, pred_view, parent_, already_removed_);

  // Normally, the above `InTryMarkUnknown` shouldn't do anything, but we have
  // it there for completeness. The reason why is because the data modelling
  // requires the predecessor of a negate to have a table, thus it should have
  // don't the unknown marking. If we have a tower of negations then the above
  // may be necessary.

  // NOTE(pag): We defer to downstream in the data flow to figure out if
  //            checking the negated view was even necessary.
  //
  // NOTE(pag): A negation can never share the same data model as its
  //            predecessor, as it might not pass through all of its
  //            predecessor's data.
  BuildEagerRemovalRegions(impl, view, context, parent,
                           view.Successors(), nullptr  /* already_removed */);
//  // Call the successors.
//  auto handle_sucesssors = [&] (PARALLEL *par) {
//    const auto let = impl->operation_regions.CreateDerived<LET>(par);
//    par->AddRegion(let);
//
//    BuildEagerRemovalRegions(impl, view, context, let, view.Successors(),
//                             table);
//  };
//
//  SERIES *seq = impl->series_regions.Create(parent);
//  parent->body.Emplace(parent, seq);
//
//  // The state is now unknown. Check the negated view. If the tuple is present
//  // then change our state to absent and keep going.
//
//  const auto negate = QueryNegate::From(view);
//
//  auto &check_negated_view_proc = context.negation_checker_procs[view];
//  if (!check_negated_view_proc) {
//    check_negated_view_proc = CreateCheckInNegatedViewProc(
//        impl, context, negate);
//  }
//
//  seq->AddRegion(CheckInNegatedView(
//      impl, negate, context, seq, true  /* checker return value */,
//      [&] (REGION *if_present) {
//        auto seq = impl->series_regions.Create(if_present);
//        auto change = BuildChangeState(
//            impl, table, seq, view.Columns(),
//            TupleState::kUnknown, TupleState::kAbsent);
//        seq->AddRegion(change);
//
//        auto par = impl->parallel_regions.Create(change);
//        change->body.Emplace(change, par);
//
//        handle_sucesssors(par);
//
//        // Return early after marking the successors.
//        seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));
//
//        return seq;
//      }));
//
//  // If we're down here then it means that the tuple's data is not in
//  // the negated view (otherwise the above code would have returned false).
//  // We will double check that indeed the data is in our view (now that we're
//  // in an unknown state).
//  const auto pred_view = view.Predecessors()[0];
//
//  std::vector<QueryColumn> pred_cols;
//  for (auto col : negate.InputColumns()) {
//    pred_cols.push_back(col);
//  }
//  for (auto col : negate.InputCopiedColumns()) {
//    pred_cols.push_back(col);
//  }
//
//  // NOTE(pag): Passing `nullptr` because a negation can't share the data
//  //            model of its predecessor, because it represents a subset of
//  //            that data.
//  const auto checker_proc = GetOrCreateTopDownChecker(
//      impl, context, pred_view, pred_cols, nullptr);
//
//  // Now call the checker procedure for our predecessor. If it returns `true`
//  // then it means that the columns are available in our predecessor, the
//  // columns are not in the negated view, and thus we have proved the presence
//  // of this tuple and can stop.
//  const auto check = impl->operation_regions.CreateDerived<CALL>(
//      impl->next_id++, seq, checker_proc);
//
//  COMMENT( check->comment = __FILE__ ": CreateBottomUpNegationRemover"; )
//
//  auto i = 0u;
//  for (auto col : pred_cols) {
//    const auto var = parent->VariableFor(impl, col);
//    assert(var != nullptr);
//    check->arg_vars.AddUse(var);
//    const auto param = checker_proc->input_vars[i++];
//    assert(var->Type() == param->Type());
//    (void) param;
//  }
//
//  seq->AddRegion(check);
//
//  // If we're down here then it means that we've proven that the tuple exists
//  // and so we want to return early.
//  auto seq_in_check = impl->series_regions.Create(check);
//  check->body.Emplace(check, seq_in_check);
//  auto change = BuildChangeState(
//      impl, table, seq_in_check, view.Columns(),
//      TupleState::kAbsentOrUnknown, TupleState::kPresent);
//  seq_in_check->AddRegion(change);
//  seq_in_check->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq_in_check));
//
//  // If we're down here then it means the data isn't present in the negated
//  // view, but it's also not present in our predecessor, so it's time to keep
//  // going.
//
//  change = BuildChangeState(
//      impl, table, seq, view.Columns(),
//      TupleState::kAbsentOrUnknown, TupleState::kAbsent);
//  seq->AddRegion(change);
//
//  auto par = impl->parallel_regions.Create(change);
//  change->body.Emplace(change, par);
//  handle_sucesssors(par);
}

}  // namespace hyde
