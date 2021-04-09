// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

template <typename C1, typename C2>
static OP *CheckInNegatedView(ProgramImpl *impl, QueryNegate negate,
                              Context &context, REGION *parent,
                              C1 with_check_present,
                              C2 with_check_absent) {
  const auto let = impl->operation_regions.CreateDerived<LET>(parent);

  std::vector<QueryColumn> view_cols;

  const auto negated_view = negate.NegatedView();
  unsigned col_index = 0u;

  for (auto col : negated_view.Columns()) {
    const QueryColumn in_col = negate.InputColumns()[col_index++];
    auto out_var = let->defined_vars.Create(
        impl->next_id++, VariableRole::kLetBinding);
    out_var->query_column = col;
    if (in_col.IsConstantOrConstantRef()) {
      out_var->query_const = QueryConstant::From(in_col);
    }

    const auto in_var = let->VariableFor(impl, in_col);
    let->used_vars.AddUse(in_var);

    // NOTE(pag): We *don't* want to use `emplace` here because multiple
    //            nodes in a "tower" might all check back on the same negated
    //            view, and we want each check to be associated with logically
    //            different variables.
    let->col_id_to_var[col.Id()] = out_var;
    let->col_id_to_var[in_col.Id()] = out_var;

    view_cols.push_back(col);
  }

  // Call the top-down checker on the tuple. If it returns `false` then it
  // means that we've not found the tuple in the negated view, and so we can
  // proceed.
  const auto [check, check_call] = CallTopDownChecker(
      impl, context, let, negated_view, view_cols, negated_view, nullptr);

  let->body.Emplace(let, check);

  // NOTE(pag): We need the extra `OP *` region here (the `LET`) because
  //            `with_check_absent` might fiddle with `sub_let->body`, and we
  //            can't pass in `check` because we might need to operate in
  //            `false_body`.
  const auto present_let = impl->operation_regions.CreateDerived<LET>(check_call);
  const auto absent_let = impl->operation_regions.CreateDerived<LET>(check_call);

  check_call->body.Emplace(check_call, present_let);
  check_call->false_body.Emplace(check_call, absent_let);

  auto present_ret = with_check_absent(present_let);
  auto absent_ret = with_check_absent(absent_let);

  if (present_ret) {
    assert(present_ret->parent == present_let);
    if (present_let->body.get() != present_ret) {
      assert(!present_let->body);
      present_let->body.Emplace(present_let, present_ret);
    } else {
      assert(present_let->body.get() == present_ret);
    }
  }

  if (absent_ret) {
    assert(absent_ret->parent == absent_let);
    if (absent_let->body.get() != absent_ret) {
      assert(!absent_let->body);
      absent_let->body.Emplace(absent_let, absent_ret);
    } else {
      assert(absent_let->body.get() == absent_ret);
    }
  }

  return let;
}

}  // namespace

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


  LET *let = nullptr;

  // Okay, if we're inside of some kind of check that our predecessor has the
  // data and so now we need to make sure that the negated view doesn't have
  // the data.
  parent->body.Emplace(parent, CheckInNegatedView(
      impl, negate, context, parent,
      [] (OP *) -> REGION * { return nullptr; },
      [&] (OP *if_absent) -> REGION * {
        let = impl->operation_regions.CreateDerived<LET>(if_absent);
        return let;
      }));

  // NOTE(pag): A negation can never share the same data model as its
  //            predecessor, as it might not pass through all of its
  //            predecessor's data.
  const QueryView view(negate);
  return BuildEagerInsertionRegions(
      impl, view, context, let, view.Successors(),
      nullptr  /* last_table */);
}

// Build a top-down checker on a negation.
REGION *BuildTopDownNegationChecker(
    ProgramImpl *impl, Context &context, REGION *proc, QueryNegate negate,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  const QueryView view(negate);
  const auto pred_views = view.Predecessors();
  assert(pred_views.size() == 1u);
  const auto pred_view = pred_views[0];

  // First, check in the non-negated view.
  const auto [check, check_call] = CallTopDownChecker(
      impl, context, proc, negate, view_cols, pred_view, already_checked);

  // If it's there, then we need to make sure it's not in the negated view.
  check_call->body.Emplace(
      check_call, CheckInNegatedView(
          impl, negate, context, check_call,
          [=] (OP *in_check) -> REGION * {
            return BuildStateCheckCaseReturnFalse(impl, in_check);
          },
          [=] (OP *in_check) -> REGION * {
            return BuildStateCheckCaseReturnTrue(impl, in_check);
          }));

  // If it's not there, then we need to make sure it is in the negated view.
  check_call->false_body.Emplace(
      check_call, CheckInNegatedView(
          impl, negate, context, check_call,
          [=] (OP *in_check) -> REGION * {
            return BuildStateCheckCaseReturnTrue(impl, in_check);
          },
          [=] (OP *in_check) -> REGION * {
            return BuildStateCheckCaseReturnFalse(impl, in_check);
          }));

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
