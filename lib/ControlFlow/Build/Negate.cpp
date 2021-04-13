// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

template <typename T>
static OP *CheckInNegatedView(ProgramImpl *impl, QueryNegate negate,
                              Context &context, REGION *parent,
                              bool call_return_value, T with_check_absent) {
  const auto let = impl->operation_regions.CreateDerived<LET>(parent);

  std::vector<QueryColumn> view_cols;

  const auto negated_view = negate.NegatedView();
  unsigned col_index = 0u;

  for (auto col : negated_view.Columns()) {
    const QueryColumn in_col = negate.InputColumns()[col_index++];
    auto out_var =
        let->defined_vars.Create(impl->next_id++, VariableRole::kLetBinding);
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
  const auto sub_let = impl->operation_regions.CreateDerived<LET>(check_call);
  if (call_return_value) {
    check_call->body.Emplace(check_call, sub_let);
  } else {
    check_call->false_body.Emplace(check_call, sub_let);
  }

  auto ret = with_check_absent(sub_let);
  assert(ret->parent == sub_let);
  assert(!check_call->body != !check_call->false_body);

  if (sub_let->body.get() != ret) {
    assert(!sub_let->body);
    sub_let->body.Emplace(sub_let, ret);
  } else {
    assert(sub_let->body.get() == ret);
  }

  return let;
}

}  // namespace

// Build an eager region for testing the absence of some data in another view.
//
// NOTE(pag): A subtle aspect of negations is that we need to add to the table,
//            *then* check if the tuple is present/absent in the negated view.
//            The reason why is because otherwise, if we detect the presence of
//            something in the negated view, and it is later deleted, then we
//            risk missing out on being able to push data through the negation
//            at the time of the tuple being deleted in the negated view.
void BuildEagerNegateRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryNegate negate, Context &context, OP *parent) {
  const QueryView view(negate);

  DataModel *const model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE *const table = model->table;

  auto seq = impl->series_regions.Create(parent);
  parent->body.Emplace(parent, seq);

  // Prevents race conditions and ensures data is in our index.
  const auto race_check =
      BuildChangeState(impl, table, seq, negate.Columns(), TupleState::kAbsent,
                       TupleState::kUnknown);
  COMMENT(race_check->comment = "Eager insert before negation to prevent race";)
  seq->AddRegion(race_check);

  // Okay, if we're inside of some kind of check that our predecessor has the
  // data and so now we need to make sure that the negated view doesn't have
  // the data.
  seq->AddRegion(CheckInNegatedView(
      impl, negate, context, seq, false /* return value of procedure */,
      [&](OP *if_absent) {
        // If the negated view doesn't have the data then we can add to our
        // view. Force `differential = false` because it'd be redundant, even
        // though this view is specifically differential.
        const auto insert = BuildInsertCheck(impl, view, context, if_absent,
                                             table, false, view.Columns());

        BuildEagerInsertionRegions(impl, view, context, insert,
                                   view.Successors(), table);

        return insert;
      }));
}

// Build a top-down checker on a negation.
void BuildTopDownNegationChecker(ProgramImpl *impl, Context &context,
                                 PROC *proc, QueryNegate negate,
                                 std::vector<QueryColumn> &view_cols,
                                 TABLE *already_checked) {
  const QueryView view(negate);
  const auto pred_views = view.Predecessors();
  assert(pred_views.size() == 1u);
  const auto pred_view = pred_views[0];
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  assert(model->table != nullptr);

  // We've found the tuple in the outputs of `view`, so we don't need to
  // call the successor. We also haven't done the state checking in the parent,
  // so it's up to us to transition the state. However, we do need to go and
  // double check in the negated view.
  auto do_check_on_true_not_checked = [&](ProgramImpl *, REGION *if_present) {
    auto seq = impl->series_regions.Create(if_present);

    // If the tuple isn't present in the negated view then we can return true.
    seq->AddRegion(CheckInNegatedView(
        impl, negate, context, seq, false /* return value of check */,
        [=](REGION *if_absent) {
          return BuildStateCheckCaseReturnTrue(impl, if_absent);
        }));

    // If we're down here, then the tuple is present in the negated view, and
    // we need to mark the tuple as absent. Note that we can't return false from
    // here because otherwise we might break out of a partial tuple scan too
    // early.

    // TODO(pag): Should we call the bottom-up remover here? Calling the bottom-
    //            up remover here would be problematic. But reaching this state
    //            suggests some other problem.

    seq->AddRegion(BuildChangeState(impl, model->table, seq, view_cols,
                                    TupleState::kPresent, TupleState::kAbsent));

    return seq;
  };

  // We've found that the tuple is marked as unknown in the outputs of the
  // view, and we haven't done any state checking in the parent. We need to
  // see if the tuple is present in our predecessor, as well as being absent
  // in the negated view.
  auto do_check_on_unknown_not_checked = [&](ProgramImpl *,
                                             REGION *if_unknown) {
    return BuildTopDownTryMarkAbsent(
        impl, model->table, if_unknown, view.Columns(), [&](PARALLEL *par) {
          par->AddRegion(CheckInNegatedView(
              impl, negate, context, par, /* expected return value */ false,
              [&](REGION *if_absent) {
                return ReturnTrueWithUpdateIfPredecessorCallSucceeds(
                    impl, context, if_absent, view, view_cols, nullptr,
                    pred_view, already_checked);
              }));
        });
  };

  auto do_check_on_unknown_checked = [&](REGION *if_unknown) {
    return CheckInNegatedView(
        impl, negate, context, if_unknown, false /* return value of check */,
        [&](REGION *if_absent) {
          return ReturnTrueWithUpdateIfPredecessorCallSucceeds(
              impl, context, if_absent, view, view_cols, model->table,
              pred_view, already_checked);
        });
  };

  proc->body.Emplace(
      proc, BuildMaybeScanPartial(
                impl, view, view_cols, model->table, proc,
                [&](REGION *in_scan, bool in_loop) -> REGION * {
                  negate.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                                        std::optional<QueryColumn> out_col) {
                    if (out_col) {
                      assert(in_col.Type() == out_col->Type());
                      in_scan->col_id_to_var[in_col.Id()] =
                          in_scan->VariableFor(impl, *out_col);
                    }
                  });

                  assert(view.CanProduceDeletions());

                  if (already_checked != model->table) {
                    already_checked = model->table;

                    auto continue_or_return =
                        in_loop ? BuildStateCheckCaseNothing
                                : BuildStateCheckCaseReturnFalse;

                    return BuildTopDownCheckerStateCheck(
                        impl, in_scan, model->table, view.Columns(),
                        do_check_on_true_not_checked, continue_or_return,
                        do_check_on_unknown_not_checked);

                  // If we're here then it means our caller has found a candidate tuple
                  // in the output of `view` and is responsible for state transitions.
                  // It also means that the state must be `unknown`.
                  } else {
                    return do_check_on_unknown_checked(in_scan);
                  }
                }));
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

  auto [parent, table, already_removed] =
      InTryMarkUnknown(impl, view, parent_, already_removed_);

  BuildEagerRemovalRegions(impl, view, context, parent, view.Successors(),
                           already_removed);

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
