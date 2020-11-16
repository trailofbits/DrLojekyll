// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

// Build an eager region for performing a comparison.
TUPLECMP *CreateCompareRegion(ProgramImpl *impl, QueryCompare view,
                              Context &context, REGION *parent) {
  auto cmp =
      impl->operation_regions.CreateDerived<TUPLECMP>(parent, view.Operator());

  auto lhs_var = parent->VariableFor(impl, view.InputLHS());
  auto rhs_var = parent->VariableFor(impl, view.InputRHS());

  const auto is_eq = view.Operator() == ComparisonOperator::kEqual;
  if (is_eq && lhs_var->id > rhs_var->id) {
    std::swap(lhs_var, rhs_var);
  }

  cmp->lhs_vars.AddUse(lhs_var);
  cmp->rhs_vars.AddUse(rhs_var);

  if (is_eq) {
    cmp->col_id_to_var.emplace(view.InputLHS().Id(), lhs_var);
    cmp->col_id_to_var.emplace(view.InputRHS().Id(), lhs_var);
    cmp->col_id_to_var.emplace(view.LHS().Id(), lhs_var);

  } else {
    cmp->col_id_to_var.emplace(view.LHS().Id(), lhs_var);
    cmp->col_id_to_var.emplace(view.RHS().Id(), rhs_var);
  }

  return cmp;
}

}  // namespace

// Build an eager region for performing a comparison.
void BuildEagerCompareRegions(ProgramImpl *impl, QueryCompare cmp,
                              Context &context, OP *parent) {
  const QueryView view(cmp);
  const auto pred_view = view.Predecessors()[0];

  // Okay, we need to figure out if we should persist this comparison node.
  // This is a bit tricky. If the comparison sets a condition then definitely
  // we need to. If it doesn't then we need to look at the successors of the
  // comparison. If any successor can receive a deletion then that mans that
  // a bottom-up checker will probably be created for this comparison node.
  // In that case, we will want to create persistent storage for this comparison
  // if this comparison's predecessor (who we have to visit before getting into
  // this function) doesn't have storage that we can depend upon.
  const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();
  auto should_persist = !!view.SetCondition();
  if (!should_persist && !pred_model->table) {
    for (auto succ : view.Successors()) {
      if (succ.IsJoin()) {
        should_persist = true;
        break;
      } else if (succ.CanReceiveDeletions()) {
        should_persist = true;
        break;
      }
    }
  }

  // If we can receive deletions, and if we're in a path where we haven't
  // actually inserted into a view, then we need to go and do a differential
  // insert/update/check.
  TABLE *table = nullptr;
  if (should_persist) {
    table = TABLE::GetOrCreate(impl, view);
    parent = BuildInsertCheck(impl, view, context, parent, table,
                              view.CanReceiveDeletions(), view.Columns());
  }

  const auto check = CreateCompareRegion(impl, cmp, context, parent);
  parent->body.Emplace(parent, check);
  BuildEagerSuccessorRegions(impl, view, context, check,
                             view.Successors(), table);
}

// Build a top-down checker on a compare.
void BuildTopDownCompareChecker(
    ProgramImpl *impl, Context &context, PROC *proc, QueryCompare cmp,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  const QueryView view(cmp);
  const QueryView pred_view = view.Predecessors()[0];
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();

  // Assign the input column IDs to variables for whatever we have. This should
  // match `view_cols` by virtue of the procedure's input variables.
  cmp.ForEachUse([=](QueryColumn in_col, InputColumnRole role,
                 std::optional<QueryColumn> out_col) {
    if (out_col) {
      if (const auto out_var = proc->col_id_to_var[out_col->Id()]; out_var) {
        proc->col_id_to_var.emplace(in_col.Id(), out_var);
      }
    }
  });

  auto series = impl->series_regions.Create(proc);
  proc->body.Emplace(proc, series);

  // If we can, do the check right away.
  bool done_check = false;
  const auto lhs_var = proc->col_id_to_var[cmp.InputLHS().Id()];
  const auto rhs_var = proc->col_id_to_var[cmp.InputRHS().Id()];
  if (lhs_var && rhs_var) {
    if (cmp.Operator() != ComparisonOperator::kEqual) {
      auto check = CreateCompareRegion(impl, cmp, context, series);
      series->regions.AddUse(check);

      // Create a new parent for all checks.
      series = impl->series_regions.Create(check);
      check->body.Emplace(check, series);
      done_check = true;

    // TODO(pag): I think this is reasonable. Basically, we're saying: if we
    //            have the output column for the equality, then we've assigned
    //            the input vars to the equality comparison to the output var,
    //            and so any scans/recursive calls will take in both values, and
    //            if they return `true` then great.
    } else {
      done_check = true;
    }
  }

  // This compare was persisted, thus we can check it. We'll do a partial or
  // complete scan. We need to re-evaluate the condition inside the scan because
  // this compare node may share its data model with its successor (e.g. a
  // merge), and that may share its data model with something else.
  if (model->table) {

    // Compares are conditional, i.e. they might admit fewer tuples through
    // than they are fed, so they never share the data model with their
    // predecessors.
    assert(model->table != pred_model->table);

    auto assign_ins_to_outs = [=] (REGION *region) {
      cmp.ForEachUse([=](QueryColumn in_col, InputColumnRole role,
                     std::optional<QueryColumn> out_col) {
        if (out_col) {
          const auto out_var = region->VariableFor(impl, *out_col);
          assert(out_var != nullptr);
          region->col_id_to_var.emplace(in_col.Id(), out_var);
        }
      });
    };

    auto call_pred = [&] (PARALLEL *parent) {
      parent->regions.AddUse(ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, parent, view, view_cols,
          model->table, pred_view, already_checked));
    };

    auto if_unknown = [&] (ProgramImpl *, REGION *parent) -> REGION * {
      if (done_check) {
        return BuildTopDownTryMarkAbsent(
            impl, model->table, parent, view.Columns(),
            call_pred);

      } else {
        assign_ins_to_outs(parent);
        auto check = CreateCompareRegion(impl, cmp, context, parent);
        check->body.Emplace(check, BuildTopDownTryMarkAbsent(
            impl, model->table, check, view.Columns(),
            call_pred));
        return check;
      }
    };

    series->regions.AddUse(BuildMaybeScanPartial(
        impl, view, view_cols, model->table, series,
        [&] (REGION *parent) -> REGION * {
          if (already_checked != model->table) {
            already_checked = model->table;
            return BuildTopDownCheckerStateCheck(
                impl, parent, model->table, view.Columns(),
                BuildStateCheckCaseReturnTrue,
                BuildStateCheckCaseNothing,
                if_unknown);

          } else {
            // If the model passed in matches the compare's model then it
            // means we should have all of the columns of the comparison
            // and thus would have done the check.
            assert(done_check);

            return ReturnTrueWithUpdateIfPredecessorCallSucceeds(
                  impl, context, parent, view, view_cols,
                  model->table, pred_view, already_checked);
          }
        }));

  // The predecessor has a table; this is good because we can scan it, and
  // then re-check the condition with the found variables.
  } else if (pred_model->table) {

    std::vector<QueryColumn> pred_view_cols;

    // Get a list of output columns of the predecessor that we have.
    cmp.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                   std::optional<QueryColumn>) {
      if (auto in_var = proc->col_id_to_var[in_col.Id()];
          in_var && QueryView::Containing(in_col) == pred_view) {
        pred_view_cols.push_back(in_col);
      }
    });

    // This sucks; we don't really have any of the predecessor columns
    // available :-/
    if (pred_view_cols.empty()) {
      goto handle_worst_case;
    }

    series->regions.AddUse(BuildMaybeScanPartial(
      impl, pred_view, pred_view_cols, pred_model->table, series,
      [&] (REGION *parent) -> REGION * {
        if (done_check) {
          return ReturnTrueWithUpdateIfPredecessorCallSucceeds(
              impl, context, parent, view, view_cols,
              model->table, pred_view, nullptr);

        } else {
          auto check = CreateCompareRegion(impl, cmp, context, parent);
          check->body.Emplace(check, ReturnTrueWithUpdateIfPredecessorCallSucceeds(
              impl, context, parent, view, view_cols,
              model->table, pred_view, nullptr));
          return check;
        }
      }));

  // This compare doesn't have persistent backing, nor does its predecessor,
  // so we have to call down to its predecessor. If we've already done the
  // check and the predecessor call returns `true` then we can return `true`.
  // Otherwise we have bigger problems :-(
  } else {
  handle_worst_case:

    // If we've done the check already then we'll trust things if the
    // recursive call to the predecessor returns true.
    if (done_check) {
      series->regions.AddUse(ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, proc, view, view_cols, nullptr, pred_view,
          nullptr));

    // The issue here is that our codegen model of top-down checking treats
    // predecessors as black boxes. We really need to recover the columns from
    // the predecessor that are used for comparison, so that we can apply the
    // check to them, but we don't (yet) have a way of doing this. This is
    // also kind of a problem for
    } else {
      assert(false && "TODO(pag): Handle worst case of top-down compare checker");
      series->regions.AddUse(BuildStateCheckCaseReturnFalse(impl, series));
    }
  }
}

void CreateBottomUpCompareRemover(ProgramImpl *impl, Context &context,
                                  QueryView view, PROC *proc,
                                  TABLE *already_checked) {
  auto cmp = CreateCompareRegion(impl, QueryCompare::From(view), context, proc);
  proc->body.Emplace(proc, cmp);

  auto parent = impl->parallel_regions.Create(cmp);
  cmp->body.Emplace(cmp, parent);

  for (auto succ_view : view.Successors()) {
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
  ret->ExecuteAfter(impl, proc);
}

}  // namespace hyde
