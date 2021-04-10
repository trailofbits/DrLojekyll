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
  COMMENT( table_remove->comment = "Remove from negated view"; )

  BuildEagerRemovalRegions(impl, negate, context, table_remove,
                           QueryView(negate).Successors(), table);

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

  // NOTE(pag): Passing `nullptr` because a negation can't share the data
  //            model of its predecessor, because it represents a subset of
  //            that data.
  const auto pred_cols = ComputeAvailableColumns(negate, negate.InputColumns());
  const auto checker_proc = GetOrCreateTopDownChecker(
      impl, context, pred_view, pred_cols, nullptr);

  // Now call the checker procedure for our predecessor. If it returns `true`
  // then it means that the columns are available in our predecessor, the
  // columns are not in the negated view, and thus we have proved the presence
  // of this tuple and can stop.
  const auto check = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, parent, checker_proc);

  COMMENT( check->comment = __FILE__ ": ReAddToNegatedView"; )

  auto i = 0u;
  for (auto [pred_col, avail_col] : pred_cols) {
    const auto var = parent->VariableFor(impl, avail_col);
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

  COMMENT( insert->comment = "Re-adding to negated view"; )

  // Now that we have everything transitioned we can call an eager region on
  // this tuple to re-insert stuff.
//  assert(view.Successors().empty() && "TODO(pag): Think about this.");
  BuildEagerInsertionRegions(impl, view, context, insert, view.Successors(),
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

    const auto seq = impl->series_regions.Create(parent);
    parent->AddRegion(seq);

    for (auto col : view.Columns()) {
      auto in_var = seq->VariableFor(impl, col);
      auto neg_out_col = negate.Columns()[col_index];
      auto neg_in_col = negate.InputColumns()[col_index];
      seq->col_id_to_var[neg_in_col.Id()] = in_var;
      seq->col_id_to_var[neg_out_col.Id()] = in_var;
      ++col_index;
      negate_cols.push_back(neg_out_col);
    }

    // For each thing that we find in the index scan, we will try to push
    // through a re-addition.
    (void) BuildMaybeScanPartial(
        impl, negate, negate_cols, negated_table, seq,
        [&](REGION *in_scan, bool) -> REGION * {

          negate.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                                std::optional<QueryColumn> out_col) {
            if (out_col) {
              in_scan->col_id_to_var[in_col.Id()] =
                  in_scan->VariableFor(impl, *out_col);
            }
          });

          return ReAddToNegatedView(impl, context, in_scan, negate,
                                    negate_cols, negated_table);
        });
  });
}

}  // namespace

// Build an eager region for tuple. If the tuple can receive differential
// updates then its data needs to be saved.
void BuildEagerTupleRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryTuple tuple, Context &context, OP *parent,
                           TABLE *last_table) {
  QueryView view(tuple);
  BuildEagerInsertionRegions(impl, tuple, context, parent, view.Successors(),
                             last_table);
}

// Build a top-down checker on a tuple. This possibly widens the tuple, i.e.
// recovering "lost" columns, and possibly re-orders arguments before calling
// down to the tuple's predecessor's checker.
REGION *BuildTopDownTupleChecker(
    ProgramImpl *impl, Context &context, REGION *proc, QueryTuple tuple,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  const QueryView view(tuple);
  const auto pred_views = view.Predecessors();

  // TODO(pag): Check conditions here!!!

  // This is the case that all inputs are constant. Our caller,
  // `BuildTopDownChecker` does the constant checking for us, because any
  // time a constant can flow up through the data flow, we must check the
  // downward flowing values coming from our caller.
  if (pred_views.empty()) {
    return BuildStateCheckCaseReturnTrue(impl, proc);
  }

  assert(pred_views.size() == 1u);

  // Dispatch to the tuple's predecessor.
  return CallTopDownChecker(
      impl, context, proc, view, view_cols, pred_views[0], already_checked,
      [=] (REGION *parent_if_true) -> REGION * {
        return BuildStateCheckCaseReturnTrue(impl, parent_if_true);
      },
      [=] (REGION *parent_if_false) -> REGION * {
        return BuildStateCheckCaseReturnFalse(impl, parent_if_false);
      });
}

void CreateBottomUpTupleRemover(ProgramImpl *impl, Context &context,
                                QueryView view, OP *root_,
                                TABLE *already_removed_) {

  auto [root, table, already_removed] = InTryMarkUnknown(
      impl, context, view, root_, already_removed_);

  PARALLEL *parent = impl->parallel_regions.Create(root);
  root->body.Emplace(root, parent);

  // If this view is used by a negation then we need to go and see if we should
  // do a delete in the negation. This means first double-checking that this is
  // a true delete and not just a speculative one.
  //
  // TODO(pag): Consider deferring the processing of the deletion? Is there a
  //            way to treat it like an induction?
  if (view.IsUsedByNegation()) {
    const auto available_cols = ComputeAvailableColumns(view, view.Columns());
    const auto checker_proc = GetOrCreateTopDownChecker(
        impl, context, view, available_cols, already_removed);

    const auto check = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, parent, checker_proc);

    COMMENT( check->comment = __FILE__ ": CreateBottomUpTupleRemover"; )

    auto i = 0u;
    for (auto [wanted_col, avail_col] : available_cols) {
      const auto var = parent->VariableFor(impl, avail_col);
      assert(var != nullptr);
      check->arg_vars.AddUse(var);

      const auto param = checker_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }

    parent->AddRegion(check);

    // The checker function returned `false`, so we know the tuple is definitely
    // gone, and we want to re-add to the negated view.
    auto tuple_is_gone = impl->parallel_regions.Create(check);
    check->false_body.Emplace(check, tuple_is_gone);

    // By this point, we know the tuple is gone, and so now we need to tell
    // the negation about the deleted tuple.
    ReAddToNegatedViews(impl, context, tuple_is_gone, view);

    // Re-parent to here; if we did the top-down check then we should benefit
    // from it.
    parent = tuple_is_gone;
  }

  auto let = impl->operation_regions.CreateDerived<LET>(parent);
  parent->AddRegion(let);

  BuildEagerRemovalRegions(impl, view, context, let, view.Successors(),
                           already_removed);

  // NOTE(pag): We don't end this with a `return-false` because removing from
  //            the tuple may trigger the insertion into a negation, which
  //            would be an eager insertion region, which could lead to
  //            something like an induction "taking over" the procedure, and we
  //            wouldn't want to return too early from the induction.
}

}  // namespace hyde
