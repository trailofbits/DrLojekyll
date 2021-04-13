// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

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

//  // If this view is used by a negation then we need to go and see if we should
//  // do a delete in the negation. This means first double-checking that this is
//  // a true delete and not just a speculative one.
//  //
//  // TODO(pag): Consider deferring the processing of the deletion? Is there a
//  //            way to treat it like an induction?
//  if (view.IsUsedByNegation()) {
//    const auto available_cols = ComputeAvailableColumns(view, view.Columns());
//    const auto checker_proc = GetOrCreateTopDownChecker(
//        impl, context, view, available_cols, already_removed);
//
//    const auto check = impl->operation_regions.CreateDerived<CALL>(
//        impl->next_id++, parent, checker_proc);
//
//    COMMENT( check->comment = __FILE__ ": CreateBottomUpTupleRemover"; )
//
//    auto i = 0u;
//    for (auto [wanted_col, avail_col] : available_cols) {
//      const auto var = parent->VariableFor(impl, avail_col);
//      assert(var != nullptr);
//      check->arg_vars.AddUse(var);
//
//      const auto param = checker_proc->input_vars[i++];
//      assert(var->Type() == param->Type());
//      (void) param;
//    }
//
//    parent->AddRegion(check);
//
//    // The checker function returned `false`, so we know the tuple is definitely
//    // gone, and we want to re-add to the negated view.
//    auto tuple_is_gone = impl->parallel_regions.Create(check);
//    check->false_body.Emplace(check, tuple_is_gone);
//
//    // By this point, we know the tuple is gone, and so now we need to tell
//    // the negation about the deleted tuple.
//    ReAddToNegatedViews(impl, context, tuple_is_gone, view);
//
//    // Re-parent to here; if we did the top-down check then we should benefit
//    // from it.
//    parent = tuple_is_gone;
//  }

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
