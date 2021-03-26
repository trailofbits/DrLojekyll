// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

// TODO(pag):
//
//    Think about identifying if the output of a JOIN is part of the output
//    of a union or not. That is, classify all pairs of views in terms of
//
//      1) if pred is in an induction, is successor in an induction?
//      2) ...?

namespace hyde {
namespace {

static unsigned MaxDepth(InductionSet *induction_set) {
  unsigned max_depth = 0u;
  for (auto merge : induction_set->all_merges) {
    if (auto depth = merge.Depth(); depth > max_depth) {
      max_depth = depth;
    }
  }
  return max_depth;
}

// A work item whose `Run` method is invoked after all initialization paths
// into an inductive region have been covered.
class ContinueInductionWorkItem final : public WorkItem {
 public:
  virtual ~ContinueInductionWorkItem(void) {}

  ContinueInductionWorkItem(Context &context, INDUCTION *induction_,
                            InductionSet *induction_set_)
      : WorkItem(context,
                 WorkItem::kContinueInductionOrder + MaxDepth(induction_set_)),
        induction(induction_),
        induction_set(induction_set_) {}

  // Find the common ancestor of all initialization regions.
  REGION *FindCommonAncestorOfInitRegions(void) const;

  void Run(ProgramImpl *impl, Context &context) override;

 private:
  INDUCTION *const induction;
  InductionSet *const induction_set;
};

// A work item whose `Run` method is invoked after all initialization and
// cyclic paths into an inductive region have been covered.
class FinalizeInductionWorkItem final : public WorkItem {
 public:
  virtual ~FinalizeInductionWorkItem(void) {}

  FinalizeInductionWorkItem(Context &context, INDUCTION *induction_,
                            InductionSet *induction_set_)
      : WorkItem(context,
                 WorkItem::kFinalizeInductionOrder + MaxDepth(induction_set_)),
        induction(induction_),
        induction_set(induction_set_) {}

  void Run(ProgramImpl *impl, Context &context) override;

 private:
  INDUCTION *const induction;
  InductionSet *const induction_set;
};

// Find the common ancestor of all initialization regions.
REGION *ContinueInductionWorkItem::FindCommonAncestorOfInitRegions(void) const {
  PROC *const proc = induction->containing_procedure;
  REGION *common_ancestor = nullptr;

  for (const auto init_append : induction->init_appends) {
    if (!common_ancestor) {
      common_ancestor = init_append;
    } else {
      common_ancestor = common_ancestor->FindCommonAncestor(init_append);
    }
  }

  if (1u >= induction->init_appends.size()) {
    common_ancestor = proc;
  }

  if (proc == common_ancestor) {
    common_ancestor = proc->body.get();
  }

  return common_ancestor->NearestRegionEnclosedByInduction();
}

// Build the cyclic regions of this INDUCTION.
void ContinueInductionWorkItem::Run(ProgramImpl *impl, Context &context) {
  const auto proc = induction->containing_procedure;

  assert(induction->state == INDUCTION::kAccumulatingInputRegions);
  induction->state = INDUCTION::kAccumulatingCycleRegions;

  // Replace the common ancestor with the INDUCTION, and move that common
  // ancestor to be the init region of this induction.
  const auto ancestor_of_inits = FindCommonAncestorOfInitRegions();
  induction->parent = ancestor_of_inits->parent;
  ancestor_of_inits->ReplaceAllUsesWith(induction);
  induction->init_region.Emplace(induction, ancestor_of_inits);
  ancestor_of_inits->parent = induction;

  // Make sure that we only enter into the cycle accumulation process once.
  assert(!induction->cyclic_region);
  const auto seq = impl->series_regions.Create(induction);
  induction->cyclic_region.Emplace(induction, seq);

  assert(!induction->output_region);
  const auto done_par = impl->parallel_regions.Create(induction);
  induction->output_region.Emplace(induction, done_par);

  // Build the primary structure of the inductive region, which goes through
  // the following phases:
  //    - send current accumulated results in `vec` to the regions that process
  //      the non-inductive output views.
  //    - swap `vec` with `swap_vec`, so that the fixpoint loop can re-fill
  //      `vec`, based off of visiting everything in `swap_vec`.
  //    - clear out `vec` so that we can re-fill it.
  //    - loop over `swap_vec`, passing its data down to the regions associated
  //      with the inductive successor views, thereby leading to us re-filling
  //      `vec`.
  //
  // TODO(pag): Consider adding a sort stage `vec` here?
  const auto output_par = impl->parallel_regions.Create(seq);
  const auto swap_par = impl->parallel_regions.Create(seq);
  const auto clear_par = impl->parallel_regions.Create(seq);
  const auto cycle_par = impl->parallel_regions.Create(seq);

  seq->AddRegion(output_par);
  seq->AddRegion(swap_par);
  seq->AddRegion(clear_par);
  seq->AddRegion(cycle_par);

  // Now build the inductive cycle regions and add them in. We'll do this
  // before we actually add the successor regions in.
  auto merge_index = 0u;
  for (auto merge : induction_set->merges) {
    const auto vec = induction->view_to_vec[merge];
    assert(!!vec);

    // We'll start by looping over `vec`, which holds the inputs, or outputs from
    // the last fixpoint iteration, and we'll send these to the output regions
    // of the induction.
    const auto output = impl->operation_regions.CreateDerived<VECTORLOOP>(
        output_par, ProgramOperation::kLoopOverInductionInputVector);
    output->vector.Emplace(output, vec);
    output_par->AddRegion(output);
    induction->output_cycles.push_back(output);

    // These are a bunch of swap vectors that we use for the sake of allowing
    // ourselves to see the results of the prior iteration, while minimizing
    // the amount of cross-iteration resident data.
    const auto swap_vec =
        proc->VectorFor(impl, VectorKind::kInduction, merge.Columns());

    // We swap the induction vector `vec` with `swap_vec`, so that we can loop
    // over `swap_vec` and in the body of the loop, fill up `vec`.
    const auto swap = impl->operation_regions.CreateDerived<VECTORSWAP>(
        swap_par, ProgramOperation::kSwapInductionVector);
    swap->lhs.Emplace(swap, vec);
    swap->rhs.Emplace(swap, swap_vec);
    swap_par->AddRegion(swap);

    // Clear the induction vector `vec`, now that we've swapped its contents
    // into `swap_vec`. Inside the cycle loop, we'll re-fill up `vec`.
    const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        clear_par, ProgramOperation::kClearInductionInputVector);
    clear->vector.Emplace(clear, vec);
    clear_par->AddRegion(clear);

    // Now we'll loop over `swap_vec`, which holds the inputs, or outputs from
    // the last fixpoint iteration.
    const auto cycle = impl->operation_regions.CreateDerived<VECTORLOOP>(
        cycle_par, ProgramOperation::kLoopOverInductionInputVector);
    cycle->vector.Emplace(cycle, swap_vec);
    cycle_par->AddRegion(cycle);
    induction->fixpoint_cycles.push_back(cycle);

    for (auto col : merge.Columns()) {

      // Add the variables to the output loop.
      const auto output_var = output->defined_vars.Create(
          impl->next_id++, VariableRole::kVectorVariable);
      output_var->query_column = col;
      output->col_id_to_var[col.Id()] = output_var;

      // Add the variables to the fixpoint loop.
      const auto cycle_var = cycle->defined_vars.Create(
          impl->next_id++, VariableRole::kVectorVariable);
      cycle_var->query_column = col;
      cycle->col_id_to_var[col.Id()] = cycle_var;
    }

    // NOTE(pag): At this point, we're done filling up the basics of the
    //            `induction->cyclic_region` and now move on to filling up
    //            `induction->output_region`.

    // In the output region we'll clear out the vectors that we've used.
    const auto done_clear_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            done_par, ProgramOperation::kClearInductionInputVector);
    done_clear_vec->vector.Emplace(done_clear_vec, vec);
    done_par->AddRegion(done_clear_vec);

    const auto done_clear_swap_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            done_par, ProgramOperation::kClearInductionInputVector);
    done_clear_swap_vec->vector.Emplace(done_clear_swap_vec, swap_vec);
    done_par->AddRegion(done_clear_swap_vec);
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // inductive successors.
  merge_index = 0u;
  for (auto merge : induction_set->merges) {
    OP *const cycle = induction->fixpoint_cycles[merge_index++];
    DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE * const table = model->table;
    BuildEagerSuccessorRegions(impl, merge, context, cycle,
                               context.inductive_successors[merge], table);
  }

  // Finally, add in an action to finish off this induction by processing
  // the outputs. It is possible that we're not actually done filling out
  // the INDUCTION's cycles, even after the above, due to WorkItems being
  // added by other nodes.
  const auto action = new FinalizeInductionWorkItem(
      context, induction, induction_set);
  context.work_list.emplace_back(action);
}

// Build the "output" regions of this induction.
//
// NOTE(pag): This is basically the same as above with some minor differences.
void FinalizeInductionWorkItem::Run(ProgramImpl *impl, Context &context) {
  const auto proc = induction->containing_procedure;

  assert(induction->state == INDUCTION::kAccumulatingCycleRegions);
  induction->state = INDUCTION::kBuildingOutputRegions;

  // Pass in the induction vectors to the handlers.
  for (auto merge : induction_set->all_merges) {
    context.view_to_work_item.erase({proc, merge.UniqueId()});
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // non-inductive successors.
  auto vec_index = 0u;
  for (auto merge : induction_set->merges) {
    OP *const cycle = induction->output_cycles[vec_index++];
    DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE * const table = model->table;
    BuildEagerSuccessorRegions(impl, merge, context, cycle,
                               context.noninductive_successors[merge], table);
  }

  // NOTE(pag): We can't add a `return-false` here because an induction
  //            may come along and fill up this procedure with something else.
}

}  // namespace

// Build an eager region for a `QueryMerge` that is part of an inductive
// loop. This is interesting because we use a WorkItem as a kind of "barrier"
// to accumulate everything leading into the inductions before proceeding.
void BuildEagerInductiveRegion(ProgramImpl *impl, QueryView pred_view,
                               QueryMerge view, Context &context, OP *parent,
                               TABLE *last_table) {

  PROC *const proc = parent->containing_procedure;
  INDUCTION *&induction = context.view_to_induction[{proc, view.UniqueId()}];
  DataModel * const model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;

  // First, check if we should add this tuple to the induction.
  if (last_table != table) {
    parent =
        BuildInsertCheck(impl, view, context, parent, table,
                         QueryView(view).CanReceiveDeletions(), view.Columns());
    last_table = table;
  }

  // This is the first time seeing any MERGE associated with this induction.
  // We'll make an INDUCTION, and a work item that will let us explore the
  // cycle of this induction.
  if (!induction) {
    induction = impl->induction_regions.Create(impl, parent);

    const auto induction_set = context.merge_sets[view].FindAs<InductionSet>();

    const auto action = new ContinueInductionWorkItem(
        context, induction, induction_set);
    context.work_list.emplace_back(action);

    for (auto other_view : induction_set->all_merges) {
      context.view_to_induction[{proc, other_view.UniqueId()}] = induction;
    }

    // Add an induction vector for each `QueryMerge` node in this induction
    // set.
    for (auto other_view : induction_set->merges) {
      auto &vec = induction->view_to_vec[other_view];
      assert(!vec);
      vec = proc->VectorFor(impl, VectorKind::kInduction, other_view.Columns());
      induction->vectors.AddUse(vec);
    }

    for (auto other_view : induction_set->all_merges) {
      context.view_to_work_item[{proc, other_view.UniqueId()}] = action;
    }
  }

  auto vec = induction->view_to_vec[view];
  assert(!!vec);

  // Add a tuple to the input vector.
  const auto append_to_vec =
      impl->operation_regions.CreateDerived<VECTORAPPEND>(
          parent, ProgramOperation::kAppendInductionInputToVector);

  for (auto col : view.Columns()) {
    const auto var = parent->VariableFor(impl, col);
    append_to_vec->tuple_vars.AddUse(var);
  }

  parent->body.Emplace(parent, append_to_vec);

  switch (induction->state) {
    case INDUCTION::kAccumulatingInputRegions:
      assert(proc == induction->containing_procedure);
      append_to_vec->vector.Emplace(append_to_vec, vec);
      induction->init_appends.push_back(append_to_vec);
      break;

    case INDUCTION::kAccumulatingCycleRegions:
      assert(proc == induction->containing_procedure);
      append_to_vec->vector.Emplace(append_to_vec, vec);
      induction->cycle_appends.push_back(append_to_vec);
      break;
    default:
      assert(proc == induction->containing_procedure);
      assert(false); break;
  }
}

// Build a top-down checker on an induction.
void BuildTopDownInductionChecker(ProgramImpl *impl, Context &context,
                                  PROC *proc, QueryMerge merge,
                                  std::vector<QueryColumn> &view_cols,
                                  TABLE *already_checked) {
  const QueryView view(merge);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto table = model->table;
  assert(table != nullptr);

  TABLE *table_to_update = table;

  auto build_rule_checks = [&](PARALLEL *par) {
    for (auto pred_view : merge.MergedViews()) {

      // Deletes signal to their successors that data should be deleted, thus
      // there isn't much we can do in terms of actually checking if something
      // is there or not because if we've made it down here, then it *isn't*
      // there.
      if (pred_view.IsDelete()) {
        continue;
      }

      const auto rec_check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, par, view, view_cols, table_to_update, pred_view,
          table);
      COMMENT( rec_check->comment = __FILE__ ": BuildTopDownInductionChecker::build_rule_checks"; )
      par->AddRegion(rec_check);
    }
  };

  auto build_unknown = [&](ProgramImpl *, REGION *parent) -> REGION * {
    return BuildTopDownTryMarkAbsent(impl, table, parent, view.Columns(),
                                     build_rule_checks);
  };

  proc->body.Emplace(
      proc,
      BuildMaybeScanPartial(
          impl, view, view_cols, table, proc,
          [&](REGION *parent, bool) -> REGION * {
            if (already_checked != table) {
              already_checked = table;
              if (view.CanProduceDeletions()) {
                return BuildTopDownCheckerStateCheck(
                    impl, parent, table, view.Columns(),
                    BuildStateCheckCaseReturnTrue, BuildStateCheckCaseNothing,
                    build_unknown);
              } else {
                return BuildTopDownCheckerStateCheck(
                    impl, parent, table, view.Columns(),
                    BuildStateCheckCaseReturnTrue, BuildStateCheckCaseNothing,
                    BuildStateCheckCaseNothing);
              }

            } else if (view.CanProduceDeletions()) {
              table_to_update = nullptr;  // The caller will update.
              return build_unknown(impl, parent);

            // If this induction can't produce deletions, then there's nothing
            // else to do because if it's not present here, then it won't be
            // present in any of the children.
            } else {
              return BuildStateCheckCaseReturnFalse(impl, parent);
            }
          }));
}

}  // namespace hyde
