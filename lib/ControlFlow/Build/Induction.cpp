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

static void BuildInductiveLoops(ProgramImpl *impl, Context &context,
                                INDUCTION *induction, QueryView merge,
                                PARALLEL *clear_par, PARALLEL *swap_par,
                                PARALLEL *cycle_par, PARALLEL *done_par,
                                bool for_add) {
  VECTOR *vec = nullptr;
  if (for_add) {
    vec = induction->view_to_vec[merge];

  } else {
    vec = induction->view_to_removal_vec[merge];
  }

  assert(!!vec);

  const auto proc = induction->containing_procedure;
  assert(clear_par->containing_procedure == proc);
  assert(swap_par->containing_procedure == proc);
  assert(cycle_par->containing_procedure == proc);
  assert(done_par->containing_procedure == proc);

  // These are a bunch of swap vectors that we use for the sake of allowing
  // ourselves to see the results of the prior iteration, while minimizing
  // the amount of cross-iteration resident data.
  VECTOR * const swap_vec = proc->VectorFor(impl, vec->kind, merge.Columns());

  // We start by clearing the swap vector, which may contain results from the
  // prior fixpoint iteration.
  const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
      clear_par, ProgramOperation::kClearInductionVector);
  clear->vector.Emplace(clear, swap_vec);
  clear_par->AddRegion(clear);

  // NOTE(pag): We need to be careful about the usage of induction and swap
  //            vectors, because the removal process may fill up an insertion
  //            vector, or vice-versa, and we don't want to accidentally lose
  //            data!

  // Next, we swap the induction vector `vec` with `swap_vec`, so that
  // we can loop over `swap_vec` and in the body of the loop, fill up `vec`.
  // `vec` is now empty (due to it being cleared above), and `swap_vec` has the
  // prior contents of `vec`.
  const auto swap = impl->operation_regions.CreateDerived<VECTORSWAP>(
      swap_par, ProgramOperation::kSwapInductionVector);
  swap->lhs.Emplace(swap, vec);
  swap->rhs.Emplace(swap, swap_vec);
  swap_par->AddRegion(swap);

  // Now we loop over `swap_vec`, which holds the inputs, or outputs from
  // the last fixpoint iteration, and we'll send these to the output regions
  // of the induction.
  const auto output_cycle = impl->operation_regions.CreateDerived<VECTORLOOP>(
      impl->next_id++, cycle_par, ProgramOperation::kLoopOverInductionVector);
  output_cycle->vector.Emplace(output_cycle, swap_vec);
  cycle_par->AddRegion(output_cycle);
  OP *output = output_cycle;

  // Now we'll loop over `swap_vec`, which holds the inputs, or outputs from
  // the last fixpoint iteration.
  const auto inductive_cycle =
      impl->operation_regions.CreateDerived<VECTORLOOP>(
          impl->next_id++, cycle_par,
          ProgramOperation::kLoopOverInductionVector);
  inductive_cycle->vector.Emplace(inductive_cycle, swap_vec);
  cycle_par->AddRegion(inductive_cycle);
  OP *cycle = inductive_cycle;

  // Fill in the variables of the output and inductive cycle loops.
  for (auto col : merge.Columns()) {

    // Add the variables to the output loop.
    const auto output_var = output_cycle->defined_vars.Create(
        impl->next_id++, VariableRole::kVectorVariable);
    output_var->query_column = col;
    output->col_id_to_var[col.Id()] = output_var;

    // Add the variables to the fixpoint loop.
    const auto cycle_var = inductive_cycle->defined_vars.Create(
        impl->next_id++, VariableRole::kVectorVariable);
    cycle_var->query_column = col;
    cycle->col_id_to_var[col.Id()] = cycle_var;
  }

  // If this merge can produce deletions, then it's possible that something
  // which was added to an induction vector has since been removed, and so we
  // can't count on pushing it forward until it is double checked.
  if (induction->is_differential) {

    std::vector<QueryColumn> available_cols;
    for (auto col : merge.Columns()) {
      available_cols.push_back(col);
    }

    const auto checker_proc = GetOrCreateTopDownChecker(
        impl, context, merge, available_cols, nullptr);

    // Call the checker procedure in the output cycle.
    const auto output_check = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, output, checker_proc);

    // Call the checker procedure in the inductive cycle.
    const auto cycle_check = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, cycle, checker_proc);

    auto i = 0u;
    for (auto col : available_cols) {
      const auto output_var = output->VariableFor(impl, col);
      assert(output_var != nullptr);
      output_check->arg_vars.AddUse(output_var);
      const auto param = checker_proc->input_vars[i++];
      assert(output_var->Type() == param->Type());

      const auto cycle_var = cycle->VariableFor(impl, col);
      assert(cycle_var != nullptr);
      cycle_check->arg_vars.AddUse(cycle_var);
    }

    // Make everything depending on the output/inductive loop go inside of
    // the context of the checker.
    output->body.Emplace(output, output_check);
    cycle->body.Emplace(cycle, cycle_check);

    // If we're adding, then we want the check to pass, which is covered by
    // `OP::body`.
    if (for_add) {
      output = output_check;
      cycle = cycle_check;

    // If we're removing, then we want the check to fail, which is covered by
    // `output_check->false_body`.
    } else {
      auto output_let = impl->operation_regions.CreateDerived<LET>(output_check);
      output_check->false_body.Emplace(output_check, output_let);

      auto cycle_let = impl->operation_regions.CreateDerived<LET>(cycle_check);
      cycle_check->false_body.Emplace(cycle_check, cycle_let);

      output = output_let;
      cycle = cycle_let;
    }
  }

  auto output_body_par = impl->parallel_regions.Create(output);
  output->body.Emplace(output, output_body_par);

  auto cycle_body_par = impl->parallel_regions.Create(cycle);
  cycle->body.Emplace(cycle, cycle_body_par);

  if (for_add) {
    induction->output_cycles.push_back(output_body_par);
    induction->fixpoint_cycles.push_back(cycle_body_par);
  } else {
    induction->output_remove_cycles.push_back(output_body_par);
    induction->fixpoint_remove_cycles.push_back(cycle_body_par);
  }

  // NOTE(pag): At this point, we're done filling up the basics of the
  //            `induction->cyclic_region` and now move on to filling up
  //            `induction->output_region`.

  // In the output region we'll clear out the vectors that we've used.
  const auto done_clear_vec =
      impl->operation_regions.CreateDerived<VECTORCLEAR>(
          done_par, ProgramOperation::kClearInductionVector);
  done_clear_vec->vector.Emplace(done_clear_vec, vec);
  done_par->AddRegion(done_clear_vec);

  const auto done_clear_swap_vec =
      impl->operation_regions.CreateDerived<VECTORCLEAR>(
          done_par, ProgramOperation::kClearInductionVector);
  done_clear_swap_vec->vector.Emplace(done_clear_swap_vec, swap_vec);
  done_par->AddRegion(done_clear_swap_vec);
}

// Build the cyclic regions of this INDUCTION.
void ContinueInductionWorkItem::Run(ProgramImpl *impl, Context &context) {

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
  const auto swap_par = impl->parallel_regions.Create(seq);
  const auto clear_par = impl->parallel_regions.Create(seq);
  const auto cycle_par = impl->parallel_regions.Create(seq);

  // NOTE(pag): We need to be careful about the usage of induction and swap
  //            vectors, because the removal process may fill up an insertion
  //            vector, or vice-versa, and we don't want to accidentally lose
  //            data!
  seq->AddRegion(clear_par);
  seq->AddRegion(swap_par);
  seq->AddRegion(cycle_par);

  // Now build the inductive cycle regions and add them in. We'll do this
  // before we actually add the successor regions in.
  auto merge_index = 0u;
  for (auto merge : induction_set->merges) {
    const auto vec = induction->view_to_vec[merge];
    assert(!!vec);

    // Build the main loops.
    BuildInductiveLoops(impl, context, induction, merge,
                        clear_par, swap_par, cycle_par, done_par, true);

    if (induction->is_differential) {
      BuildInductiveLoops(impl, context, induction, merge,
                          clear_par, swap_par, cycle_par, done_par, false);
    }
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // inductive successors.
  merge_index = 0u;
  for (auto merge : induction_set->merges) {
    PARALLEL *const cycle_par = induction->fixpoint_cycles[merge_index++];
    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE * const table = model->table;
    BuildEagerInsertionRegions(impl, merge, context, cycle,
                               context.inductive_successors[merge], table);
  }

  if (induction->is_differential) {
    merge_index = 0u;
    for (auto merge : induction_set->merges) {
      PARALLEL *const cycle_par = induction->fixpoint_remove_cycles[merge_index++];
      LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
      cycle_par->AddRegion(cycle);

      DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
      TABLE * const table = model->table;

      BuildEagerRemovalRegions(impl, merge, context, cycle,
                               context.inductive_successors[merge], table);
    }
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
  auto merge_index = 0u;
  for (auto merge : induction_set->merges) {
    PARALLEL *const cycle_par = induction->output_cycles[merge_index++];
    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE * const table = model->table;
    BuildEagerInsertionRegions(impl, merge, context, cycle,
                               context.noninductive_successors[merge], table);
  }

  if (induction->is_differential) {
    merge_index = 0u;
    for (auto merge : induction_set->merges) {
      PARALLEL *const cycle_par = induction->output_remove_cycles[merge_index++];
      LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
      cycle_par->AddRegion(cycle);

      DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
      TABLE * const table = model->table;

      BuildEagerRemovalRegions(impl, merge, context, cycle,
                               context.noninductive_successors[merge], table);
    }
  }

  // NOTE(pag): We can't add a `return-false` here because an induction
  //            may come along and fill up this procedure with something else.
}

static std::tuple<PROC *, INDUCTION *, bool>
GetOrInitInduction(ProgramImpl *impl, QueryView view, Context &context,
                   OP *parent) {
  PROC *const proc = parent->containing_procedure;
  INDUCTION *&induction = context.view_to_induction[{proc, view.UniqueId()}];
  auto added = false;

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

      // Figure out if the induction can produce deletions. This could feasibly
      // be an over-approximation, i.e. on of the inductions is non-
      // differential, but feeds another induction that is differential. For
      // simplicity we'll assume if one is differential then all are
      // differential.
      if (other_view.CanProduceDeletions()) {
        induction->is_differential = true;
      }
    }

    // Add an induction vector for each `QueryMerge` node in this induction
    // set.
    for (auto other_view : induction_set->merges) {
      auto &vec = induction->view_to_vec[other_view];
      assert(!vec);
      vec = proc->VectorFor(impl, VectorKind::kInductionAdditions,
                            other_view.Columns());
      induction->vectors.AddUse(vec);

      // If this induction can produce deletions, then we will also need to
      // iterate over those.
      if (induction->is_differential) {
        auto &removal_vec = induction->view_to_removal_vec[other_view];
        assert(!removal_vec);

        removal_vec = proc->VectorFor(impl, VectorKind::kInductionRemovals,
                                      other_view.Columns());
        induction->vectors.AddUse(removal_vec);
      }
    }

    for (auto other_view : induction_set->all_merges) {
      context.view_to_work_item[{proc, other_view.UniqueId()}] = action;
    }

    added = true;
  }

  return {proc, induction, added};
}

}  // namespace

// Build an eager region for a `QueryMerge` that is part of an inductive
// loop. This is interesting because we use a WorkItem as a kind of "barrier"
// to accumulate everything leading into the inductions before proceeding.
void BuildEagerInductiveRegion(ProgramImpl *impl, QueryView pred_view,
                               QueryMerge view, Context &context, OP *parent,
                               TABLE *last_table) {

  DataModel * const model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;
  assert(table != nullptr);

  // First, check if we should add this tuple to the induction.
  if (last_table != table) {
    parent =
        BuildInsertCheck(impl, view, context, parent, table,
                         QueryView(view).CanReceiveDeletions(), view.Columns());
    last_table = table;
  }

  auto [proc, induction, added] = GetOrInitInduction(
      impl, view, context, parent);

  auto vec = induction->view_to_vec[view];
  assert(!!vec);

  // Add a tuple to the input vector.
  const auto append_to_vec =
      impl->operation_regions.CreateDerived<VECTORAPPEND>(
          parent, ProgramOperation::kAppendToInductionVector);

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

void CreateBottomUpInductionRemover(ProgramImpl *impl, Context &context,
                                    QueryView view, OP *parent_,
                                    TABLE *already_removed_) {

  auto [parent, table, already_removed] = InTryMarkUnknown(
      impl, view, parent_, already_removed_);

  auto [proc, induction, added] = GetOrInitInduction(
      impl, view, context, parent);

  VECTOR *vec = induction->view_to_removal_vec[view];
  assert(vec != nullptr);

  // Add a tuple to the removal vector.
  const auto append_to_vec =
      impl->operation_regions.CreateDerived<VECTORAPPEND>(
          parent, ProgramOperation::kAppendToInductionVector);

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
          [&](REGION *parent, bool in_loop) -> REGION * {
            if (already_checked != table) {
              already_checked = table;

              auto continue_or_return = in_loop ? BuildStateCheckCaseNothing :
                                        BuildStateCheckCaseReturnFalse;

              if (view.CanProduceDeletions()) {
                return BuildTopDownCheckerStateCheck(
                    impl, parent, table, view.Columns(),
                    BuildStateCheckCaseReturnTrue, continue_or_return,
                    build_unknown);
              } else {
                return BuildTopDownCheckerStateCheck(
                    impl, parent, table, view.Columns(),
                    BuildStateCheckCaseReturnTrue, continue_or_return,
                    continue_or_return);
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
