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

  auto num_init_appends = 0u;
  for (const auto &[view, init_appends] : induction->view_to_init_appends) {
    for (REGION *const init_append : init_appends) {
      ++num_init_appends;
      if (!common_ancestor) {
        common_ancestor = init_append;
      } else {
        common_ancestor = common_ancestor->FindCommonAncestor(init_append);
      }
    }
  }

  if (1u >= num_init_appends) {
    common_ancestor = proc;
  }

  if (proc == common_ancestor) {
    common_ancestor = proc->body.get();
  }

  return common_ancestor->NearestRegionEnclosedByInduction();
}

// Build the cyclic regions of this INDUCTION.
void ContinueInductionWorkItem::Run(ProgramImpl *impl, Context &context) {
  induction->state = INDUCTION::kAccumulatingCycleRegions;

  // Replace the common ancestor with the INDUCTION, and move that common
  // ancestor to be the init region of this induction.
  const auto ancestor_of_inits = FindCommonAncestorOfInitRegions();
  induction->parent = ancestor_of_inits->parent;
  ancestor_of_inits->ReplaceAllUsesWith(induction);
  induction->init_region.Emplace(induction, ancestor_of_inits);
  ancestor_of_inits->parent = induction;

  // Pass in the induction vectors to the handlers.
  for (auto merge : induction_set->merges) {
    context.view_to_work_item.erase({induction->containing_procedure,
                                     merge.UniqueId()});
  }

  // We haven't yet built the cyclic function.
  const auto handler_proc = induction->cycle_proc;
  if (!handler_proc->body) {

    const auto seq = impl->series_regions.Create(handler_proc);
    handler_proc->body.Emplace(handler_proc, seq);

    std::vector<VECTOR *> swap_vecs;
    auto merge_index = 0u;
    for (auto merge : induction_set->merges) {
      const auto vec = handler_proc->input_vecs[merge_index++];
      const auto swap_vec = handler_proc->VectorFor(
          impl, VectorKind::kInduction, merge.Columns());

      // TODO(pag): Consider sorting the vector here?

      const auto swap = impl->operation_regions.CreateDerived<VECTORSWAP>(
          seq, ProgramOperation::kSwapInductionVector);
      seq->regions.AddUse(swap);
      swap->lhs.Emplace(swap, vec);
      swap->rhs.Emplace(swap, swap_vec);

      swap_vecs.push_back(swap_vec);
    }

    // Cycle  through all of the input vectors (by way of the local swapped
    // vectors).
    const auto cycle_par = impl->parallel_regions.Create(seq);
    seq->regions.AddUse(cycle_par);

    std::vector<OP *> cycles;

    // Now build the inductive cycle regions and add them in. We'll do this
    // before we actually add the successor regions in.
    merge_index = 0u;
    for (auto merge : induction_set->merges) {
      const auto input_vec = handler_proc->input_vecs[merge_index];
      const auto swap_vec = swap_vecs[merge_index];
      ++merge_index;

      const auto cycle = impl->operation_regions.CreateDerived<VECTORLOOP>(
          cycle_par, ProgramOperation::kLoopOverInductionInputVector);
      cycle->ExecuteAlongside(impl, cycle_par);

      for (auto col : merge.Columns()) {
        const auto var = cycle->defined_vars.Create(
            impl->next_id++, VariableRole::kVectorVariable);
        var->query_column = col;
        cycle->col_id_to_var[col.Id()] = var;
      }

      cycle->vector.Emplace(cycle, swap_vec);
      induction->view_to_cycle_input_vec.emplace(merge, input_vec);
      induction->view_to_cycle_induction_vec.emplace(merge, swap_vec);

      cycles.push_back(cycle);
    }

    // Now that we have all of the regions arranged and the loops, add in the
    // inductive successors.
    merge_index = 0u;
    for (auto merge : induction_set->merges) {
      OP *const cycle = cycles[merge_index++];
      DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
      TABLE * const table = model->table;
      BuildEagerSuccessorRegions(impl, merge, context, cycle,
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

  induction->state = INDUCTION::kBuildingOutputRegions;

  // Add a terminating return statement to the end of the cycle handler, and
  // clear out all of its local induction vectors.
  PROC * const cycle_handler = induction->cycle_proc;
  if (!EndsWithReturn(cycle_handler)) {
    const auto seq = impl->series_regions.Create(cycle_handler);
    seq->regions.AddUse(cycle_handler->body.get());
    cycle_handler->body.Emplace(cycle_handler, seq);

    for (auto [merge, vec] : induction->view_to_cycle_induction_vec) {
      const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
          seq, ProgramOperation::kClearInductionInputVector);
      clear->vector.Emplace(clear, vec);
      seq->regions.AddUse(clear);
      (void) merge;
    }

    seq->regions.AddUse(BuildStateCheckCaseReturnFalse(impl, seq));
  }

  // Nothing to do if we've already generated code for the output region.
  const auto handler_proc = induction->output_proc;
  if (handler_proc->body.get()) {
    return;
  }

  const auto cycle_par = impl->parallel_regions.Create(handler_proc);
  handler_proc->body.Emplace(handler_proc, cycle_par);

  std::vector<OP *> cycles;

  auto vec_index = 0u;
  for (auto merge : induction_set->merges) {

    // TODO(pag): Consider sorting the vector here?

    // Create a loop over the induction vector to process the accumulated
    // results.
    const auto cycle = impl->operation_regions.CreateDerived<VECTORLOOP>(
        cycle_par, ProgramOperation::kLoopOverInductionInputVector);
    cycle_par->regions.AddUse(cycle);

    for (auto col : merge.Columns()) {
      const auto var = cycle->defined_vars.Create(
          impl->next_id++, VariableRole::kVectorVariable);
      var->query_column = col;
      cycle->col_id_to_var[col.Id()] = var;
    }

    cycle->vector.Emplace(cycle, handler_proc->input_vecs[vec_index++]);
    cycles.push_back(cycle);
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // non-inductive successors.
  vec_index = 0u;
  for (auto merge : induction_set->merges) {
    OP *const cycle = cycles[vec_index++];
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
    for (auto other_view : induction_set->merges) {
      context.view_to_induction[{proc, other_view.UniqueId()}] = induction;
      induction->view_to_init_appends.emplace(other_view, induction);
    }

    const auto action = new ContinueInductionWorkItem(
        context, induction, induction_set);
    context.work_list.emplace_back(action);

    // Create the inductive cycle procedure if it's missing.
    auto &cycle_proc = context.induction_cycle_funcs[induction_set];
    if (!cycle_proc) {
      cycle_proc = impl->procedure_regions.Create(
          impl->next_id++, ProcedureKind::kInductionCycleHandler);

      // Add in the vector parameters for this induction handler procedure.
      for (auto merge : induction_set->merges) {
        cycle_proc->VectorFor(impl, VectorKind::kInputOutputParameter,
                              merge.Columns());
      }
    }

    // Tell the induction about the vector parameters for the handler. Done
    // separately if the induction handler is created separately.
    auto vec_index = 0u;
    for (auto merge : induction_set->merges) {
      const auto input_vec = cycle_proc->input_vecs[vec_index++];
      induction->view_to_cycle_input_vec.emplace(merge, input_vec);
    }

    // Create the output procedure if it's missing.
    auto &output_proc = context.induction_output_funcs[induction_set];
    if (!output_proc) {
      output_proc = impl->procedure_regions.Create(
          impl->next_id++, ProcedureKind::kInductionOutputHandler);

      // Add in the vector parameters for this induction handler procedure.
      for (auto merge : induction_set->merges) {
        (void) output_proc->VectorFor(
            impl, VectorKind::kParameter, merge.Columns());
      }
    }

    induction->cycle_proc = cycle_proc;
    induction->output_proc = output_proc;

    // Add an induction vector for each `QueryMerge` node in this induction
    // set.
    for (auto merge : induction_set->merges) {
      auto &vec = induction->view_to_vec[merge];
      const auto incoming_vec =
          proc->VectorFor(impl, VectorKind::kInduction, merge.Columns());
      vec.Emplace(induction, incoming_vec);
      induction->vectors.AddUse(incoming_vec);
    }

    const auto cycle_seq = impl->series_regions.Create(induction);
    induction->cyclic_region.Emplace(induction, cycle_seq);

    const auto output_seq = impl->series_regions.Create(induction);
    induction->output_region.Emplace(induction, output_seq);

    // Call the cycle function and the output function in the induction's
    // cycle region.
    const auto call_output = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, cycle_seq, output_proc);

    const auto call_cycle = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, cycle_seq, cycle_proc);

    // NOTE(pag): We call the output function *first*, before calling the
    //            cycle function! This is so that we can immediately process
    //            the induction's base case through the rest of the dataflow,
    //            then we use the base base to build up the next case in the
    //            cycle function, which does vector referencing/swapping.
    cycle_seq->regions.AddUse(call_output);
    cycle_seq->regions.AddUse(call_cycle);

    // Now build the inductive output regions and add them in. We'll do this
    // before we actually add the successor regions in.
    for (auto merge : induction_set->merges) {
      context.view_to_work_item[{proc, merge.UniqueId()}] = action;

      const auto vec = induction->view_to_vec[merge].get();
      assert(!!vec);

      call_cycle->arg_vecs.AddUse(vec);
      call_output->arg_vecs.AddUse(vec);

      // Clear out the induction vectors in the induction's output region.
      const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
          output_seq, ProgramOperation::kClearInductionInputVector);
      clear->vector.Emplace(clear, vec);

      output_seq->regions.AddUse(clear);
    }
  } else {
    assert(proc == induction->containing_procedure);
  }

  auto &vec = induction->view_to_vec[view];
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
      append_to_vec->vector.Emplace(append_to_vec, vec.get());
      induction->view_to_init_appends.find(view)->second.AddUse(append_to_vec);
      break;
    case INDUCTION::kAccumulatingCycleRegions:
      if (auto input_vec = induction->view_to_cycle_input_vec[view]; input_vec) {
        append_to_vec->vector.Emplace(append_to_vec, input_vec);
      } else {
        assert(false);
      }
      break;
    default:
      assert(false); break;
  }
}

// Build a top-down checker on an induction.
void BuildTopDownInductionChecker(ProgramImpl *impl, Context &context,
                                  PROC *proc, QueryMerge view,
                                  std::vector<QueryColumn> &view_cols,
                                  TABLE *already_checked) {

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto table = model->table;
  assert(table != nullptr);

  TABLE *table_to_update = table;

  auto build_rule_checks = [=, &context, &view_cols](PARALLEL *par) {
    for (auto pred_view : view.MergedViews()) {

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
      rec_check->comment = __FILE__ ": BuildTopDownInductionChecker::build_rule_checks";
      par->regions.AddUse(rec_check);
    }
  };

  auto build_unknown = [&](ProgramImpl *, REGION *parent) -> REGION * {
    // If this induction can't receive deletions, then there's nothing else to
    // do because if it's not present here, then it won't be present in any of
    // the children.
    if (!QueryView::From(view).CanReceiveDeletions()) {
      return BuildStateCheckCaseReturnFalse(impl, parent);
    }

    return BuildTopDownTryMarkAbsent(impl, table, parent, view.Columns(),
                                     build_rule_checks);
  };

  proc->body.Emplace(
      proc,
      BuildMaybeScanPartial(
          impl, view, view_cols, table, proc, [&](REGION *parent) -> REGION * {
            if (already_checked != table) {
              already_checked = table;
              return BuildTopDownCheckerStateCheck(
                  impl, parent, table, view.Columns(),
                  BuildStateCheckCaseReturnTrue, BuildStateCheckCaseNothing,
                  build_unknown);

            } else {
              table_to_update = nullptr;  // The caller will update.
              return build_unknown(impl, parent);
            }
          }));
}

}  // namespace hyde
