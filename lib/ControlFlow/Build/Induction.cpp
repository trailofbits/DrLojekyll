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
  for (auto merge : induction_set->merges) {
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

  ContinueInductionWorkItem(INDUCTION *induction_, InductionSet *induction_set_)
      : WorkItem(WorkItem::kContinueInductionOrder + MaxDepth(induction_set_)),
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

  FinalizeInductionWorkItem(INDUCTION *induction_, InductionSet *induction_set_)
      : WorkItem(WorkItem::kFinalizeInductionOrder + MaxDepth(induction_set_)),
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

  const auto proc = induction->containing_procedure;

  // Replace the common ancestor with the INDUCTION, and move that common
  // ancestor to be the init region of this induction.
  const auto ancestor_of_inits = FindCommonAncestorOfInitRegions();
  induction->parent = ancestor_of_inits->parent;
  ancestor_of_inits->ReplaceAllUsesWith(induction);
  induction->init_region.Emplace(induction, ancestor_of_inits);

  const auto cycle_par = impl->parallel_regions.Create(induction);
  induction->cyclic_region.Emplace(induction, cycle_par);

  // Now build the inductive cycle regions and add them in. We'll do this
  // before we actually add the successor regions in.
  for (auto merge : induction_set->merges) {
    auto &vec = induction->view_to_vec[merge];
    if (!vec) {
      const auto incoming_vec =
          proc->VectorFor(impl, VectorKind::kInduction, merge.Columns());
      vec.Emplace(induction, incoming_vec);
      induction->vectors.AddUse(incoming_vec);
    }

    const auto cycle = impl->operation_regions.CreateDerived<VECTORLOOP>(
        cycle_par, ProgramOperation::kLoopOverInductionInputVector);
    cycle->ExecuteAlongside(impl, cycle_par);

    for (auto col : merge.Columns()) {
      const auto var = cycle->defined_vars.Create(
          impl->next_id++, VariableRole::kVectorVariable);
      var->query_column = col;
      cycle->col_id_to_var.emplace(col.Id(), var);
    }

    cycle->vector.Emplace(cycle, vec.get());
    induction->view_to_cycle_loop[merge].Emplace(induction, cycle);
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // inductive successors.
  for (auto merge : induction_set->merges) {
    OP *const cycle = induction->view_to_cycle_loop[merge]->AsOperation();
    assert(cycle != nullptr);

    const auto table = TABLE::GetOrCreate(impl, merge);
    BuildEagerSuccessorRegions(impl, merge, context, cycle,
                               context.inductive_successors[merge], table);
  }

  // Finally, add in an action to finish off this induction by processing
  // the outputs. It is possible that we're not actually done filling out
  // the INDUCTION's cycles, even after the above, due to WorkItems being
  // added by other nodes.
  const auto action = new FinalizeInductionWorkItem(induction, induction_set);
  context.work_list.emplace_back(action);
}

// Build the "output" regions of this induction.
//
// NOTE(pag): This is basically the same as above with some minor differences.
void FinalizeInductionWorkItem::Run(ProgramImpl *impl, Context &context) {
  induction->state = INDUCTION::kBuildingOutputRegions;

  const auto seq = impl->series_regions.Create(induction);
  induction->output_region.Emplace(induction, seq);

  const auto proc = induction->containing_procedure;
  const auto cycle_par = impl->parallel_regions.Create(seq);
  cycle_par->ExecuteAfter(impl, seq);

  // Now build the inductive output regions and add them in. We'll do this
  // before we actually add the successor regions in.
  for (auto merge : induction_set->merges) {
    context.view_to_work_item.erase(merge);

    auto &vec = induction->view_to_vec[merge];
    if (!vec) {
      const auto incoming_vec =
          proc->VectorFor(impl, VectorKind::kInduction, merge.Columns());
      vec.Emplace(induction, incoming_vec);
      induction->vectors.AddUse(incoming_vec);
    }

    // Clear out the vector after the output loop.
    const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearInductionInputVector);
    clear->vector.Emplace(clear, vec.get());
    clear->ExecuteAfter(impl, seq);

    // Create a loop over the induction vector to process the accumulated
    // results.
    const auto cycle = impl->operation_regions.CreateDerived<VECTORLOOP>(
        cycle_par, ProgramOperation::kLoopOverInductionInputVector);
    cycle->ExecuteAlongside(impl, cycle_par);

    for (auto col : merge.Columns()) {
      const auto var = cycle->defined_vars.Create(
          impl->next_id++, VariableRole::kVectorVariable);
      var->query_column = col;
      cycle->col_id_to_var.emplace(col.Id(), var);
    }

    cycle->vector.Emplace(cycle, vec.get());
    induction->view_to_output_loop[merge].Emplace(induction, cycle);
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // non-inductive successors.
  for (auto merge : induction_set->merges) {
    OP *const cycle = induction->view_to_output_loop[merge]->AsOperation();
    assert(cycle != nullptr);
    const auto table = TABLE::GetOrCreate(impl, merge);
    BuildEagerSuccessorRegions(impl, merge, context, cycle,
                               context.noninductive_successors[merge], table);
  }
}

}  // namespace

// Build an eager region for a `QueryMerge` that is part of an inductive
// loop. This is interesting because we use a WorkItem as a kind of "barrier"
// to accumulate everything leading into the inductions before proceeding.
void BuildEagerInductiveRegion(ProgramImpl *impl, QueryView pred_view,
                               QueryMerge view, Context &context, OP *parent,
                               TABLE *last_model) {

  PROC *const proc = parent->containing_procedure;
  INDUCTION *&induction = context.view_to_induction[view];

  // First, check if we should add this tuple to the induction.
  if (const auto table = TABLE::GetOrCreate(impl, view);
      last_model != table) {
    parent = BuildInsertCheck(impl, view, context, parent, table,
                              QueryView(view).CanReceiveDeletions(),
                              view.Columns());
    last_model = table;
  }

  // This is the first time seeing any MERGE associated with this induction.
  // We'll make an INDUCTION, and a work item that will let us explore the
  // cycle of this induction.
  if (!induction) {
    induction = impl->induction_regions.Create(impl, parent);

    const auto induction_set = context.merge_sets[view].FindAs<InductionSet>();
    const auto action = new ContinueInductionWorkItem(induction, induction_set);
    context.work_list.emplace_back(action);

    for (auto view : induction_set->merges) {
      context.view_to_induction.emplace(view, induction);
      induction->view_to_cycle_appends.emplace(view, induction);
      induction->view_to_init_appends.emplace(view, induction);
    }
  }

  auto &vec = induction->view_to_vec[view];
  if (!vec) {
    const auto incoming_vec =
        proc->VectorFor(impl, VectorKind::kInduction, view.Columns());
    vec.Emplace(induction, incoming_vec);
    induction->vectors.AddUse(incoming_vec);
  }

  // Add a tuple to the input vector.
  const auto append_to_vec =
      impl->operation_regions.CreateDerived<VECTORAPPEND>(
          parent, ProgramOperation::kAppendInductionInputToVector);

  for (auto col : view.Columns()) {
    const auto var = parent->VariableFor(impl, col);
    append_to_vec->tuple_vars.AddUse(var);
  }

  append_to_vec->vector.Emplace(append_to_vec, vec.get());
  parent->body.Emplace(parent, append_to_vec);

  switch (induction->state) {
    case INDUCTION::kAccumulatingInputRegions:
      induction->view_to_init_appends.find(view)->second.AddUse(append_to_vec);
      break;
    case INDUCTION::kAccumulatingCycleRegions:
      induction->view_to_cycle_appends.find(view)->second.AddUse(append_to_vec);
      break;
    default: assert(false); break;
  }
}

// Build a top-down checker on an induction.
void BuildTopDownInductionChecker(
    ProgramImpl *impl, Context &context, PROC *proc,
    QueryMerge view, std::vector<QueryColumn> &view_cols,
    TABLE *) {

  const auto table = TABLE::GetOrCreate(impl, view);

  auto build_rule_checks = [=, &context] (PARALLEL *par) {
    for (auto pred_view : view.MergedViews()) {

      // Deletes signal to their successors that data should be deleted, thus
      // there isn't much we can do in terms of actually checking if something
      // is there or not because if we've made it down here, then it *isn't*
      // there.
      if (pred_view.IsDelete()) {
        continue;
      }

      const auto rec_check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
            impl, context, proc, view, view_cols, table, pred_view,
            table);

      rec_check->ExecuteAlongside(impl, par);
    }
  };

  auto build_unknown = [=] (ProgramImpl *, REGION *parent) -> REGION * {

    // If this induction can't receive deletions, then there's nothing else to
    // do because if it's not present here, then it won't be present in any of
    // the children.
    if (!QueryView::From(view).CanReceiveDeletions()) {
      return BuildStateCheckCaseReturnFalse(impl, parent);
    }

    return BuildTopDownTryMarkAbsent(
        impl, table, parent, view.Columns(), build_rule_checks);
  };

  const auto region = BuildMaybeScanPartial(
      impl, view, view_cols, table, proc,
      [&] (REGION *parent) -> REGION * {

    return BuildTopDownCheckerStateCheck(
        impl, proc, table, view.Columns(),
        BuildStateCheckCaseReturnTrue,
        BuildStateCheckCaseNothing,
        build_unknown);
  });

  proc->body.Emplace(proc, region);
}

}  // namespace hyde
