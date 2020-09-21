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
  UseRef<REGION>(induction, ancestor_of_inits).Swap(induction->init_region);

  const auto cycle_par = impl->parallel_regions.Create(induction);
  UseRef<REGION>(induction, cycle_par).Swap(induction->cyclic_region);

  // Now build the inductive cycle regions and add them in. We'll do this
  // before we actually add the successor regions in.
  for (auto merge : induction_set->merges) {
    auto &vec = induction->view_to_vec[merge];
    if (!vec) {
      const auto incoming_vec =
          proc->VectorFor(impl, VectorKind::kInduction, merge.Columns());
      UseRef<VECTOR>(induction, incoming_vec).Swap(vec);
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
    UseRef<VECTOR>(cycle, vec.get()).Swap(cycle->vector);
    UseRef<REGION>(induction, cycle).Swap(induction->view_to_cycle_loop[merge]);
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
  UseRef<REGION>(induction, seq).Swap(induction->output_region);

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
      UseRef<VECTOR>(induction, incoming_vec).Swap(vec);
      induction->vectors.AddUse(incoming_vec);
    }

    // Clear out the vector after the output loop.
    const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearInductionInputVector);
    UseRef<VECTOR>(clear, vec.get()).Swap(clear->vector);
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

    UseRef<VECTOR>(cycle, vec.get()).Swap(cycle->vector);
    UseRef<REGION>(induction, cycle)
        .Swap(induction->view_to_output_loop[merge]);
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
  const auto table = TABLE::GetOrCreate(impl, view);
  if (last_model != table) {
    const auto insert =
        impl->operation_regions.CreateDerived<TABLEINSERT>(parent);
    UseRef<TABLE>(insert, table).Swap(insert->table);
    UseRef<REGION>(parent, insert).Swap(parent->body);

    for (auto col : view.Columns()) {
      const auto var = parent->VariableFor(impl, col);
      insert->col_values.AddUse(var);
    }

    parent = insert;
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
    UseRef<VECTOR>(induction, incoming_vec).Swap(vec);
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

  UseRef<VECTOR>(append_to_vec, vec.get()).Swap(append_to_vec->vector);
  UseRef<REGION>(parent, append_to_vec).Swap(parent->body);

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

}  // namespace hyde
