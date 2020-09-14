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

// A work item whose `Run` method is invoked after all initialization paths
// into an inductive region have been covered.
class ContinueInductionWorkItem final : public WorkItem {
 public:
  virtual ~ContinueInductionWorkItem(void) {}

  ContinueInductionWorkItem(INDUCTION *induction_,
                            InductionSet *induction_set_)
      : WorkItem(WorkItem::kRunAsLateAsPossible),
        induction(induction_),
        induction_set(induction_set_) {}

  // Find the common ancestor of all initialization regions.
  REGION *FindCommonAncestorOfInitRegions(void) const;

  void Run(ProgramImpl *program, Context &context) override;

 private:
  INDUCTION * const induction;
  InductionSet * const induction_set;
};

// A work item whose `Run` method is invoked after all initialization and
// cyclic paths into an inductive region have been covered.
class FinalizeInductionWorkItem final : public WorkItem {
 public:
  virtual ~FinalizeInductionWorkItem(void) {}

  FinalizeInductionWorkItem(INDUCTION *induction_,
                            InductionSet *induction_set_)
      : WorkItem(WorkItem::kRunAsLateAsPossible),
        induction(induction_),
        induction_set(induction_set_) {}

  void Run(ProgramImpl *program, Context &context) override;

 private:
  INDUCTION * const induction;
  InductionSet * const induction_set;
};

// Find the common ancestor of all initialization regions.
REGION *ContinueInductionWorkItem::FindCommonAncestorOfInitRegions(void) const {
  PROC * const proc = induction->containing_procedure;
  REGION *common_ancestor = nullptr;

  auto num_init_appends = 0u;
  for (const auto &[view, init_appends] : induction->view_to_init_appends) {
    for (REGION * const init_append : init_appends) {
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

  return common_ancestor;
}

// Build the cyclic regions of this INDUCTION.
void ContinueInductionWorkItem::Run(ProgramImpl *program, Context &context) {
  induction->state = INDUCTION::kAccumulatingCycleRegions;

  const auto proc = induction->containing_procedure;

  // Replace the common ancestor with the INDUCTION, and move that common
  // ancestor to be the init region of this induction.
  const auto ancestor_of_inits = FindCommonAncestorOfInitRegions();
  induction->parent = ancestor_of_inits->parent;
  ancestor_of_inits->ReplaceAllUsesWith(induction);
  UseRef<REGION>(induction, ancestor_of_inits).Swap(induction->init_region);

  const auto cycle_par = program->parallel_regions.Create(induction);
  UseRef<REGION>(induction, cycle_par).Swap(induction->cyclic_region);

  // Now build the inductive cycle regions and add them in. We'll do this
  // before we actually add the successor regions in.
  for (auto view : induction_set->merges) {
    auto &vec = induction->view_to_vec[view];
    if (!vec) {
      UseRef<TABLE>(induction, proc->VectorFor(view.Columns())).Swap(vec);
    }

    const auto cycle = program->operation_regions.Create(
        cycle_par, ProgramOperation::kLoopOverInductionInputVector);
    cycle->ExecuteAlongside(program, cycle_par);

    for (auto col : view.Columns()) {
      const auto var = proc->VariableFor(col);
      cycle->variables.AddUse(var);
    }
    cycle->variables.Unique();
    cycle->tables.AddUse(vec.get());

    UseRef<REGION>(induction, cycle).Swap(induction->view_to_cycle_loop[view]);
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // inductive successors.
  for (auto view : induction_set->merges) {
    OP * const cycle = induction->view_to_cycle_loop[view]->AsOperation();
    assert(cycle != nullptr);

    BuildEagerSuccessorRegions(
        program, view, context, cycle, context.inductive_successors[view]);
  }

  // Finally, add in an action to finish off this induction by processing
  // the outputs. It is possible that we're not actually done filling out
  // the INDUCTION's cycles, even after the above, due to WorkItems being
  // added by other nodes.
  const auto action = new FinalizeInductionWorkItem(
      induction, induction_set);
  context.work_list.emplace_back(action);
}

// Build the "output" regions of this induction.
//
// NOTE(pag): This is basically the same as above with some minor differences.
void FinalizeInductionWorkItem::Run(ProgramImpl *program, Context &context) {
  induction->state = INDUCTION::kBuildingOutputRegions;

  const auto proc = induction->containing_procedure;
  const auto cycle_par = program->parallel_regions.Create(induction);
  UseRef<REGION>(induction, cycle_par).Swap(induction->output_region);

  // Now build the inductive output regions and add them in. We'll do this
  // before we actually add the successor regions in.
  for (auto view : induction_set->merges) {
    auto &vec = induction->view_to_vec[view];
    if (!vec) {
      UseRef<TABLE>(induction, proc->VectorFor(view.Columns())).Swap(vec);
    }

    const auto cycle = program->operation_regions.Create(
        cycle_par, ProgramOperation::kLoopOverInductionInputVector);
    cycle->ExecuteAlongside(program, cycle_par);

    for (auto col : view.Columns()) {
      const auto var = proc->VariableFor(col);
      cycle->variables.AddUse(var);
    }
    cycle->variables.Unique();
    cycle->tables.AddUse(vec.get());

    UseRef<REGION>(induction, cycle).Swap(induction->view_to_output_loop[view]);
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // non-inductive successors.
  for (auto view : induction_set->merges) {
    OP * const cycle = induction->view_to_output_loop[view]->AsOperation();
    assert(cycle != nullptr);

    BuildEagerSuccessorRegions(
        program, view, context, cycle, context.noninductive_successors[view]);
  }
}

}  // namespace

// Build an eager region for a `QueryMerge` that is part of an inductive
// loop. This is interesting because we use a WorkItem as a kind of "barrier"
// to accumulate everything leading into the inductions before proceeding.
void BuildEagerInductiveRegion(ProgramImpl *impl, QueryView pred_view,
                               QueryMerge view, Context &context, OP *parent) {

  PROC * const proc = parent->containing_procedure;
  INDUCTION *&induction = context.view_to_induction[view];

  // First, check if we should add this tuple to the induction.
  OP * const insert = impl->operation_regions.Create(
      parent, ProgramOperation::kInsertIntoView);
  for (auto col : view.Columns()) {
    const auto var = proc->VariableFor(col);
    insert->variables.AddUse(var);
  }
  insert->views.AddUse(
      TABLE::GetOrCreate(impl, view.Columns(), pred_view));
  insert->variables.Unique();
  UseRef<REGION>(parent, insert).Swap(parent->body);

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
    UseRef<TABLE>(induction, proc->VectorFor(view.Columns())).Swap(vec);
  }

  // Add a tuple to the input vector.
  OP * const append_to_vec = impl->operation_regions.Create(
      insert, ProgramOperation::kAppendInductionInputToVector);
  append_to_vec->tables.AddUse(vec.get());
  for (auto col : view.Columns()) {
    const auto var = proc->VariableFor(col);
    insert->variables.AddUse(var);
  }

  UseRef<REGION>(insert, append_to_vec).Swap(insert->body);

  switch (induction->state) {
    case INDUCTION::kAccumulatingInputRegions:
      induction->view_to_init_appends.find(view)->second.AddUse(append_to_vec);
      break;
    case INDUCTION::kAccumulatingCycleRegions:
      induction->view_to_cycle_appends.find(view)->second.AddUse(append_to_vec);
      break;
    default:
      assert(false);
      break;
  }
}

}  // namespace hyde
