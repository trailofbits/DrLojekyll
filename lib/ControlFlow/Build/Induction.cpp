// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

#include <sstream>

namespace hyde {

// A work item whose `Run` method is invoked after all initialization paths
// into an inductive region have been covered.
class ContinueInductionWorkItem final : public WorkItem {
 public:
  virtual ~ContinueInductionWorkItem(void) {}

  ContinueInductionWorkItem(Context &context, QueryMerge merge,
                            INDUCTION *induction_)
      : WorkItem(context,
                 (kContinueInductionOrder | *(merge.InductionDepthId()))),
        induction(induction_) {}

  // Find the common ancestor of all initialization regions.
  REGION *FindCommonAncestorOfInitRegions(
      const std::vector<REGION *> &regions) const;

  void Run(ProgramImpl *impl, Context &context) override;

  // NOTE(pag): Multiple `ContinueInductionWorkItem` workers might share
  //            the same `induction`.
  INDUCTION *const induction;
};

// A work item whose `Run` method is invoked after all initialization and
// cyclic paths into an inductive region have been covered.
class FinalizeInductionWorkItem final : public WorkItem {
 public:
  virtual ~FinalizeInductionWorkItem(void) {}

  FinalizeInductionWorkItem(Context &context, QueryMerge merge,
                            INDUCTION *induction_)
      : WorkItem(context,
                 (kFinalizeInductionOrder | *(merge.InductionDepthId()))),
        induction(induction_) {}

  void Run(ProgramImpl *impl, Context &context) override;

  // NOTE(pag): Multiple `ContinueInductionWorkItem` workers might share
  //            the same `induction`.
  INDUCTION *const induction;
};

// Find the common ancestor of all initialization regions.
REGION *ContinueInductionWorkItem::FindCommonAncestorOfInitRegions(
    const std::vector<REGION *> &regions) const {

  PROC *const proc = induction->containing_procedure;
  REGION *common_ancestor = nullptr;

  for (const auto init_append : regions) {
    if (!common_ancestor) {
      common_ancestor = init_append;
    } else {
      common_ancestor = common_ancestor->FindCommonAncestor(init_append);
    }
  }

  if (1u >= regions.size()) {
    common_ancestor = proc;
  }

  if (proc == common_ancestor) {
    common_ancestor = proc->body.get();
  }

  // NOTE(pag): We *CAN'T* go any higher than `common_ancestor`, because then
  //            we might accidentally "capture" the vector appends for an
  //            unrelated induction, thereby introducing super weird ordering
  //            problems where an induction A is contained in the init region
  //            of an induction B, and B's fixpoint cycle region appends to
  //            A's induction vector.
  return common_ancestor;
}

namespace {

static bool NeedsInductionCycleVector(QueryMerge merge) {
  return !merge.NonInductivePredecessors().empty() ||
         merge.IsOwnIndirectInductiveSuccessor();
}

static bool NeedsInductionOutputVector(QueryMerge merge) {
  return !merge.NonInductiveSuccessors().empty();
}

static void BuildInductiveSwaps(ProgramImpl *impl, Context &context,
                                INDUCTION *induction, QueryView merge,
                                PARALLEL *clear_par, PARALLEL *unique_par,
                                PARALLEL *swap_par, bool for_add) {

  // NOTE(pag): We can use the same vector for insertion and removal, because
  //            we use `CHECKSTATE` to figure out what to do!
  VECTOR *vec = nullptr;
  if (for_add) {
    vec = induction->view_to_add_vec[merge];
  } else {
    vec = induction->view_to_remove_vec[merge];
  }

  VECTOR * const swap_vec = induction->view_to_swap_vec[merge];

  assert(vec && swap_vec);

  const auto proc = induction->containing_procedure;
  assert(clear_par->containing_procedure == proc);
  assert(swap_par->containing_procedure == proc);
  (void) proc;

  // We start by clearing the swap vector, which may contain results from the
  // prior fixpoint iteration.
  const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
      clear_par, ProgramOperation::kClearInductionVector);
  clear->vector.Emplace(clear, swap_vec);
  clear_par->AddRegion(clear);

  // Next, we'll unique the vector on which we want to operate so that we don't
  // process (too much) redundant stuff, which happens as a result of our
  // opportunistic append /then/ check approach (needed for parallelizing
  // computations).
  const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
      unique_par, ProgramOperation::kSortAndUniqueInductionVector);
  unique->vector.Emplace(unique, vec);
  unique_par->AddRegion(unique);

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
}

static void BuildFixpointLoop(ProgramImpl *impl, Context &context,
                              INDUCTION *induction, QueryMerge merge,
                              PARALLEL *cycle_par, bool for_add) {

  // NOTE(pag): We can use the same vector for insertion and removal, because
  //            we use `CHECKSTATE` to figure out what to do!
  VECTOR * const swap_vec = induction->view_to_swap_vec[merge];
  assert(swap_vec);

  const auto proc = induction->containing_procedure;
  assert(cycle_par->containing_procedure == proc);
  (void) proc;

  // Here  we'll loop over `swap_vec`, which holds the inputs, or outputs from
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

    // Add the variables to the fixpoint loop.
    const auto cycle_var = inductive_cycle->defined_vars.Create(
        impl->next_id++, VariableRole::kVectorVariable);
    cycle_var->query_column = col;
    cycle->col_id_to_var[col.Id()] = cycle_var;
  }

  DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
  TABLE * const table = model->table;
  assert(table != nullptr);


  // If this merge can produce deletions, then it's possible that something
  // which was added to an induction vector has since been removed, and so we
  // can't count on pushing it forward until it is double checked.
  if (induction->is_differential) {

    // We *don't* call a top-down checker function here, and instead do a
    // simple state transition check. Consider the following:
    //
    //            .--- TUPLE1 ---.
    // -- UNION --+              +--- JOIN ---.
    //      |     '--- TUPLE2 ---'            |
    //      '---------------------------------'
    //
    // This roughly models transitive closure. If UNION, TUPLE1, and TUPLE2
    // have different tables / data models, then a deletion flowing into the
    // UNION needs can't be "double checked" via a finder function, otherwise
    // the finder function may be able to too-eagerly prove the presence of the
    // tuple in terms of tables for TUPLE1 and TUPLE2. Thus, in differential
    // updates, we want
    const auto cycle_check = impl->operation_regions.CreateDerived<CHANGESTATE>(
        cycle,
        for_add ? TupleState::kAbsent : TupleState::kPresent  /* from_state */,
        for_add ? TupleState::kPresent : TupleState::kUnknown  /* to_state */);
    cycle_check->table.Emplace(cycle_check, table);

    for (auto col : merge.Columns()) {
      const auto cycle_var = cycle->VariableFor(impl, col);
      assert(cycle_var != nullptr);
      cycle_check->col_values.AddUse(cycle_var);
    }

    // Make everything depending on the output/inductive loop go inside of
    // the context of the checker.
    cycle->body.Emplace(cycle, cycle_check);

    // TODO(pag): Should we invoke a finder?
    cycle = cycle_check;
  }

  auto cycle_body_par = impl->parallel_regions.Create(cycle);
  cycle->body.Emplace(cycle, cycle_body_par);

  // Add a tuple to the output vector. We don't need to compute a worker ID
  // because we know we're dealing with only worker-specific data in this
  // cycle.
  if (NeedsInductionOutputVector(merge)) {

    VECTOR *output_vec = induction->view_to_output_vec[merge];
    assert(output_vec != nullptr);

    const auto append_to_output_vec =
        impl->operation_regions.CreateDerived<VECTORAPPEND>(
            cycle_body_par, ProgramOperation::kAppendToInductionVector);
    append_to_output_vec->vector.Emplace(append_to_output_vec, output_vec);
    for (auto col : merge.Columns()) {
      append_to_output_vec->tuple_vars.AddUse(cycle->VariableFor(impl, col));
    }
    cycle_body_par->AddRegion(append_to_output_vec);
  }

  if (for_add) {
    induction->fixpoint_add_cycles[merge] = cycle_body_par;
  } else {
    induction->fixpoint_remove_cycles[merge] = cycle_body_par;
  }
}

static void BuildOutputLoop(ProgramImpl *impl, Context &context,
                            INDUCTION *induction, QueryMerge merge,
                            PARALLEL *output_par) {

  const auto proc = induction->containing_procedure;
  assert(output_par->containing_procedure == proc);
  (void) proc;

  const auto output_seq = impl->series_regions.Create(output_par);
  output_par->AddRegion(output_seq);

  // In the output region we'll clear out the vectors that we've used.
  VECTOR * const output_vec = induction->view_to_output_vec[merge];
  assert(output_vec != nullptr);
  // NOTE(pag): We can use the same vector for insertion and removal, because
  //            we use `CHECKSTATE` to figure out what to do!

  // We sort & unique the `output_vec`, so that we don't send extraneous
  // stuff forward.
  const auto output_unique =
      impl->operation_regions.CreateDerived<VECTORUNIQUE>(
          output_seq, ProgramOperation::kSortAndUniqueInductionVector);
  output_unique->vector.Emplace(output_unique, output_vec);
  output_seq->AddRegion(output_unique);

  // Then, loop over `output_vec`, which holds the inputs, or outputs from
  // the all iterations, and we'll send these to the output regions
  // of the induction.
  const auto output_cycle = impl->operation_regions.CreateDerived<VECTORLOOP>(
      impl->next_id++, output_seq, ProgramOperation::kLoopOverInductionVector);
  output_cycle->vector.Emplace(output_cycle, output_vec);
  output_seq->AddRegion(output_cycle);
  OP *output = output_cycle;

  // Fill in the variables of the output and inductive cycle loops.
  for (auto col : merge.Columns()) {

    // Add the variables to the output loop.
    const auto output_var = output_cycle->defined_vars.Create(
        impl->next_id++, VariableRole::kVectorVariable);
    output_var->query_column = col;
    output->col_id_to_var[col.Id()] = output_var;
  }

  // If this merge can produce deletions, then it's possible that something
  // which was added to an induction vector has since been removed, and so we
  // can't count on pushing it forward until it is double checked.
  if (merge.CanReceiveDeletions()) {
    const auto available_cols = ComputeAvailableColumns(merge, merge.Columns());
    const auto checker_proc = GetOrCreateTopDownChecker(
        impl, context, merge, available_cols, nullptr);

    // Call the checker procedure in the output cycle.
    const auto output_check = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, output, checker_proc);

    for (auto [merge_col, avail_col] : available_cols) {
      const auto output_var = output->VariableFor(impl, avail_col);
      assert(output_var != nullptr);
      output_check->arg_vars.AddUse(output_var);
    }

    // Make everything depending on the output/inductive loop go inside of
    // the context of the checker.
    output->body.Emplace(output, output_check);

    auto output_added_par = impl->parallel_regions.Create(output_check);
    output_check->body.Emplace(output, output_added_par);

    auto output_removed_par = impl->parallel_regions.Create(output_check);
    output_check->false_body.Emplace(output, output_removed_par);

    induction->output_add_cycles[merge] = output_added_par;
    induction->output_remove_cycles[merge] = output_removed_par;

  } else {
    auto output_added_par = impl->parallel_regions.Create(output);
    output->body.Emplace(output, output_added_par);
    induction->output_add_cycles[merge] = output_added_par;
  }
}

static void BuildInductiveClear(ProgramImpl *impl, Context &context,
                                INDUCTION *induction, QueryView merge,
                                PARALLEL *done_par) {
  // In the output region we'll clear out the vectors that we've used.
  auto add_vec_it = induction->view_to_add_vec.find(merge);
  auto remove_vec_it = induction->view_to_remove_vec.find(merge);
  auto swap_vec_it = induction->view_to_swap_vec.find(merge);

  if (add_vec_it != induction->view_to_add_vec.end() && add_vec_it->second) {
    VECTOR * const add_vec = add_vec_it->second;
    const auto done_clear_add_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            done_par, ProgramOperation::kClearInductionVector);
    done_clear_add_vec->vector.Emplace(done_clear_add_vec, add_vec);
    done_par->AddRegion(done_clear_add_vec);
  }

  if (remove_vec_it != induction->view_to_remove_vec.end() &&
      remove_vec_it->second) {
    VECTOR * const remove_vec = remove_vec_it->second;
    const auto done_clear_remove_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            done_par, ProgramOperation::kClearInductionVector);
    done_clear_remove_vec->vector.Emplace(done_clear_remove_vec, remove_vec);
    done_par->AddRegion(done_clear_remove_vec);
  }

  if (swap_vec_it != induction->view_to_swap_vec.end() && swap_vec_it->second) {
    VECTOR * const swap_vec = swap_vec_it->second;
    const auto done_clear_swap_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            done_par, ProgramOperation::kClearInductionVector);
    done_clear_swap_vec->vector.Emplace(done_clear_swap_vec, swap_vec);
    done_par->AddRegion(done_clear_swap_vec);
  }
}

static void BuildOutputClear(ProgramImpl *impl, Context &context,
                             INDUCTION *induction, QueryView merge,
                             PARALLEL *done_par) {

  // NOTE(pag): We can use the same vector for insertion and removal, because
  //            we use `CHECKSTATE` to figure out what to do!

  VECTOR * const output_vec = induction->view_to_output_vec[merge];
  assert(output_vec);

  const auto proc = induction->containing_procedure;
  assert(done_par->containing_procedure == proc);
  (void) proc;

  // NOTE(pag): At this point, we're done filling up the basics of the
  //            `induction->cyclic_region` and now move on to filling up
  //            `induction->output_region`.

  const auto done_clear_output_vec =
      impl->operation_regions.CreateDerived<VECTORCLEAR>(
          done_par, ProgramOperation::kClearInductionVector);
  done_clear_output_vec->vector.Emplace(done_clear_output_vec, output_vec);
  done_par->AddRegion(done_clear_output_vec);
}

}  // namespace

// Build the cyclic regions of this INDUCTION.
void ContinueInductionWorkItem::Run(ProgramImpl *impl, Context &context) {

  // Once we run the first continue worker, it means we've reached all inductive
  // unions on the previous frontier, and so we can reset this, so any new
  // reached ones represent a new frontier.
  const auto merge_depth = *(induction->merges[0].InductionDepthId());
  auto &pending_action = context.pending_induction_action[merge_depth];
  assert(pending_action == this);
  pending_action = nullptr;

  assert(induction->state == INDUCTION::kAccumulatingInputRegions);
  induction->state = INDUCTION::kAccumulatingCycleRegions;

  // Replace the common ancestor with the INDUCTION, and move that common
  // ancestor to be the init region of this induction.
  std::vector<REGION *> regions;
  if (auto r1 = FindCommonAncestorOfInitRegions(induction->init_appends_add)) {
    regions.push_back(r1);
  }
  if (auto r2 = FindCommonAncestorOfInitRegions(
          induction->init_appends_remove)) {
    regions.push_back(r2);
  }

  auto ancestor_of_inits = FindCommonAncestorOfInitRegions(regions);
  induction->parent = ancestor_of_inits->parent;
  ancestor_of_inits->ReplaceAllUsesWith(induction);
  induction->init_region.Emplace(induction, ancestor_of_inits);
  ancestor_of_inits->parent = induction;

  // Make sure that we only enter into the cycle accumulation process once.
  assert(!induction->cyclic_region);
  const auto seq = impl->series_regions.Create(induction);
  induction->cyclic_region.Emplace(induction, seq);

  assert(!induction->output_region);
  const auto done_seq = impl->series_regions.Create(induction);
  induction->output_region.Emplace(induction, done_seq);

  const auto output_par = impl->parallel_regions.Create(done_seq);
  const auto done_par = impl->parallel_regions.Create(done_seq);
  done_seq->AddRegion(output_par);
  done_seq->AddRegion(done_par);

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
  const auto clear_remove_par = impl->parallel_regions.Create(seq);
  const auto unique_remove_par = impl->parallel_regions.Create(seq);
  const auto swap_remove_par = impl->parallel_regions.Create(seq);
  const auto cycle_remove_par = impl->parallel_regions.Create(seq);

  const auto clear_add_par = impl->parallel_regions.Create(seq);
  const auto unique_add_par = impl->parallel_regions.Create(seq);
  const auto swap_add_par = impl->parallel_regions.Create(seq);
  const auto cycle_add_par = impl->parallel_regions.Create(seq);

  // NOTE(pag): We need to be careful about the usage of induction and swap
  //            vectors, because the removal process may fill up an insertion
  //            vector, or vice-versa, and we don't want to accidentally lose
  //            data!
  seq->AddRegion(clear_remove_par);
  seq->AddRegion(unique_remove_par);
  seq->AddRegion(swap_remove_par);
  seq->AddRegion(cycle_remove_par);

  seq->AddRegion(clear_add_par);
  seq->AddRegion(unique_add_par);
  seq->AddRegion(swap_add_par);
  seq->AddRegion(cycle_add_par);

  // Now build the inductive cycle regions and add them in. We'll do this
  // before we actually add the successor regions in.
  for (auto merge : induction->merges) {
    const auto has_inputs = NeedsInductionCycleVector(merge);
    const auto has_outputs = NeedsInductionOutputVector(merge);

    if (has_inputs) {
      // If we have to support removals, then do the removals first. We use the
      // same swap vector for insertions/removals.
      if (merge.CanReceiveDeletions()) {
        BuildInductiveSwaps(impl, context, induction, merge,
                            clear_remove_par, unique_remove_par,
                            swap_remove_par, false);

        BuildFixpointLoop(impl, context, induction, merge, cycle_remove_par,
                          false);
      }

      BuildInductiveSwaps(impl, context, induction, merge,
                          clear_add_par, unique_add_par, swap_add_par, true);

      // Build the main loops. The output and cycle regions match.
      BuildFixpointLoop(impl, context, induction, merge, cycle_add_par, true);
      BuildInductiveClear(impl, context, induction, merge, output_par);
    }

    if (has_outputs) {
      BuildOutputLoop(impl, context, induction, merge, output_par);
      BuildOutputClear(impl, context, induction, merge, done_par);
    }
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // inductive successors.
  for (auto merge : induction->merges) {
    if (!NeedsInductionCycleVector(merge)) {
      continue;
    }

    PARALLEL *const cycle_par = induction->fixpoint_add_cycles[merge];
    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE * const table = model->table;
    BuildEagerInsertionRegions(impl, merge, context, cycle,
                               merge.InductiveSuccessors(), table);
  }

  for (auto merge : induction->merges) {
    if (!merge.CanReceiveDeletions() || !NeedsInductionCycleVector(merge)) {
      continue;
    }

    PARALLEL *const cycle_par =
        induction->fixpoint_remove_cycles[merge];

    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE * const table = model->table;

    BuildEagerRemovalRegions(impl, merge, context, cycle,
                             merge.InductiveSuccessors(), table);
  }

  // Finally, add in an action to finish off this induction by processing
  // the outputs. It is possible that we're not actually done filling out
  // the INDUCTION's cycles, even after the above, due to WorkItems being
  // added by other nodes.
  const auto action = new FinalizeInductionWorkItem(
      context, induction->merges[0], induction);
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
  for (auto merge : induction->merges) {
    context.view_to_work_item.erase({proc, merge.UniqueId()});
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // non-inductive successors.
  for (auto merge : induction->merges) {
    if (!NeedsInductionOutputVector(merge)) {
      continue;
    }
    PARALLEL *const cycle_par = induction->output_add_cycles[merge];
    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE * const table = model->table;
    BuildEagerInsertionRegions(impl, merge, context, cycle,
                               merge.NonInductiveSuccessors(), table);
  }

  for (auto merge : induction->merges) {
    if (!merge.CanReceiveDeletions() || !NeedsInductionOutputVector(merge)) {
      continue;
    }
    PARALLEL *const cycle_par = induction->output_remove_cycles[merge];
    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel * const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE * const table = model->table;

    BuildEagerRemovalRegions(impl, merge, context, cycle,
                             merge.NonInductiveSuccessors(), table);
  }

  // NOTE(pag): We can't add a `return-false` here because an induction
  //            may come along and fill up this procedure with something else.
}

namespace {

static INDUCTION *GetOrInitInduction(ProgramImpl *impl, QueryMerge view,
                                     Context &context, OP *parent) {
  PROC *const proc = parent->containing_procedure;
  INDUCTION *&induction = context.view_to_induction[{proc, view.UniqueId()}];

  if (induction) {
    return induction;
  }

  const auto merge_depth = *(view.InductionDepthId());

  // This is the first time seeing any MERGE associated with this induction.
  // We'll make an INDUCTION, and a work item that will let us explore the
  // cycle of this induction.

  // The current "pending" induction. Consider the following:
  //
  //        UNION0        UNION1
  //           \            /
  //            '-- JOIN --'
  //                  |
  //
  // In this case, we don't want UNION0 to be nest inside UNION1 or vice
  // versa, they should both "activate" at the same time. The work list
  // operates in such a way that we exhaust all JOINs before any UNIONs,
  // so in this process, we want to discover the frontiers to as many
  // inductive UNIONs as possible, so that they can all share the same
  // INDUCTION.
  ContinueInductionWorkItem *action = nullptr;
  auto &pending_action = context.pending_induction_action[merge_depth];
  if (pending_action) {
    action = pending_action;
    induction = pending_action->induction;
  } else {
    induction = impl->induction_regions.Create(impl, parent);
    action = new ContinueInductionWorkItem(context, view, induction);
    pending_action = action;
    context.work_list.emplace_back(action);
  }

  for (auto other_view : view.InductiveSet()) {
    const auto other_merge = QueryMerge::From(other_view);

    induction->merges.push_back(other_merge);

    context.view_to_work_item[{proc, other_view.UniqueId()}] = action;
    context.view_to_induction[{proc, other_view.UniqueId()}] = induction;

    // Figure out if the induction can produce deletions. This could feasibly
    // be an over-approximation, i.e. on of the inductions is non-
    // differential, but feeds another induction that is differential. For
    // simplicity we'll assume if one is differential then all are
    // differential.
    if (other_view.CanReceiveDeletions()) {
      induction->is_differential = true;
    }

    // Figure out if we need a vector for tracking additions/removals.
    if (NeedsInductionCycleVector(other_merge)) {
      auto &add_vec = induction->view_to_add_vec[other_view];
      auto &swap_vec = induction->view_to_swap_vec[other_view];

      add_vec = proc->VectorFor(impl, VectorKind::kInductionAdditions,
                                other_view.Columns());
      induction->vectors.AddUse(add_vec);

      // We may also need a vector for removals.
      if (other_view.CanReceiveDeletions()) {
        auto &remove_vec = induction->view_to_remove_vec[other_view];
        remove_vec = proc->VectorFor(impl, VectorKind::kInductionRemovals,
                                     other_view.Columns());
        induction->vectors.AddUse(remove_vec);
      }

      // These are a bunch of swap vectors that we use for the sake of allowing
      // ourselves to see the results of the prior iteration, while minimizing
      // the amount of cross-iteration resident data.
      swap_vec = proc->VectorFor(impl, VectorKind::kInductionSwaps,
                                 other_view.Columns());
    }

    // Figure out if we need a vector to track outputs.
    if (NeedsInductionOutputVector(other_merge)) {
      auto &output_vec = induction->view_to_output_vec[other_view];
      output_vec = proc->VectorFor(impl, VectorKind::kInductionOutputs,
                                   other_view.Columns());
    }
  }

  return induction;
}

static void AppendToInductionVectors(
    ProgramImpl *impl, QueryView view, Context &context, OP *parent,
    INDUCTION *induction, bool for_add) {

  // NOTE(pag): We can use the same vector for insertion and removal, because
  //            we use `CHECKSTATE` to figure out what to do!
  VECTOR *vec = nullptr;

  if (for_add) {
    vec = induction->view_to_add_vec[view];
  } else {
    vec = induction->view_to_remove_vec[view];
  }

  assert(vec != nullptr);

  // Hash the variables together to form a worker ID.
  WORKERID * const hash =
      impl->operation_regions.CreateDerived<WORKERID>(parent);
  VAR * const worker_id = new VAR(impl->next_id++, VariableRole::kWorkerId);
  hash->worker_id.reset(worker_id);
  parent->body.Emplace(parent, hash);

  const auto par = impl->parallel_regions.Create(hash);
  hash->body.Emplace(parent, par);

  // Add a tuple to the removal vector.
  const auto append_to_vec =
      impl->operation_regions.CreateDerived<VECTORAPPEND>(
          par, ProgramOperation::kAppendToInductionVector);
  append_to_vec->vector.Emplace(append_to_vec, vec);
  append_to_vec->worker_id.Emplace(append_to_vec, worker_id);

  for (auto col : view.Columns()) {
    const auto var = par->VariableFor(impl, col);
    hash->hashed_vars.AddUse(var);
    append_to_vec->tuple_vars.AddUse(var);
  }

  par->AddRegion(append_to_vec);

  switch (induction->state) {
    case INDUCTION::kAccumulatingInputRegions:
      if (for_add) {
        induction->init_appends_add.push_back(append_to_vec);
      } else {
        induction->init_appends_remove.push_back(append_to_vec);
      }
      break;

    case INDUCTION::kAccumulatingCycleRegions:
      induction->cycle_appends.push_back(append_to_vec);
      break;

    default:
//      view.SetTableId(99999);
//      break;
      assert(false); break;
  }
}

}  // namespace

// Build an eager region for a `QueryMerge` that is part of an inductive
// loop. This is interesting because we use a WorkItem as a kind of "barrier"
// to accumulate everything leading into the inductions before proceeding.
void BuildEagerInductiveRegion(ProgramImpl *impl, QueryView pred_view,
                               QueryMerge view, Context &context, OP *parent,
                               TABLE *already_added) {
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;
  assert(table != nullptr);
  assert(already_added != table);

  INDUCTION * const induction = GetOrInitInduction(impl, view, context, parent);
  if (induction->view_to_add_vec.count(view)) {
    AppendToInductionVectors(impl, view, context, parent, induction, true);

  } else {
    auto [new_parent, table, last_table] =
        InTryInsert(impl, context, view, parent, already_added,
                    false  /* defer_to_inductions */);
    BuildEagerUnionRegion(impl, pred_view, view, context, new_parent,
                          last_table);
  }
}

void CreateBottomUpInductionRemover(ProgramImpl *impl, Context &context,
                                    QueryView view, OP *parent,
                                    TABLE *already_removed) {
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;
  assert(table != nullptr);
  assert(already_removed != table);

  auto merge = QueryMerge::From(view);
  INDUCTION * const induction = GetOrInitInduction(impl, merge, context, parent);
  if (induction->view_to_remove_vec.count(view)) {
    AppendToInductionVectors(impl, view, context, parent, induction, false);

  } else {
    auto [new_parent, table, last_table] = InTryMarkUnknown(
          impl, context, view, parent, already_removed,
          false  /* defer_to_inductions */);
    CreateBottomUpUnionRemover(impl, context, view, new_parent, last_table);
  }
}

// Build a top-down checker on an induction.
REGION *BuildTopDownInductionChecker(
    ProgramImpl *impl, Context &context, REGION *proc, QueryMerge merge,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  const QueryView view(merge);

  // Organize the checking so that we check the non-inductive predecessors
  // first, then the inductive predecessors.
  //
  // TODO(pag): Break it down further by differential and non-differential?
  const auto seq = impl->series_regions.Create(proc);
  const auto par_init = impl->parallel_regions.Create(seq);
  const auto par_cyclic = impl->parallel_regions.Create(seq);
  seq->AddRegion(par_init);
  seq->AddRegion(par_cyclic);
  seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));

  auto do_rec_check = [&] (QueryView pred_view, PARALLEL *parent) -> REGION * {
    return CallTopDownChecker(
        impl, context, parent, view, view_cols, pred_view, already_checked,
        [=] (REGION *parent_if_true) -> REGION * {
          return BuildStateCheckCaseReturnTrue(impl, parent_if_true);
        },
        [] (REGION *) -> REGION * { return nullptr; });
  };

  // If it's not an inductive predecessor, then check it in `par_init`.
  for (auto pred_view : merge.NonInductivePredecessors()) {
    const auto rec_check = do_rec_check(pred_view, par_init);
    par_init->AddRegion(rec_check);

    COMMENT( rec_check->comment =
        __FILE__ ": BuildTopDownInductionChecker call init predecessor"; )
  }

  // If it's an inductive predecessor, then check it in `par_cyclic`.
  for (auto pred_view : merge.InductivePredecessors()) {
    const auto rec_check = do_rec_check(pred_view, par_cyclic);
    par_cyclic->AddRegion(rec_check);

    COMMENT( rec_check->comment =
        __FILE__ ": BuildTopDownInductionChecker call inductive predecessor"; )
  }

  return seq;
}

}  // namespace hyde
