// Copyright 2020, Trail of Bits. All rights reserved.

#include "Induction.h"

#include <sstream>

namespace hyde {

bool NeedsInductionCycleVector(QueryView view) {
  if (view.InductionGroupId().has_value()) {
    if (view.IsMerge()) {
      return true;  // TODO(pag): Needed to avoid stack overflow?! :O
    } else {
      return !view.NonInductivePredecessors().empty() ||
             view.IsOwnIndirectInductiveSuccessor();
    }
  } else {
    return false;
  }
}

bool NeedsInductionOutputVector(QueryView view) {
  return !view.NonInductiveSuccessors().empty();
}

ContinueInductionWorkItem::ContinueInductionWorkItem(Context &context,
                                                     QueryView merge,
                                                     INDUCTION *induction_)
    : WorkItem(context,
               (kContinueInductionOrder |
                (merge.InductionDepth().value() << kInductionDepthShift))),
      induction(induction_) {}

// A work item whose `Run` method is invoked after all initialization and
// cyclic paths into an inductive region have been covered.
class FinalizeInductionWorkItem final : public WorkItem {
 public:
  virtual ~FinalizeInductionWorkItem(void) {}

  FinalizeInductionWorkItem(Context &context, QueryView merge,
                            INDUCTION *induction_)
      : WorkItem(context,
                 (kContinueInductionOrder | kFinalizeInductionOrder |
                  (merge.InductionDepth().value() << kInductionDepthShift))),
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

static void BuildInductiveSwaps(ProgramImpl *impl, Context &context,
                                INDUCTION *induction, QueryView vec_view,
                                QueryView inductive_view, SERIES *view_seq) {

  // NOTE(pag): We can use the same vector for insertion and removal, because
  //            we use `CHECKTUPLE` to figure out what to do!
  VECTOR *const vec = induction->view_to_add_vec[vec_view];
  VECTOR *const swap_vec = induction->view_to_swap_vec[vec_view];

  assert(vec && swap_vec);

  // In the case of a PRODUCT that straddles the border of an inductive region,
  // we need to make sure that the vectors associated with the non-inductive
  // predecessors are not cleared out.
  if (swap_vec == vec) {
    assert(inductive_view.IsJoin());
    assert(!QueryJoin::From(inductive_view).NumPivotColumns());
  }

  // We start by clearing the swap vector, which may contain results from the
  // prior fixpoint iteration.
  const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
      view_seq, ProgramOperation::kClearInductionVector);
  clear->vector.Emplace(clear, swap_vec);
  view_seq->AddRegion(clear);

  // Next, we'll unique the vector on which we want to operate so that we don't
  // process (too much) redundant stuff, which happens as a result of our
  // opportunistic append /then/ check approach (needed for parallelizing
  // computations).
  const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
      view_seq, ProgramOperation::kSortAndUniqueInductionVector);
  unique->vector.Emplace(unique, vec);
  view_seq->AddRegion(unique);

  // NOTE(pag): We need to be careful about the usage of induction and swap
  //            vectors, because the removal process may fill up an insertion
  //            vector, or vice-versa, and we don't want to accidentally lose
  //            data!

  // Next, we swap the induction vector `vec` with `swap_vec`, so that
  // we can loop over `swap_vec` and in the body of the loop, fill up `vec`.
  // `vec` is now empty (due to it being cleared above), and `swap_vec` has the
  // prior contents of `vec`.
  const auto swap = impl->operation_regions.CreateDerived<VECTORSWAP>(
      view_seq, ProgramOperation::kSwapInductionVector);
  swap->lhs.Emplace(swap, vec);
  swap->rhs.Emplace(swap, swap_vec);
  view_seq->AddRegion(swap);
}

static void BuildFixpointLoop(ProgramImpl *impl, Context &context,
                              INDUCTION *induction, QueryView view,
                              PARALLEL *cycle_par, bool for_add) {


  OP *cycle = nullptr;
  PARALLEL *cycle_body_par = nullptr;


  // Fill in the variables of the output and inductive cycle loops.
  std::vector<QueryColumn> view_cols;
  for (auto col : view.Columns()) {
    view_cols.push_back(col);
  }

  if (view.IsMerge()) {

    // NOTE(pag): We can use the same vector for insertion and removal, because
    //            we use `CHECKTUPLE` to figure out what to do!
    VECTOR *const swap_vec = induction->view_to_swap_vec[view];
    assert(swap_vec);

    // Here  we'll loop over `swap_vec`, which holds the inputs, or outputs from
    // the last fixpoint iteration.
    const auto inductive_cycle =
        impl->operation_regions.CreateDerived<VECTORLOOP>(
            impl->next_id++, cycle_par,
            ProgramOperation::kLoopOverInductionVector);
    inductive_cycle->vector.Emplace(inductive_cycle, swap_vec);
    cycle_par->AddRegion(inductive_cycle);
    cycle = inductive_cycle;

    DataModel *const model = impl->view_to_model[view]->FindAs<DataModel>();
    TABLE *const table = model->table;
    assert(table != nullptr);

    // Keep track of whether or not this loop is connected with a table for
    // an inductive UNION. This helps us later on in analyzing the control-flow
    // IR to find the provenance of column data.
    inductive_cycle->induction_table.Emplace(inductive_cycle, table);

    // Fill in the variables of the output and inductive cycle loops.
    for (auto col : view_cols) {
      const auto cycle_var = inductive_cycle->defined_vars.Create(
          impl->next_id++, VariableRole::kVectorVariable);
      cycle_var->query_column = col;
      cycle->col_id_to_var[col.Id()] = cycle_var;
    }

    // If this merge can produce deletions, then it's possible that something
    // which was added to an induction vector has since been removed, and so we
    // can't count on pushing it forward until it is double checked.
    if (view.CanProduceDeletions()) {

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
      // UNION can't be "double checked" via a finder function, otherwise
      // the finder function may be able to too-eagerly prove the presence of the
      // tuple in terms of tables for TUPLE1 and TUPLE2. Thus, in differential
      // updates, we want to make sure that we push the deletions through as far
      // as possible.
      OP *parent_out = nullptr;
      CHECKTUPLE *const check = BuildTopDownCheckerStateCheck(
          impl, cycle, table, view_cols,
          [&](ProgramImpl *impl_, REGION *in_check) -> OP * {
            if (for_add) {
              parent_out =
                  impl_->operation_regions.CreateDerived<LET>(in_check);
              return parent_out;
            } else {
              return nullptr;
            }
          },
          BuildStateCheckCaseNothing,
          [&](ProgramImpl *impl_, REGION *in_check) -> OP * {
            if (!for_add) {
              parent_out =
                  impl_->operation_regions.CreateDerived<LET>(in_check);
              return parent_out;
            } else {
              return nullptr;
            }
          });
      cycle->body.Emplace(cycle, check);

      // Make everything depending on the output/inductive loop go inside of
      // the context of the checker.
      cycle = parent_out;
    }

    cycle_body_par = impl->parallel_regions.Create(cycle);
    cycle->body.Emplace(cycle, cycle_body_par);

  // Negation fixpoint loops are similar but different than merge fixpoint
  // loops. With inductive unions, we can't call a top-down checker because
  // we need to "removal" status pushed around the loop. E.g. in the case of
  // transitive closure, of the form `tc(F, T) : tc(F, X), tc(X, T).`, the join
  // is entirely contained in the inductive cycle, and the base case, and thus
  // removal of the base case comes from a non-inductive predecessor. If an
  // edge was provable by the join, but was ultimately derived via the edge
  // itself, then the removal of the edge might be ignored if we did a top-down
  // check, as the check would find the data in the join itself. We don't have
  // this issue with inductive negations because we know that inductive
  // necessarily straddle the inside and outside of an inductive cycle. Unlike
  // inductive MERGEs, we can't depend on the presence of a TABLE, so we'll
  // invoke the top-down checker and let it figure out how to verify if the
  // data is present or absent.
  } else if (view.IsNegate()) {
    assert(view.InductivePredecessors().size() == 1u);
    assert(view.NonInductivePredecessors().size() == 1u);

    // NOTE(pag): We can use the same vector for insertion and removal, because
    //            we use a call to a top-down checker to decide if we're adding
    //            or removing.
    VECTOR *const swap_vec = induction->view_to_swap_vec[view];
    assert(swap_vec);

    // Here  we'll loop over `swap_vec`, which holds the inputs, or outputs from
    // the last fixpoint iteration.
    const auto inductive_cycle =
        impl->operation_regions.CreateDerived<VECTORLOOP>(
            impl->next_id++, cycle_par,
            ProgramOperation::kLoopOverInductionVector);
    inductive_cycle->vector.Emplace(inductive_cycle, swap_vec);
    cycle_par->AddRegion(inductive_cycle);
    cycle = inductive_cycle;

    // Keep track of whether or not this loop is connected with a table for
    // an inductive NEGATE. This helps us later on in analyzing the control-flow
    // IR to find the provenance of column data.
    DataModel *const model = impl->view_to_model[view]->FindAs<DataModel>();
    if (TABLE *const table = model->table; table ) {
      inductive_cycle->induction_table.Emplace(inductive_cycle, table);
    }

    // Fill in the variables of the output and inductive cycle loops.
    for (auto col : view_cols) {
      const auto cycle_var = inductive_cycle->defined_vars.Create(
          impl->next_id++, VariableRole::kVectorVariable);
      cycle_var->query_column = col;
      cycle->col_id_to_var[col.Id()] = cycle_var;
    }

    const auto [check, check_call] = CallTopDownChecker(
        impl, context, cycle, view, view_cols, view, nullptr);
    cycle->body.Emplace(cycle, check);

    cycle_body_par = impl->parallel_regions.Create(check_call);

    if (for_add) {
      check_call->body.Emplace(check_call, cycle_body_par);
    } else {
      check_call->false_body.Emplace(check_call, cycle_body_par);
    }

  // We don't need to loop over the join here. This is EVIL!!!
  //
  // So, the induction's `ContinueInductionWorker::Run` is going to execute
  // first, *before* any induction JOINs have had their corresponding
  // `ContinueJoinWoker::Run` methods run. Yet, for the inductive edes to work,
  // we need to have variables against which we can bind. What we do is create
  // a LET that defines some variables, and the its the responsibility of the
  // `ContinueJoinWoker::Run` to fill in values. Suuuuper evil.
  } else if (view.IsJoin()) {
    assert(0u < view.InductivePredecessors().size());
    assert(0u < view.NonInductivePredecessors().size());

    const QueryJoin join = QueryJoin::From(view);
    if (const auto num_pivots = join.NumPivotColumns()) {
      if (for_add) {

        LET *const let = impl->operation_regions.CreateDerived<LET>(cycle_par);
        let->view.emplace(view);
        cycle_par->AddRegion(let);

        auto i = 0u;
        for (auto col : view_cols) {
          auto role = i < num_pivots ? VariableRole::kJoinPivot
                                     : VariableRole::kJoinNonPivot;

          ++i;
          VAR *const var = let->defined_vars.Create(impl->next_id++, role);
          var->query_column.emplace(col);
          let->col_id_to_var[col.Id()] = var;
        }

        cycle_body_par = impl->parallel_regions.Create(let);
        let->body.Emplace(let, cycle_body_par);

      // With removals, we just make a dummy parallel region; we never actually
      // use it. Inductive joins will be reached in the course of processing the
      // inductive successors of UNIONs, and so we handle those without the use
      // of induction vectors.
      } else {
        cycle_body_par = impl->parallel_regions.Create(cycle_par);
        cycle_par->AddRegion(cycle_body_par);
      }

    // This is a cross-product.
    } else {
      if (for_add) {

        LET *const let = impl->operation_regions.CreateDerived<LET>(cycle_par);
        let->view.emplace(view);
        cycle_par->AddRegion(let);

        for (auto col : view_cols) {
          VAR *const var = let->defined_vars.Create(
              impl->next_id++, VariableRole::kProductOutput);
          var->query_column.emplace(col);
          let->col_id_to_var[col.Id()] = var;
        }

        cycle_body_par = impl->parallel_regions.Create(let);
        let->body.Emplace(let, cycle_body_par);

      // With removals, we just make a dummy parallel region; we never actually
      // use it. Inductive joins will be reached in the course of processing the
      // inductive successors of UNIONs, and so we handle those without the use
      // of induction vectors.
      } else {
        cycle_body_par = impl->parallel_regions.Create(cycle_par);
        cycle_par->AddRegion(cycle_body_par);
      }
    }

    //      // Here  we'll loop over `swap_vec`, which holds the inputs, or outputs from
    //      // the last fixpoint iteration.
    //      const auto inductive_cycle =
    //          impl->operation_regions.CreateDerived<VECTORLOOP>(
    //              impl->next_id++, cycle_par,
    //              ProgramOperation::kLoopOverInductionVector);
    //      inductive_cycle->vector.Emplace(inductive_cycle, swap_vec);
    //      cycle_par->AddRegion(inductive_cycle);
    //      cycle = inductive_cycle;
    //
    //      DataModel * const model = impl->view_to_model[view]->FindAs<DataModel>();
    //      TABLE * const table = model->table;
    //      assert(table != nullptr);
    //
    //      // Fill in the variables of the output and inductive cycle loops.
    //      for (auto col : view.Columns()) {
    //
    //        // Add the variables to the fixpoint loop.
    //        const auto cycle_var = inductive_cycle->defined_vars.Create(
    //            impl->next_id++, VariableRole::kVectorVariable);
    //        cycle_var->query_column = col;
    //        cycle->col_id_to_var[col.Id()] = cycle_var;
    //      }
    //
    //    }

  } else {
    assert(false);
  }

  // Add a tuple to the output vector. We don't need to compute a worker ID
  // because we know we're dealing with only worker-specific data in this
  // cycle.
  //
  // NOTE(pag): Don't do anything for removals of JOINs/PRODUCTs. This function
  //            is called at the beginning of processing the nodes inside of the
  //            cycle, and we know that inductive JOINs straddle inside/outside,
  //            so when we processor the inductive successors of the UNIONs in
  //            this inductive region, we'll eventually come back to the JOINs
  //            and at that point we'll do the right thing.
  if (NeedsInductionOutputVector(view) &&
      !(!for_add && view.IsJoin())) {
    cycle_body_par->AddRegion(AppendToInductionOutputVectors(
        impl, view, context, induction, cycle_body_par));
  }

  if (for_add) {
    induction->fixpoint_add_cycles[view] = cycle_body_par;
  } else {
    induction->fixpoint_remove_cycles[view] = cycle_body_par;
  }
}

static void BuildOutputLoop(ProgramImpl *impl, Context &context,
                            INDUCTION *induction, QueryView view,
                            PARALLEL *output_par) {

  const auto proc = induction->containing_procedure;
  assert(output_par->containing_procedure == proc);
  (void) proc;

  const auto output_seq = impl->series_regions.Create(output_par);
  output_par->AddRegion(output_seq);

  // In the output region we'll clear out the vectors that we've used.
  VECTOR *const output_vec = induction->view_to_output_vec[view];
  assert(output_vec != nullptr);

  // NOTE(pag): We can use the same vector for insertion and removal, because
  //            we use `CHECKTUPLE` to figure out what to do!

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

  // Keep track of whether or not this loop is connected with a table for
  // an inductive UNION. This helps us later on in analyzing the control-flow
  // IR to find the provenance of column data.
  if (view.IsMerge() || view.IsNegate()) {
    TABLE *view_table = impl->view_to_model[view]->FindAs<DataModel>()->table;
    if (view_table) {
      output_cycle->induction_table.Emplace(output_cycle, view_table);
    }
  }

  // Fill in the variables of the output and inductive cycle loops.
  for (auto col : view.Columns()) {

    // Add the variables to the output loop.
    const auto output_var = output_cycle->defined_vars.Create(
        impl->next_id++, VariableRole::kVectorVariable);
    output_var->query_column = col;
    output->col_id_to_var[col.Id()] = output_var;
  }

  // If this merge can produce deletions, then it's possible that something
  // which was added to an induction vector has since been removed, and so we
  // can't count on pushing it forward until it is double checked.
  if (view.CanProduceDeletions()) {
    const auto available_cols = ComputeAvailableColumns(view, view.Columns());
    const auto checker_proc =
        GetOrCreateTopDownChecker(impl, context, view, available_cols, nullptr);

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

    induction->output_add_cycles[view] = output_added_par;
    induction->output_remove_cycles[view] = output_removed_par;

  } else {
    auto output_added_par = impl->parallel_regions.Create(output);
    output->body.Emplace(output, output_added_par);
    induction->output_add_cycles[view] = output_added_par;
  }
}

static void BuildInductiveClear(ProgramImpl *impl, Context &context,
                                INDUCTION *induction, QueryView merge,
                                PARALLEL *done_par) {

  // In the output region we'll clear out the vectors that we've used.
  auto add_vec_it = induction->view_to_add_vec.find(merge);

  //  auto remove_vec_it = induction->view_to_remove_vec.find(merge);
  auto swap_vec_it = induction->view_to_swap_vec.find(merge);

  VECTOR *add_vec = nullptr;

  if (add_vec_it != induction->view_to_add_vec.end() && add_vec_it->second) {
    add_vec = add_vec_it->second;
    const auto done_clear_add_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            done_par, ProgramOperation::kClearInductionVector);
    done_clear_add_vec->vector.Emplace(done_clear_add_vec, add_vec);
    done_par->AddRegion(done_clear_add_vec);
  }
  //
  //  if (remove_vec_it != induction->view_to_remove_vec.end() &&
  //      remove_vec_it->second && remove_vec_it->second != add_vec) {
  //    VECTOR * const remove_vec = remove_vec_it->second;
  //    const auto done_clear_remove_vec =
  //        impl->operation_regions.CreateDerived<VECTORCLEAR>(
  //            done_par, ProgramOperation::kClearInductionVector);
  //    done_clear_remove_vec->vector.Emplace(done_clear_remove_vec, remove_vec);
  //    done_par->AddRegion(done_clear_remove_vec);
  //  }

  if (swap_vec_it != induction->view_to_swap_vec.end() && swap_vec_it->second) {
    VECTOR *const swap_vec = swap_vec_it->second;

    // NOTE(pag): The add and swap vectors are identical for non-inductive
    //            predecessors of PRODUCT regions that straddle an inductive
    //            region.
    if (add_vec != swap_vec) {
      const auto done_clear_swap_vec =
          impl->operation_regions.CreateDerived<VECTORCLEAR>(
              done_par, ProgramOperation::kClearInductionVector);
      done_clear_swap_vec->vector.Emplace(done_clear_swap_vec, swap_vec);
      done_par->AddRegion(done_clear_swap_vec);
    }
  }
}

}  // namespace

// Build the cyclic regions of this INDUCTION.
void ContinueInductionWorkItem::Run(ProgramImpl *impl, Context &context) {

  // Once we run the first continue worker, it means we've reached all inductive
  // unions on the previous frontier, and so we can reset this, so any new
  // reached ones represent a new frontier.
  const auto merge_depth = *(induction->views[0].InductionDepth());
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
  if (auto r2 =
          FindCommonAncestorOfInitRegions(induction->init_appends_remove)) {
    regions.push_back(r2);
  }

  auto ancestor_of_inits = FindCommonAncestorOfInitRegions(regions);

  induction->parent = ancestor_of_inits->parent;
  ancestor_of_inits->ReplaceAllUsesWith(induction);
  ancestor_of_inits->parent = induction;
  induction->init_region.Emplace(induction, ancestor_of_inits);

  // Make sure that we only enter into the cycle accumulation process once.
  assert(!induction->cyclic_region);
  PARALLEL *const cycle_par = impl->parallel_regions.Create(induction);
  induction->cyclic_region.Emplace(induction, cycle_par);

  assert(!induction->output_region);
  SERIES *const done_seq = impl->series_regions.Create(induction);
  induction->output_region.Emplace(induction, done_seq);

  PARALLEL *const output_par = impl->parallel_regions.Create(done_seq);
  PARALLEL *const done_par = impl->parallel_regions.Create(done_seq);
  done_seq->AddRegion(output_par);
  done_seq->AddRegion(done_par);

  // Now build the inductive cycle regions and add them in. We'll do this
  // before we actually add the successor regions in.
  //
  // TODO(pag): A 'more optimal' ordering could likely be achieved. That is,
  //            if we know that some things are likely to lead into others,
  //            then we can use a SERIES to order them so that we can observe
  //            in-loop progress.
  for (QueryView view : induction->views) {
    SERIES *const view_seq = impl->series_regions.Create(cycle_par);
    cycle_par->AddRegion(view_seq);

    const auto has_inputs = NeedsInductionCycleVector(view);
    const auto has_outputs = NeedsInductionOutputVector(view);

    if (has_inputs) {
      if (!view.IsJoin() || QueryJoin::From(view).NumPivotColumns()) {
        BuildInductiveSwaps(impl, context, induction, view, view, view_seq);

      // Cross-products have vectors for their predecessor views.
      } else {
        assert(!view.NonInductivePredecessors().empty());
        for (auto pred_view : view.InductivePredecessors()) {
          BuildInductiveSwaps(impl, context, induction, pred_view, view,
                              view_seq);
        }
      }

      PARALLEL *const view_cycle_par = impl->parallel_regions.Create(view_seq);
      view_seq->AddRegion(view_cycle_par);

      // If we have to support removals, then do the removals first. We use the
      // same swap vector for insertions/removals.
      if (view.CanProduceDeletions()) {
        BuildFixpointLoop(impl, context, induction, view, view_cycle_par,
                          false);
      }

      // Build the main loops. The output and cycle regions match.
      BuildFixpointLoop(impl, context, induction, view, view_cycle_par, true);
      BuildInductiveClear(impl, context, induction, view, output_par);
    }

    if (has_outputs) {
      BuildOutputLoop(impl, context, induction, view, output_par);
    }
  }

  // Clear the add and swap vectors.
  for (auto [view, vec] : induction->view_to_add_vec) {
    const auto done_clear_add_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            output_par, ProgramOperation::kClearInductionVector);
    done_clear_add_vec->vector.Emplace(done_clear_add_vec, vec);
    output_par->AddRegion(done_clear_add_vec);
  }

  for (auto [view, vec] : induction->view_to_swap_vec) {
    const auto done_clear_swap_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            output_par, ProgramOperation::kClearInductionVector);
    done_clear_swap_vec->vector.Emplace(done_clear_swap_vec, vec);
    output_par->AddRegion(done_clear_swap_vec);
  }

  // Clear the output vectors
  for (auto [view, vec] : induction->view_to_output_vec) {
    const auto done_clear_output_vec =
        impl->operation_regions.CreateDerived<VECTORCLEAR>(
            done_par, ProgramOperation::kClearInductionVector);
    done_clear_output_vec->vector.Emplace(done_clear_output_vec, vec);
    done_par->AddRegion(done_clear_output_vec);
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // inductive successors.
  for (auto merge : induction->views) {
    if (!NeedsInductionCycleVector(merge)) {
      continue;
    }

    PARALLEL *const cycle_par = induction->fixpoint_add_cycles[merge];
    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel *const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE *const table = model->table;
    BuildEagerInsertionRegions(impl, merge, context, cycle,
                               merge.InductiveSuccessors(), table);
  }

  for (auto view : induction->views) {
    if (!view.CanProduceDeletions() || !NeedsInductionCycleVector(view)) {
      continue;
    }

    // Join cycle vectors are really pivot vectors, which is the only way to
    // unify the columns of the non-uniform joined views; however, in the
    // case of deletions, we don't use pivot vectors, as deletions are actually
    // record-specific. Thus, for inductive removals, we ignore joins and let
    // the bottom-up join removers handle them in their own way when they are
    // eventually reached via visiting the inductive successors of the one or
    // more UNIONs that must necessarily be part of `induction->views`.
    if (view.IsJoin()) {
      assert(QueryJoin::From(view).NumPivotColumns() && "TODO: Cross-products");
      continue;
    }

    PARALLEL *const cycle_par = induction->fixpoint_remove_cycles[view];

    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel *const model = impl->view_to_model[view]->FindAs<DataModel>();
    TABLE *const table = model->table;

    BuildEagerRemovalRegions(impl, view, context, cycle,
                             view.InductiveSuccessors(), table);
  }

  // Finally, add in an action to finish off this induction by processing
  // the outputs. It is possible that we're not actually done filling out
  // the INDUCTION's cycles, even after the above, due to WorkItems being
  // added by other nodes.
  const auto action =
      new FinalizeInductionWorkItem(context, induction->views[0], induction);
  context.work_list.emplace_back(action);
}

// Build the "output" regions of this induction.
//
// NOTE(pag): This is basically the same as above with some minor differences.
void FinalizeInductionWorkItem::Run(ProgramImpl *impl, Context &context) {
  assert(induction->state == INDUCTION::kAccumulatingCycleRegions);
  induction->state = INDUCTION::kBuildingOutputRegions;

  // Pass in the induction vectors to the handlers.
  for (auto view : induction->views) {
    context.view_to_induction_action.erase(view);
  }

  // Now that we have all of the regions arranged and the loops, add in the
  // non-inductive successors.
  for (auto merge : induction->views) {
    if (!NeedsInductionOutputVector(merge)) {
      continue;
    }
    PARALLEL *const cycle_par = induction->output_add_cycles[merge];
    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel *const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE *const table = model->table;
    BuildEagerInsertionRegions(impl, merge, context, cycle,
                               merge.NonInductiveSuccessors(), table);
  }

  for (auto merge : induction->views) {
    if (!merge.CanReceiveDeletions() || !NeedsInductionOutputVector(merge)) {
      continue;
    }
    PARALLEL *const cycle_par = induction->output_remove_cycles[merge];
    LET *const cycle = impl->operation_regions.CreateDerived<LET>(cycle_par);
    cycle_par->AddRegion(cycle);

    DataModel *const model = impl->view_to_model[merge]->FindAs<DataModel>();
    TABLE *const table = model->table;

    BuildEagerRemovalRegions(impl, merge, context, cycle,
                             merge.NonInductiveSuccessors(), table);
  }

  // NOTE(pag): We can't add a `return-false` here because an induction
  //            may come along and fill up this procedure with something else.
}

INDUCTION *GetOrInitInduction(ProgramImpl *impl, QueryView view,
                              Context &context, OP *parent) {
  PROC *const proc = parent->containing_procedure;
  INDUCTION *&induction = context.view_to_induction[view];

  if (induction) {
    return induction;
  }

  const auto merge_depth = *(view.InductionDepth());

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

#ifndef NDEBUG
    const auto merge_group = *(view.InductionGroupId());
    std::stringstream ss;
    ss << "set " << merge_group << " depth " << merge_depth;
    induction->comment = ss.str();
#endif

    action = new ContinueInductionWorkItem(context, view, induction);
    pending_action = action;
    context.work_list.emplace_back(action);
  }

  for (auto other_view : view.InductiveSet()) {
    const auto has_inputs = NeedsInductionCycleVector(other_view);
    const auto has_outputs = NeedsInductionOutputVector(other_view);
    induction->views.push_back(other_view);

    context.view_to_induction_action[other_view] = action;
    context.view_to_induction[other_view] = induction;

    // Figure out if the induction can produce deletions. This could feasibly
    // be an over-approximation, i.e. on of the inductions is non-
    // differential, but feeds another induction that is differential. For
    // simplicity we'll assume if one is differential then all are
    // differential.
    if (other_view.CanProduceDeletions()) {
      induction->is_differential = true;
    }

    // If we're dealing with an inductive join, then our induction vector is
    // the vector of join pivots.
    if (other_view.IsJoin()) {
      const QueryJoin join = QueryJoin::From(other_view);

      if (join.NumPivotColumns()) {
        auto &add_vec = induction->view_to_add_vec[other_view];
        auto &swap_vec = induction->view_to_swap_vec[other_view];

        // If the join has no inductive inputs, but does have inductive outputs,
        // the the pivot vector isn't marked as an inductive pivot vector, nor
        // is it added to the induction's list of tested vectors.
        if (!has_inputs) {
          if (!add_vec) {
            assert(!swap_vec);
            add_vec = proc->VectorFor(impl, VectorKind::kJoinPivots,
                                      join.PivotColumns());
          }
          swap_vec = add_vec;

        } else {
          if (!add_vec) {
            add_vec = proc->VectorFor(impl, VectorKind::kInductiveJoinPivots,
                                      join.PivotColumns());
            induction->vectors.AddUse(add_vec);
          }

          // These are a bunch of swap vectors that we use for the sake of allowing
          // ourselves to see the results of the prior iteration, while minimizing
          // the amount of cross-iteration resident data.
          if (!swap_vec) {
            swap_vec =
                proc->VectorFor(impl, VectorKind::kInductiveJoinPivotSwaps,
                                join.PivotColumns());
          }
        }

        if (auto &join_action = context.view_to_join_action[other_view];
            !join_action) {
          join_action = new ContinueJoinWorkItem(context, other_view, add_vec,
                                                 swap_vec, induction);
          context.work_list.emplace_back(join_action);
        }

      // This is a cross-product. We need vectors for each predecessor of
      // a PRODUCT, but we don't want swap vectors for the non-inductive
      // predecessors of a PRODUCT, because those vectors need to stay around
      // over the course of all of the iterations, so that we can cross
      // against them (the inductive predecessor vectors will be cleared).
      } else {

        for (auto inductive_pred : other_view.InductivePredecessors()) {
          auto &add_vec = induction->view_to_add_vec[inductive_pred];
          auto &swap_vec = induction->view_to_swap_vec[inductive_pred];

          if (!add_vec) {
            add_vec = proc->VectorFor(impl, VectorKind::kInductiveProductInput,
                                      inductive_pred.Columns());
            induction->vectors.AddUse(add_vec);
          }

          // These are a bunch of swap vectors that we use for the sake of allowing
          // ourselves to see the results of the prior iteration, while minimizing
          // the amount of cross-iteration resident data.
          if (!swap_vec) {
            swap_vec = proc->VectorFor(impl, VectorKind::kInductiveProductSwaps,
                                       inductive_pred.Columns());
          }
        }

        // NOTE(pag): We *don't* add to the induction vectors, otherwise we'd
        //            have an infinite loop.
        for (auto non_inductive_pred : other_view.NonInductivePredecessors()) {
          auto &add_vec = induction->view_to_add_vec[non_inductive_pred];
          auto &swap_vec = induction->view_to_swap_vec[non_inductive_pred];

          if (!add_vec) {
            assert(!swap_vec);
            add_vec = proc->VectorFor(impl, VectorKind::kProductInput,
                                      non_inductive_pred.Columns());
          }
          swap_vec = add_vec;
        }

        if (auto &product_action = context.view_to_product_action[other_view];
            !product_action) {
          product_action =
              new ContinueProductWorkItem(context, other_view, induction);
          context.work_list.emplace_back(product_action);
        }
      }

    // If we're dealing with an inductive merge, then our induction vector
    // is the join pivots themselves.
    } else if (other_view.IsMerge() || other_view.IsNegate()) {

      // Figure out if we need a vector for tracking additions/removals.
      if (has_inputs) {
        auto &add_vec = induction->view_to_add_vec[other_view];
        if (!add_vec) {
          add_vec = proc->VectorFor(impl, VectorKind::kInductionInputs,
                                    other_view.Columns());
          induction->vectors.AddUse(add_vec);
        }

        // These are a bunch of swap vectors that we use for the sake of allowing
        // ourselves to see the results of the prior iteration, while minimizing
        // the amount of cross-iteration resident data.
        auto &swap_vec = induction->view_to_swap_vec[other_view];
        if (!swap_vec) {
          swap_vec = proc->VectorFor(impl, VectorKind::kInductionSwaps,
                                     other_view.Columns());
        }
      }

    } else {
      assert(false);
    }

    // Figure out if we need a vector to track outputs. Output vectors always
    // have the same shape, regardless of if they're for UNIONs, JOINs, or
    // NEGATEs.
    if (has_outputs) {
      auto &output_vec = induction->view_to_output_vec[other_view];
      if (!output_vec) {
        output_vec = proc->VectorFor(impl, VectorKind::kInductionOutputs,
                                     other_view.Columns());
      }
    }
  }

  return induction;
}

REGION *AppendToInductionOutputVectors(ProgramImpl *impl, QueryView view,
                                       Context &context, INDUCTION *induction,
                                       REGION *parent) {
  VECTOR *output_vec = induction->view_to_output_vec[view];
  assert(output_vec != nullptr);

  const auto append_to_output_vec =
      impl->operation_regions.CreateDerived<VECTORAPPEND>(
          parent, ProgramOperation::kAppendToInductionVector);
  append_to_output_vec->vector.Emplace(append_to_output_vec, output_vec);
  for (auto col : view.Columns()) {
    append_to_output_vec->tuple_vars.AddUse(parent->VariableFor(impl, col));
  }

  return append_to_output_vec;
}

void AppendToInductionInputVectors(ProgramImpl *impl, QueryView vec_view,
                                   QueryView inductive_view, Context &context,
                                   OP *parent, INDUCTION *induction,
                                   bool for_add) {

  // NOTE(pag): We can use the same vector for insertion and removal, because
  //            we use `CHECKTUPLE` to figure out what to do!
  VECTOR *const vec = induction->view_to_add_vec[vec_view];
  assert(vec != nullptr);

  WORKERID *hash = nullptr;
  VAR *worker_id = nullptr;
  PARALLEL *par = nullptr;
  VECTORAPPEND *append_to_vec = nullptr;

  // Add a tuple to the vector.
  auto add_append_to_vec = [&](void) {
    append_to_vec = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        par, ProgramOperation::kAppendToInductionVector);
    append_to_vec->vector.Emplace(append_to_vec, vec);
    append_to_vec->worker_id.Emplace(append_to_vec, worker_id);
  };

  auto add_hash_and_par = [&](void) {
    // Hash the variables together to form a worker ID.
    hash = impl->operation_regions.CreateDerived<WORKERID>(parent);
    worker_id = new VAR(impl->next_id++, VariableRole::kWorkerId);
    hash->worker_id.reset(worker_id);
    parent->body.Emplace(parent, hash);

    par = impl->parallel_regions.Create(hash);
    hash->body.Emplace(hash, par);
    add_append_to_vec();
  };

  auto add_par = [&](void) {
    worker_id = impl->zero;
    par = impl->parallel_regions.Create(parent);
    parent->body.Emplace(parent, par);
    add_append_to_vec();
  };

  if (inductive_view.IsMerge()) {
    add_hash_and_par();

    for (auto col : vec_view.Columns()) {
      const auto var = par->VariableFor(impl, col);
      hash->hashed_vars.AddUse(var);
      append_to_vec->tuple_vars.AddUse(var);
    }

  // Negations differ from merges only in that they hash just the negated
  // columns, and not all of the columns of the negation. It's similar to how
  // joins hash the pivot columns.
  } else if (inductive_view.IsNegate()) {
    add_hash_and_par();

    QueryNegate negate = QueryNegate::From(inductive_view);
    for (auto col : negate.NegatedColumns()) {
      const auto var = par->VariableFor(impl, col);
      hash->hashed_vars.AddUse(var);
    }

    for (auto col : vec_view.Columns()) {
      const auto var = par->VariableFor(impl, col);
      append_to_vec->tuple_vars.AddUse(var);
    }

  // Joins hash and append just the pivot columns.
  } else if (inductive_view.IsJoin()) {
    QueryJoin join = QueryJoin::From(inductive_view);
    if (join.NumPivotColumns()) {
      add_hash_and_par();

      for (auto col : join.PivotColumns()) {
        const auto var = par->VariableFor(impl, col);
        hash->hashed_vars.AddUse(var);
        append_to_vec->tuple_vars.AddUse(var);
      }

    } else {
      add_par();

      for (auto col : vec_view.Columns()) {
        const auto var = par->VariableFor(impl, col);
        append_to_vec->tuple_vars.AddUse(var);
      }
    }

  } else {
    assert(false);
    return;
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

    default: assert(false); break;
  }
}

// Build an eager region for a `QueryMerge` that is part of an inductive
// loop. This is interesting because we use a WorkItem as a kind of "barrier"
// to accumulate everything leading into the inductions before proceeding.
void BuildEagerInductiveRegion(ProgramImpl *impl, QueryView pred_view,
                               QueryMerge view, Context &context, OP *parent_,
                               TABLE *already_added_) {
  auto [parent, table, already_added] =
      InTryInsert(impl, context, view, parent_, already_added_);

  INDUCTION *const induction = GetOrInitInduction(impl, view, context, parent);
  if (NeedsInductionCycleVector(view)) {
    AppendToInductionInputVectors(impl, view, view, context, parent, induction,
                                  true);

  } else {
    BuildEagerUnionRegion(impl, pred_view, view, context, parent,
                          already_added);
  }
}

void CreateBottomUpInductionRemover(ProgramImpl *impl, Context &context,
                                    QueryView view, OP *parent_,
                                    TABLE *already_removed_) {
  auto [parent, table, already_removed] =
      InTryMarkUnknown(impl, context, view, parent_, already_removed_);

  auto merge = QueryMerge::From(view);
  INDUCTION *const induction = GetOrInitInduction(impl, merge, context, parent);
  if (NeedsInductionCycleVector(view)) {
    AppendToInductionInputVectors(impl, view, view, context, parent, induction,
                                  false);

  } else {
    CreateBottomUpUnionRemover(impl, context, view, parent, already_removed);
  }
}

// Build a top-down checker on an induction.
REGION *BuildTopDownInductionChecker(ProgramImpl *impl, Context &context,
                                     REGION *proc, QueryMerge merge,
                                     std::vector<QueryColumn> &view_cols,
                                     TABLE *already_checked) {

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

  auto do_rec_check = [&](QueryView pred_view, PARALLEL *parent) -> REGION * {
    return CallTopDownChecker(
        impl, context, parent, view, view_cols, pred_view, already_checked,
        [=](REGION *parent_if_true) -> REGION * {
          return BuildStateCheckCaseReturnTrue(impl, parent_if_true);
        },
        [](REGION *) -> REGION * { return nullptr; });
  };

  // If it's not an inductive predecessor, then check it in `par_init`.
  for (auto pred_view : view.NonInductivePredecessors()) {
    const auto rec_check = do_rec_check(pred_view, par_init);
    par_init->AddRegion(rec_check);

    COMMENT(rec_check->comment = __FILE__
            ": BuildTopDownInductionChecker call init predecessor";)
  }

  // If it's an inductive predecessor, then check it in `par_cyclic`.
  for (auto pred_view : view.InductivePredecessors()) {
    const auto rec_check = do_rec_check(pred_view, par_cyclic);
    par_cyclic->AddRegion(rec_check);

    COMMENT(rec_check->comment = __FILE__
            ": BuildTopDownInductionChecker call inductive predecessor";)
  }

  return seq;
}

}  // namespace hyde
