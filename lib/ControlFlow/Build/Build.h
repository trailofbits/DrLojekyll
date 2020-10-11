// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Util/DefUse.h>

#include <set>
#include <unordered_map>
#include <unordered_set>

#include "../Program.h"

namespace hyde {

// Equivalence classes of `QueryMerge`s. Two or more `QueryMerge`s belong to
// the same equivalence class iff their cycles intersect.
class InductionSet : public DisjointSet {
 public:
  using DisjointSet::DisjointSet;

  std::vector<QueryView> merges;
};

class Context;

// Generic action for lifting to the control flow representation. Work item
// actions enable us to defer some liting to the control flow representation
// until `Run` is invoked.
class WorkItem {
 public:
  static constexpr unsigned kContinueInductionOrder = (~0u) >> 2u;
  static constexpr unsigned kFinalizeInductionOrder = (~0u) >> 1u;

  virtual ~WorkItem(void);
  virtual void Run(ProgramImpl *program, Context &context) = 0;

  explicit WorkItem(unsigned order_) : order(order_) {}

  const unsigned order;
};

using WorkItemPtr = std::unique_ptr<WorkItem>;

// General wrapper around data used when lifting from the data flow to the
// control flow representation.
class Context {
 public:
  // Mapping of `QueryMerge` instances to their equivalence classes.
  std::unordered_map<QueryView, InductionSet> merge_sets;

  // Set of successors of a `QueryMerge` that may lead back to the merge
  // (inductive) and never lead back to the merge (noninductive), respectively.
  std::unordered_map<QueryView, std::unordered_set<QueryView>>
      inductive_successors;
  std::unordered_map<QueryView, std::unordered_set<QueryView>>
      noninductive_successors;

  // Set of predecessors of a `QueryMerge` whose data transitively depends upon
  // an inductive
  std::unordered_map<QueryView, std::unordered_set<QueryView>>
      inductive_predecessors;
  std::unordered_map<QueryView, std::unordered_set<QueryView>>
      noninductive_predecessors;

  // Set of regions that execute eagerly. In practice this means that these
  // regions are directly needed in order to produce an output message.
  std::unordered_set<QueryView> eager;

  // Map `QueryMerge`s to INDUCTIONs. One or more `QueryMerge`s might map to
  // the same INDUCTION if they belong to the same "inductive set". This happens
  // when two or more `QueryMerge`s are cyclic, and their cycles intersect.
  std::unordered_map<QueryView, INDUCTION *> view_to_induction;

  // Maps tables to their product input vectors.
  std::unordered_map<TABLE *, VECTOR *> product_vector;

  //  // Boolean variable to test if we've ever produced anything for this product,
  //  // and thus should push data through.
  //  std::unordered_map<QueryView, VAR *> product_guard_var;

  // Work list of actions to invoke to build the execution tree.
  std::vector<WorkItemPtr> work_list;
  std::unordered_map<QueryView, WorkItem *> view_to_work_item;
};

// Build an eager region. This guards the execution of the region in
// conditionals if the view itself is conditional.
void BuildEagerRegion(ProgramImpl *impl, QueryView pred_view, QueryView view,
                      Context &context, OP *parent, TABLE *last_model);

// Build an eager region for a `QueryMerge` that is part of an inductive
// loop. This is interesting because we use a WorkItem as a kind of "barrier"
// to accumulate everything leading into the inductions before proceeding.
void BuildEagerInductiveRegion(ProgramImpl *impl, QueryView pred_view,
                               QueryMerge view, Context &context, OP *parent,
                               TABLE *last_model);

// Build a top-down checker on an induction. This applies to inductions as
// well as differential unions.
void BuildTopDownInductionChecker(ProgramImpl *impl, Context &context,
                                  PROC *proc, QueryMerge view);

// Build an eager region for a `QueryMerge` that is NOT part of an inductive
// loop, and thus passes on its data to the next thing down as long as that
// data is unique.
void BuildEagerUnionRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryMerge view, Context &context, OP *parent,
                           TABLE *last_model);

// Build a top-down checker on a union. This applies to non-differential
// unions.
void BuildTopDownUnionChecker(ProgramImpl *impl, Context &context,
                              PROC *proc, QueryMerge view);

// Build an eager region for publishing data, or inserting it. This might end
// up passing things through if this isn't actually a message publication.
void BuildEagerInsertRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryInsert view, Context &context, OP *parent,
                            TABLE *last_model);

// Build an eager region for deleting it.
void BuildEagerDeleteRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryInsert view, Context &context, OP *parent);

// Build an eager region for a join.
void BuildEagerJoinRegion(ProgramImpl *impl, QueryView pred_view,
                          QueryJoin view, Context &context, OP *parent,
                          TABLE *last_model);

// Build an eager region for cross-product.
void BuildEagerProductRegion(ProgramImpl *impl, QueryView pred_view,
                             QueryJoin view, Context &context, OP *parent,
                             TABLE *last_model);

// Build an eager region for performing a comparison.
void BuildEagerCompareRegions(ProgramImpl *impl, QueryCompare view,
                              Context &context, OP *parent);

// Build an eager region for a `QueryMap`.
void BuildEagerGenerateRegion(ProgramImpl *impl, QueryMap view,
                              Context &context, OP *parent);

// Build an eager region for tuple. If the tuple can receive differential
// updates then its data needs to be saved.
void BuildEagerTupleRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryTuple tuple, Context &context, OP *parent,
                           TABLE *last_model);

// Build a top-down checker on a tuple. This possibly widens the tuple, i.e.
// recovering "lost" columns, and possibly re-orders arguments before calling
// down to the tuple's predecessor's checker.
void BuildTopDownTupleChecker(ProgramImpl *impl, Context &context,
                              PROC *proc, QueryTuple tuple);

// Builds an initialization function which does any work that depends purely
// on constants.
void BuildInitProcedure(ProgramImpl *impl, Context &context);

// Complete a procedure by exhausting the work list.
void CompleteProcedure(ProgramImpl *impl, PROC *proc, Context &context);

// Returns a global reference count variable associated with a query condition.
VAR *ConditionVariable(ProgramImpl *impl, QueryCondition cond);

// Build a top-down checker that tries to figure out if some tuple (passed as
// arguments to this function) is present or not.
PROC *GetOrCreateTopDownChecker(ProgramImpl *impl, Context &context,
                                QueryView view);

// Build a bottom-up tuple remover, which marks tuples as being in the
// UNKNOWN state (for later top-down checking).
PROC *GetOrCreateBottomUpRemover(ProgramImpl *impl, Context &context,
                                 QueryView view, TABLE *table);

// Returns `true` if `view` might need to have its data persisted for the
// sake of supporting differential updates / verification.
bool MayNeedToBePersisted(QueryView view);

using ColPair = std::pair<QueryColumn, QueryColumn>;

// Get a mapping of `(input_col, output_col)` where the columns are in order
// of `input_col`, and no input columns are repeated.
std::vector<ColPair> GetColumnMap(QueryView view, QueryView pred_view);

// Build a check for inserting into a view.
template <typename Columns>
OP *BuildInsertCheck(ProgramImpl *impl, QueryView view, Context &context,
                     OP *parent, TABLE *table, bool differential,
                     Columns &&columns) {

  // If we can receive deletions, then we need to call a functor that will
  // tell us if this tuple doesn't actually exist.
  if (differential) {
    const auto check = impl->operation_regions.CreateDerived<CALL>(
        parent, GetOrCreateTopDownChecker(impl, context, view),
        ProgramOperation::kCallProcedureCheckFalse);

    for (auto col : columns) {
      const auto var = parent->VariableFor(impl, col);
      check->arg_vars.AddUse(var);
    }

    UseRef<REGION>(parent, check).Swap(parent->body);
    parent = check;
  }

  const auto insert = impl->operation_regions.CreateDerived<CHANGESTATE>(
      parent, TupleState::kAbsentOrUnknown, TupleState::kPresent);

  for (auto col : columns) {
    const auto var = parent->VariableFor(impl, col);
    insert->col_values.AddUse(var);
  }

  UseRef<TABLE>(insert, table).Swap(insert->table);
  UseRef<REGION>(parent, insert).Swap(parent->body);
  return insert;
}

// Add in all of the successors of a view inside of `parent`, which is
// usually some kind of loop. The successors execute in parallel.
template <typename List>
void BuildEagerSuccessorRegions(ProgramImpl *impl, QueryView view,
                                Context &context, OP *parent, List &&successors,
                                TABLE *last_model) {

  // Proving this `view` might set a condition. If we set a condition, then
  // we need to make sure than a CHANGESTATE actually happened. That could
  // mean re-parenting all successors within an CHANGESTATE.
  if (auto set_cond = view.SetCondition(); set_cond) {
    if (const auto table = TABLE::GetOrCreate(impl, view);
        table != last_model) {
      last_model = table;
      const auto insert = impl->operation_regions.CreateDerived<CHANGESTATE>(
          parent, TupleState::kAbsentOrUnknown, TupleState::kPresent);

      for (auto col : view.Columns()) {
        const auto var = parent->VariableFor(impl, col);
        insert->col_values.AddUse(var);
      }

      UseRef<TABLE>(insert, table).Swap(insert->table);
      UseRef<REGION>(parent, insert).Swap(parent->body);
      parent = insert;
    }

    const auto seq = impl->series_regions.Create(parent);
    UseRef<REGION>(parent, seq).Swap(parent->body);

    // Now that we know that the data has been dealt with, we increment the
    // condition variable.
    const auto set = impl->operation_regions.CreateDerived<ASSERT>(
        seq, ProgramOperation::kIncrementAll);
    set->cond_vars.AddUse(ConditionVariable(impl, *set_cond));
    set->ExecuteAfter(impl, seq);

    // Create a dummy/empty LET binding so that we have an `OP *` as a parent
    // going forward.
    parent = impl->operation_regions.CreateDerived<LET>(seq);
    parent->ExecuteAfter(impl, seq);
  }

  // Check if all outputs are constant or references to constants (i.e. constant
  // if reached by data flow).
  bool all_const = true;
  for (auto col : view.Columns()) {
    if (!col.IsConstantOrConstantRef()) {
      all_const = false;
      break;
    }
  }

  // All successors execute in a PARALLEL region, even if there are zero or
  // one successors. Empty and trivial PARALLEL regions are optimized out later.
  //
  // A key benefit of PARALLEL regions is that within them, CSE can be performed
  // to identify and eliminate repeated branches.
  PARALLEL *par = nullptr;

  // If all outputs are constant and if this is always an insert-only view,
  // then we shouldn't re-execute it if we've previously done it.
  if (all_const && !view.CanProduceDeletions()) {
    auto &ref_count = impl->const_view_to_var[view];
    if (!ref_count) {
      ref_count = impl->global_vars.Create(impl->next_id++,
                                           VariableRole::kConditionRefCount);
    }

    const auto test = impl->operation_regions.CreateDerived<EXISTS>(
        parent, ProgramOperation::kTestAllZero);
    UseRef<REGION>(parent, test).Swap(parent->body);

    const auto seq = impl->series_regions.Create(test);
    UseRef<REGION>(test, seq).Swap(test->body);

    const auto set = impl->operation_regions.CreateDerived<ASSERT>(
        seq, ProgramOperation::kIncrementAll);

    test->cond_vars.AddUse(ref_count);
    set->cond_vars.AddUse(ref_count);

    last_model = nullptr;
    par = impl->parallel_regions.Create(seq);
    set->ExecuteAfter(impl, seq);
    par->ExecuteAfter(impl, seq);

  } else {
    par = impl->parallel_regions.Create(parent);
    UseRef<REGION>(parent, par).Swap(parent->body);
  }


  for (QueryView succ_view : successors) {
    const auto let = impl->operation_regions.CreateDerived<LET>(par);

    let->ExecuteAlongside(impl, par);

    succ_view.ForEachUse([=](QueryColumn in_col, InputColumnRole role,
                             std::optional<QueryColumn> out_col) {
      switch (role) {
        case InputColumnRole::kPassThrough:
        case InputColumnRole::kCopied:
        case InputColumnRole::kJoinPivot:
        case InputColumnRole::kJoinNonPivot:
        case InputColumnRole::kMergedColumn:
        case InputColumnRole::kIndexKey:
        case InputColumnRole::kFunctorInput:
        case InputColumnRole::kAggregateConfig:
        case InputColumnRole::kAggregateGroup:
          if (out_col && in_col.Id() != out_col->Id() &&
              (QueryView::Containing(in_col) == view || in_col.IsConstant())) {

            const auto src_var = par->VariableFor(impl, in_col);
            let->used_vars.AddUse(src_var);
            const auto dst_var = let->defined_vars.Create(
                impl->next_id++, VariableRole::kLetBinding);
            dst_var->query_column = *out_col;
            let->col_id_to_var.emplace(out_col->Id(), dst_var);
          }
          break;

        case InputColumnRole::kIndexValue:
        case InputColumnRole::kCompareLHS:
        case InputColumnRole::kCompareRHS:
        case InputColumnRole::kAggregatedColumn: break;
      }
    });

    BuildEagerRegion(impl, view, succ_view, context, let, last_model);
  }
}

}  // namespace hyde
