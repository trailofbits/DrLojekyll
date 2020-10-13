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

  // Maps views to procedures for top-down execution. The top-down executors
  // exist to determine if a tuple is actually PRESENT (returning false if so),
  // converting unprovable UNKNOWNs into ABSENTs.
  std::unordered_map<std::string, PROC *>
      view_to_top_down_checker;

  enum DeferDetermination {
    kDeferUnknown,
    kCanDeferToPredecessor,
    kCantDeferToPredecessor
  };

  // Caches whether or not we've decided if
  std::unordered_map<std::pair<QueryView, QueryView>, DeferDetermination>
      can_defer_to_predecessor;

  // A work list of top-down checkers to build.
  std::vector<std::tuple<QueryView, std::vector<QueryColumn>, PROC *, TABLE *>>
      top_down_checker_work_list;
};

//using ColPair = std::pair<QueryColumn, QueryColumn>;
//
//// Get a mapping of `(input_col, output_col)` where the columns are in order
//// of `input_col`, and no input columns are repeated.
//std::vector<ColPair> GetColumnMap(QueryView view, QueryView pred_view);

OP *BuildStateCheckCaseReturnFalse(ProgramImpl *impl, REGION *parent);
OP *BuildStateCheckCaseReturnTrue(ProgramImpl *impl, REGION *parent);
OP *BuildStateCheckCaseNothing(ProgramImpl *impl, REGION *parent);

template <typename Cols, typename IfPresent, typename IfAbsent, typename IfUnknown>
static CHECKSTATE *BuildTopDownCheckerStateCheck(
    ProgramImpl *impl, REGION *parent, TABLE *table, Cols &&cols,
    IfPresent if_present_cb, IfAbsent if_absent_cb, IfUnknown if_unknown_cb) {

  const auto check = impl->operation_regions.CreateDerived<CHECKSTATE>(parent);
  for (auto col : cols) {
    const auto var = parent->VariableFor(impl, col);
    check->col_values.AddUse(var);
  }

  UseRef<TABLE>(check, table).Swap(check->table);

  if (REGION *present_op = if_present_cb(impl, check); present_op) {
    UseRef<REGION>(check, present_op).Swap(check->OP::body);
  }

  if (REGION *absent_op = if_absent_cb(impl, check); absent_op) {
    UseRef<REGION>(check, absent_op).Swap(check->absent_body);
  }

  if (REGION *unknown_op = if_unknown_cb(impl, check); unknown_op) {
    UseRef<REGION>(check, unknown_op).Swap(check->unknown_body);
  }

  return check;
}

// Change the state of some relation.
template <typename Cols>
static CHANGESTATE *BuildChangeState(
    ProgramImpl *impl, TABLE *table, REGION *parent,
    Cols &&cols, TupleState from_state, TupleState to_state) {

  const auto state_change =
      impl->operation_regions.CreateDerived<CHANGESTATE>(
          parent, from_state, to_state);

  UseRef<TABLE>(state_change, table).Swap(state_change->table);
  for (auto col : cols) {
    const auto var = parent->VariableFor(impl, col);
    state_change->col_values.AddUse(var);
  }

  return state_change;
}

template <typename Cols, typename AfterChangeState>
static CHANGESTATE *BuildTopDownCheckerResetAndProve(
    ProgramImpl *impl, TABLE *table, REGION *parent,
    Cols &&cols, AfterChangeState with_par_node) {

  // Change the tuple's state to mark it as deleted so that we can't use it
  // as its own base case.
  const auto table_remove = BuildChangeState(
      impl, table, parent, cols, TupleState::kUnknown, TupleState::kAbsent);

  // Now that we've established the base case (marking the tuple absent), we
  // need to go and actually check all the possibilities.
  const auto par = impl->parallel_regions.Create(table_remove);
  UseRef<REGION>(table_remove, par).Swap(table_remove->OP::body);

  with_par_node(par);

  return table_remove;
}


// We know that `view`s data is persistently backed by `table`, and that we
// want to check for a tuple with values available in `view_cols`. The issue
// is that `view_cols` might represent a subset of the columns actually
// in `view`, and so we need a way to "complete" the other columns before we
// can actually check for the existence of anything. Thus what we will do is
// if we have a strict subset of the columns, we'll perform a table scan.
//
// NOTE(pag): This mutates `available_cols` in place, so that by the time that
//            `cb` is called, `available_cols` contains all columns.
template <typename ForEachTuple>
static REGION *BuildMaybeScanPartial(
    ProgramImpl *impl, QueryView view,
    std::vector<QueryColumn> &view_cols,
    TABLE *table, REGION *parent, ForEachTuple cb) {

  // Figure out if we even need an index scan; if we've got all the columns
  // then we can just use `cb`, which assumes the availability of all columns.
  const auto num_cols = view.Columns().size();
  if (view_cols.size() == num_cols) {
    return cb(parent);
  }

  std::vector<unsigned> in_col_indices;
  std::vector<bool> indexed_cols(num_cols);
  for (auto view_col : view_cols) {
    const unsigned in_col_index = *(view_col.Index());
    in_col_indices.push_back(in_col_index);
    indexed_cols[in_col_index] = true;
  }

  // Figure out what columns we're selecting.
  std::vector<QueryColumn> selected_cols;
  for (auto col : view.Columns()) {
    if (!indexed_cols[*(col.Index())]) {
      selected_cols.push_back(col);
    }
  }

  assert(0u < selected_cols.size());

  const auto index = table->GetOrCreateIndex(impl, std::move(in_col_indices));
  const auto seq = impl->series_regions.Create(parent);
  const auto proc = seq->containing_procedure;
  const auto vec = proc->vectors.Create(
      impl->next_id++, VectorKind::kTableScan, selected_cols);

  // Scan an index, using the columns from the tuple to find the columns
  // from the tuple's predecessor.
  const auto scan = impl->operation_regions.CreateDerived<TABLESCAN>(seq);
  scan->ExecuteAfter(impl, seq);
  UseRef<TABLE>(scan, table).Swap(scan->table);
  UseRef<TABLEINDEX>(scan, index).Swap(scan->index);
  UseRef<VECTOR>(scan, vec).Swap(scan->output_vector);

  for (auto view_col : view_cols) {
    const auto in_var = parent->VariableFor(impl, view_col);
    scan->in_vars.AddUse(in_var);
  }

  for (auto table_col : table->columns) {
    if (indexed_cols[table_col->index]) {
      scan->in_cols.AddUse(table_col);

    } else {
      scan->out_cols.AddUse(table_col);
    }
  }

  // Loop over the results of the table scan.
  const auto loop = impl->operation_regions.CreateDerived<VECTORLOOP>(
      seq, ProgramOperation::kLoopOverScanVector);
  loop->ExecuteAfter(impl, seq);
  UseRef<VECTOR>(loop, vec).Swap(loop->vector);

  for (auto col : selected_cols) {
    const auto var = loop->defined_vars.Create(
        impl->next_id++, VariableRole::kScanOutput);
    var->query_column = col;
    loop->col_id_to_var.emplace(col.Id(), var);
  }

  for (auto pred_col : view.Columns()) {
    if (!indexed_cols[*(pred_col.Index())]) {
      selected_cols.push_back(pred_col);
    }
  }

  // Mutable in place so that `cb` can observe an updates set of available
  // columns.
  view_cols.clear();
  for (auto col : view.Columns()) {
    view_cols.push_back(col);
  }

  auto in_loop = cb(loop);
  UseRef<REGION>(loop, in_loop).Swap(loop->body);

//  const auto sub_seq = impl->series_regions.Create(check);
//  UseRef<REGION>(check, sub_seq).Swap(check->body);
//
//  // Clear out the scan vector if we've proven the tuple.
//  const auto clear_found = impl->operation_regions.CreateDerived<VECTORCLEAR>(
//      sub_seq, ProgramOperation::kClearScanVector);
//  UseRef<VECTOR>(clear_found, vec).Swap(clear_found->vector);
//  clear_found->ExecuteAfter(impl, sub_seq);
//
//  // Change the tuple's state if we've proven it.
//  const auto table_insert =
//      impl->operation_regions.CreateDerived<CHANGESTATE>(
//          sub_seq, TupleState::kAbsentOrUnknown, TupleState::kPresent);
//  for (auto col : tuple.Columns()) {
//    const auto var = proc->VariableFor(impl, col);
//    table_insert->col_values.AddUse(var);
//  }
//
//  UseRef<TABLE>(table_insert, table).Swap(table_insert->table);
//  table_insert->ExecuteAfter(impl, sub_seq);
//
//  // Return `true` if we've proven the tuple.
//  const auto ret = impl->operation_regions.CreateDerived<RETURN>(
//      sub_seq, ProgramOperation::kReturnTrueFromProcedure);
//  ret->ExecuteAfter(impl, sub_seq);
//
//  // Clear out the scan vector after the loop. We'll let the caller inject
//  // a `return-false`.
//  const auto clear_notfound = impl->operation_regions.CreateDerived<VECTORCLEAR>(
//      seq, ProgramOperation::kClearScanVector);
//  UseRef<VECTOR>(clear_notfound, vec).Swap(clear_notfound->vector);
//  clear_notfound->ExecuteAfter(impl, seq);

  return seq;
}

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
void BuildTopDownInductionChecker(
    ProgramImpl *impl, Context &context, PROC *proc, QueryMerge view,
    std::vector<QueryColumn> &available_cols, TABLE *already_checked);

// Build an eager region for a `QueryMerge` that is NOT part of an inductive
// loop, and thus passes on its data to the next thing down as long as that
// data is unique.
void BuildEagerUnionRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryMerge view, Context &context, OP *parent,
                           TABLE *last_model);

// Build a top-down checker on a union. This applies to non-differential
// unions.
void BuildTopDownUnionChecker(
    ProgramImpl *impl, Context &context, PROC *proc,
    QueryMerge view, std::vector<QueryColumn> &available_cols,
    TABLE *already_checked);

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
void BuildTopDownTupleChecker(
    ProgramImpl *impl, Context &context, PROC *proc, QueryTuple tuple,
    std::vector<QueryColumn> &available_cols, TABLE *already_checked);

// Builds an initialization function which does any work that depends purely
// on constants.
void BuildInitProcedure(ProgramImpl *impl, Context &context);

// Complete a procedure by exhausting the work list.
void CompleteProcedure(ProgramImpl *impl, PROC *proc, Context &context);

// Returns a global reference count variable associated with a query condition.
VAR *ConditionVariable(ProgramImpl *impl, QueryCondition cond);

// Calls a top-down checker that tries to figure out if some tuple (passed as
// arguments to this function) is present or not.
//
// The idea is that we have the output columns of `succ_view`, and we want to
// check if a tuple on `view` exists.
CALL *CallTopDownChecker(ProgramImpl *impl, Context &context,
                         REGION *parent, QueryView succ_view,
                         QueryView view, ProgramOperation call_op);

// We want to call the checker for `view`, but we only have the columns
// `succ_cols` available for use.
CALL *CallTopDownChecker(
    ProgramImpl *impl, Context &context, REGION *parent, QueryView succ_view,
    const std::vector<QueryColumn> &succ_cols, QueryView view,
    ProgramOperation call_op, TABLE *already_checked=nullptr);

// Call the predecessor view's checker function, and if it succeeds, return
// `true`. If we have a persistent table then update the tuple's state in that
// table.
CALL *ReturnTrueWithUpdateIfPredecessorCallSucceeds(
    ProgramImpl *impl, Context &context, REGION *parent,
    QueryView view, const std::vector<QueryColumn> &view_cols, TABLE *table,
    QueryView pred_view, TABLE *already_checked=nullptr);

// Build a bottom-up tuple remover, which marks tuples as being in the
// UNKNOWN state (for later top-down checking).
PROC *GetOrCreateBottomUpRemover(ProgramImpl *impl, Context &context,
                                 QueryView view, TABLE *table);

// Returns `true` if `view` might need to have its data persisted for the
// sake of supporting differential updates / verification.
bool MayNeedToBePersisted(QueryView view);

// Decides whether or not `view` can depend on `pred_view` for persistence
// of its data.
bool CanDeferPersistingToPredecessor(ProgramImpl *impl, Context &context,
                                     QueryView view, QueryView pred_view);

// Build a check for inserting into a view.
template <typename Columns>
OP *BuildInsertCheck(ProgramImpl *impl, QueryView view, Context &context,
                     OP *parent, TABLE *table, bool differential,
                     Columns &&columns) {

  // If we can receive deletions, then we need to call a functor that will
  // tell us if this tuple doesn't actually exist.
  if (differential) {
    const auto check = CallTopDownChecker(
        impl, context, parent, view, view,
        ProgramOperation::kCallProcedureCheckFalse);
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
