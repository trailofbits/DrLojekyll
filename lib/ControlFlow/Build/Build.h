// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Util/DefUse.h>

#include <algorithm>
#include <map>
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

  // The set of non-dominated merges that need to have persistent backing.
  std::vector<QueryView> merges;

  // The set of all merges that belong to this co-inductive set.
  std::vector<QueryView> all_merges;
};

class Context;

// Generic action for lifting to the control flow representation. Work item
// actions enable us to defer some liting to the control flow representation
// until `Run` is invoked.
class WorkItem {
 public:
  static constexpr unsigned kContinueJoinOrder = 0u;
  static constexpr unsigned kContinueInductionOrder = 1u << 30;  // (~0u) >> 2u;
  static constexpr unsigned kFinalizeInductionOrder = 2u << 30;  // (~0u) >> 1u;

  virtual ~WorkItem(void);
  virtual void Run(ProgramImpl *program, Context &context) = 0;

  explicit WorkItem(Context &context, unsigned order_);

  const unsigned order;
};

using WorkItemPtr = std::unique_ptr<WorkItem>;

class ContinueInductionWorkItem;
class ContinueJoinWorkItem;
class ContinueProductWorkItem;

// General wrapper around data used when lifting from the data flow to the
// control flow representation.
class Context {
 public:
  PROC *init_proc{nullptr};
  PROC *entry_proc{nullptr};

  // Maps views to the functions that check if their conditions are satisfied
  // or not.
  std::unordered_map<QueryView, PROC *> cond_checker_procs;

  // Maps negations to the checker procedure that tries to figure out if the
  // negated view has some data or not.
  //  std::unordered_map<QueryView, PROC *> negation_checker_procs;

  // Vectors that are associated with differential messages that we will
  // publish. We unique the contents of these at the end of the data flow
  // procedure, then iterate, check, and publish.
  std::unordered_map<ParsedMessage, VECTOR *> publish_vecs;
  std::unordered_map<ParsedMessage, QueryView> published_view;

  // Map `QueryMerge`s to INDUCTIONs. One or more `QueryMerge`s might map to
  // the same INDUCTION if they belong to the same "inductive set". This happens
  // when two or more `QueryMerge`s are cyclic, and their cycles intersect.
  std::unordered_map<QueryView, INDUCTION *> view_to_induction;

  //  // Boolean variable to test if we've ever produced anything for this product,
  //  // and thus should push data through.
  //  std::unordered_map<QueryView, VAR *> product_guard_var;

  // The current "pending" induction. Consider the following:
  //
  //        UNION0        UNION1
  //           \            /
  //            '-- JOIN --'
  //                  |
  //
  // In this case, we don't want UNION0 to be nest inside UNION1 or vice
  // versa, they should both "activate" at the same time. The work list
  // operates in such a way that we exhaust all JOINs before any UNIONs, so in
  // this process, we want to discover the frontiers to as many inductive UNIONs
  // as possible, so that they can all share the same INDUCTION.
  std::unordered_map<unsigned, ContinueInductionWorkItem *>
      pending_induction_action;

  // Work list of actions to invoke to build the execution tree.
  std::vector<WorkItemPtr> work_list;

  std::unordered_map<QueryView, ContinueJoinWorkItem *> view_to_join_action;
  std::unordered_map<QueryView, ContinueProductWorkItem *>
      view_to_product_action;
  std::unordered_map<QueryView, ContinueInductionWorkItem *>
      view_to_induction_action;

  // Maps views to procedures for top-down execution. The top-down executors
  // exist to determine if a tuple is actually PRESENT (returning false if so),
  // converting unprovable UNKNOWNs into ABSENTs.
  std::unordered_map<std::string, PROC *> view_to_top_down_checker;

  // Maps views to procedures for bottom-up proving that goes and removes
  // tuples. Removal of tuples changes their state from PRESENT to UNKNOWN.
  std::unordered_map<std::string, PROC *> view_to_bottom_up_remover;

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

  // A work list of bottom-up provers (that remove tuples) to build.
  std::vector<std::tuple<QueryView, QueryView, PROC *, TABLE *>>
      bottom_up_removers_work_list;
};

OP *BuildStateCheckCaseReturnFalse(ProgramImpl *impl, REGION *parent);
OP *BuildStateCheckCaseReturnTrue(ProgramImpl *impl, REGION *parent);
OP *BuildStateCheckCaseNothing(ProgramImpl *impl, REGION *parent);

bool NeedsInductionCycleVector(QueryView view);
bool NeedsInductionOutputVector(QueryView view);

template <typename Cols, typename IfPresent, typename IfAbsent,
          typename IfUnknown>
static CHECKSTATE *
BuildTopDownCheckerStateCheck(ProgramImpl *impl, REGION *parent, TABLE *table,
                              Cols &&cols, IfPresent if_present_cb,
                              IfAbsent if_absent_cb, IfUnknown if_unknown_cb) {

  const auto check = impl->operation_regions.CreateDerived<CHECKSTATE>(parent);
  for (auto col : cols) {
    const auto var = check->VariableFor(impl, col);
    check->col_values.AddUse(var);
  }

  check->table.Emplace(check, table);

  if (REGION *present_op = if_present_cb(impl, check); present_op) {
    assert(present_op->parent == check);
    check->OP::body.Emplace(check, present_op);
  }

  if (REGION *absent_op = if_absent_cb(impl, check); absent_op) {
    assert(absent_op->parent == check);
    check->absent_body.Emplace(check, absent_op);
  }

  if (REGION *unknown_op = if_unknown_cb(impl, check); unknown_op) {
    assert(unknown_op->parent == check);
    check->unknown_body.Emplace(check, unknown_op);
  }

  return check;
}

// Change the state of some relation.
template <typename Cols>
static CHANGESTATE *
BuildChangeState(ProgramImpl *impl, TABLE *table, REGION *parent, Cols &&cols,
                 TupleState from_state, TupleState to_state) {

  const auto state_change = impl->operation_regions.CreateDerived<CHANGESTATE>(
      parent, from_state, to_state);

  state_change->table.Emplace(state_change, table);
  for (auto col : cols) {
    const auto var = parent->VariableFor(impl, col);
    state_change->col_values.AddUse(var);
  }

  return state_change;
}

template <typename Cols, typename AfterChangeState>
static CHANGESTATE *BuildTopDownTryMarkAbsent(ProgramImpl *impl, TABLE *table,
                                              REGION *parent, Cols &&cols,
                                              AfterChangeState with_par_node) {

  // Change the tuple's state to mark it as deleted so that we can't use it
  // as its own base case.
  const auto table_remove = BuildChangeState(
      impl, table, parent, cols, TupleState::kUnknown, TupleState::kAbsent);

  // Now that we've established the base case (marking the tuple absent), we
  // need to go and actually check all the possibilities.
  const auto par = impl->parallel_regions.Create(table_remove);
  table_remove->OP::body.Emplace(table_remove, par);

  with_par_node(par);

#ifndef NDEBUG
  for (auto region : par->regions) {
    assert(region->parent == par);
  }
#endif

  return table_remove;
}

template <typename Cols, typename AfterChangeState>
static CHANGESTATE *
BuildBottomUpTryMarkUnknown(ProgramImpl *impl, TABLE *table, REGION *parent,
                            Cols &&cols, AfterChangeState with_par_node) {

  // Change the tuple's state to mark it as deleted so that we can't use it
  // as its own base case.
  const auto table_remove = BuildChangeState(
      impl, table, parent, cols, TupleState::kPresent, TupleState::kUnknown);

  // Now that we've established the base case (marking the tuple absent), we
  // need to go and actually check all the possibilities.
  const auto par = impl->parallel_regions.Create(table_remove);
  table_remove->OP::body.Emplace(table_remove, par);

  with_par_node(par);

#ifndef NDEBUG
  for (auto region : par->regions) {
    assert(region->parent == par);
  }
#endif

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
static bool BuildMaybeScanPartial(ProgramImpl *impl, QueryView view,
                                  std::vector<QueryColumn> &view_cols,
                                  TABLE *table, SERIES *seq, ForEachTuple cb) {

  // Sort and unique out the relevant columns.
  std::sort(view_cols.begin(), view_cols.end(),
            [](QueryColumn a, QueryColumn b) {
              assert(!a.IsConstant());
              assert(!b.IsConstant());
              assert(QueryView::Containing(a) == QueryView::Containing(b));
              return *(a.Index()) < *(b.Index());
            });

  auto it = std::unique(view_cols.begin(), view_cols.end());
  view_cols.erase(it, view_cols.end());

  // Figure out if we even need an index scan; if we've got all the columns
  // then we can just use `cb`, which assumes the availability of all columns.
  const auto num_cols = view.Columns().size();
  if (view_cols.size() == num_cols) {
    const auto ret = cb(seq, false);
    if (ret) {
      assert(ret->parent == seq);
      seq->AddRegion(ret);
    }
    return false;
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

  TABLEINDEX *index = nullptr;
  if (!in_col_indices.empty()) {
    index = table->GetOrCreateIndex(impl, std::move(in_col_indices));
  }

  // Scan an index, using the columns from the tuple to find the columns
  // from the tuple's predecessor.
  const auto scan = impl->operation_regions.CreateDerived<TABLESCAN>(
      impl->next_id++, seq);
  seq->AddRegion(scan);
  scan->table.Emplace(scan, table);
  if (index) {
    scan->index.Emplace(scan, index);
  }

  for (QueryColumn view_col : view_cols) {
    const auto in_var = seq->VariableFor(impl, view_col);
    scan->in_vars.AddUse(in_var);
  }

  // Scans are funny. Even though we're looking into an index, we permit the
  // index to be slightly faulty, and so we double check all results.
  TUPLECMP * const cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
      scan, ComparisonOperator::kEqual);
  scan->body.Emplace(scan, cmp);

  auto i = 0u;
  auto j = 0u;
  for (TABLECOLUMN *table_col : table->columns) {

    VAR *out_var = scan->out_vars.Create(
        impl->next_id++, VariableRole::kScanOutput);

    QueryColumn view_col = view.NthColumn(i++);
    out_var->query_column = view_col;

    if (indexed_cols[table_col->index]) {
      assert(index != nullptr);
      scan->in_cols.AddUse(table_col);

      VAR *in_var = scan->in_vars[j++];
      cmp->lhs_vars.AddUse(in_var);
      cmp->rhs_vars.AddUse(out_var);
      cmp->col_id_to_var[view_col.Id()] = in_var;

    } else {
      scan->out_cols.AddUse(table_col);
      cmp->col_id_to_var[view_col.Id()] = out_var;
    }
  }

  // Mutable in place so that `cb` can observe an updates set of available
  // columns.
  view_cols.clear();
  for (auto col : view.Columns()) {
    view_cols.push_back(col);
  }

  REGION * const in_loop = cb(cmp, true);
  assert(!cmp->body);
  if (in_loop) {
    assert(in_loop->parent == cmp);
    cmp->body.Emplace(cmp, in_loop);
  }
  return true;
}


// Build an eager region for removing data.
void BuildEagerRemovalRegion(ProgramImpl *impl, QueryView pred_view,
                             QueryView view, Context &context, OP *parent,
                             TABLE *already_removed);

// Build an eager region for adding data.
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
REGION *BuildTopDownInductionChecker(ProgramImpl *impl, Context &context,
                                     REGION *proc, QueryMerge view,
                                     std::vector<QueryColumn> &available_cols,
                                     TABLE *already_checked);

// Build an eager region for a `QueryMerge` that is NOT part of an inductive
// loop, and thus passes on its data to the next thing down as long as that
// data is unique.
void BuildEagerUnionRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryMerge view, Context &context, OP *parent,
                           TABLE *last_model);

// Build a top-down checker on a union. This applies to non-differential
// unions.
REGION *BuildTopDownUnionChecker(ProgramImpl *impl, Context &context,
                                 REGION *parent, QueryMerge view,
                                 std::vector<QueryColumn> &available_cols,
                                 TABLE *already_checked);

// Build a top-down checker on a join.
REGION *BuildTopDownJoinChecker(ProgramImpl *impl, Context &context,
                                REGION *parent, QueryJoin view,
                                std::vector<QueryColumn> &available_cols,
                                TABLE *already_checked);

// Build an eager region for publishing data, or inserting it. This might end
// up passing things through if this isn't actually a message publication.
void BuildEagerInsertRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryInsert view, Context &context, OP *parent,
                            TABLE *last_model);

// Build an eager region for testing the absence of some data in another view.
void BuildEagerNegateRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryNegate negate, Context &context, OP *parent_,
                            TABLE *last_table_);

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
void BuildEagerGenerateRegion(ProgramImpl *impl, QueryView pred_view,
                              QueryMap view, Context &context, OP *parent,
                              TABLE *last_model);

// Build an eager region for tuple. If the tuple can receive differential
// updates then its data needs to be saved.
void BuildEagerTupleRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryTuple tuple, Context &context, OP *parent,
                           TABLE *last_model);

// Build a top-down checker on a tuple. This possibly widens the tuple, i.e.
// recovering "lost" columns, and possibly re-orders arguments before calling
// down to the tuple's predecessor's checker.
REGION *BuildTopDownTupleChecker(ProgramImpl *impl, Context &context,
                                 REGION *proc, QueryTuple tuple,
                                 std::vector<QueryColumn> &available_cols,
                                 TABLE *already_checked);

// Build a top-down checker on a negation.
REGION *BuildTopDownNegationChecker(ProgramImpl *impl, Context &context,
                                    REGION *proc, QueryNegate negate,
                                    std::vector<QueryColumn> &available_cols,
                                    TABLE *already_checked);

// Build a top-down checker on a select.
REGION *BuildTopDownSelectChecker(ProgramImpl *impl, Context &context,
                                  REGION *proc, QuerySelect select,
                                  std::vector<QueryColumn> &available_cols,
                                  TABLE *already_checked);

// Build a top-down checker on a compare.
REGION *BuildTopDownCompareChecker(ProgramImpl *impl, Context &context,
                                   REGION *proc, QueryCompare cmp,
                                   std::vector<QueryColumn> &available_cols,
                                   TABLE *already_checked);

// Build a top-down checker on a map / generator.
REGION *BuildTopDownGeneratorChecker(ProgramImpl *impl, Context &context,
                                     REGION *proc, QueryMap gen,
                                     std::vector<QueryColumn> &view_cols,
                                     TABLE *already_checked);

// Builds an initialization function which does any work that depends purely
// on constants.
void BuildInitProcedure(ProgramImpl *impl, Context &context, Query query);

// Build the primary and entry data flow procedures.
void BuildEagerProcedure(ProgramImpl *impl, Context &context, Query query);

// Complete a procedure by exhausting the work list.
void CompleteProcedure(ProgramImpl *impl, PROC *proc, Context &context,
                       bool add_return = true);

// Returns `true` if all paths through `region` ends with a `return` region.
inline bool EndsWithReturn(REGION *region) {
  if (region) {
    return region->EndsWithReturn();
  } else {
    return false;
  }
}

// Returns a global reference count variable associated with a query condition.
VAR *ConditionVariable(ProgramImpl *impl, QueryCondition cond);

// Expand the set of available columns.
void ExpandAvailableColumns(
    QueryView view, std::unordered_map<unsigned, QueryColumn> &wanted_to_avail);

// Filter out only the available columns that are part of the view we care
// about.
std::vector<std::pair<QueryColumn, QueryColumn>> FilterAvailableColumns(
    QueryView view,
    const std::unordered_map<unsigned, QueryColumn> &wanted_to_avail);

// Computes the actual set of available columns.
template <typename Cols>
static std::vector<std::pair<QueryColumn, QueryColumn>>
ComputeAvailableColumns(QueryView view, Cols &&available_cols) {

  std::unordered_map<unsigned, QueryColumn> wanted_to_avail;

  for (QueryColumn param_col : available_cols) {
    if (QueryView::Containing(param_col) == view) {
      wanted_to_avail.emplace(param_col.Id(), param_col);
    } else {
      assert(false);
    }
  }

  ExpandAvailableColumns(view, wanted_to_avail);
  return FilterAvailableColumns(view, wanted_to_avail);
}

// Gets or creates a top down checker function.
PROC *GetOrCreateTopDownChecker(
    ProgramImpl *impl, Context &context, QueryView view,
    const std::vector<std::pair<QueryColumn, QueryColumn>> &wanted_to_avail,
    TABLE *already_checked);

// We want to call the checker for `view`, but we only have the columns
// `succ_cols` available for use.
std::pair<OP *, CALL *>
CallTopDownChecker(ProgramImpl *impl, Context &context, REGION *parent,
                   QueryView succ_view,
                   const std::vector<QueryColumn> &succ_cols, QueryView view,
                   TABLE *already_checked = nullptr);

// Call the predecessor view's checker function, and if it succeeds, return
// `true`. If we have a persistent table then update the tuple's state in that
// table.
template <typename CB1, typename CB2>
OP *CallTopDownChecker(ProgramImpl *impl, Context &context, REGION *parent,
                       QueryView view,
                       const std::vector<QueryColumn> &view_cols,
                       QueryView pred_view, TABLE *already_checked, CB1 if_true,
                       CB2 if_false) {

  const auto [check, check_call] = CallTopDownChecker(
      impl, context, parent, view, view_cols, pred_view, already_checked);

  if (REGION *body_if_true = if_true(check_call); body_if_true) {
    check_call->body.Emplace(check_call, body_if_true);
  }

  if (REGION *body_if_false = if_false(check_call); body_if_false) {
    check_call->false_body.Emplace(check_call, body_if_false);
  }

  return check;
}

// Call the predecessor view's checker function, and if it succeeds, return
// `true`. If we have a persistent table then update the tuple's state in that
// table.
OP *ReturnTrueWithUpdateIfPredecessorCallSucceeds(
    ProgramImpl *impl, Context &context, REGION *parent, QueryView view,
    const std::vector<QueryColumn> &view_cols, TABLE *table,
    QueryView pred_view, TABLE *already_checked = nullptr);

// Possibly add a check to into `parent` to transition the tuple with the table
// associated with `view` to be in an present state. Returns the table of `view`
// and the updated `already_removed`.
//
// NOTE(pag): If the table associated with `view` is also associated with an
//            induction, then we defer insertion until we get into that
//            induction.
std::tuple<OP *, TABLE *, TABLE *>
InTryInsert(ProgramImpl *impl, Context &context, QueryView view, OP *parent,
            TABLE *already_added);

// Possibly add a check to into `parent` to transition the tuple with the table
// associated with `view` to be in an unknown state. Returns the table of `view`
// and the updated `already_removed`.
//
// NOTE(pag): If the table associated with `view` is also associated with an
//            induction, then we defer removal until we get into that
//            induction.
std::tuple<OP *, TABLE *, TABLE *>
InTryMarkUnknown(ProgramImpl *impl, Context &context, QueryView view,
                 OP *parent, TABLE *already_removed);

// Build a bottom-up tuple remover, which marks tuples as being in the
// UNKNOWN state (for later top-down checking).
PROC *GetOrCreateBottomUpRemover(ProgramImpl *impl, Context &context,
                                 QueryView from_view, QueryView to_view,
                                 TABLE *already_checked = nullptr);

void CreateBottomUpInsertRemover(ProgramImpl *impl, Context &context,
                                 QueryView view, OP *parent,
                                 TABLE *already_checked);

void CreateBottomUpGenerateRemover(ProgramImpl *impl, Context &context,
                                   QueryMap map, ParsedFunctor functor,
                                   OP *parent, TABLE *already_checked);

void CreateBottomUpInductionRemover(ProgramImpl *impl, Context &context,
                                    QueryView view, OP *parent,
                                    TABLE *already_removed);

void CreateBottomUpUnionRemover(ProgramImpl *impl, Context &context,
                                QueryView view, OP *parent,
                                TABLE *already_checked);

void CreateBottomUpTupleRemover(ProgramImpl *impl, Context &context,
                                QueryView view, OP *parent,
                                TABLE *already_checked);

void CreateBottomUpNegationRemover(ProgramImpl *impl, Context &context,
                                   QueryView view, OP *parent,
                                   TABLE *already_checked);

void CreateBottomUpCompareRemover(ProgramImpl *impl, Context &context,
                                  QueryView view, OP *root,
                                  TABLE *already_checked);

void CreateBottomUpJoinRemover(ProgramImpl *impl, Context &context,
                               QueryView from_view, QueryJoin join, OP *root,
                               TABLE *already_checked);

// Returns `true` if `view` might need to have its data persisted.
bool MayNeedToBePersisted(QueryView view);

// Returns `true` if `view` might need to have its data persisted for the
// sake of supporting differential updates / verification.
bool MayNeedToBePersistedDifferential(QueryView view);

// Build and dispatch to the bottom-up remover regions for `view`. The idea
// is that we've just removed data from `view`, and now want to tell the
// successors of this.
void BuildEagerRemovalRegionsImpl(ProgramImpl *impl, QueryView view,
                                  Context &context, OP *parent_,
                                  const std::vector<QueryView> &successors,
                                  TABLE *already_removed_);

// Build and dispatch to the bottom-up remover regions for `view`. The idea
// is that we've just removed data from `view`, and now want to tell the
// successors of this.
template <typename List>
static void BuildEagerRemovalRegions(ProgramImpl *impl, QueryView view,
                                     Context &context, OP *parent,
                                     List &&successors_,
                                     TABLE *already_removed) {

  std::vector<QueryView> successors;
  for (auto succ_view : successors_) {
    successors.push_back(succ_view);
  }

  BuildEagerRemovalRegionsImpl(impl, view, context, parent, successors,
                               already_removed);
}

// Add in all of the successors of a view inside of `parent`, which is
// usually some kind of loop. The successors execute in parallel.
void BuildEagerInsertionRegionsImpl(ProgramImpl *impl, QueryView view,
                                    Context &context, OP *parent_,
                                    const std::vector<QueryView> &successors,
                                    TABLE *last_table_);

// Add in all of the successors of a view inside of `parent`, which is
// usually some kind of loop. The successors execute in parallel.
template <typename List>
static void BuildEagerInsertionRegions(ProgramImpl *impl, QueryView view,
                                       Context &context, OP *parent_,
                                       List &&successors_, TABLE *last_table) {
  std::vector<QueryView> successors;
  for (auto succ_view : successors_) {
    successors.push_back(succ_view);
  }

  BuildEagerInsertionRegionsImpl(impl, view, context, parent_, successors,
                                 last_table);
}

}  // namespace hyde
