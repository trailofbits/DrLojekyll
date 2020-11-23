// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

#include <algorithm>
#include <sstream>

// IDEAS
//
//    - born_on_same_day:
//        -   We want both sides to contribute their data to the JOIN, but the
//            actual execution of the JOIN should be deferred as a pipeline
//            blocker at a higher level. So what will happen is each side
//            contributes and records the JOIN pivots into a vector, which is
//            later sorted and uniqued.
//
//        - sort child nodes of a parallel by a hash, but where the hash is
//          not based on children and only on the superficial aspects of the
//          region itself, so that we can look for opportunities to join
//          region bodies
//
//    - Make sure all SELECTs reachable from a RECV are entrypoints, put
//      the INSERTs for them into vectors.
//
//    - Right now, no guarantee that the arity of two identical sets of column
//      IDs will match, and so ARITY is a key issue. Need to add a notion of
//      provenance or selectors to a table. For example:
//
//                 .--- COMPARE --.
//          RECV --+              +-- JOIN
//                 '--- TUPLE ----'
//
//      In this case, the COMPARE and TUPLE might have identical columns, and
//      thus appear equivalent, but we should really have an additional field
//      per row that tells us if the row is present for both the COMPARE and the
//      TUPLE.
//
//    - Think about if the TABLEs entering a JOIN should be based on all data
//      in the predecessor, or just the data used by the JOIN. Right now it's
//      the former.
namespace hyde {
namespace {

// Return the set of all views that contribute data to `view`. This includes
// things like conditions.
static std::set<QueryView> TransitivePredecessorsOf(QueryView output) {
  std::set<QueryView> dependencies;
  std::vector<QueryView> frontier;
  frontier.push_back(output);

  while (!frontier.empty()) {
    const auto view = frontier.back();
    frontier.pop_back();
    for (auto pred_view : view.Predecessors()) {
      if (!dependencies.count(pred_view)) {
        dependencies.insert(pred_view);
        frontier.push_back(pred_view);
      }
    }
  }

  return dependencies;
}

// Return the set of all views that are transitively derived from `input`.
static std::set<QueryView> TransitiveSuccessorsOf(QueryView input) {
  std::set<QueryView> dependents;
  std::vector<QueryView> frontier;
  frontier.push_back(input);

  while (!frontier.empty()) {
    const auto view = frontier.back();
    frontier.pop_back();
    for (auto succ_view : view.Successors()) {
      if (!dependents.count(succ_view)) {
        dependents.insert(succ_view);
        frontier.push_back(succ_view);
      }
    }
  }

  return dependents;
}

// Build an eager region where this eager region is being unconditionally
// executed, i.e. ignoring whether or not `view.PositiveConditions()` or
// `view.NegativeConditions()` have elements.
static void BuildUnconditionalEagerRegion(ProgramImpl *impl,
                                          QueryView pred_view, QueryView view,
                                          Context &context, OP *parent,
                                          TABLE *last_model) {
  if (view.IsJoin()) {
    const auto join = QueryJoin::From(view);
    if (join.NumPivotColumns()) {
      BuildEagerJoinRegion(impl, pred_view, join, context, parent, last_model);
    } else {
      BuildEagerProductRegion(impl, pred_view, join, context, parent,
                              last_model);
    }

  } else if (view.IsMerge()) {
    const auto merge = QueryMerge::From(view);
    if (context.inductive_successors.count(view)) {
      BuildEagerInductiveRegion(impl, pred_view, merge, context, parent,
                                last_model);
    } else {
      BuildEagerUnionRegion(impl, pred_view, merge, context, parent,
                            last_model);
    }

  } else if (view.IsAggregate()) {
    assert(false && "TODO(pag): Aggregates");

  } else if (view.IsKVIndex()) {
    assert(false && "TODO(pag): KV Indices.");

  } else if (view.IsMap()) {
    auto map = QueryMap::From(view);
    if (map.Functor().IsPure()) {
      BuildEagerGenerateRegion(impl, map, context, parent);

    } else {
      assert(false && "TODO(pag): Impure functors");
    }

  } else if (view.IsCompare()) {
    BuildEagerCompareRegions(impl, QueryCompare::From(view), context, parent);

  } else if (view.IsSelect()) {
    BuildEagerSuccessorRegions(impl, view, context, parent, view.Successors(),
                               last_model);

  } else if (view.IsTuple()) {
    BuildEagerTupleRegion(impl, pred_view, QueryTuple::From(view), context,
                          parent, last_model);

  } else if (view.IsInsert()) {
    const auto insert = QueryInsert::From(view);
    BuildEagerInsertRegion(impl, pred_view, insert, context,
                                 parent, last_model);

  } else if (view.IsDelete()) {
    BuildEagerDeleteRegion(impl, view, context, parent);

  } else {
    assert(false);
  }
}

// Map all variables to their defining regions.
static void MapVariables(REGION *region) {
  if (!region) {
    return;

  } else if (auto op = region->AsOperation(); op) {
    if (auto let = op->AsLetBinding(); let) {
      for (auto var : let->defined_vars) {
        var->defining_region = region;
      }
    } else if (auto loop = op->AsVectorLoop(); loop) {
      for (auto var : loop->defined_vars) {
        var->defining_region = region;
      }
    } else if (auto join = op->AsTableJoin(); join) {
      for (auto var : join->pivot_vars) {
        var->defining_region = region;
      }
      for (const auto &var_list : join->output_vars) {
        for (auto var : var_list) {
          var->defining_region = region;
        }
      }
    }

    MapVariables(op->body.get());

  } else if (auto induction = region->AsInduction(); induction) {
    MapVariables(induction->init_region.get());
    MapVariables(induction->cyclic_region.get());
    MapVariables(induction->output_region.get());

  } else if (auto par = region->AsParallel(); par) {
    for (auto sub_region : par->regions) {
      MapVariables(sub_region);
    }
  } else if (auto series = region->AsSeries(); series) {
    for (auto sub_region : series->regions) {
      MapVariables(sub_region);
    }
  }
}

// Create a procedure for a view.
static void BuildEagerProcedure(ProgramImpl *impl, QueryIO io,
                                Context &context) {
  const auto receives = io.Receives();
  if (receives.empty()) {
    return;
  }

  const auto proc = impl->procedure_regions.Create(
      impl->next_id++, ProcedureKind::kMessageHandler);
  proc->io = io;

  const auto vec =
      proc->VectorFor(impl, VectorKind::kInput, receives[0].Columns());
  const auto loop = impl->operation_regions.CreateDerived<VECTORLOOP>(
      proc, ProgramOperation::kLoopOverInputVector);
  auto par = impl->parallel_regions.Create(loop);

  for (auto col : receives[0].Columns()) {
    const auto var = loop->defined_vars.Create(impl->next_id++,
                                               VariableRole::kVectorVariable);
    var->query_column = col;
    loop->col_id_to_var.emplace(col.Id(), var);
  }

  loop->body.Emplace(loop, par);
  loop->vector.Emplace(loop, vec);
  proc->body.Emplace(proc, loop);

  context.view_to_induction.clear();
  context.work_list.clear();
  context.view_to_work_item.clear();
  context.product_vector.clear();

  for (auto receive : io.Receives()) {
    auto let = impl->operation_regions.CreateDerived<LET>(par);
    let->ExecuteAlongside(impl, par);

    auto i = 0u;
    for (auto col : receive.Columns()) {
      auto first_col = receives[0].Columns()[i++];
      if (col.Id() != first_col.Id()) {
        let->used_vars.AddUse(par->VariableFor(impl, first_col));
        const auto var = let->defined_vars.Create(impl->next_id++,
                                                  VariableRole::kLetBinding);
        var->query_column = col;
        loop->col_id_to_var.emplace(col.Id(), var);
      }
    }

    BuildEagerSuccessorRegions(impl, receive, context, let,
                               receive.Successors(), nullptr);
  }

  CompleteProcedure(impl, proc, context);
}

// Analyze the MERGE/UNION nodes and figure out which ones are inductive.
static void DiscoverInductions(const Query &query, Context &context) {
  unsigned merge_id = 0u;
  for (auto view : query.Merges()) {
    context.merge_sets.emplace(view, merge_id++);
    auto preds = TransitivePredecessorsOf(view);

    // This is not an inductive merge.
    if (!preds.count(view)) {
      continue;
    }

    for (auto succ_view : QueryView(view).Successors()) {
      if (preds.count(succ_view)) {
        context.inductive_successors[view].insert(succ_view);
      } else {
        context.noninductive_successors[view].insert(succ_view);
      }
    }

    auto succs = TransitiveSuccessorsOf(view);
    for (auto pred_view : QueryView(view).Predecessors()) {
      if (succs.count(pred_view)) {
        context.inductive_predecessors[view].insert(pred_view);
      } else {
        context.noninductive_predecessors[view].insert(pred_view);
      }
    }
  }

  // Now group together the merges into co-inductive sets, i.e. when one
  // induction is tied with another induction.
  std::set<QueryView> seen;
  std::vector<QueryView> frontier;
  std::set<std::pair<QueryView, QueryView>> disallowed_edges;

  for (auto &[view, noninductive_predecessors] :
       context.noninductive_predecessors) {
    for (QueryView pred_view : noninductive_predecessors) {
      disallowed_edges.emplace(pred_view, view);
    }
  }

  for (const auto &[view, inductive_successors] :
       context.inductive_successors) {
    if (inductive_successors.empty()) {
      continue;
    }

    frontier.clear();
    seen.clear();

    for (QueryView succ_view : inductive_successors) {
      frontier.push_back(succ_view);
    }

    while (!frontier.empty()) {
      const auto view = frontier.back();
      frontier.pop_back();
      for (auto succ_view : view.Successors()) {
        if (!seen.count(succ_view) &&
            !disallowed_edges.count({view, succ_view})) {
          seen.insert(succ_view);
          frontier.push_back(succ_view);
        }
      }
    }

    DisjointSet &base_set = context.merge_sets[view];
    for (auto reached_view : seen) {
      if (reached_view.IsMerge()) {
        QueryMerge reached_merge = QueryMerge::From(reached_view);
        if (context.inductive_successors.count(reached_merge)) {
          auto &reached_set = context.merge_sets[reached_merge];
          DisjointSet::Union(&base_set, &reached_set);
        }
      }
    }
  }

  for (auto &[merge, merge_set] : context.merge_sets) {
    merge_set.FindAs<InductionSet>()->merges.push_back(merge);
  }
}

// Building the data model means figuring out which `QueryView`s can share the
// same backing storage. This doesn't mean that all views will be backed by
// such storage, but when we need backing storage, we can maximally share it
// among other places where it might be needed.
static void BuildDataModel(const Query &query, ProgramImpl *program) {
  query.ForEachView([=](QueryView view) {
    auto model = new DataModel;
    program->models.emplace_back(model);
    program->view_to_model.emplace(view, model);
  });

  auto all_cols_match = [](auto cols, auto pred_cols) {
    const auto num_cols = cols.size();
    if (num_cols != pred_cols.size()) {
      return false;
    }

    for (auto i = 0u; i < num_cols; ++i) {
      if (cols[i].Index() != pred_cols[i].Index()) {
        return false;
      }
    }

    return true;
  };

  // If the view tests any conditions then it can't share a data model
  // with its predecessor.
  //
  // NOTE(pag): Conditions are a tire fire.
  auto is_conditional = [](QueryView view) {
    return !view.NegativeConditions().empty() ||
           !view.PositiveConditions().empty() || view.IsCompare() ||
           view.IsMap();
  };

  query.ForEachView([=](QueryView view) {
    if (is_conditional(view)) {
      return;
    }

    const auto model = program->view_to_model[view];
    const auto preds = view.Predecessors();

    // UNIONs can share the data of any of their predecessors so long as
    // those predecessors don't themselves have other successors, i.e. they
    // only lead into the UNION.
    //
    // We also have to be careful about merges that receive deletions. If so,
    // then we need to be able to distinguish where data is from. This is
    // especially important for comparisons or maps leading into merges.
    if (view.IsMerge()) {
      const auto can_receive_deletions = view.CanReceiveDeletions();
      for (auto pred : preds) {
        if (!pred.IsDelete() &&
            pred.Successors().size() == 1u &&
            !can_receive_deletions) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
      }

    // If a TUPLE "perfectly" passes through its data, then it shares the
    // same data model as its predecessor.
    } else if (view.IsTuple()) {
      if (preds.size() == 1u) {
        const auto pred = preds[0];
        if (!pred.IsDelete() &&
            all_cols_match(view.Columns(), pred.Columns())) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
      }

    // INSERTs have no output columns. If an insert has only a single
    // predecessor (it should), and if all columns of the predecessor are
    // used and in the same order, then this INSERT and its predecessor
    // share the same data model.
    } else if (view.IsInsert()) {
      if (preds.size() == 1u) {
        const auto pred = preds[0];
        const auto insert = QueryInsert::From(view);
        const auto cols = insert.InputColumns();
        const auto pred_cols = pred.Columns();
        if (!pred.IsDelete() && all_cols_match(cols, pred_cols)) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
      }

    // NOTE(pag): DELETE nodes don't have a data model per se. They exist
    //            to delete things in the data model of their /successor/,
    //            they can't delete anything in the model of their predecessor.
    //
    // NOTE(pag): If a DELETE flows into a DELETE then we permit them to
    //            share a data model, for whatever that's worth.
    } else if (view.IsDelete()) {
      if (preds.size() == 1u) {
        const auto insert = QueryDelete::From(view);
        const auto cols = insert.InputColumns();
        const auto pred_cols = preds[0].Columns();
        if (all_cols_match(cols, pred_cols)) {
          const auto pred_model = program->view_to_model[preds[0]];
          DisjointSet::Union(model, pred_model);
        }
      }

    // Select predecessors are INSERTs, which don't have output columns.
    // In theory, there could be more than one INSERT. Selects always share
    // the data model with their corresponding INSERTs.
    //
    // TODO(pag): This more about the interplay with conditional inserts.
    } else if (view.IsSelect()) {
      for (auto pred : preds) {
        if (pred.IsInsert()) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
      }
    }
  });
}

// Returns `true` if all paths through `region` ends with a `return` region.
static bool EndsWithReturn(REGION *region) {
  if (!region) {
    return false;

  } else if (auto proc = region->AsProcedure(); proc) {
    return EndsWithReturn(proc->body.get());

  } else if (auto series = region->AsSeries(); series) {
    if (auto num_regions = series->regions.Size(); num_regions) {
      return EndsWithReturn(series->regions[num_regions - 1u]);
    } else {
      return false;
    }

  } else if (auto par = region->AsParallel(); par) {
    if (par->regions.Empty()) {
      return false;
    }

    for (auto sub_region : par->regions) {
      if (!EndsWithReturn(sub_region)) {
        return false;
      }
    }

    return true;

  } else if (auto induction = region->AsInduction(); induction) {
    if (auto output = induction->output_region.get(); output) {
      return EndsWithReturn(output);
    } else {
      return false;
    }

  } else if (auto op = region->AsOperation(); op) {
    if (op->AsReturn()) {
      return true;

    } else if (auto cs = op->AsCheckState(); cs) {
      return EndsWithReturn(cs->body.get()) &&
             EndsWithReturn(cs->absent_body.get()) &&
             EndsWithReturn(cs->unknown_body.get());
    }

  }

  return false;
}

// Build out all the bottom-up (negative) provers that are used to mark tuples
// as being in an unknown state. We want to do this after building all
// (positive) bottom-up provers so that we can know which views correspond
// with which tables.
static void BuildBottomUpRemovalProvers(ProgramImpl *impl, Context &context) {
  while (!context.bottom_up_removers_work_list.empty()) {
    auto [from_view, to_view, proc, already_checked] =
        context.bottom_up_removers_work_list.back();
    context.bottom_up_removers_work_list.pop_back();

    if (to_view.IsTuple()) {
      CreateBottomUpTupleRemover(impl, context, to_view, proc, already_checked);

    } else if (to_view.IsCompare()) {
      CreateBottomUpCompareRemover(impl, context, to_view, proc, already_checked);

    } else if (to_view.IsInsert()) {
      CreateBottomUpInsertRemover(impl, context, to_view, proc,
                                  already_checked);

    } else if (to_view.IsDelete()) {
      CreateBottomUpDeleteRemover(impl, context, to_view, proc);

    // NOTE(pag): We don't need to distinguish between unions that are inductions
    //            and unions that are merges.
    } else if (to_view.IsMerge()) {
      CreateBottomUpUnionRemover(impl, context, to_view, proc, already_checked);

    } else if (to_view.IsJoin()) {
      auto join = QueryJoin::From(to_view);
      if (join.NumPivotColumns()) {
        CreateBottomUpJoinRemover(impl, context, from_view, join,
                                  proc, already_checked);
      } else {
        assert(false && "TODO: Cross-products!");
      }
    } else if (to_view.IsAggregate()) {
      assert(false && "TODO Aggregates!");

    } else if (to_view.IsKVIndex()) {
      assert(false && "TODO Key Values!");

    } else if (to_view.IsMap()) {
      auto map = QueryMap::From(to_view);
      auto functor = map.Functor();
      if (functor.IsPure()) {
        CreateBottomUpGenerateRemover(impl, context, map, functor, proc);

      } else {
        assert(false && "TODO Impure Functors!");
      }

    // NOTE(pag): This shouldn't be reachable, as the bottom-up INSERT
    //            removers jump past SELECTs.
    } else if (to_view.IsSelect()) {
      assert(false);

    } else {
      assert(false);
    }

    if (!EndsWithReturn(proc)) {
      const auto ret = impl->operation_regions.CreateDerived<RETURN>(
          proc, ProgramOperation::kReturnFalseFromProcedure);
      ret->ExecuteAfter(impl, proc);
    }
  }
}

// Build out all the top-down checkers. We want to do this after building all
// bottom-up provers so that we can know which views correspond with which
// tables.
static void BuildTopDownCheckers(ProgramImpl *impl, Context &context) {

  while (!context.top_down_checker_work_list.empty()) {
    auto [view, view_cols, proc, already_checked] =
        context.top_down_checker_work_list.back();
    context.top_down_checker_work_list.pop_back();

    assert(!view_cols.empty());

    if (view.IsJoin()) {
      const auto join = QueryJoin::From(view);
      if (join.NumPivotColumns()) {
        BuildTopDownJoinChecker(impl, context, proc, join, view_cols,
                                 already_checked);
      } else {
        assert(false && "TODO");
      }

    } else if (view.IsMerge()) {
      const auto merge = QueryMerge::From(view);
      if (context.inductive_successors.count(view)) {
        BuildTopDownInductionChecker(impl, context, proc, merge, view_cols,
                                     already_checked);

      } else {
        BuildTopDownUnionChecker(impl, context, proc, merge, view_cols,
                                 already_checked);
      }

    } else if (view.IsAggregate()) {
      assert(false && "TODO");

    } else if (view.IsKVIndex()) {
      assert(false && "TODO");

    } else if (view.IsMap()) {
      assert(false && "TODO");

    } else if (view.IsCompare()) {
      BuildTopDownCompareChecker(impl, context, proc, QueryCompare::From(view),
                                 view_cols, already_checked);

    } else if (view.IsSelect()) {
      const auto select = QuerySelect::From(view);

      // The base case is that we get to a SELECT from a a stream. We treat
      // data received as ephemeral, and so there is no way to actually check
      // if the tuple exists, and so we treat it as not existing.
      if (select.IsStream()) {
        // Nothing to do.

      } else {
        assert(false && "TODO");
      }

    } else if (view.IsTuple()) {
      BuildTopDownTupleChecker(impl, context, proc, QueryTuple::From(view),
                               view_cols, already_checked);

    } else if (view.IsInsert()) {
      const auto insert = QueryInsert::From(view);

      if (insert.IsStream()) {
        // Nothing to do.

      } else {
        assert(false && "TODO");
      }

    } else if (view.IsDelete()) {
      // Nothing to do.

    // Not possible?
    } else {
      assert(false);
    }

    // This view is conditional, wrap whatever we had generated in a big
    // if statement.
    const auto pos_conds = view.PositiveConditions();
    const auto neg_conds = view.NegativeConditions();
    const auto proc_body = proc->body.get();

    // Innermost test for negative conditions.
    if (!neg_conds.empty()) {
      auto test = impl->operation_regions.CreateDerived<EXISTS>(
          proc, ProgramOperation::kTestAllZero);

      for (auto cond : neg_conds) {
        test->cond_vars.AddUse(ConditionVariable(impl, cond));
      }

      proc->body.Emplace(proc, test);
      test->body.Emplace(test, proc_body);
    }

    // Outermost test for positive conditions.
    if (!pos_conds.empty()) {
      auto test = impl->operation_regions.CreateDerived<EXISTS>(
          proc, ProgramOperation::kTestAllNonZero);

      for (auto cond : pos_conds) {
        test->cond_vars.AddUse(ConditionVariable(impl, cond));
      }

      proc->body.Emplace(proc, test);
      test->body.Emplace(test, proc_body);
    }

    if (!EndsWithReturn(proc)) {
      const auto ret = impl->operation_regions.CreateDerived<RETURN>(
          proc, ProgramOperation::kReturnFalseFromProcedure);
      ret->ExecuteAfter(impl, proc);
    }
  }
}

}  // namespace

// Returns a global reference count variable associated with a query condition.
VAR *ConditionVariable(ProgramImpl *impl, QueryCondition cond) {
  auto &cond_var = impl->cond_ref_counts[cond];
  if (!cond_var) {
    cond_var = impl->global_vars.Create(impl->next_id++,
                                        VariableRole::kConditionRefCount);
    cond_var->query_cond = cond;
  }
  return cond_var;
}

OP *BuildStateCheckCaseReturnFalse(ProgramImpl *impl, REGION *parent) {
  return impl->operation_regions.CreateDerived<RETURN>(
      parent, ProgramOperation::kReturnFalseFromProcedure);
}

OP *BuildStateCheckCaseReturnTrue(ProgramImpl *impl, REGION *parent) {
  return impl->operation_regions.CreateDerived<RETURN>(
      parent, ProgramOperation::kReturnTrueFromProcedure);
}

OP *BuildStateCheckCaseNothing(ProgramImpl *, REGION *) {
  return nullptr;
}

// Calls a top-down checker that tries to figure out if some tuple (passed as
// arguments to this function) is present or not.
//
// The idea is that we have the output columns of `succ_view`, and we want to
// check if a tuple on `view` exists.
CALL *CallTopDownChecker(ProgramImpl *impl, Context &context, REGION *parent,
                         QueryView succ_view, QueryView view,
                         ProgramOperation call_op) {
  assert(!view.IsDelete());
  assert(!succ_view.IsInsert());

  std::vector<QueryColumn> succ_cols;
  for (auto succ_view_col : succ_view.Columns()) {
    succ_cols.push_back(succ_view_col);
  }

  return CallTopDownChecker(impl, context, parent, succ_view,
                            succ_cols, view, call_op);
}

// We want to call the checker for `view`, but we only have the columns
// `succ_cols` available for use.
CALL *CallTopDownChecker(
    ProgramImpl *impl, Context &context, REGION *parent, QueryView succ_view,
    const std::vector<QueryColumn> &succ_cols, QueryView view,
    ProgramOperation call_op, TABLE *already_checked) {

  assert(!succ_view.IsInsert());

  std::vector<bool> available_cols_map(succ_view.Columns().size());
  for (auto succ_view_col : succ_cols) {
    available_cols_map[*(succ_view_col.Index())] = true;
  }

  std::vector<QueryColumn> available_cols;
  std::unordered_map<QueryColumn, QueryColumn> inout_map;

  if (view != succ_view) {
    succ_view.ForEachUse([&](QueryColumn view_col, InputColumnRole role,
                             std::optional<QueryColumn> succ_view_col) {
      if (InputColumnRole::kIndexValue != role &&
          InputColumnRole::kAggregatedColumn != role &&
          succ_view_col && QueryView::Containing(view_col) == view &&
          available_cols_map[*(succ_view_col->Index())]) {
        available_cols.push_back(view_col);
        inout_map.emplace(view_col, *succ_view_col);
      }
    });

    std::sort(available_cols.begin(), available_cols.end(),
              [] (QueryColumn a, QueryColumn b) {
                return *(a.Index()) < *(b.Index());
              });

    auto it = std::unique(available_cols.begin(), available_cols.end(),
                          [] (QueryColumn a, QueryColumn b) {
                            return *(a.Index()) == *(b.Index());
                          });

    available_cols.erase(it, available_cols.end());

  // Everything is available, yay!
  } else {
    for (auto col : view.Columns()) {
      if (available_cols_map[*(col.Index())]) {
        available_cols.push_back(col);
        inout_map.emplace(col, col);
      }
    }
  }

  assert(!available_cols.empty());

  // Make up a string that captures what we have available.
  std::stringstream ss;
  ss << view.UniqueId();
  for (auto view_col : available_cols) {
    ss << ',' << view_col.Id();
  }
  ss << ':' << reinterpret_cast<uintptr_t>(already_checked);

  // We haven't made this procedure before; so we'll declare it now and
  // enqueue it for building. We enqueue all such procedures for building so
  // that we can build top-down checkers after all bottom-up provers have been
  // created. Doing so lets us determine which views, and thus data models,
  // are backed by what tables.
  auto &proc = context.view_to_top_down_checker[ss.str()];
  if (!proc) {
    proc = impl->procedure_regions.Create(
        impl->next_id++, ProcedureKind::kTupleFinder);

    for (auto param_col : available_cols) {
      const auto var = proc->input_vars.Create(
          impl->next_id++, VariableRole::kParameter);
      var->query_column = param_col;
      proc->col_id_to_var.emplace(param_col.Id(), var);
    }

    context.top_down_checker_work_list.emplace_back(
        view, available_cols, proc, already_checked);
  }

  // Now call the checker procedure.
  const auto check = impl->operation_regions.CreateDerived<CALL>(
      parent, proc, call_op);

  assert(!available_cols.empty());
  for (auto col : available_cols) {
    const auto var = parent->VariableFor(impl, inout_map.find(col)->second);
    assert(var != nullptr);
    check->arg_vars.AddUse(var);
  }

  assert(check->arg_vars.Size() == proc->input_vars.Size());
  return check;
}

// Call the predecessor view's checker function, and if it succeeds, return
// `true`. If we have a persistent table then update the tuple's state in that
// table.
CALL *ReturnTrueWithUpdateIfPredecessorCallSucceeds(
    ProgramImpl *impl, Context &context, REGION *parent,
    QueryView view, const std::vector<QueryColumn> &view_cols, TABLE *table,
    QueryView pred_view, TABLE *already_checked) {

  const auto check = CallTopDownChecker(
      impl, context, parent, view, view_cols, pred_view,
      ProgramOperation::kCallProcedureCheckTrue, already_checked);

  // Change the tuple's state to mark it as present now that we've proven
  // it true via one of the paths into this node.
  if (table) {
    assert(view_cols.size() == view.Columns().size());
    auto change_state = BuildChangeState(
        impl, table, check, view_cols,
        TupleState::kAbsentOrUnknown, TupleState::kPresent);
    check->body.Emplace(check, change_state);

    const auto ret_true = BuildStateCheckCaseReturnTrue(impl, check);
    ret_true->ExecuteAfter(impl, change_state);

  // No table, just return `true` to the caller.
  } else {
    const auto ret_true = BuildStateCheckCaseReturnTrue(impl, check);
    check->body.Emplace(check, ret_true);
  }

  return check;
}

// Build a bottom-up tuple remover, which marks tuples as being in the
// UNKNOWN state (for later top-down checking).
PROC *GetOrCreateBottomUpRemover(ProgramImpl *impl, Context &context,
                                 QueryView from_view, QueryView to_view,
                                 TABLE *already_checked) {
  std::vector<QueryColumn> available_cols;

  // Inserts are annoying because they don't have output columns.
  if (to_view.IsInsert()) {
    for (auto col : QueryInsert::From(to_view).InputColumns()) {
      available_cols.push_back(col);
    }

  } else {
    for (auto col : from_view.Columns()) {
      available_cols.push_back(col);
    }
  }

  assert(!available_cols.empty());

  std::stringstream ss;
  ss << to_view.UniqueId();
  ss << ':' << reinterpret_cast<uintptr_t>(already_checked);
  for (auto col : available_cols) {
    ss << ':' << col.Id();
  }

  auto &proc = context.view_to_bottom_up_remover[ss.str()];
  if (proc) {
    return proc;
  }

  proc = impl->procedure_regions.Create(
      impl->next_id++, ProcedureKind::kTupleRemover);

  // Add parameters to procedure.
  for (auto param_col : available_cols) {
    const auto var = proc->input_vars.Create(
        impl->next_id++, VariableRole::kParameter);
    var->query_column = param_col;
    proc->col_id_to_var.emplace(param_col.Id(), var);
  }

  bool is_equality_cmp = false;
  if (from_view.IsCompare()) {
    auto from_cmp = QueryCompare::From(from_view);
    is_equality_cmp = from_cmp.Operator() == ComparisonOperator::kEqual;
  }

  // Create variable bindings for input-to-output columns. Our function has
  // as many columns as `from_view` has, which may be different than what
  // `to_view` uses (i.e. it might use fewer columns).
  to_view.ForEachUse([=] (QueryColumn in_col, InputColumnRole role,
                          std::optional<QueryColumn> out_col) {
    if (QueryView::Containing(in_col) != from_view ||
        !out_col ||
        !proc->col_id_to_var.count(in_col.Id()) ||
        proc->col_id_to_var.count(out_col->Id())) {
      return;
    }

    switch (role) {
      case InputColumnRole::kAggregatedColumn:
      case InputColumnRole::kIndexValue:
        return;
      case InputColumnRole::kCompareLHS:
      case InputColumnRole::kCompareRHS:
        if (is_equality_cmp) {
          return;
        }
        [[clang::fallthrough]];
      default:
        break;
    }

    proc->col_id_to_var.emplace(out_col->Id(), proc->VariableFor(impl, in_col));
  });

  // Add it to a work list that will be processed after all main bottom-up
  // (positive) provers are created, so that we have proper access to all
  // data models.
  context.bottom_up_removers_work_list.emplace_back(
      from_view, to_view, proc, already_checked);

  return proc;
}

// Returns `true` if `view` might need to have its data persisted for the
// sake of supporting differential updates / verification.
bool MayNeedToBePersisted(QueryView view) {

  // If this view sets a condition then its data must be tracked; if it
  // tests a condition, then we might jump back in at some future point if
  // things transition states.
  if (view.SetCondition() ||
      !view.PositiveConditions().empty() ||
      !view.NegativeConditions().empty()) {
    return true;
  }

  if (view.CanReceiveDeletions() || view.CanProduceDeletions()) {
    return true;
  }

  // If any successor of `view` can receive a deletion, then we may need to
  // support re-proving of a tuple in `succ`, but to do so, we may need to
  // also do a top-down execution into `view`, and have `view` provide the
  // base case.
  for (auto succ : view.Successors()) {
    if (succ.CanReceiveDeletions()) {
      return true;
    }
  }

  return false;
}

// Decides whether or not `view` can depend on `pred_view` for persistence
// of its data.
bool CanDeferPersistingToPredecessor(ProgramImpl *impl, Context &context,
                                     QueryView view, QueryView pred_view) {

  auto &det = context.can_defer_to_predecessor[std::make_pair(view, pred_view)];

  // Check out cache; we may have already determined this.
  if (det != Context::kDeferUnknown) {
    return det == Context::kCanDeferToPredecessor;
  }

  // If this view sets a condition, then the reference counter of that condition
  // partially reflects the arity of this view, i.e. number of records in the
  // view.
  if (view.SetCondition() ||
      !view.PositiveConditions().empty() ||
      !view.NegativeConditions().empty()) {
    det = Context::kCantDeferToPredecessor;
    return false;
  }

  // If this tuple can receive deletions, then whatever is providing data is
  // sending deletions, so we can depend on how it handles persistence.
  if (view.CanReceiveDeletions()) {
    det = Context::kCanDeferToPredecessor;
    return true;
  }

  // NOTE(pag): The special casing on JOINs here is a kind of optimization.
  //            If this TUPLE's predecessor is a JOIN, then it must be using
  //            at least one of the output columns, which is related to one or
  //            more of the views feeding the JOIN, and thus we can recover
  //            the tuple based off of partial information from the JOIN's
  //            data.
  //
  // NOTE(pag): The special casing on a MERGE and SELECT is similar to the
  //            JOIN case, where we know that the MERGE will be persisted.
  if (pred_view.IsJoin() || pred_view.IsMerge() || pred_view.IsSelect()) {
    det = Context::kCanDeferToPredecessor;
    return true;
  }

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();

  // If the data model of `view` and `pred_view` match, then we can defer
  // to `pred_model`.
  if (model == pred_model) {
    det = Context::kCanDeferToPredecessor;
    return true;
  }

  // The data models of `view` and `pred_view` don't match, so we want to
  // figure out if `pred_view` will be persisted.
  for (auto succ_of_pred : pred_view.Successors()) {

    // If one of the successors of `pred_view` is a JOIN then all of
    // `pred_view` is persisted, and thus we can depend upon it. This applies
    // to both equi-joins and cross-products.
    if (succ_of_pred.IsJoin()) {
      det = Context::kCanDeferToPredecessor;
      return true;

    // If one of the successors of `pred_view` is a merge then we have to
    // figure out if `succ_of_pred` is an induction or just a plain old
    // union. If it's an induction, then we know that `pred_view` will
    // be persisted.
    } else if (succ_of_pred.IsMerge()) {

      // It's an induction, and thus `pred_view` and `succ_of_pred` should
      // both share the same data model, but further, it should be persisted.
      if (context.inductive_predecessors.count(succ_of_pred)) {
        det = Context::kCanDeferToPredecessor;
        return true;

      // It's a union, and it will persist the data.
      } else if (MayNeedToBePersisted(succ_of_pred)) {
        det = Context::kCanDeferToPredecessor;
        return true;
      }

    // If one of the successors of `pred_view` inserts into a relation, and if
    // that INSERT's data model is the same as our predecessor's data model,
    // then we can use it.
    } else if (succ_of_pred.IsInsert() &&
               QueryInsert::From(succ_of_pred).IsRelation()) {
      const auto succ_of_pred_model = \
          impl->view_to_model[pred_view]->FindAs<DataModel>();

      if (succ_of_pred_model == pred_model) {
        det = Context::kCanDeferToPredecessor;
        return true;
      }
    }
  }

  det = Context::kCantDeferToPredecessor;
  return false;
}

// Complete a procedure by exhausting the work list.
void CompleteProcedure(ProgramImpl *impl, PROC *proc, Context &context) {
  while (!context.work_list.empty()) {

    //    context.view_to_work_item.clear();
    std::stable_sort(context.work_list.begin(), context.work_list.end(),
                     [](const WorkItemPtr &a, const WorkItemPtr &b) {
                       return a->order > b->order;
                     });

    WorkItemPtr action = std::move(context.work_list.back());
    context.work_list.pop_back();
    action->Run(impl, context);
  }

  // Add a default `return false` at the end of normal procedures.
  const auto ret = impl->operation_regions.CreateDerived<RETURN>(
      proc, ProgramOperation::kReturnFalseFromProcedure);
  ret->ExecuteAfter(impl, proc);
}

// Build an eager region. This guards the execution of the region in
// conditionals if the view itself is conditional.
void BuildEagerRegion(ProgramImpl *impl, QueryView pred_view, QueryView view,
                      Context &usage, OP *parent, TABLE *last_model) {
  const auto pos_conds = view.PositiveConditions();
  const auto neg_conds = view.NegativeConditions();

  // Innermost test for negative conditions.
  if (!neg_conds.empty()) {
    auto test = impl->operation_regions.CreateDerived<EXISTS>(
        parent, ProgramOperation::kTestAllZero);

    for (auto cond : neg_conds) {
      test->cond_vars.AddUse(ConditionVariable(impl, cond));
    }

    parent->body.Emplace(parent, test);
    parent = test;
    last_model = nullptr;
  }

  // Outermost test for positive conditions.
  if (!pos_conds.empty()) {
    auto test = impl->operation_regions.CreateDerived<EXISTS>(
        parent, ProgramOperation::kTestAllNonZero);

    for (auto cond : pos_conds) {
      test->cond_vars.AddUse(ConditionVariable(impl, cond));
    }

    parent->body.Emplace(parent, test);
    parent = test;
    last_model = nullptr;
  }

  BuildUnconditionalEagerRegion(impl, pred_view, view, usage, parent,
                                last_model);
}

WorkItem::~WorkItem(void) {}

// Build a program from a query.
std::optional<Program> Program::Build(const ::hyde::Query &query,
                                      const ErrorLog &) {
  auto impl = std::make_shared<ProgramImpl>(query);
  const auto program = impl.get();

  BuildDataModel(query, program);

  Context context;

  // Conditions need to be eagerly updated. Transmits and queries may need to
  // depend on them so they must be up-to-date.
  for (auto cond : query.Conditions()) {
    for (auto setter : cond.Setters()) {
      auto deps = TransitivePredecessorsOf(setter);
      context.eager.insert(deps.begin(), deps.end());
    }
  }

  // Transmits are messages that we send out "ASAP," i.e. when new data is
  // ingested by a receive, we want to compute stuff and send out messages
  // (e.g. for making orchestration decisions).
  for (auto io : query.IOs()) {
    for (auto transmit : io.Transmits()) {
      auto deps = TransitivePredecessorsOf(transmit);
      context.eager.insert(deps.begin(), deps.end());
    }
  }

  // Create constant variables.
  for (auto const_val : query.Constants()) {
    const auto var =
        impl->const_vars.Create(impl->next_id++, VariableRole::kConstant);
    var->query_const = const_val;
    impl->const_to_var.emplace(const_val, var);
  }

  // Go figure out which merges are inductive, and then classify their
  // predecessors and successors in terms of which ones are inductive and
  // which aren't.
  DiscoverInductions(query, context);

  // Build the initialization procedure, needed to start data flows from
  // things like constant tuples.
  BuildInitProcedure(program, context);

  // Build bottom-up procedures starting from message receives.
  for (auto io : query.IOs()) {
    BuildEagerProcedure(program, io, context);
  }

  // Build top-down provers.
  BuildTopDownCheckers(program, context);

  // Build bottom-up removers. These follow Stefan Brass's push method of
  // pipelined Datalog execution.
  BuildBottomUpRemovalProvers(program, context);

  impl->Optimize();

  // Assign defining regions to each variable.
  //
  // NOTE(pag): We don't really want to map variables throughout the building
  //            process because otherwise every time we replaced all uses of
  //            one region with another, we'd screw up the mapping.
  for (auto proc : impl->procedure_regions) {
    MapVariables(proc->body.get());
  }

  return Program(std::move(impl));
}

}  // namespace hyde
