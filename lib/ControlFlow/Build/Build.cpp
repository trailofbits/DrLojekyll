// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

#include <algorithm>
#include <sstream>

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

    // TODO(pag): Think about this.
    if (false && view.IsNegate()) {
      const auto negate = QueryNegate::From(view);
      const auto pred_view = negate.NegatedView();
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

    // TODO(pag): Think about this.
    if (false && view.IsUsedByNegation()) {
      view.ForEachNegation([&] (QueryNegate negate) {
        if (!dependents.count(negate)) {
          dependents.insert(negate);
          frontier.push_back(negate);
        }
      });
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
                                          TABLE *last_table) {

  if (view.IsJoin()) {
    const auto join = QueryJoin::From(view);
    if (join.NumPivotColumns()) {
      BuildEagerJoinRegion(impl, pred_view, join, context, parent, last_table);
    } else {
      BuildEagerProductRegion(impl, pred_view, join, context, parent,
                              last_table);
    }

  } else if (view.IsMerge()) {
    const auto merge = QueryMerge::From(view);
    if (context.inductive_successors.count(view) &&
        !context.dominated_merges.count(view)) {
      BuildEagerInductiveRegion(impl, pred_view, merge, context, parent,
                                last_table);
    } else {
      BuildEagerUnionRegion(impl, pred_view, merge, context, parent,
                            last_table);
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
                               last_table);

  } else if (view.IsTuple()) {
    BuildEagerTupleRegion(impl, pred_view, QueryTuple::From(view), context,
                          parent, last_table);

  } else if (view.IsInsert()) {
    const auto insert = QueryInsert::From(view);
    BuildEagerInsertRegion(impl, pred_view, insert, context, parent,
                           last_table);

  } else if (view.IsDelete()) {
    BuildEagerDeleteRegion(impl, view, context, parent);

  } else if (view.IsNegate()) {
    const auto negate = QueryNegate::From(view);
    BuildEagerNegateRegion(impl, pred_view, negate, context, parent);

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
    } else if (auto gen = op->AsGenerate(); gen) {
      for (auto var : gen->defined_vars) {
        var->defining_region = region;
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
  } else if (auto proc = region->AsProcedure(); proc) {
    for (auto var : proc->input_vars) {
      var->defining_region = proc;
    }
    MapVariables(proc->body.get());
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
      proc->VectorFor(impl, VectorKind::kParameter, receives[0].Columns());
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

  assert(context.work_list.empty());
  assert(context.view_to_work_item.empty());

  context.work_list.clear();
//  context.view_to_work_item.clear();
//  context.view_to_induction.clear();
//  context.product_vector.clear();

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
  std::unordered_set<QueryView> reached_cycles;
  std::vector<QueryView> frontier;
  std::set<std::pair<QueryView, QueryView>> disallowed_edges;

  for (const auto &[view, noninductive_predecessors] :
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
    reached_cycles.clear();

    // This is a variant of transitive successors, except that we don't allow
    // ourselves to walk through non-inductive output paths of merges because
    // otherwise we would end up merging two unrelated induction sets.

    for (QueryView succ_view : inductive_successors) {
      frontier.push_back(succ_view);
    }

    // We want to express something similar to dominance analysis here.
    // Specifically, suppose we have a set of UNIONs that all logically belong
    // to the same co-inductive set. That is, the outputs of each of the unions
    // somehow cycle into all of the other unions.

    InductionSet &base_set = context.merge_sets[view];

    bool appears_dominated = true;

    while (!frontier.empty()) {
      const auto frontier_view = frontier.back();
      frontier.pop_back();

      // We've cycled back to ourselves. If we get back to ourselves along
      // some path that doesn't itself go through another inductive merge/cycle,
      // which would have been caught by the `else if` case below, then it means
      // that this view isn't subordinate to any other one, and that is actually
      // does in fact need storage.
      if (frontier_view == view) {
        appears_dominated = false;
        continue;

      // We've cycled to a UNION that is inductive.
      } else if (context.inductive_successors.count(frontier_view)) {
        reached_cycles.insert(frontier_view);
        InductionSet &reached_set = context.merge_sets[frontier_view];
        DisjointSet::Union(&base_set, &reached_set);

      // We need to follow the frontier view's successors.
      } else {
        for (auto succ_view : frontier_view.Successors()) {
          if (!seen.count(succ_view) &&
              !disallowed_edges.count({frontier_view, succ_view})) {
            seen.insert(succ_view);
            frontier.push_back(succ_view);
          }
        }
      }
    }

    // All inductive paths out of this union lead to another inductive union.
    if (appears_dominated && reached_cycles.size() == 1u) {
      context.dominated_merges.insert(view);
    }
  }

  for (auto &[merge, merge_set] : context.merge_sets) {
    InductionSet * const set = merge_set.FindAs<InductionSet>();
    set->all_merges.push_back(merge);
    if (!context.dominated_merges.count(merge)) {
      set->merges.push_back(merge);
    }
  }

  // Do a final pass over the induction sets. It's possible that the approximate
  // dominance analysis led us astray, as it doesn't consider the graph as a
  // whole.
  for (auto &[view, set_] : context.merge_sets) {
    InductionSet &set = set_;
    if (set.all_merges.empty()) {
      continue;
    }

    for (auto merge : set.all_merges) {

      // Even though this merge appears dominated, it needs to be treated as
      // undominated because it has some non-inductive successors. Non-inductive
      // successors are processed with the same induction vectors as the cyclic
      // cases.
      if (context.dominated_merges.count(merge) &&
          !context.noninductive_successors[merge].empty()) {
        context.dominated_merges.erase(merge);
        set.merges.push_back(merge);
      }
    }

    // We've got a perfect cycle, and none of the unions in the cycle have a
    // direct output successor. The output it probably guarded behind a join.
    // We'll be conservative and just assume all unions need to be
    // co-represented.
    if (set.merges.empty()) {
      set.merges = set.all_merges;
      for (auto merge : set.all_merges) {
        context.dominated_merges.erase(merge);
      }
    }
  }
}

// Figure out what data definitely must be stored persistently. We need to do
// this ahead-of-time, as opposed to just-in-time, because otherwise we run
// into situations where a node N will have two successors S1 and S2, e.g.
// two identically-shaped TUPLEs, and those tuples will feed into JOINs.
// Those tuples will both need persistent storage (because they feed JOINs),
// but we'll only observe this when we are generating code for the JOINs, and
// thus when we generate the state changes for the join, we'll generate two
// identical state changes, where one will make the other one unsatisfiable.
// For example:
//
//      join-tables
//        vector-loop ...
//        select ...
//        select ...
//          par
//            if-transition-state {@A:29} in %table:43[...] from ...
//              ...
//            if-transition-state {@A:29} in %table:43[...] from ...
//              ...
static void FillDataModel(const Query &query, ProgramImpl *impl,
                          Context &context) {

  // NOTE(pag): TUPLEs are technically the only view types allowed to be used
  //            by negations.
  //
  // The semantics of the usage of positive/negative conditions is that we
  // perform the operation, add the data to the table, *then* test the
  // conditions. That way, if the conditions cannot be satisfied now, but can
  // be satisfied later, then we have the data that we need to send through.
  //
  // Set conditions are similar, and they apply *after* conditions are tested.
  auto is_conditional = +[] (QueryView view) {
    return view.SetCondition() || view.IsUsedByNegation() ||
           !view.PositiveConditions().empty() ||
           !view.NegativeConditions().empty();
  };

  for (auto merge : query.Merges()) {
    const QueryView view(merge);

    if (is_conditional(view)) {
      (void) TABLE::GetOrCreate(impl, view);

    // All inductions need to have persistent storage.
    } else if (context.inductive_successors.count(view) &&
               !context.dominated_merges.count(view)) {
      (void) TABLE::GetOrCreate(impl, view);

    // UNIONs that aren't dominating their inductions, or that aren't part of
    // inductions, need to be persisted if they are differential.
    } else if (MayNeedToBePersistedDifferential(view)) {
      (void) TABLE::GetOrCreate(impl, view);
    }
  }

  // Inserting into a relation requires a table.
  for (auto insert : query.Inserts()) {
    if (insert.IsRelation()) {
      (void) TABLE::GetOrCreate(impl, insert);
    }
  }

  // Selecting from a relation requires a model.
  for (auto select : query.Selects()) {
    if (select.IsRelation()) {
      (void) TABLE::GetOrCreate(impl, select);
    }
  }

  // Negations must be backed by tables.
  for (auto negate : query.Negations()) {
    (void) TABLE::GetOrCreate(impl, negate);
  }

  // All data feeding into a join must be persistently backed.
  for (auto join : query.Joins()) {
    for (QueryView pred : QueryView(join).Predecessors()) {
      (void) TABLE::GetOrCreate(impl, pred);
    }

    // If this join sets a condition, then persist it.
    if (is_conditional(join)) {
      (void) TABLE::GetOrCreate(impl, join);
    }
  }

  for (auto cmp : query.Compares()) {
    if (is_conditional(cmp)) {
      (void) TABLE::GetOrCreate(impl, cmp);
    }
  }

  for (auto map : query.Maps()) {
    if (is_conditional(map)) {
      (void) TABLE::GetOrCreate(impl, map);
    }
  }

  for (auto tuple : query.Tuples()) {
    const QueryView view(tuple);

    if (is_conditional(view)) {
      (void) TABLE::GetOrCreate(impl, view);
    }

    // NOTE(pag): TUPLEs are the only view types allowed to have all-constant
    //            inputs.
    auto predecessors = view.Predecessors();
    if (predecessors.empty()) {
      continue;
    }

    const auto pred_view = predecessors[0];
    if (MayNeedToBePersistedDifferential(view) &&
        !CanDeferPersistingToPredecessor(impl, context, view, pred_view)) {
      (void) TABLE::GetOrCreate(impl, view);
    }
  }

  for (auto changed = true; changed; ) {
    changed = false;

    // Okay, we need to figure out if we should persist this comparison node.
    // This is a bit tricky. If the comparison sets a condition then definitely
    // we need to. If it doesn't then we need to look at the successors of the
    // comparison. If any successor can receive a deletion then that means that
    // a bottom-up checker will probably be created for this comparison node.
    // In that case, we will want to create persistent storage for this
    // comparison if this comparison's predecessor (who we have to visit before
    // getting into this function) doesn't have storage that we can depend upon.
    for (auto cmp : query.Compares()) {
      const QueryView view(cmp);
      DataModel *model = impl->view_to_model[view]->FindAs<DataModel>();
      if (model->table) {
        continue;
      }

      const auto pred_view = view.Predecessors()[0];
      const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();
      auto should_persist = !!view.SetCondition();
      if (!should_persist && !pred_model->table) {
        for (auto succ : view.Successors()) {
          if (succ.IsJoin()) {
            should_persist = true;
            break;
          } else if (succ.CanReceiveDeletions()) {
            should_persist = true;
            break;
          }
        }
      }

      if (should_persist) {
        (void) TABLE::GetOrCreate(impl, view);
        changed = true;
      }
    }

    for (auto map : query.Maps()) {
      const QueryView view(map);
      DataModel *model = impl->view_to_model[view]->FindAs<DataModel>();
      if (model->table) {
        continue;
      }

      if (!!view.SetCondition() || view.CanReceiveDeletions()) {
        (void) TABLE::GetOrCreate(impl, view);
        changed = true;
      }
    }
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

  // Inserts and selects from the same relation share the same data models.
  for (auto rel : query.Relations()) {
    DataModel *last_model = nullptr;
    for (auto view : rel.Inserts()) {
      auto curr_model = program->view_to_model[view]->FindAs<DataModel>();
      if (last_model) {
        DisjointSet::Union(curr_model, last_model);
      } else {
        last_model = curr_model;
      }
    }
    for (auto view : rel.Inserts()) {
      auto curr_model = program->view_to_model[view]->FindAs<DataModel>();
      if (last_model) {
        DisjointSet::Union(curr_model, last_model);
      } else {
        last_model = curr_model;
      }
    }
  }

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

  // If this view might admit fewer tuples through than its predecessor, then
  // we can't have it share a data model with its predecessor.
  auto may_admit_fewer_tuples_than_pred = +[](QueryView view) {
    return view.IsCompare() || view.IsMap() || view.IsNegate();
  };

  // If the output of `view` is conditional, i.e. dependent on the refcount
  // condition variables, or if a condition variable is dependent on the
  // output, then successors of `view` can't share the data model with `view`.
  auto output_is_conditional = +[](QueryView view) {
    return view.SetCondition() || !view.PositiveConditions().empty() ||
           !view.NegativeConditions().empty();
  };

  // With maps, we try to avoid saving the outputs and attached columns
  // when the maps are differential.
  //
  // TODO(pag): Eventually revisit this idea. It needs corresponding support
  //            in Data.cpp, `TABLE::GetOrCreate`.
  auto is_diff_map = +[](QueryView view) {
    return false;
//
//    if (!view.IsMap()) {
//      return false;
//    }
//
//    const auto functor = QueryMap::From(view).Functor();
//    if (!functor.IsPure()) {
//      return false;  // All output columns are stored.
//    }
//
//    // These are the conditions for whether or not to persist the data of a
//    // map. If the map is persisted and it's got a pure functor then we don't
//    // actually store the outputs of the functor.
//    return view.CanReceiveDeletions() || !!view.SetCondition();
  };

  query.ForEachView([=](QueryView view) {
    if (may_admit_fewer_tuples_than_pred(view)) {
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
      auto has_delete_pred = false;
      for (auto pred : preds) {
        if (pred.IsDelete()) {
          has_delete_pred = true;
          break;
        }
      }
      if (!has_delete_pred) {
        for (auto pred : preds) {
          assert(!pred.IsDelete());

          if (!is_diff_map(pred) &&
              !output_is_conditional(pred) &&
              pred.Successors().size() == 1u) {
            const auto pred_model = program->view_to_model[pred];
            DisjointSet::Union(model, pred_model);
          }
        }
      }

    // If a TUPLE "perfectly" passes through its data, then it shares the
    // same data model as its predecessor.
    } else if (view.IsTuple()) {
      if (preds.size() == 1u) {
        const auto pred = preds[0];
        if (!is_diff_map(pred) &&
            !output_is_conditional(pred) &&
            !pred.IsDelete() &&
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
        if (!is_diff_map(pred) && !pred.IsDelete() &&
            !output_is_conditional(pred) &&
            all_cols_match(cols, pred_cols)) {
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
      assert(preds.size() == 1u);
      const auto pred = preds[0];
      const auto del = QueryDelete::From(view);
      const auto cols = del.InputColumns();
      const auto pred_cols = pred.Columns();
      if (!is_diff_map(pred) &&
          !output_is_conditional(pred) &&
          all_cols_match(cols, pred_cols)) {
        const auto pred_model = program->view_to_model[pred];
        DisjointSet::Union(model, pred_model);
      }

#ifndef NDEBUG
//      for (auto succ : view.Successors()) {
//        assert(succ.IsMerge());
//      }
#endif

    // Select predecessors are INSERTs, which don't have output columns.
    // In theory, there could be more than one INSERT. Selects always share
    // the data model with their corresponding INSERTs.
    //
    // TODO(pag): This more about the interplay with conditional inserts.
    } else if (view.IsSelect()) {
      for (auto pred : preds) {
        assert(pred.IsInsert());
        assert(!output_is_conditional(pred));
        const auto pred_model = program->view_to_model[pred];
        DisjointSet::Union(model, pred_model);
      }
    }
  });
}

// Build out all the bottom-up (negative) provers that are used to mark tuples
// as being in an unknown state. We want to do this after building all
// (positive) bottom-up provers so that we can know which views correspond
// with which tables.
static bool BuildBottomUpRemovalProvers(ProgramImpl *impl, Context &context) {
  auto changed = false;
  while (!context.bottom_up_removers_work_list.empty()) {
    auto [from_view, to_view, proc, already_checked] =
        context.bottom_up_removers_work_list.back();
    context.bottom_up_removers_work_list.pop_back();

    changed = true;

    assert(context.work_list.empty());
    context.work_list.clear();

    if (to_view.IsTuple()) {
      CreateBottomUpTupleRemover(impl, context, to_view, proc, already_checked);

    } else if (to_view.IsCompare()) {
      CreateBottomUpCompareRemover(impl, context, to_view, proc,
                                   already_checked);

    } else if (to_view.IsInsert()) {
      CreateBottomUpInsertRemover(impl, context, to_view, proc,
                                  already_checked);

    } else if (to_view.IsDelete()) {
      CreateBottomUpDeleteRemover(impl, context, to_view, proc);

    // NOTE(pag): We don't need to distinguish between unions that are
    //            inductions and unions that are merges.
    } else if (to_view.IsMerge()) {
      CreateBottomUpUnionRemover(impl, context, to_view, proc, already_checked);

    } else if (to_view.IsJoin()) {
      auto join = QueryJoin::From(to_view);
      if (join.NumPivotColumns()) {
        CreateBottomUpJoinRemover(impl, context, from_view, join, proc,
                                  already_checked);
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
        CreateBottomUpGenerateRemover(impl, context, map, functor, proc,
                                      already_checked);

      } else {
        assert(false && "TODO Impure Functors!");
      }

    } else if (to_view.IsNegate()) {
      CreateBottomUpNegationRemover(impl, context, to_view, proc);

    // NOTE(pag): This shouldn't be reachable, as the bottom-up INSERT
    //            removers jump past SELECTs.
    } else if (to_view.IsSelect()) {
      assert(false);

    } else {
      assert(false);
    }

    CompleteProcedure(impl, proc, context);
  }

  return changed;
}

// Build out all the top-down checkers. We want to do this after building all
// bottom-up provers so that we can know which views correspond with which
// tables.
static bool BuildTopDownCheckers(ProgramImpl *impl, Context &context) {
  auto changed = false;

  while (!context.top_down_checker_work_list.empty()) {
    auto [view, view_cols, proc, already_checked] =
        context.top_down_checker_work_list.back();
    context.top_down_checker_work_list.pop_back();
    changed = true;

    assert(context.work_list.empty());
    context.work_list.clear();

    if (view.IsJoin()) {
      const auto join = QueryJoin::From(view);
      if (join.NumPivotColumns()) {
        BuildTopDownJoinChecker(impl, context, proc, join, view_cols,
                                already_checked);
      } else {
        assert(false && "TODO: Checker for cross-product.");
      }

    } else if (view.IsMerge()) {
      const auto merge = QueryMerge::From(view);
      if (context.inductive_successors.count(view) &&
          !context.dominated_merges.count(view)) {
        BuildTopDownInductionChecker(impl, context, proc, merge, view_cols,
                                     already_checked);

      } else {
        BuildTopDownUnionChecker(impl, context, proc, merge, view_cols,
                                 already_checked);
      }

    } else if (view.IsAggregate()) {
      assert(false && "TODO: Checker for aggregates.");

    } else if (view.IsKVIndex()) {
      assert(false && "TODO: Checker for k/v indices.");

    } else if (view.IsMap()) {
      BuildTopDownGeneratorChecker(impl, context, proc, QueryMap::From(view),
                                   view_cols, already_checked);

    } else if (view.IsCompare()) {
      BuildTopDownCompareChecker(impl, context, proc, QueryCompare::From(view),
                                 view_cols, already_checked);

    } else if (view.IsSelect()) {
      BuildTopDownSelectChecker(impl, context, proc, QuerySelect::From(view),
                                view_cols, already_checked);

    } else if (view.IsTuple()) {
      BuildTopDownTupleChecker(impl, context, proc, QueryTuple::From(view),
                               view_cols, already_checked);

    } else if (view.IsInsert()) {
      const auto insert = QueryInsert::From(view);

      if (insert.IsStream()) {
        // Nothing to do.

      } else {
        BuildTopDownInsertChecker(impl, context, proc, QueryInsert::From(view),
                                  view_cols, already_checked);
      }

    } else if (view.IsDelete()) {

      // Nothing to do.

    } else if (view.IsNegate()) {
      BuildTopDownNegationChecker(impl, context, proc, QueryNegate::From(view),
                                  view_cols, already_checked);


    // Not possible?
    } else {
      assert(false);
    }

    CompleteProcedure(impl, proc, context);

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
  return changed;
}

// Add entry point records for each query to the program.
static void BuildQueryEntryPointImpl(
    ProgramImpl * impl, Context &context,
    ParsedDeclaration decl, QueryInsert insert) {

  const QueryView view(insert);
  const auto query = ParsedQuery::From(decl);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  assert(model->table != nullptr);

  std::vector<QueryColumn> cols;
  std::vector<unsigned> col_indices;
  for (auto param : decl.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      col_indices.push_back(param.Index());
    }
    cols.push_back(insert.NthInputColumn(param.Index()));
  }

  const DataTable table(model->table);
  std::optional<ProgramProcedure> checker_proc;
  std::optional<ProgramProcedure> forcer_proc;
  std::optional<DataIndex> scanned_index;

  if (!col_indices.empty()) {
    const auto index = model->table->GetOrCreateIndex(impl, col_indices);
    scanned_index.emplace(DataIndex(index));
  }

  if (view.CanReceiveDeletions()) {
    const auto checker = GetOrCreateTopDownChecker(
        impl, context, view, cols, nullptr);
    impl->query_checkers.AddUse(checker);
    checker_proc.emplace(ProgramProcedure(checker));
    checker->has_raw_use = true;
  }

  impl->queries.emplace_back(
      query, table, scanned_index, checker_proc, forcer_proc);
}


// Add entry point records for each query to the program.
static void BuildQueryEntryPoint(ProgramImpl * impl, Context &context,
                                 ParsedDeclaration decl, QueryInsert insert) {
  std::unordered_set<std::string> seen_variants;

  for (auto redecl : decl.Redeclarations()) {

    // We may have duplicate redeclarations, so don't repeat any.
    std::string binding(redecl.BindingPattern());
    if (seen_variants.count(binding)) {
      continue;
    }
    seen_variants.insert(std::move(binding));
    BuildQueryEntryPointImpl(impl, context, redecl, insert);
  }
}

static bool CanImplementTopDownChecker(
    ProgramImpl *impl, QueryView view,
    const std::vector<QueryColumn> &available_cols) {

  if (view.IsSelect() && QuerySelect::From(view).IsStream()) {
    return true;  // The top-down checker will return false;

  // Join checkers are based off of their predecessors, which are guaranteed
  // to have models.
  } else if (view.IsJoin()) {
    return true;
  }

  // We have a model, so worst case, we can do a full table scan.
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (model->table) {
    return true;
  }

  // We need
  return !available_cols.empty();
}

}  // namespace

// Returns `true` if all paths through `region` ends with a `return` region.
bool EndsWithReturn(REGION *region) {
  if (!region) {
    return false;

  } else if (auto proc = region->AsProcedure(); proc) {
    assert(proc->body.get() != proc);
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

  return CallTopDownChecker(impl, context, parent, succ_view, succ_cols, view,
                            call_op);
}

// Gets or creates a top down checker function.
PROC *GetOrCreateTopDownChecker(
    ProgramImpl *impl, Context &context, QueryView view,
    const std::vector<QueryColumn> &available_cols, TABLE *already_checked) {

  assert(CanImplementTopDownChecker(impl, view, available_cols));
  (void) CanImplementTopDownChecker;

  // Make up a string that captures what we have available.
  std::stringstream ss;
  ss << view.UniqueId();
  for (auto view_col : available_cols) {
    ss << ',' << view_col.Id() << '/'
       << static_cast<uint32_t>(view_col.Type().Kind());
  }
  ss << ':' << reinterpret_cast<uintptr_t>(already_checked);
  auto &proc = context.view_to_top_down_checker[ss.str()];

  // We haven't made this procedure before; so we'll declare it now and
  // enqueue it for building. We enqueue all such procedures for building so
  // that we can build top-down checkers after all bottom-up provers have been
  // created. Doing so lets us determine which views, and thus data models,
  // are backed by what tables.
  if (!proc) {
    proc = impl->procedure_regions.Create(impl->next_id++,
                                          ProcedureKind::kTupleFinder);

    for (auto param_col : available_cols) {
      const auto var =
          proc->input_vars.Create(impl->next_id++, VariableRole::kParameter);
      var->query_column = param_col;
      proc->col_id_to_var.emplace(param_col.Id(), var);
    }

    // Map available inputs to output vars.
    view.ForEachUse([=](QueryColumn in_col, InputColumnRole role,
                       std::optional<QueryColumn> out_col) {
      if (in_col.IsConstantOrConstantRef()) {
        (void) proc->VariableFor(impl, in_col);
      }

      if (out_col && InputColumnRole::kIndexValue != role &&
          InputColumnRole::kAggregatedColumn != role) {
        if (proc->col_id_to_var.count(out_col->Id())) {
          proc->col_id_to_var.emplace(
              in_col.Id(), proc->VariableFor(impl, *out_col));

        } else if (proc->col_id_to_var.count(in_col.Id())) {
          proc->col_id_to_var.emplace(
              out_col->Id(), proc->VariableFor(impl, in_col));

        } else if (out_col->IsConstantRef()) {
          proc->col_id_to_var.emplace(
              in_col.Id(), proc->VariableFor(impl, *out_col));
        }
      }
    });

    context.top_down_checker_work_list.emplace_back(view, available_cols, proc,
                                                    already_checked);
  }

  return proc;
}

// We want to call the checker for `view`, but we only have the columns
// `succ_cols` available for use.
CALL *CallTopDownChecker(ProgramImpl *impl, Context &context, REGION *parent,
                         QueryView succ_view,
                         const std::vector<QueryColumn> &succ_cols,
                         QueryView view, ProgramOperation call_op,
                         TABLE *already_checked) {

  // List of output columns of `view` that can be derived from the output
  // columns of `succ_view`, as available in `succ_cols`.
  std::vector<QueryColumn> available_cols;

  // Inserts only have input columns, and that what gets passed in here.
  if (succ_view.IsInsert()) {
    available_cols = succ_cols;
  }

  // Everything is available, yay!
  if (view == succ_view) {
    for (auto col : view.Columns()) {
      available_cols.push_back(col);
    }
  }

  // If any of the columns of the view we want to call are constant references
  // then they are available. We know the outputs of a view are never themselves
  // constants.
  for (auto col : view.Columns()) {
    if (col.IsConstantRef()) {
      available_cols.push_back(col);
      (void) parent->VariableFor(impl, col);
    }
  }

  // Now we need to map the outputs of `succ_view` back to the outputs of
  // `view`.
  succ_view.ForEachUse([&](QueryColumn view_col, InputColumnRole role,
                           std::optional<QueryColumn> succ_view_col) {

    // `view_col` is unrelated to `view`.
    if (QueryView::Containing(view_col) != view) {
      return;

    // If it's a constant ref then we have it. Note that we take `view_col`
    // as the lookup for the `VariableFor` and not `*succ_view_col` because
    // that might give us the output of a mutable column in a key/value store.
    } else if (view_col.IsConstantRef()) {
      const auto succ_var = parent->VariableFor(impl, view_col);
      parent->col_id_to_var[view_col.Id()] = succ_var;
      available_cols.push_back(view_col);

    // The input column from `view` is not present in `succ_view`.
    } else if (!succ_view_col) {
      return;

    // We cannot depend on the output value of this column
    // (from `succ_view_col`) to derive the input value `view_col`. However,
    // if the input column is a constant or constant ref then we have it.
    } else if (InputColumnRole::kIndexValue == role ||
               InputColumnRole::kAggregatedColumn == role) {
      return;

//    // Two input values are conditionally merged into one output value. We can
//    // never convert an output into an input in this case.
//    } else if ((InputColumnRole::kCompareLHS == role ||
//                InputColumnRole::kCompareRHS == role) &&
//               (QueryCompare::From(succ_view).Operator() ==
//                ComparisonOperator::kEqual)) {
//      return;

    // `*succ_view_col` is available in `view_cols`, thus `view_col` is
    // also available.
    } else if (std::find(succ_cols.begin(), succ_cols.end(), *succ_view_col) !=
               succ_cols.end()) {
      available_cols.push_back(view_col);
      const auto succ_var = parent->VariableFor(impl, *succ_view_col);
      parent->col_id_to_var[view_col.Id()] = succ_var;

    // Realistically, this shouldn't be possible, as it suggests a possible
    // bug in the dataflow optimizers.
    } else if (succ_view_col->IsConstantRef()) {
      assert(false);
      available_cols.push_back(view_col);
      const auto succ_var = parent->VariableFor(impl, *succ_view_col);
      parent->col_id_to_var[view_col.Id()] = succ_var;
    }
  });

  // Sort and unique out the available columns.
  std::sort(available_cols.begin(), available_cols.end(),
            [] (QueryColumn a, QueryColumn b) {
              return *(a.Index()) < *(b.Index());
            });
  auto it = std::unique(available_cols.begin(), available_cols.end(),
                        [] (QueryColumn a, QueryColumn b) {
                          return *(a.Index()) == *(b.Index());
                        });
  available_cols.erase(it, available_cols.end());

  const auto proc = GetOrCreateTopDownChecker(
      impl, context, view, available_cols, already_checked);

  // Now call the checker procedure.
  const auto check =
      impl->operation_regions.CreateDerived<CALL>(
          impl->next_id++, parent, proc, call_op);

  auto i = 0u;
  for (auto col : available_cols) {
    const auto var = parent->VariableFor(impl, col);
    assert(var != nullptr);
    check->arg_vars.AddUse(var);
    const auto param = proc->input_vars[i++];
    assert(var->Type() == param->Type());
    (void) param;
  }

  assert(check->arg_vars.Size() == proc->input_vars.Size());
  return check;
}

// Call the predecessor view's checker function, and if it succeeds, return
// `true`. If we have a persistent table then update the tuple's state in that
// table.
CALL *ReturnTrueWithUpdateIfPredecessorCallSucceeds(
    ProgramImpl *impl, Context &context, REGION *parent, QueryView view,
    const std::vector<QueryColumn> &view_cols, TABLE *table,
    QueryView pred_view, TABLE *already_checked) {

  const auto check = CallTopDownChecker(
      impl, context, parent, view, view_cols, pred_view,
      ProgramOperation::kCallProcedureCheckTrue, already_checked);

  // Change the tuple's state to mark it as present now that we've proven
  // it true via one of the paths into this node.
  if (table) {
    if (view.IsInsert()) {
      assert(view_cols.size() == QueryInsert::From(view).InputColumns().size());
    } else {
      assert(view_cols.size() == view.Columns().size());
    }
    auto change_state =
        BuildChangeState(impl, table, check, view_cols,
                         TupleState::kAbsentOrUnknown, TupleState::kPresent);
    check->body.Emplace(check, change_state);

    const auto ret_true = BuildStateCheckCaseReturnTrue(impl, change_state);
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

    for (auto col : to_view.Predecessors()[0].Columns()) {
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

  proc = impl->procedure_regions.Create(impl->next_id++,
                                        ProcedureKind::kTupleRemover);

  // Add parameters to procedure.
  for (auto param_col : available_cols) {
    const auto var =
        proc->input_vars.Create(impl->next_id++, VariableRole::kParameter);
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
  if (from_view != to_view) {
    to_view.ForEachUse([=](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> out_col) {
      if (QueryView::Containing(in_col) != from_view || !out_col ||
          !proc->col_id_to_var.count(in_col.Id()) ||
          proc->col_id_to_var.count(out_col->Id())) {
        return;
      }

      switch (role) {
        case InputColumnRole::kAggregatedColumn:
        case InputColumnRole::kIndexValue: return;
        case InputColumnRole::kCompareLHS:
        case InputColumnRole::kCompareRHS:
          if (is_equality_cmp) {
            return;
          }
          [[clang::fallthrough]];
        default: break;
      }

      const auto var = proc->VariableFor(impl, in_col);
      assert(var != nullptr);
      proc->col_id_to_var.emplace(out_col->Id(), var);
    });

  } else {
    to_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> out_col) {
      if (!out_col) {
        return;
      }

      switch (role) {
        case InputColumnRole::kAggregatedColumn:
        case InputColumnRole::kIndexValue: return;
        case InputColumnRole::kCompareLHS:
        case InputColumnRole::kCompareRHS:
          if (is_equality_cmp) {
            return;
          }
          [[clang::fallthrough]];
        default: break;
      }
      const auto var = proc->VariableFor(impl, *out_col);
      assert(var != nullptr);
      proc->col_id_to_var.emplace(in_col.Id(), var);
    });
  }

  // Add it to a work list that will be processed after all main bottom-up
  // (positive) provers are created, so that we have proper access to all
  // data models.
  context.bottom_up_removers_work_list.emplace_back(from_view, to_view, proc,
                                                    already_checked);

  return proc;
}

// Returns `true` if `view` might need to have its data persisted for the
// sake of supporting differential updates / verification.
bool MayNeedToBePersistedDifferential(QueryView view) {
  if (MayNeedToBePersisted(view)) {
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

// Returns `true` if `view` might need to have its data persisted.
bool MayNeedToBePersisted(QueryView view) {

  // If this view sets a condition then its data must be tracked; if it
  // tests a condition, then we might jump back in at some future point if
  // things transition states.
  return view.SetCondition() ||
         !view.PositiveConditions().empty() ||
         !view.NegativeConditions().empty() ||
         view.IsNegate() ||
         view.IsUsedByNegation();
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
  // view. Similarly, if this view has the possiblity of filtering or increasing
  // the number of tuples admitted by the predecessor, then we can't defer to
  // the predecessor.
  if (view.SetCondition() ||
      !view.PositiveConditions().empty() ||
      !view.NegativeConditions().empty() ||
      view.IsNegate() ||
      view.IsCompare() ||
      view.IsMap()) {
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
  if (pred_view.IsJoin() || pred_view.IsMerge() || pred_view.IsSelect() ||
      pred_view.IsNegate()) {
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
      } else if (MayNeedToBePersistedDifferential(succ_of_pred)) {
        det = Context::kCanDeferToPredecessor;
        return true;
      }

    // If one of the successors of `pred_view` inserts into a relation, and if
    // that INSERT's data model is the same as our predecessor's data model,
    // then we can use it.
    } else if (succ_of_pred.IsInsert() &&
               QueryInsert::From(succ_of_pred).IsRelation()) {
      const auto succ_of_pred_model =
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

    std::stable_sort(context.work_list.begin(), context.work_list.end(),
                     [](const WorkItemPtr &a, const WorkItemPtr &b) {
                       return a->order > b->order;
                     });

    WorkItemPtr action = std::move(context.work_list.back());
    context.work_list.pop_back();
    action->Run(impl, context);
  }

  // Add a default `return false` at the end of normal procedures.
  if (!EndsWithReturn(proc)) {
    const auto ret = impl->operation_regions.CreateDerived<RETURN>(
        proc, ProgramOperation::kReturnFalseFromProcedure);
    ret->ExecuteAfter(impl, proc);
  }
}

// Build an eager region. This guards the execution of the region in
// conditionals if the view itself is conditional.
void BuildEagerRegion(ProgramImpl *impl, QueryView pred_view, QueryView view,
                      Context &usage, OP *parent, TABLE *last_model) {

  view.ForEachUse([=](QueryColumn in_col, InputColumnRole role,
                      std::optional<QueryColumn> out_col) {

    if (!out_col) {
      return;
    }

    assert(in_col.Id() != out_col->Id());
    assert(QueryView::Containing(*out_col) == view);

    // Comparisons merge two inputs into a single output.
    if ((InputColumnRole::kCompareLHS == role ||
         InputColumnRole::kCompareRHS == role) &&
        (ComparisonOperator::kEqual == QueryCompare::From(view).Operator())) {
      return;

    // Index values are merged with prior values to form the output. We don't
    // overwrite join outputs otherwise they don't necessarily get assigned to
    // the right selection variables.
    } else if (InputColumnRole::kIndexValue == role) {
      return;

    } else if (QueryView::Containing(in_col) == pred_view ||
               in_col.IsConstantOrConstantRef()) {
      const auto src_var = parent->VariableFor(impl, in_col);
      parent->col_id_to_var[out_col->Id()] = src_var;
    }
  });

  BuildUnconditionalEagerRegion(impl, pred_view, view, usage, parent,
                                last_model);
}

WorkItem::WorkItem(Context &context, unsigned order_)
    : order(order_) {}

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

  // Now that we've identified our inductions, we can fill our data model,
  // i.e. assign persistent tables to each disjoint set of views.
  FillDataModel(query, program, context);

  // Build the initialization procedure, needed to start data flows from
  // things like constant tuples.
  BuildInitProcedure(program, context);

  // Build bottom-up procedures starting from message receives.
  for (auto io : query.IOs()) {
    BuildEagerProcedure(program, io, context);
  }

  for (auto insert : query.Inserts()) {
    if (insert.IsRelation()) {
      auto decl = insert.Relation().Declaration();
      if (decl.IsQuery()) {
        BuildQueryEntryPoint(program, context, decl, insert);
      }
    }
  }

  // Build top-down provers and the bottom-up removers.
  while (BuildTopDownCheckers(program, context) ||
         BuildBottomUpRemovalProvers(program, context)) {}

  for (auto proc : impl->procedure_regions) {
    if (!EndsWithReturn(proc)) {
      BuildStateCheckCaseReturnFalse(impl.get(), proc)->ExecuteAfter(
          impl.get(), proc);
    }
  }

  impl->Optimize();

  // Assign defining regions to each variable.
  //
  // NOTE(pag): We don't really want to map variables throughout the building
  //            process because otherwise every time we replaced all uses of
  //            one region with another, we'd screw up the mapping.
  for (auto proc : impl->procedure_regions) {
    MapVariables(proc);
  }

  return Program(std::move(impl));
}

}  // namespace hyde
