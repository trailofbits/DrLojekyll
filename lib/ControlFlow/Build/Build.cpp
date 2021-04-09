// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

#include <drlojekyll/Parse/ErrorLog.h>

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
      if (auto [it, added] = dependencies.insert(pred_view); added) {
        frontier.push_back(pred_view);
      }
    }

    if (view.IsNegate()) {
      const auto negate = QueryNegate::From(view);
      const auto pred_view = negate.NegatedView();
      if (auto [it, added] = dependencies.insert(pred_view); added) {
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
      if (auto [it, added] = dependents.insert(succ_view); added) {
        frontier.push_back(succ_view);
      }
    }

    view.ForEachNegation([&] (QueryNegate negate) {
      if (auto [it, added] = dependents.insert(negate); added) {
        frontier.push_back(negate);
      }
    });
  }

  return dependents;
}

// Analyze the MERGE/UNION nodes and figure out which ones are inductive.
static void DiscoverInductions(const Query &query, Context &context,
                               const ErrorLog &log) {
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

  auto missing_base_case = [&] (QueryView view) -> bool {
    auto size = 0ull;
    if (auto it = context.noninductive_predecessors.find(view);
        it != context.noninductive_predecessors.end()) {
      size = it->second.size();
    }
    return size == 0u;
  };

  // Sanity check; look for programs that cannot be linearized into our
  // control-flow format.
  std::unordered_map<ParsedClause, std::vector<ParsedVariable>> bad_vars;
  for (const auto &[view, cyclic_pred_list] : context.inductive_predecessors) {
    if (!missing_base_case(view)) {
      continue;
    }

    // This inductive merge has no "true" base case. Lets go make sure that
    // at least on of the inductions in this merge's induction set has a proper
    // base case.
    auto all_missing = true;
    const auto &set = context.merge_sets[view];
    for (auto other_view : set.all_merges) {
      if (!missing_base_case(other_view)) {
        all_missing = false;
        break;
      }
    }

    // At least one of them does.
    if (!all_missing) {
      continue;
    }

    // None of them do :-(
    for (QueryColumn col : view.Columns()) {
      if (col.IsConstant()) {
        continue;
      }

      auto var = col.Variable();
      auto clause = ParsedClause::Containing(var);
      bad_vars[clause].push_back(var);
    }
  }

  // Complain about this if the declarations aren't marked as divergent.
  for (const auto &[clause, vars] : bad_vars) {
    auto decl = ParsedDeclaration::Of(clause);
    if (decl.IsDivergent()) {
      continue;
    }

    auto err = log.Append(clause.SpellingRange());
    err << "Clause introduces non-linearizable induction cycle; it seems like "
        << "every body of this clause (in)directly depends upon itself -- at "
        << "least one body must depend on something else";

    err.Note(decl.SpellingRange())
          << "This error can be disabled (at your own risk) by marking this "
          << "declaration with the '@divergent' pragma";
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
      (void) TABLE::GetOrCreate(impl, context, view);

    // All inductions need to have persistent storage.
    } else if (context.inductive_successors.count(view) &&
               !context.dominated_merges.count(view)) {
      (void) TABLE::GetOrCreate(impl, context, view);

    // UNIONs that aren't dominating their inductions, or that aren't part of
    // inductions, need to be persisted if they are differential.
    } else if (MayNeedToBePersistedDifferential(view)) {
      (void) TABLE::GetOrCreate(impl, context, view);
    }
  }

  // Inserting into a relation requires a table.
  for (auto insert : query.Inserts()) {
    if (insert.IsRelation()) {
      (void) TABLE::GetOrCreate(impl, context, insert);
    }
  }

  // Selecting from a relation requires a model.
  for (auto select : query.Selects()) {
    if (select.IsRelation()) {
      (void) TABLE::GetOrCreate(impl, context, select);
    }
  }

  // Negations must be backed by tables.
  for (auto negate : query.Negations()) {
//    (void) TABLE::GetOrCreate(impl, context, negate);
    (void) TABLE::GetOrCreate(impl, context, negate.NegatedView());
    for (QueryView pred : QueryView(negate).Predecessors()) {
      (void) TABLE::GetOrCreate(impl, context, pred);
    }
  }

  // All data feeding into a join must be persistently backed.
  for (auto join : query.Joins()) {
    for (QueryView pred : QueryView(join).Predecessors()) {
      (void) TABLE::GetOrCreate(impl, context, pred);
    }

    // If this join sets a condition, then persist it.
    if (is_conditional(join)) {
      (void) TABLE::GetOrCreate(impl, context, join);
    }
  }

  for (auto cmp : query.Compares()) {
    if (is_conditional(cmp)) {
      (void) TABLE::GetOrCreate(impl, context, cmp);
    }
  }

  for (auto map : query.Maps()) {
    if (is_conditional(map)) {
      (void) TABLE::GetOrCreate(impl, context, map);
    }
  }

  for (auto tuple : query.Tuples()) {
    const QueryView view(tuple);

    if (is_conditional(view)) {
      (void) TABLE::GetOrCreate(impl, context, view);
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
      (void) TABLE::GetOrCreate(impl, context, view);
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

          } else {
            assert(!view.CanProduceDeletions());
          }
        }
      }

      if (should_persist) {
        (void) TABLE::GetOrCreate(impl, context, view);
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
        (void) TABLE::GetOrCreate(impl, context, view);
        changed = true;
      }
    }
  }

  // TODO(pag): Needed to make Solypsis work for now. Hit the worst case of a
  //            top-down checker call's behaviour.
  for (auto merge : query.Merges()) {
    (void) TABLE::GetOrCreate(impl, context, merge);
  }

  // We need the tables to know about *all* views associated with them. This
  // helps with debugging, but more importantly, it helps us find the "top"
  // merge associated with a table.
  query.ForEachView([&] (QueryView view) {
    DataModel *model = impl->view_to_model[view]->FindAs<DataModel>();
    if (model->table) {
      (void) TABLE::GetOrCreate(impl, context, view);
    }

//    else {
//      // TODO(pag): !!! REMOVE ME!!!!
//      (void) TABLE::GetOrCreate(impl, context, view);
//    }
  });
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
    for (auto view : rel.Selects()) {
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

      // Join together the tables of inserts and their predecessors, which are
      // always TUPLEs.
      for (auto pred_view : view.Predecessors()) {
        auto pred_model = program->view_to_model[pred_view];
        DisjointSet::Union(last_model, pred_model);
      }
    }
  }


  // TODO(pag): Data modelling disabled until further notice!!!
  //            Subtle bugs abound.
  return;



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

  // Simple tests to figure out if `view` (treated as a predecessor) can
  // share its data model with its successors.
  auto can_share = +[](QueryView view, QueryView pred) {
    if (pred.IsNegate()) {
      return false;
    }
    // With any special cases, we need to watch out for the following kind of
    // pattern:
    //
    //                               ...
    //      ... ----.                 |
    //           UNION1 -- TUPLE -- UNION2
    //      ... ----'
    //
    // In this case, suppose TUPLE perfectly forwards data of UNION1 to
    // UNION2. Thus, UNION1 is a subset of UNION2. We don't want to accidentally
    // merge the data models of UNION1 and UNION2, otherwise we'd lose this
    // subset relation. At the same time, we don't want to break all sorts of
    // other stuff out, so we have a bunch of special cases to try to be more
    // aggressive about merging data models without falling prey to this
    // specific case.
    //
    // Another situation comes up with things like:
    //
    //          UNION1 -- INSERT -- SELECT -- UNION2
    //
    // In this situation, we want UNION1 and the INSERT/SELECT to share the
    // same data model, but UNION2 should not be allowed to share it. Similarly,
    // in this situation:
    //
    //
    //          UNION1 -- INSERT -- SELECT -- TUPLE -- UNION2
    //
    // We want the UNION1, INSERT, SELECT, and TUPLE to share the same data
    // model, but not UNION2.


    // Here we also need to check on the number of successors of the tuple's
    // predecessor, e.g.
    //
    //             --> flow -->
    //
    //      TUPLE1 -- TUPLE2 -- UNION1
    //         |
    //         '----- TUPLE3 -- UNION2
    //                            |
    //                TUPLE4 -----'
    //
    // In this case, UNION1 and TUPLE2 will share their data models, but we
    // can't let TUPLE1 and TUPLE2 or TUPLE1 and TUPLE3 share their data models,
    // otherwise the UNION1 might end up sharing its data model with completely
    // unrelated stuff in UNION2 (via TUPLE4).
    return pred.Successors().size() == 1u;

//    if () {
//      return true;
//
//    // Special case for letting us share a data model with a predecessor.
//    } else if (view.IsInsert()) {
//      return true;
//
//    } else if (view.Successors().size() == 1 &&
//               view.Successors()[0].IsJoin()) {
//      return true;
//
//    } else {
//      return false;
//    }
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
    //
    // If `pred` is another UNION, then `pred` may be a subset of `view`, thus
    // we cannot merge `pred` and `view`.
    if (view.IsMerge()) {
      for (auto pred : preds) {
        if (can_share(view, pred) &&
            !is_diff_map(pred) &&
            !output_is_conditional(pred) &&
            !pred.IsMerge()) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
      }

    // If a TUPLE "perfectly" passes through its data, then it shares the
    // same data model as its predecessor.
    } else if (view.IsTuple()) {
      if (preds.size() == 1u) {
        const auto pred = preds[0];
        const auto tuple = QueryTuple::From(view);
        if (can_share(view, pred) &&
            !is_diff_map(pred) &&
            !output_is_conditional(pred) &&
            all_cols_match(tuple.InputColumns(), pred.Columns())) {
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
        if (can_share(view, pred) &&
            !is_diff_map(pred) &&
            !output_is_conditional(pred) &&
            all_cols_match(insert.InputColumns(), pred.Columns())) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
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
        assert(can_share(view, pred));
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

    auto let = impl->operation_regions.CreateDerived<LET>(proc);
      proc->body.Emplace(proc, let);

    if (to_view.IsTuple()) {
      CreateBottomUpTupleRemover(impl, context, to_view, let, already_checked);

    } else if (to_view.IsCompare()) {
      CreateBottomUpCompareRemover(impl, context, to_view, let,
                                   already_checked);

    } else if (to_view.IsInsert()) {
      CreateBottomUpInsertRemover(impl, context, to_view, let,
                                  already_checked);

    } else if (to_view.IsMerge()) {
      if (context.inductive_successors.count(to_view) &&
          !context.dominated_merges.count(to_view)) {
        CreateBottomUpInductionRemover(impl, context, to_view, let,
                                       already_checked);
      } else {
        CreateBottomUpUnionRemover(impl, context, to_view, let, already_checked);
      }

    } else if (to_view.IsJoin()) {
      auto join = QueryJoin::From(to_view);
      if (join.NumPivotColumns()) {
        CreateBottomUpJoinRemover(impl, context, from_view, join, let,
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
        CreateBottomUpGenerateRemover(impl, context, map, functor, let,
                                      already_checked);

      } else {
        assert(false && "TODO Impure Functors!");
      }

    } else if (to_view.IsNegate()) {
      CreateBottomUpNegationRemover(impl, context, to_view, let,
                                    already_checked);

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

static REGION *RemoveFromNegatedView(
    ProgramImpl *impl, Context &context, QueryView view, REGION *parent) {
  assert(false && "TODO");
  return nullptr;
}

static REGION *MaybeReAddToNegatedView(
    ProgramImpl *impl, Context &context, QueryView view, REGION *parent) {
  assert(false && "TODO");
  return nullptr;
}

static void BuildTopDownChecker(
    ProgramImpl *impl, Context &context, QueryView view,
    std::vector<QueryColumn> &view_cols, PROC *proc,
    TABLE *already_checked) {

  // If we have a table, and if we don't have all of the columns, then go
  // and get all of the columns via a recursive check and a scan over the
  // backing store. The fall-through at the end is to return false if none
  // of the recursive calls returns true.
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (const auto table = model->table; table) {

    // If we have a table, and our caller has a table, and they don't match
    // then clear out `already_checked`, as it's unrelated to our table.
    if (already_checked != table) {
      already_checked = nullptr;
    }

    // Recursively call the top-down checker with all columns available.
    //
    // Key dependencies: `already_checked`, `view`, `view_cols`.
    auto call_self = [&] (REGION *parent) -> CALL * {
      const auto available_cols = ComputeAvailableColumns(view, view_cols);
      const auto checker_proc = GetOrCreateTopDownChecker(
          impl, context, view, available_cols, already_checked);

      const auto check = impl->operation_regions.CreateDerived<CALL>(
          impl->next_id++, parent, checker_proc);

      // Pass in the arguments.
      auto i = 0u;
      for (auto [col, avail_col] : available_cols) {
        const auto var = parent->VariableFor(impl, avail_col);
        assert(var != nullptr);
        check->arg_vars.AddUse(var);

        const auto param = checker_proc->input_vars[i++];
        assert(var->Type() == param->Type());
        (void) param;
      }

      return check;
    };

    SERIES *seq = impl->series_regions.Create(proc);

    // Try to build a scan. If we can't then return `nullptr`, and `did_scan`
    // will be `false`. If we do build a scan, then do a recursive call in the
    // scan, where we'll have all columns available. If the recursive call
    // returns `true` then it means we've proven the tuple exists, and we can
    // return true.
    const auto did_scan = BuildMaybeScanPartial(
        impl, view, view_cols, model->table, seq,
        [&](REGION *parent, bool in_loop) -> REGION * {
          if (!in_loop) {
            return nullptr;
          }

          assert(!already_checked);

          const auto check = call_self(parent);

          // If the call succeeds, then `return-true`. If it fails, then do
          // nothing, i.e. continue in our partial scan to the next tuple
          // from our index.
          check->body.Emplace(
              check, BuildStateCheckCaseReturnTrue(impl, check));

          return check;
        });

    // If we did a scan, and if execution falls through post-scan, then it means
    // that none of the recursive calls to finders succeeded, so we must return
    // false because we failed to prove the tuple.
    if (did_scan) {
      assert(!already_checked);

      proc->body.Emplace(proc, seq);
      seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));
      return;

    // If this view can't produce deletions, and if we have a table for it, then
    // all we need to do is check the state.
    } else if (!view.CanProduceDeletions()) {
      assert(view.PositiveConditions().empty());
      assert(view.NegativeConditions().empty());

      seq->parent = nullptr;

      // If our caller did a check and we got to here, and we can't produce
      // deletions, then this is really weird, but also just means the data
      // wasn't found and all we can do is return false.
      if (already_checked == model->table) {
        proc->body.Emplace(proc, BuildStateCheckCaseReturnFalse(impl, proc));

      // Our called didn't do a check, so we'll do it, and we'll just return
      // whatever the check tells us. In practice, the `if-unknown` case should
      // never execute.
      //
      // TODO(pag): Would be good to have an "abort" block.
      } else {
        proc->body.Emplace(proc, BuildTopDownCheckerStateCheck(
            impl, proc, model->table, view_cols,
            BuildStateCheckCaseReturnTrue,
            BuildStateCheckCaseReturnFalse,
            BuildStateCheckCaseReturnFalse));
      }
      return;

    // This node can produce differential updates, it has a model, and we
    // haven't yet checked the state of the tuple. We're down here so we know
    // the tuple once existed, because we found it in an index. We haven't
    // actually checked the tuple's state, though, so now we'll check it, and
    // if it's unknown then we'll recursively call ourselves and try to prove
    // it in its own absence.
    } else if (already_checked == nullptr) {

      seq->parent = nullptr;

      // It's possible that we'll need to re-try this (or a nearly similar)
      // call due to a race condition. In that event, we'll make a tail-call.
      SERIES *retry_seq = impl->series_regions.Create(proc);
      proc->body.Emplace(proc, retry_seq);

      // These will be executed if the recursive call returns `true` or
      // `false`, respectively.
      SERIES *true_seq = nullptr;
      SERIES *false_seq = nullptr;

      retry_seq->AddRegion(BuildTopDownCheckerStateCheck(
          impl, retry_seq, model->table, view.Columns(),
          BuildStateCheckCaseReturnTrue,
          BuildStateCheckCaseReturnFalse,
          [&](ProgramImpl *, REGION *parent) -> REGION * {

            // Change the tuple's state to mark it as absent so that we can't
            // use it as its own base case.
            const auto table_remove = BuildChangeState(
                impl, table, parent, view_cols, TupleState::kUnknown,
                TupleState::kAbsent);

            already_checked = model->table;
            const auto recursive_call_if_changed = call_self(table_remove);
            table_remove->body.Emplace(table_remove, recursive_call_if_changed);

            // If we're proven the tuple, then try to mark it as present.
            const auto table_add = BuildChangeState(
                impl, table, recursive_call_if_changed, view_cols,
                TupleState::kAbsent, TupleState::kPresent);

            recursive_call_if_changed->body.Emplace(
                recursive_call_if_changed, table_add);

            // If updating the state succeeded, then we'll go down the `true`
            // path.
            true_seq = impl->series_regions.Create(table_add);
            table_add->body.Emplace(table_add, true_seq);

            // If the recursive call failed, and we were the ones to change
            // the tuple's state to absent, then we'll go down the `false`
            // path.
            false_seq = impl->series_regions.Create(recursive_call_if_changed);
            recursive_call_if_changed->false_body.Emplace(
                recursive_call_if_changed, false_seq);

            return table_remove;
          }));

      // If this view is used by a negation, then on the true or false paths
      // we may need to adjust things.
      if (view.IsUsedByNegation()) {
        true_seq->AddRegion(RemoveFromNegatedView(impl, context, view, true_seq));
        false_seq->AddRegion(MaybeReAddToNegatedView(impl, context, view, true_seq));
      }

      // Make sure that we `return-true` and `return-false` to our callers.
      true_seq->AddRegion(BuildStateCheckCaseReturnTrue(impl, true_seq));
      false_seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, false_seq));

      // If we fall through to the end, then a race condition has occurred
      // during one of the above state transitions. To recover, call ourselves
      // recursively and return the result.
      already_checked = nullptr;
      const auto recursive_call_if_race = call_self(retry_seq);
      retry_seq->AddRegion(recursive_call_if_race);

      recursive_call_if_race->body.Emplace(
          recursive_call_if_race,
          BuildStateCheckCaseReturnTrue(impl, recursive_call_if_race));

      recursive_call_if_race->false_body.Emplace(
          recursive_call_if_race,
          BuildStateCheckCaseReturnTrue(impl, recursive_call_if_race));

      return;

    // Mark the series as "dead". Fall through to the "actual" child calls.
    } else {
      seq->parent = nullptr;
    }

  // No table associated with this view.
  } else {
    assert(!view.IsUsedByNegation());
    assert(!view.IsUsedByJoin());
    already_checked = nullptr;

    // TODO(pag): Consider returning false?
  }

  // If we're down here, then it means one of the following:
  //
  //    1)  `view` doesn't have a table, and so we didn't do a scan.
  //    2)  `view_cols` is "complete" for `view`, and so we didn't do a scan.
  //    3)  we've been recursively called in the context of a scan.

  REGION *child = nullptr;
  REGION *parent = proc;
  UseRef<REGION> *parent_body = &(proc->body);

  // Before proceeding, we must ensure that if any constants flowed up through
  // this node, then on our way down, we check that the columns we have match
  // the constants that flowed up.

  std::vector<std::pair<QueryColumn, QueryColumn>> constants_to_check;
  view.ForEachUse([&] (QueryColumn in_col, InputColumnRole role,
                       std::optional<QueryColumn> out_col) {
    if (out_col && in_col.IsConstantOrConstantRef() &&
        std::find(view_cols.begin(), view_cols.end(), *out_col) !=
            view_cols.end()) {
      switch (role) {
        case InputColumnRole::kIndexValue:
        case InputColumnRole::kAggregatedColumn: return;
        default:
          constants_to_check.emplace_back(*out_col, in_col);
          break;
      }
    }
  });

  // We need to compare some of the arguments against constants. This may
  // be tricky because some of the argument columns may be marked as
  // constant refs, so we can't trust `VariableFor`; we need to find the
  // arguments "by index."
  if (!constants_to_check.empty()) {
    const auto cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
        parent, ComparisonOperator::kEqual);
    parent_body->Emplace(parent, cmp);

    for (auto [out_col, in_col] : constants_to_check) {
      assert(in_col.IsConstantOrConstantRef());
      cmp->lhs_vars.AddUse(proc->input_vars[*(out_col.Index())]);
      cmp->rhs_vars.AddUse(proc->VariableFor(impl, in_col));
    }

    // NOTE(pag): We *don't* do conditional constant propagation (via injecting
    //            stuff into `col_id_to_var`) here because `view` might be a
    //            comparison, and so there could be repeats of `out_col` in
    //            `constants_to_check` that would screw up our checks going
    //            down.

    // If the comparison failed then return false.
    cmp->false_body.Emplace(cmp, BuildStateCheckCaseReturnFalse(impl, cmp));

    // Everything else will nest inside of the comparison.
    parent = cmp;
    parent_body = &(cmp->body);
  }

  // Alright, now it's finally time to call the view-specific checkers.

  // If we have a table, then being down here implies we've already checked
  // the state and transitioned it to absent. Or, we don't have a table.
  assert(!model->table || already_checked);

  if (view.IsJoin()) {
    const auto join = QueryJoin::From(view);
    if (join.NumPivotColumns()) {
      child = BuildTopDownJoinChecker(impl, context, parent, join, view_cols,
                                      already_checked);
    } else {
      assert(false && "TODO: Checker for cross-product.");
    }

  } else if (view.IsMerge()) {
    const auto merge = QueryMerge::From(view);
    if (context.inductive_successors.count(view) &&
        !context.dominated_merges.count(view)) {
      child = BuildTopDownInductionChecker(
          impl, context, parent, merge, view_cols, already_checked);

    } else {
      child = BuildTopDownUnionChecker(
          impl, context, parent, merge, view_cols, already_checked);
    }

  } else if (view.IsAggregate()) {
    assert(false && "TODO: Checker for aggregates.");

  } else if (view.IsKVIndex()) {
    assert(false && "TODO: Checker for k/v indices.");

  } else if (view.IsMap()) {
    const auto map = QueryMap::From(view);
    child = BuildTopDownGeneratorChecker(
        impl, context, parent, map, view_cols, already_checked);

  } else if (view.IsCompare()) {
    const auto cmp = QueryCompare::From(view);
    child = BuildTopDownCompareChecker(
        impl, context, parent, cmp, view_cols, already_checked);

  } else if (view.IsSelect()) {
    const auto select = QuerySelect::From(view);
    child = BuildTopDownSelectChecker(
        impl, context, parent, select, view_cols, already_checked);

  } else if (view.IsTuple()) {
    const auto tuple = QueryTuple::From(view);
    child = BuildTopDownTupleChecker(
        impl, context, parent, tuple, view_cols, already_checked);

  // The only way, from the top-down, to reach an INSERT is via a SELECT, but
  // the top-down SELECT checker skips over the INSERTs and jumps into the
  // TUPLEs that precede them.
  } else if (view.IsInsert()) {
    assert(false);

  } else if (view.IsNegate()) {
    const auto negate = QueryNegate::From(view);
    child = BuildTopDownNegationChecker(
        impl, context, parent, negate, view_cols, already_checked);

  // Not possible?
  } else {
    assert(false);
  }

  if (child) {
    assert(child->parent == parent);
    parent_body->Emplace(parent, child);
  }

  CompleteProcedure(impl, proc, context);

  // This view is conditional, wrap whatever we had generated in a big
  // if statement.
  const auto pos_conds = view.PositiveConditions();
  const auto neg_conds = view.NegativeConditions();
  const auto proc_body = proc->body.get();

  // Innermost test for negative conditions.
  if (!neg_conds.empty()) {
    auto test = impl->operation_regions.CreateDerived<TUPLECMP>(
        proc, ComparisonOperator::kEqual);

    for (auto cond : neg_conds) {
      test->lhs_vars.AddUse(ConditionVariable(impl, cond));
      test->rhs_vars.AddUse(impl->zero);
    }

    proc->body.Emplace(proc, test);
    test->body.Emplace(test, proc_body);
  }

  // Outermost test for positive conditions.
  if (!pos_conds.empty()) {
    auto test = impl->operation_regions.CreateDerived<TUPLECMP>(
        proc, ComparisonOperator::kNotEqual);

    for (auto cond : pos_conds) {
      test->lhs_vars.AddUse(ConditionVariable(impl, cond));
      test->rhs_vars.AddUse(impl->zero);
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

    BuildTopDownChecker(impl, context, view, view_cols, proc, already_checked);
  }
  return changed;
}

// Add entry point records for each query of the program.
static void BuildQueryEntryPointImpl(
    ProgramImpl * impl, Context &context,
    ParsedDeclaration decl, QueryInsert insert) {

  const QueryView view(insert);
  const auto query = ParsedQuery::From(decl);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  assert(model->table != nullptr);

  std::vector<std::pair<QueryColumn, QueryColumn>> available_cols;
  std::vector<unsigned> col_indices;
  for (auto param : decl.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      col_indices.push_back(param.Index());
    }

    const auto in_col = insert.NthInputColumn(param.Index());
    available_cols.emplace_back(in_col, in_col);
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
    auto pred_view = view.Predecessors()[0];
    assert(pred_view.IsTuple());

    const auto checker = GetOrCreateTopDownChecker(
        impl, context, pred_view, available_cols, nullptr);
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

  } else if (view.IsInsert()) {
    return false;
  }

  // We have a model, so worst case, we can do a full table scan.
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (model->table) {
    return true;
  }

  // We need some columns.
  return !available_cols.empty();
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

      MapVariables(gen->empty_body.get());

    } else if (auto call = op->AsCall(); call) {
      MapVariables(call->false_body.get());
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

}  // namespace

// Returns a global reference count variable associated with a query condition.
VAR *ConditionVariable(ProgramImpl *impl, QueryCondition cond) {
  auto &cond_var = impl->cond_ref_counts[cond];
  if (!cond_var) {
    cond_var = impl->global_vars.Create(
        impl->next_id++, VariableRole::kConditionRefCount);
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

// Expand the set of available columns.
void ExpandAvailableColumns(
    QueryView view,
    std::unordered_map<unsigned, QueryColumn> &wanted_to_avail) {

  // Now, map outputs to inputs.
  view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                      std::optional<QueryColumn> out_col) {

    if (out_col &&
        InputColumnRole::kIndexValue != role &&
        InputColumnRole::kAggregatedColumn != role) {

      if (auto it = wanted_to_avail.find(out_col->Id());
          it != wanted_to_avail.end()) {
        wanted_to_avail.emplace(in_col.Id(), it->second);
      }
    }
  });

  // The same input column may be used multiple times, and so if we have one
  // of the outputs, then we can find the other output via the inputs.
  auto pivot_ins_to_outs = [&] (void) {
    view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                        std::optional<QueryColumn> out_col) {
      if (out_col &&
          InputColumnRole::kIndexValue != role &&
          InputColumnRole::kAggregatedColumn != role) {

        if (auto it = wanted_to_avail.find(in_col.Id());
            it != wanted_to_avail.end()) {
          wanted_to_avail.emplace(out_col->Id(), it->second);
        }
      }
    });
  };

  pivot_ins_to_outs();

  // Finally, some of the inputs may be constants. We have to do constants
  // last because something in `available_cols` might be a "variable" that
  // takes on a different value than a constant, and thus needs to be checked
  // against that constant.
  view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                      std::optional<QueryColumn> out_col) {
    if (out_col &&
        InputColumnRole::kIndexValue != role &&
        InputColumnRole::kAggregatedColumn != role &&
        in_col.IsConstantOrConstantRef()) {
      wanted_to_avail.emplace(out_col->Id(), in_col);
    }
  });

  pivot_ins_to_outs();
}

// Filter out only the available columns that are part of the view we care
// about.
std::vector<std::pair<QueryColumn, QueryColumn>>
FilterAvailableColumns(
    QueryView view,
    const std::unordered_map<unsigned, QueryColumn> &wanted_to_avail) {
  std::vector<std::pair<QueryColumn, QueryColumn>> ret;
  for (auto col : view.Columns()) {
    if (auto it = wanted_to_avail.find(col.Id()); it != wanted_to_avail.end()) {
      ret.emplace_back(col, it->second);
    }
  }
  return ret;
}

// Gets or creates a top down checker function.
PROC *GetOrCreateTopDownChecker(
    ProgramImpl *impl, Context &context, QueryView view,
    const std::vector<std::pair<QueryColumn, QueryColumn>> &available_cols,
    TABLE *already_checked) {

  // There is a pretty evil situation we might encounter:
  //
  //            ...   .----.
  //              \  /      \        .
  //              UNION     |
  //             /    \     |
  //          TUPLE    .....'
  //           /       /   \         .
  //         ...
  //
  // In this situation, the data model of the TUPLE and the inductive UNION
  // are identical. If we're calling the top-down checker for the TUPLE, and
  // we have `already_checked == nullptr`, then it's the tuple's responsibility
  // to assert the absence of the row/tuple in the table, then call the
  // predecessors to try to prove the presence of that row/tuple in its own
  // absence. The tricky bit here, though, is that this is strictly downward-
  // facing: the TUPLE will ask its predecessors, and totally ignore the
  // inductive UNION which it feeds. Thus, it's preventing that UNION and its
  // predecessors (which includes the TUPLE) from even participating in this
  // decision. This is bad because the TUPLE and the UNION share their data
  // models!
  if (const auto model = impl->view_to_model[view]->FindAs<DataModel>();
      !already_checked && model->table &&
      model->table->views[0].IsMerge() && model->table->views[0] != view) {

    std::vector<QueryColumn> available_cols_in_merge;

    auto top_merge = model->table->views[0];
    auto top_merge_cols = top_merge.Columns();

    for (auto [col, avail_col] : available_cols) {
      available_cols_in_merge.push_back(top_merge_cols[*(col.Index())]);
    }

    auto sub_wanted_to_avail = ComputeAvailableColumns(
        view, available_cols_in_merge);

    return GetOrCreateTopDownChecker(
        impl, context, top_merge, sub_wanted_to_avail,
        already_checked  /* nullptr */);
  }

//  assert(CanImplementTopDownChecker(impl, view, available_cols));
  (void) CanImplementTopDownChecker;

  // Make up a string that captures what we have available.
  std::stringstream ss;
  ss << view.KindName() << ':' << view.UniqueId();
  for (auto [view_col, avail_col] : available_cols) {
    ss << ',' << view_col.Id() << '/'
       << static_cast<uint32_t>(view_col.Type().Kind());
  }
  if (already_checked) {
    ss << ':' << already_checked->id;
  }

  auto &proc = context.view_to_top_down_checker[ss.str()];

  // We haven't made this procedure before; so we'll declare it now and
  // enqueue it for building. We enqueue all such procedures for building so
  // that we can build top-down checkers after all bottom-up provers have been
  // created. Doing so lets us determine which views, and thus data models,
  // are backed by what tables.
  if (!proc) {
    proc = impl->procedure_regions.Create(impl->next_id++,
                                          ProcedureKind::kTupleFinder);

    std::vector<QueryColumn> view_cols;
    std::unordered_map<unsigned, QueryColumn> wanted_to_avail;

    for (auto [view_col, avail_col] : available_cols) {
      const auto var =
          proc->input_vars.Create(impl->next_id++, VariableRole::kParameter);
      var->query_column = view_col;
      proc->col_id_to_var[view_col.Id()] = var;
      wanted_to_avail.emplace(view_col.Id(), view_col);
      view_cols.push_back(view_col);
    }

    // Now, map outputs to inputs.
    ExpandAvailableColumns(view, wanted_to_avail);
    for (auto [col_id, avail_col] : wanted_to_avail) {
      proc->col_id_to_var.emplace(col_id, proc->VariableFor(impl, avail_col));
    }

    context.top_down_checker_work_list.emplace_back(
        view, view_cols, proc, already_checked);
  }

  return proc;
}

// We want to call the checker for `view`, but we only have the columns
// `succ_cols` available for use.
//
// Return value is either `{call, call}` or `{cmp, call}` where the `cmp`
// contains the `call`.
std::pair<OP *, CALL *> CallTopDownChecker(
    ProgramImpl *impl, Context &context, REGION *parent, QueryView succ_view,
    const std::vector<QueryColumn> &succ_cols, QueryView view,
    TABLE *already_checked) {

  assert(!succ_view.IsInsert());
  assert(!view.IsInsert());

  std::unordered_map<unsigned, QueryColumn> wanted_to_avail;
  for (auto succ_col : succ_cols) {
    wanted_to_avail.emplace(succ_col.Id(), succ_col);
  }

  ExpandAvailableColumns(succ_view, wanted_to_avail);
  if (succ_view != view) {
    ExpandAvailableColumns(view, wanted_to_avail);
  }
  const auto available_cols = FilterAvailableColumns(view, wanted_to_avail);

  // Map the variables for the call.
  const auto let = impl->operation_regions.CreateDerived<LET>(parent);
  OP *call_parent = let;
  for (auto [wanted_col, avail_col] : available_cols) {
    let->col_id_to_var[wanted_col.Id()] = parent->VariableFor(impl, avail_col);
  }

  // Also map in the available columnns, but don't override anything that's
  // there (hence use of `emplace`).
  for (auto [wanted_col, avail_col] : available_cols) {
    let->col_id_to_var.emplace(avail_col.Id(),
                               parent->VariableFor(impl, avail_col));
  }

  // List of output columns of `view` that can be derived from the output
  // columns of `succ_view`, as available in `succ_cols`.
//  std::vector<QueryColumn> view_cols;

//  // Everything is available, yay!
//  if (view == succ_view) {
//    for (auto col : view.Columns()) {
//      available_cols.push_back(col);
//    }
//  }
//
//  // If any of the columns of the view we want to call are constant references
//  // then they are available. We know the outputs of a view are never themselves
//  // constants.
//  for (auto col : view.Columns()) {
//    if (col.IsConstantRef()) {
//      available_cols.push_back(col);
//      (void) parent->VariableFor(impl, col);
//    }
//  }
//
//  const auto let = impl->operation_regions.CreateDerived<LET>(parent);
//  OP *call_parent = let;
//
//  // Now we need to map the outputs of `succ_view` back to the outputs of
//  // `view`.
//  succ_view.ForEachUse([&](QueryColumn view_col, InputColumnRole role,
//                           std::optional<QueryColumn> succ_view_col) {
//
//    // `view_col` is unrelated to `view`.
//    if (QueryView::Containing(view_col) != view) {
//      return;
//
//    // The input column from `view` is not present in `succ_view`.
//    } else if (!succ_view_col) {
//      return;
//
//    // We cannot depend on the output value of this column
//    // (from `succ_view_col`) to derive the input value `view_col`. However,
//    // if the input column is a constant or constant ref then we have it.
//    } else if (InputColumnRole::kIndexValue == role ||
//               InputColumnRole::kAggregatedColumn == role) {
//      return;
//
////    // Two input values are conditionally merged into one output value. We can
////    // never convert an output into an input in this case.
////    } else if ((InputColumnRole::kCompareLHS == role ||
////                InputColumnRole::kCompareRHS == role) &&
////               (QueryCompare::From(succ_view).Operator() ==
////                ComparisonOperator::kEqual)) {
////      return;
//
//    // `*succ_view_col` is available in `view_cols`, thus `view_col` is
//    // also available.
//    } else if (std::find(succ_cols.begin(), succ_cols.end(), *succ_view_col) !=
//               succ_cols.end()) {
//      available_cols.push_back(view_col);
//      const auto succ_var = let->VariableFor(impl, *succ_view_col);
//      let->col_id_to_var[view_col.Id()] = succ_var;
//
//    // If it's a constant ref then we have it. Note that we take `view_col`
//    // as the lookup for the `VariableFor` and not `*succ_view_col` because
//    // that might give us the output of a mutable column in a key/value store.
//    //
//    // NOTE(pag): This follows a check that `view_col` belongs to `view` so that
//    //            multiple constants flowing into a column in a MERGE/UNION
//    //            don't end up all getting matched.
//    } else if (succ_view_col->IsConstantRef()) {
//      const auto succ_var = let->VariableFor(impl, *succ_view_col);
//      let->col_id_to_var[view_col.Id()] = succ_var;
//      available_cols.push_back(view_col);
//    }
//  });
//
//  // Sort and unique out the available columns.
//  std::sort(available_cols.begin(), available_cols.end(),
//            [] (QueryColumn a, QueryColumn b) {
//              return *(a.Index()) < *(b.Index());
//            });
//  auto it = std::unique(available_cols.begin(), available_cols.end(),
//                        [] (QueryColumn a, QueryColumn b) {
//                          return *(a.Index()) == *(b.Index());
//                        });
//  available_cols.erase(it, available_cols.end());




  // We need to create a mapping of input-to-output columns in `succ_view`.
  // For example, we might have the following case:
  //
  //           +-------+---+---+
  //           |       | A | B |
  //           | TUPLE +---+---+    succ_view
  //           |       | C | C |
  //           +-------+-+-+-+-+
  //                     |  /
  //                     | /
  //                     |/
  //           +-------+-+-+
  //           |       | C |
  //           | TUPLE +---+        view
  //           |       | . |
  //           +-------+---+
  //
  // Here, column `C` from `view` is projected into columns `A` and `B` of
  // succ_view. In a top-down checker, this turns into a requirement to
  // only call `view` if `A == B`.
  std::unordered_map<QueryColumn, std::vector<QueryColumn>> in_to_out;
  succ_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> succ_view_col) {

    // The inputs that align with the outputs don't necessarily relate in
    // a straightforward way, due to going through merge/aggregate functors.
    //
    // For inequality comparisons, the comparison node will do the checking. For
    // equality comparisons, two inputs are merged into one output, so there is
    // no burden of comparison.
    if (InputColumnRole::kIndexValue == role ||
        InputColumnRole::kAggregatedColumn == role ||
        InputColumnRole::kCompareLHS == role ||
        InputColumnRole::kCompareRHS == role) {
      return;
    }

    if (!succ_view_col) {
      return;
    }

    in_to_out[in_col].push_back(*succ_view_col);
  });

  const auto proc = GetOrCreateTopDownChecker(
      impl, context, view, available_cols, already_checked);

  std::vector<std::pair<QueryColumn, QueryColumn>> must_be_equal;
  for (auto [wanted_col, avail_col] : available_cols) {
    const std::vector<QueryColumn> &cols = in_to_out[wanted_col];
    for (auto i = 1u; i < cols.size(); ++i) {
      must_be_equal.emplace_back(cols[i - 1u], cols[i]);
    }
  }

  TUPLECMP *cmp = nullptr;
  if (!must_be_equal.empty()) {
    cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
        let, ComparisonOperator::kEqual);
    let->body.Emplace(let, cmp);
    COMMENT( cmp->comment = "Ensuring downward equality of projection"; )

    // NOTE(pag): We're looking up the variables in `parent` and *not* in `let`
    //            because in the above code, when finding the `available_cols`,
    //            we forced a bunch of variable bindings in the `let` for the
    //            convenience of the `call`, and those bindings would work
    //            against this comparison, making it looking like it was
    //            comparing variables against themselves, thereby resulting in
    //            the comparison eventually being optimized away.
    for (auto [lhs_col, rhs_col] : must_be_equal) {
      const auto lhs_var = parent->VariableFor(impl, lhs_col);
      const auto rhs_var = parent->VariableFor(impl, rhs_col);
      cmp->lhs_vars.AddUse(lhs_var);
      cmp->rhs_vars.AddUse(rhs_var);
    }

    call_parent = cmp;
  }

  // Now call the checker procedure.
  const auto check = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, call_parent, proc);

  call_parent->body.Emplace(call_parent, check);

  auto i = 0u;
  for (auto [wanted_col, avail_col] : available_cols) {
    const auto var = call_parent->VariableFor(impl, wanted_col);
    assert(var != nullptr);
    check->arg_vars.AddUse(var);
    const auto param = proc->input_vars[i++];
    assert(var->Type() == param->Type());
    (void) param;
  }

  assert(check->arg_vars.Size() == proc->input_vars.Size());
  return {let, check};
}

// Call the predecessor view's checker function, and if it succeeds, return
// `true`. If we have a persistent table then update the tuple's state in that
// table.
OP *ReturnTrueWithUpdateIfPredecessorCallSucceeds(
    ProgramImpl *impl, Context &context, REGION *parent, QueryView view,
    const std::vector<QueryColumn> &view_cols, TABLE *table,
    QueryView pred_view, TABLE *already_checked) {

  const auto [check, check_call] = CallTopDownChecker(
      impl, context, parent, view, view_cols, pred_view, already_checked);

  const auto ret_true = BuildStateCheckCaseReturnTrue(impl, check_call);
  check_call->body.Emplace(check_call, ret_true);

  return check;
}

// If any of the views associated with this table are associated with an
// inductive union, then we will defer the state transitioning to the
// inductive union.
static bool ShouldDeferStateTransition(Context &context, TABLE *table) {
  for (auto view : table->views) {
    if (context.inductive_predecessors.count(view)) {
      return true;
    }
  }
  return false;
}

// Possibly add a check to into `parent` to transition the tuple with the table
// associated with `view` to be in an present state. Returns the table of `view`
// and the updated `already_removed`.
std::tuple<OP *, TABLE *, TABLE *> InTryInsert(
    ProgramImpl *impl, Context &context, QueryView view, OP *parent,
    TABLE *already_added) {

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;
  if (table) {

    if (ShouldDeferStateTransition(context, table)) {
      return {parent, table, already_added};
    }

    if (already_added != table) {

      // Figure out what columns to pass in for marking.
      std::vector<QueryColumn> cols;
      if (view.IsInsert()) {
        auto insert = QueryInsert::From(view);
        cols.insert(cols.end(), insert.InputColumns().begin(),
                    insert.InputColumns().end());
      } else {
        cols.insert(cols.end(), view.Columns().begin(), view.Columns().end());
      }

      assert(!cols.empty());

      // Do the marking.
      const auto table_remove = BuildChangeState(
          impl, table, parent, cols, TupleState::kAbsentOrUnknown,
          TupleState::kPresent);

      parent->body.Emplace(parent, table_remove);
      parent = table_remove;
      already_added = table;
    }
  } else {
    already_added = nullptr;
  }

  return {parent, table, already_added};
}

// Possibly add a check to into `parent` to transition the tuple with the table
// associated with `view` to be in an unknown state. Returns the table of `view`
// and the updated `already_removed`.
std::tuple<OP *, TABLE *, TABLE *> InTryMarkUnknown(
    ProgramImpl *impl, Context &context, QueryView view, OP *parent,
    TABLE *already_removed) {

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;
  if (table) {

    if (ShouldDeferStateTransition(context, table)) {
      return {parent, table, already_removed};
    }

    if (already_removed != table) {

      // Figure out what columns to pass in for marking.
      std::vector<QueryColumn> cols;
      if (view.IsInsert()) {
        auto insert = QueryInsert::From(view);
        cols.insert(cols.end(), insert.InputColumns().begin(),
                    insert.InputColumns().end());
      } else {
        cols.insert(cols.end(), view.Columns().begin(), view.Columns().end());
      }

      assert(!cols.empty());

      // Do the marking.
      const auto table_remove = BuildChangeState(
          impl, table, parent, cols, TupleState::kPresent, TupleState::kUnknown);

      parent->body.Emplace(parent, table_remove);
      parent = table_remove;
      already_removed = table;
    }
  } else {
    already_removed = nullptr;
  }

  return {parent, table, already_removed};
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

      // TODO(pag): Think about implication of MERGEs here, especially w.r.t.
      //            constants. Things appear OK given that we're assigning to
      //            `in_col.Id()`.

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
void CompleteProcedure(ProgramImpl *impl, PROC *proc, Context &context,
                       bool add_return) {
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
  if (add_return && !EndsWithReturn(proc)) {
    const auto ret = impl->operation_regions.CreateDerived<RETURN>(
        proc, ProgramOperation::kReturnFalseFromProcedure);
    ret->ExecuteAfter(impl, proc);
  }
}

static void MapVariablesInEagerRegion(ProgramImpl *impl, QueryView pred_view,
                                      QueryView view, OP *parent) {
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

    } else if (QueryView::Containing(in_col) == pred_view) {
      const auto src_var = parent->VariableFor(impl, in_col);
      parent->col_id_to_var[out_col->Id()] = src_var;

    // NOTE(pag): This is subtle. We use `emplace` here instead of `[...] =`
    //            to give preference to the constant matching the incoming view.
    //            The key issue here is when we have a column of a MERGE node
    //            taking in a lot constants.
    } else if (in_col.IsConstantOrConstantRef() &&
               InputColumnRole::kMergedColumn != role) {
      const auto src_var = parent->VariableFor(impl, in_col);
      parent->col_id_to_var.emplace(out_col->Id(), src_var);
    }
  });
}

// Build an eager region for removing data.
void BuildEagerRemovalRegion(ProgramImpl *impl, QueryView from_view,
                             QueryView to_view, Context &context, OP *parent,
                             TABLE *already_checked) {
  MapVariablesInEagerRegion(impl, from_view, to_view, parent);

  if (to_view.IsTuple()) {
    CreateBottomUpTupleRemover(impl, context, to_view, parent, already_checked);

  } else if (to_view.IsCompare()) {
    CreateBottomUpCompareRemover(impl, context, to_view, parent,
                                 already_checked);

  } else if (to_view.IsInsert()) {
    CreateBottomUpInsertRemover(impl, context, to_view, parent,
                                already_checked);

  } else if (to_view.IsMerge()) {
    if (context.inductive_successors.count(to_view) &&
        !context.dominated_merges.count(to_view)) {
      CreateBottomUpInductionRemover(impl, context, to_view, parent,
                                     already_checked);
    } else {
      CreateBottomUpUnionRemover(impl, context, to_view, parent,
                                 already_checked);
    }

  } else if (to_view.IsJoin()) {
    auto join = QueryJoin::From(to_view);
    if (join.NumPivotColumns()) {
      CreateBottomUpJoinRemover(impl, context, from_view, join, parent,
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
      CreateBottomUpGenerateRemover(impl, context, map, functor, parent,
                                    already_checked);

    } else {
      assert(false && "TODO Impure Functors!");
    }

  } else if (to_view.IsNegate()) {
    CreateBottomUpNegationRemover(impl, context, to_view, parent,
                                  already_checked);

  // NOTE(pag): This shouldn't be reachable, as the bottom-up INSERT
  //            removers jump past SELECTs.
  } else if (to_view.IsSelect()) {
    assert(false);

  } else {
    assert(false);
  }
}

// Build an eager region. This guards the execution of the region in
// conditionals if the view itself is conditional.
void BuildEagerRegion(ProgramImpl *impl, QueryView pred_view, QueryView view,
                      Context &context, OP *parent, TABLE *last_table) {

  MapVariablesInEagerRegion(impl, pred_view, view, parent);

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
    BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                               last_table);

  } else if (view.IsTuple()) {
    BuildEagerTupleRegion(impl, pred_view, QueryTuple::From(view), context,
                          parent, last_table);

  } else if (view.IsInsert()) {
    const auto insert = QueryInsert::From(view);
    BuildEagerInsertRegion(impl, pred_view, insert, context, parent,
                           last_table);

  } else if (view.IsNegate()) {
    const auto negate = QueryNegate::From(view);
    BuildEagerNegateRegion(impl, pred_view, negate, context,
                           parent, last_table);

  } else {
    assert(false);
  }
}

WorkItem::WorkItem(Context &context, unsigned order_)
    : order(order_) {}

WorkItem::~WorkItem(void) {}

// Build a program from a query.
std::optional<Program> Program::Build(const ::hyde::Query &query,
                                      IRFormat format_,
                                      const ErrorLog &log) {
  auto impl = std::make_shared<ProgramImpl>(query, format_);
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
  DiscoverInductions(query, context, log);
  if (!log.IsEmpty()) {
    return std::nullopt;
  }

  // Now that we've identified our inductions, we can fill our data model,
  // i.e. assign persistent tables to each disjoint set of views.
  FillDataModel(query, program, context);

  // Build the initialization procedure, needed to start data flows from
  // things like constant tuples.
  BuildInitProcedure(program, context);

  // Build bottom-up procedures starting from message receives.
  BuildEagerProcedure(program, context, query);

  for (auto insert : query.Inserts()) {
    if (insert.IsRelation()) {
      auto decl = insert.Relation().Declaration();
      if (decl.IsQuery()) {
        BuildQueryEntryPoint(program, context, decl, insert);
      }
    }
  }

  // Build top-down provers and the bottom-up removers (bottom-up removers are
  // separate procedures when using the `IRFormat::kRecursive`; when using
  // `IRFormat::kIterative`, the bottom-up removers are iterative and in-line
  // with the inserters.
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
