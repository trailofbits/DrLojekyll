// Copyright 2021, Trail of Bits. All rights reserved.

#include <algorithm>
#include <set>
#include <vector>

#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Util/DisjointSet.h>

#include "Query.h"

namespace hyde {
namespace {

template <typename T>
static void ForEachPredecessorOf(VIEW *view, T cb) {
  for (auto pred_view : view->predecessors) {
    cb(pred_view);
  }

  if (auto negate = view->AsNegate()) {
    cb(negate->negated_view.get());
  }
}

template <typename T>
static void ForEachSuccessorOf(VIEW *view, T cb) {
  for (auto succ_view : view->successors) {
    cb(succ_view);
  }

  view->ForEachUse<NEGATION>([&] (NEGATION *negate, VIEW *) {
    cb(negate);
  });
}

// Return the set of all views that contribute data to `view`. This includes
// things like conditions.
static std::set<VIEW *> TransitivePredecessorsOf(VIEW *output) {
  std::set<VIEW *> dependencies;
  std::vector<VIEW *> frontier;
  frontier.push_back(output);

  while (!frontier.empty()) {
    const auto view = frontier.back();
    frontier.pop_back();
    ForEachPredecessorOf(view, [&] (VIEW *pred_view) {
      if (auto [it, added] = dependencies.insert(pred_view); added) {
        frontier.push_back(pred_view);
      }
    });
  }

  return dependencies;
}

// Return the set of all views that are transitively derived from `input`.
static std::set<VIEW *> TransitiveSuccessorsOf(VIEW *input) {
  std::set<VIEW *> dependents;
  std::vector<VIEW *> frontier;
  frontier.push_back(input);

  while (!frontier.empty()) {
    const auto view = frontier.back();
    frontier.pop_back();

    ForEachSuccessorOf(view, [&] (VIEW *succ_view) {
      if (auto [it, added] = dependents.insert(succ_view); added) {
        frontier.push_back(succ_view);
      }
    });
  }

  return dependents;
}

class MergeSet : public DisjointSet {
 public:
  using DisjointSet::DisjointSet;
  std::shared_ptr<WeakUseList<VIEW>> related_merges;
  bool is_linearizable{false};
};

}  // namespace

// Identify the inductive unions in the data flow.
void QueryImpl::IdentifyInductions(const ErrorLog &log) {

  // Mapping of inductive MERGEs to their equivalence classes.
  std::unordered_map<MERGE *, MergeSet> merge_sets;
  std::set<std::pair<MERGE *, VIEW *>> eventually_noninductive_successors;

  std::set<VIEW *> seen;
  std::vector<VIEW *> frontier;

  // Start with the basic identification of which merges are inductive, and
  // which aren't.
  unsigned merge_id = 0u;
  for (MERGE *view : merges) {
    auto preds = TransitivePredecessorsOf(view);

    // This is not an inductive merge.
    if (!preds.count(view)) {
      continue;
    }

    merge_sets.emplace(view, merge_id++);

    // The issue with negations is that they are a sort of hidden successor,
    // not captured by `view->successors`, and so we don't want to add negations
    // into the (non)inductive successor lists when they don't appear in the
    // normal successor lists.
    assert(!view->is_used_by_negation);

    for (auto succ_view : view->successors) {
      if (preds.count(succ_view)) {
        view->inductive_successors.AddUse(succ_view);

      } else {
        view->noninductive_successors.AddUse(succ_view);
      }
    }

    auto succs = TransitiveSuccessorsOf(view);
    for (auto pred_view : view->predecessors) {
      if (succs.count(pred_view)) {
        view->inductive_predecessors.AddUse(pred_view);
      } else {
        view->noninductive_predecessors.AddUse(pred_view);
      }
    }
  }

  for (auto &[view_, _] : merge_sets) {
    MERGE *const view = view_;

    if (view->inductive_successors.Empty()) {
      continue;
    }

    seen.clear();

    // This is a variant of transitive successors, except that we don't allow
    // ourselves to walk through non-inductive output paths of merges because
    // otherwise we would end up merging two unrelated induction sets.

    // We want to express something similar to dominance analysis here.
    // Specifically, suppose we have a set of UNIONs that all logically belong
    // to the same co-inductive set. That is, the outputs of each of the unions
    // somehow cycle into all of the other unions.

    DisjointSet &base_set = merge_sets[view];

    bool seen_another_induction = false;

    for (auto succ_view : view->inductive_successors) {
      frontier.clear();
      frontier.push_back(succ_view);
      seen.insert(succ_view);

      while (!frontier.empty()) {
        const auto frontier_view = frontier.back();
        frontier.pop_back();

        // We've cycled back to ourselves. If we get back to ourselves along
        // some path that doesn't itself go through another inductive merge/cycle,
        // which would have been caught by the `else if` case below, then it means
        // that this view isn't subordinate to any other one, and that is actually
        // does in fact need storage.
        if (frontier_view == view) {
          view->can_reach_self_not_through_another_induction = true;
          continue;

        // We've cycled to another UNION that is also inductive.
        } else if (frontier_view->IsInductive()) {
          seen_another_induction = true;
          MERGE * const frontier_merge = frontier_view->AsMerge();
          assert(frontier_merge != nullptr);
          DisjointSet &reached_set = merge_sets[frontier_merge];
          DisjointSet::Union(&base_set, &reached_set);

        // We need to follow the frontier view's successors.
        } else {

          // If we've reached an insert with no successors, then this is either
          // a MATERIALIZE or a PUBLISH, and thus we've discovered an inductive
          // successor (`succ_view`) that leads out of the induction. Later,
          // we'll try to narrow down on the specific source of non-induction,
          // and then
          if (auto insert = frontier_view->AsInsert();
              insert && insert->successors.Empty()) {
            eventually_noninductive_successors.emplace(view, succ_view);
          }

          ForEachSuccessorOf(frontier_view, [&] (VIEW *frontier_succ_view) {
            if (auto [it, added] = seen.insert(frontier_succ_view); added) {
              frontier.push_back(frontier_succ_view);
            }
          });
        }
      }
    }

    // All inductive paths out of this union lead to another inductive union.
    if (!view->can_reach_self_not_through_another_induction &&
        seen_another_induction) {
      view->all_inductive_successors_reach_other_inductions = true;
    }
  }

  std::set<VIEW *> injection_sites;

  // Some of the inductive successors of a merge may actually indirectly lead
  // to leaving the induction. We want to find the "injection" sites where we
  // should inject in a new MERGE that will belong to the same general group
  // of inductions. That way, all non-inductive successors are properly
  // associated with UNIONs.
  //
  //                                 MATERIALIZE
  //    MATERIALIZE                     \    .-------.
  //       \    .-------.                \  /        |
  //        \  /        |               UNION        |
  //       TUPLE        |                 |          |
  //          \        ...   INTO       TUPLE        |
  //           \        |                  \        ...
  //          UNION     |                   \        |
  //            |       |                  UNION     |
  //     ... ---+-------'                    |       |
  //                                  ... ---+-------'
  //
  for (auto [merge_, succ_view] : eventually_noninductive_successors) {
    MERGE * const merge = merge_;
    frontier.clear();
    seen.clear();
    seen.insert(merge);
    seen.insert(succ_view);
    frontier.push_back(succ_view);

    while (!frontier.empty()) {
      VIEW * const frontier_view = frontier.back();
      frontier.pop_back();

      ForEachSuccessorOf(frontier_view, [&] (VIEW *frontier_succ_view) {
        const auto frontier_succs = TransitiveSuccessorsOf(frontier_succ_view);
        if (!frontier_succs.count(merge)) {
          injection_sites.insert(frontier_view);
        } else if (!seen.count(frontier_succ_view)) {
          seen.insert(frontier_succ_view);
          frontier.push_back(frontier_succ_view);
        }
      });
    }
  }

  std::set<User *> successor_users;

  // There is am inductive successor of `merge` that reaches `view`, and the
  // edge from `view` to `succ_view` leads out of the UNION.
  for (VIEW *view : injection_sites) {

    MERGE * const new_union = merges.Create();
    new_union->color = 0x00ff00u;

    auto col_index = 0u;
    for (auto col : view->columns) {
      const auto union_col = new_union->columns.Create(
          col->var, new_union, col->id, col_index++);

      col->ReplaceAllUsesWith(union_col);
    }

    // We don't want to replace the weak uses of `this` in any condition's
    // `positive_users` or `negative_users`.
    view->VIEW::ReplaceUsesWithIf<User>(new_union, [=] (User *user, VIEW *) {

      // If there's a negation of `view`, then we'll leave it there, as it
      // might cycle back to another induction.
      if (auto negate = dynamic_cast<NEGATION *>(user)) {
        return negate->negated_view.get() != view;
      }

      // We'll let all conditions continue to use `view`.
      return !dynamic_cast<COND *>(user);
    });

    view->CopyDifferentialAndGroupIdsTo(new_union);

    new_union->merged_views.AddUse(view);

    // NOTE(pag): We don't both with the successors/predecessors, as we'll
    //            call `LinkViews` again which will fix it all up.
  }

  // If we injected any new UNIONs then reset and re-run.
  if (!injection_sites.empty()) {
    for (MERGE *view : merges) {
      view->inductive_predecessors.Clear();
      view->inductive_successors.Clear();
      view->noninductive_predecessors.Clear();
      view->noninductive_successors.Clear();
      view->can_reach_self_not_through_another_induction = false;
      view->all_inductive_successors_reach_other_inductions = false;
    }

    LinkViews();
    IdentifyInductions(log);
    return;
  }

  // We didn't inject any new UNIONs :-) Now we can label all the merges
  // belonging to the same merge set, and make all the merges in a set know
  // about all the other merges in that set.
  auto group_id = 0u;

  for (auto &[view, set] : merge_sets) {
    MergeSet *merge_set = set.FindAs<MergeSet>();
    if (merge_set->related_merges) {
      auto &merges = *(merge_set->related_merges);
      view->merge_set_id.emplace(*(merges[0]->AsMerge()->merge_set_id));
    } else {
      merge_set->related_merges.reset(new WeakUseList<VIEW>(view));
      view->merge_set_id.emplace(group_id++);
    }
    view->related_merges = merge_set->related_merges;
    merge_set->related_merges->AddUse(view);
  }

  const auto num_induction_groups = group_id;

  std::vector<std::vector<unsigned>> directly_reachable_from(num_induction_groups);
  std::vector<bool> group_is_reachable(num_induction_groups);
  std::vector<unsigned> group_to_label(num_induction_groups);

  // Now that we have a an assignment of inductions to IDs, we want to find
  // a partial order of inductions. That is, do the non-inductive outputs of
  // one induction lead to the non-inductive inputs of another induction. We
  // need to know this during control-flow IR generation, because we generally
  // group all inductions that are at the "frontier" of some set of views, e.g.
  // all inductions directly reachable from RECEIVEs. Some of these grouped
  // inductions might actually be reachable from the outputs of others, though.
  for (MERGE *view : merges) {
    if (!view->IsInductive()) {
      continue;
    }

    const auto merge_id = view->merge_set_id.value();
    auto &reached_ids = directly_reachable_from[merge_id];

    seen.clear();
    frontier.clear();

    for (VIEW *succ_view : view->noninductive_successors) {
      frontier.push_back(succ_view);
      seen.insert(succ_view);
    }

    while (!frontier.empty()) {
      const auto frontier_view = frontier.back();
      frontier.pop_back();

      // We've cycled back to ourselves. That shouldn't happen because we
      // followed the *non-inductive* successors of `view`. :-S
      if (frontier_view == view) {
        assert(false);

      // We've cycled to another UNION that is also inductive.
      } else if (frontier_view->IsInductive()) {
        MERGE * const frontier_merge = frontier_view->AsMerge();
        assert(frontier_merge != nullptr);
        const auto frontier_merge_id = frontier_merge->merge_set_id.value();

        if (merge_id != frontier_merge_id) {
          group_is_reachable[frontier_merge_id] = true;
          reached_ids.push_back(frontier_merge_id);
        }

      // We need to follow the frontier view's successors.
      } else {
        ForEachSuccessorOf(frontier_view, [&] (VIEW *frontier_succ_view) {
          if (auto [it, added] = seen.insert(frontier_succ_view); added) {
            frontier.push_back(frontier_succ_view);
          }
        });
      }
    }

    std::sort(reached_ids.begin(), reached_ids.end());
    auto it = std::unique(reached_ids.begin(), reached_ids.end());
    reached_ids.erase(it, reached_ids.end());
  }

  // First, label every unreachable-via-another-induction induction with label
  // 1. These can all be processed in parallel, if need be.
  auto last_id = 1u;
  for (group_id = 0; group_id < num_induction_groups; ++group_id) {
    if (!group_is_reachable[group_id]) {
      group_to_label[group_id] = last_id;
    }
  }

  // Then push the labels forward, one depth at a time. Some might get
  // re-visited, pushing their labels further down. We're basically doing
  // a topological sort, where we label by depth.
  for (auto changed = true; changed; ++last_id) {
    changed = false;
    for (group_id = 0; group_id < num_induction_groups; ++group_id) {
      if (group_to_label[group_id] == last_id) {
        for (auto reached_id : directly_reachable_from[group_id]) {
          group_to_label[reached_id] = last_id + 1u;
          changed = true;
        }
      }
    }
  }

  for (MERGE *view : merges) {
    if (!view->IsInductive()) {
      continue;
    }

    const auto merge_id = view->merge_set_id.value();
    view->merge_depth_id.emplace(group_to_label[merge_id]);
  }

  // Now do some error checking on if the inductions are even linearizable.
  // What we're looking for is an inductive group, where none of the inductions
  // have either proper non-inductive predecessors or successors, and thus
  // can't really said to be ordered anywhere.
  std::unordered_map<ParsedClause, std::vector<ParsedVariable>> bad_vars;

  for (auto &[_, set] : merge_sets) {
    MergeSet *merge_set = set.FindAs<MergeSet>();
    if (merge_set->is_linearizable) {
      continue;
    }
    merge_set->is_linearizable = true;

    auto has_inputs = false;
    auto has_outputs = false;
    for (VIEW *view : *merge_set->related_merges) {
      MERGE * const merge = view->AsMerge();
      if (merge->noninductive_predecessors.Size()) {
        has_inputs = true;
      }
      if (merge->noninductive_successors.Size()) {
        has_outputs = true;
      }
    }

    if (!has_inputs || !has_outputs) {
      const auto &merges = *(merge_set->related_merges);
      VIEW * view = merges[0];
      for (auto col : view->columns) {
        auto clause = ParsedClause::Containing(col->var);
        bad_vars[clause].push_back(col->var);
      }
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

#ifndef NDEBUG
  // Sanity checking.
  for (MERGE *merge : merges) {
    if (merge->IsInductive()) {

      const auto merge_id = *(merge->merge_set_id);
      const auto merge_depth = *(merge->merge_depth_id);
      assert(0u < merge_depth);

      for (auto succ_view : merge->inductive_successors) {
        auto succs = TransitiveSuccessorsOf(succ_view);
        assert(succs.count(merge));
      }

      for (auto succ_view : merge->noninductive_successors) {
        auto succs = TransitiveSuccessorsOf(succ_view);
        assert(!succs.count(merge));

        for (auto reached_view : succs) {
          if (auto reached_merge = reached_view->AsMerge();
              reached_merge && reached_merge->IsInductive()) {
            assert(merge_id != *(reached_merge->merge_set_id));
            assert(merge_depth < *(reached_merge->merge_depth_id));
          }
        }
      }
    } else {
      auto succs = TransitiveSuccessorsOf(merge);
      assert(!succs.count(merge));
    }
  }
#endif
}

}  // namespace hyde
