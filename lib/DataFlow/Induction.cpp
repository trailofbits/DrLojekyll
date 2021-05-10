// Copyright 2021, Trail of Bits. All rights reserved.

#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Util/DisjointSet.h>

#include <algorithm>
#include <set>
#include <vector>

#include "Query.h"

namespace hyde {

InductionInfo::InductionInfo(Node<QueryView> *owner)
    : inductive_predecessors(owner),
      inductive_successors(owner),
      noninductive_predecessors(owner),
      noninductive_successors(owner) {}

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

  auto found = false;
  view->ForEachUse<NEGATION>([&](NEGATION *negate, VIEW *) {
    cb(negate);
    found = true;
  });
  if (!found) {
    assert(!view->is_used_by_negation);
  } else {
    assert(view->is_used_by_negation);
  }
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
    ForEachPredecessorOf(view, [&](VIEW *pred_view) {
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

    ForEachSuccessorOf(view, [&](VIEW *succ_view) {
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
  unsigned merge_set_id{0u};
};

}  // namespace

// Identify the inductive unions in the data flow.
void QueryImpl::IdentifyInductions(const ErrorLog &log, bool recursive) {

  if (recursive) {
    const_cast<const QueryImpl *>(this)->ForEachView(
        [](VIEW *v) { v->induction_info.reset(); });
  }

  // Mapping of inductive MERGEs to their equivalence classes.
  std::unordered_map<VIEW *, MergeSet> merge_sets;
  std::set<std::pair<VIEW *, VIEW *>> eventually_noninductive_successors;

  // Places where we need to inject UNION nodes so that we can better capture
  // the non-inductive successors of an inductive set. In the below example,
  // `TUPLE` will be discovered as an injection site. It is an inductive
  // successor of UNION0, but UNION0 and UNION1 are not in the same strongly
  // connected component.
  //                         ___
  //                     \  /   \             .
  //                    UNION1  |
  //                      |     |
  //                    JOIN    |
  //                    /   \   |
  //                   /     '--'
  //               TUPLE
  //          \    /   \                      .
  //          UNION0   |
  //              \    |
  //               '--...
  std::set<VIEW *> injection_sites;

  std::set<VIEW *> seen;
  std::vector<VIEW *> frontier;

  // Tracks whether or not `entry.first` reaches `entry.second` along any path.
  std::set<std::pair<VIEW *, VIEW *>> reached_inductions;

  for (MERGE *view : merges) {
    frontier.push_back(view);
  }
  for (JOIN *view : joins) {
    frontier.push_back(view);
  }
  for (NEGATION *view : negations) {
    frontier.push_back(view);
  }

  unsigned merge_id = 0u;
  while (!frontier.empty()) {
    VIEW *const view = frontier.back();
    frontier.pop_back();

    auto preds = TransitivePredecessorsOf(view);

    // This is not an inductive merge/join/negate.
    if (!preds.count(view)) {
      continue;
    }

    InductionInfo *const info = new InductionInfo(view);
    view->induction_info.reset(info);
    merge_sets.emplace(view, merge_id++);

    ForEachSuccessorOf(view, [=](VIEW *succ_view) {
      info->successors.push_back(succ_view);
      if (preds.count(succ_view)) {
        info->inductive_successors_mask.push_back(true);

      } else {
        info->inductive_successors_mask.push_back(false);
      }
    });

    auto succs = TransitiveSuccessorsOf(view);

    // Maintain this so that later we can figure out the strongly connected
    // components via union-find.
    for (auto succ_view : succs) {
      if (succ_view->AsMerge() || succ_view->AsJoin() ||
          succ_view->AsNegate()) {
        reached_inductions.emplace(view, succ_view);
      }
    }

    ForEachPredecessorOf(view, [&](VIEW *pred_view) {
      info->predecessors.push_back(pred_view);
      if (succs.count(pred_view)) {
        info->inductive_predecessors_mask.push_back(true);
      } else {
        info->inductive_predecessors_mask.push_back(false);
      }
    });
  }

  // If an inductive successor of A reaches B, and if and inductive successor
  // of B reaches A, then A and B are part of the same "co-inductive" set.
  for (auto [from_view, to_view] : reached_inductions) {
    if (from_view->IsInductive() && to_view->IsInductive()) {
      if (reached_inductions.count({to_view, from_view})) {
        MergeSet &set_1 = merge_sets[from_view];
        MergeSet &set_2 = merge_sets[to_view];
        DisjointSet::Union(&set_1, &set_2);
      }
    }
  }

  // Our next goal is to see if, in this inductive successor, whether /any/
  // path out of an inductive successor leads to an INSERT or to a different
  // induction. If that happens, then there is a way for this inductive
  // successor to have info leave.
  for (auto &[view_, merge_set_] : merge_sets) {
    VIEW *const view = view_;
    InductionInfo *const info = view->induction_info.get();
    MergeSet *const merge_set = merge_set_.FindAs<MergeSet>();
    assert(info != nullptr);

    auto succ_view_i = 0u;
    for (VIEW *succ_view : info->successors) {
      if (!info->inductive_successors_mask[succ_view_i++]) {
        continue;  // Not marked as being an inductive successor.
      }

      seen.clear();
      frontier.clear();
      frontier.push_back(succ_view);

      while (!frontier.empty()) {
        VIEW *const frontier_view = frontier.back();
        frontier.pop_back();

        if (auto frontier_merge_set_it = merge_sets.find(frontier_view);
            frontier_merge_set_it != merge_sets.end()) {
          MergeSet *const frontier_merge_set =
              frontier_merge_set_it->second.FindAs<MergeSet>();

          // We've reached back to this induction along this path.
          if (frontier_merge_set == merge_set) {
            continue;

          // We've found a way of exiting the induction, to a different
          // induction.
          } else {
            eventually_noninductive_successors.emplace(view, succ_view);
            continue;
          }
        }

        if (INSERT *insert = frontier_view->AsInsert()) {
          if (insert->successors.Empty()) {
            eventually_noninductive_successors.emplace(view, succ_view);
            continue;
          }
        }

        ForEachSuccessorOf(frontier_view, [&](VIEW *frontier_succ_view) {
          auto [it, added] = seen.emplace(frontier_succ_view);
          if (added) {
            frontier.emplace_back(frontier_succ_view);
          }
        });
      }
    }
  }

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
  for (auto [merge_, succ_view_] : eventually_noninductive_successors) {
    VIEW *const merge = merge_;
    MergeSet *const merge_set = merge_sets[merge].FindAs<MergeSet>();
    VIEW *const succ_view = succ_view_;
    frontier.clear();
    seen.clear();
    seen.insert(merge);
    seen.insert(succ_view);
    frontier.push_back(succ_view);

    while (!frontier.empty()) {
      VIEW *const frontier_view = frontier.back();
      frontier.pop_back();


      ForEachSuccessorOf(frontier_view, [&](VIEW *frontier_succ_view) {
        // We've walked into the induction that is ourselves; ignore.
        if (auto succ_merge_set_it = merge_sets.find(frontier_succ_view);
            succ_merge_set_it != merge_sets.end()) {
          MergeSet *const succ_merge_set =
              succ_merge_set_it->second.FindAs<MergeSet>();

          // Reached a different induction.
          if (succ_merge_set != merge_set) {
            injection_sites.insert(frontier_view);
          }

          return;
        }

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

  // In the case of JOINs and NEGATEs, if they have no non-inductive
  // predecessors, but have non-inductive successors, then add them to the
  // injection sites.
  for (auto &[view_, merge_set_] : merge_sets) {
    VIEW *const view = view_;
    InductionInfo *const info = view->induction_info.get();
    if (!info || view->AsMerge()) {
      continue;
    }

    bool has_non_inductive_preds = false;
    for (auto is_inductive : info->inductive_predecessors_mask) {
      if (!is_inductive) {
        has_non_inductive_preds = true;
        break;
      }
    }

    bool has_non_inductive_succs = false;
    for (auto is_inductive : info->inductive_successors_mask) {
      if (!is_inductive) {
        has_non_inductive_succs = true;
        break;
      }
    }

    if (!has_non_inductive_preds && has_non_inductive_succs) {
      injection_sites.insert(view);
    }
  }

  seen.clear();
  frontier.clear();

  // There is am inductive successor of `merge` that reaches `view`, and the
  // edge from `view` to `succ_view` leads out of the UNION.
  for (VIEW *view : injection_sites) {

    assert(!view->AsMerge());
    assert(!view->AsSelect());
    assert(!view->AsInsert());

    MERGE *const new_union = merges.Create();
    auto col_index = 0u;
    for (auto col : view->columns) {
      const auto union_col = new_union->columns.Create(
          col->var, col->type, new_union, col->id, col_index++);

      col->ReplaceAllUsesWith(union_col);
    }

    // We don't want to replace the weak uses of `this` in any condition's
    // `positive_users` or `negative_users`.
    view->VIEW::ReplaceUsesWithIf<User>(new_union, [=](User *user, VIEW *) {
      // We'll let all conditions continue to use `view`.
      //
      // NOTE(pag): CONDitions are not allowed to be cyclic.
      // TODO(pag): Make sure CONDitions are never cyclic.
      return !dynamic_cast<COND *>(user);
    });

    view->CopyDifferentialAndGroupIdsTo(new_union);

    new_union->merged_views.AddUse(view);

    // NOTE(pag): We don't both with the successors/predecessors, as we'll
    //            call `LinkViews` again which will fix it all up.
  }

  // If we injected any new UNIONs then reset and re-run.
  if (!injection_sites.empty()) {
    LinkViews(true);
    IdentifyInductions(log, true);
    return;
  }

  // By this point, the non/inductive successors/predecessors have settled.
  // Either they were all good initially, or we've had to inject some UNIONs
  // and did re-linking and re-identification. Now we can go through and upgrade
  // the masked (non-)inductive predecessors/successors into proper use lists.
  for (auto &[view_, set] : merge_sets) {
    VIEW *const view = view_;
    InductionInfo *const info = view->induction_info.get();
    auto i = 0u;

    for (VIEW *const succ_view : info->successors) {
      if (info->inductive_successors_mask[i]) {
        info->inductive_successors.AddUse(succ_view);
      } else {
        info->noninductive_successors.AddUse(succ_view);
      }
      ++i;
    }

    i = 0u;
    for (VIEW *const succ_view : info->predecessors) {
      if (info->inductive_predecessors_mask[i]) {
        info->inductive_predecessors.AddUse(succ_view);
      } else {
        info->noninductive_predecessors.AddUse(succ_view);
      }
      ++i;
    }

    // Views living "fully" inside other inductive back-edges can be marked as
    // not being actually inductive after all.
    if (info->noninductive_predecessors.Empty() &&
        info->noninductive_successors.Empty()) {

      if (view->AsJoin() || view->AsNegate()) {
        view->induction_info.reset();
      }
    }
  }

  for (auto &[view_, set] : merge_sets) {
    VIEW *const view = view_;
    InductionInfo *const info = view->induction_info.get();
    if (!info) {
      continue;
    }

    seen.clear();
    frontier.clear();

    for (VIEW *succ_view : info->inductive_successors) {
      frontier.push_back(succ_view);
    }

    // Next, we want to know if there's a trivial cycle on `view`. That is,
    // can `view` cycle back to itself without first going through another
    // UNION, JOIN, or NEGATE.
    while (!frontier.empty()) {
      VIEW *const frontier_view = frontier.back();
      frontier.pop_back();

      if (frontier_view == view) {
        info->can_reach_self_not_through_another_induction = true;
        break;
      }

      //      // JOINs and NEGATIONs require their predecessors to have tables, so
      //      // there's always something "blocking" us from accidentally doing infinite
      //      // inserts.
      //      } else if (frontier_view->AsJoin() || frontier_view->AsNegate()) {
      //        continue;
      //
      //      } else if (frontier_view->AsMerge()) {
      //
      //      }

      if (InductionInfo *const frontier_info =
              frontier_view->induction_info.get()) {

        // We've reached another inductive thing, and this thing has non-
        // inductive predecessors, so it will have a table.
        if (!frontier_info->noninductive_predecessors.Empty()) {
          continue;

        // We've reached another inductive thing, and that thing will have
        // non-inductive successors, so it will have a table.
        } else if (!frontier_info->noninductive_successors.Empty()) {
          continue;
        }
      }

      ForEachSuccessorOf(frontier_view, [&](VIEW *frontier_succ_view) {
        if (auto [it, added] = seen.insert(frontier_succ_view); added) {
          frontier.push_back(frontier_succ_view);
        }
      });
    }

    //    // Views living "fully" inside other inductive back-edges can be marked as
    //    // not being actually inductive after all.
    //    if (info->noninductive_predecessors.Empty() &&
    //        info->noninductive_successors.Empty() &&
    //        !info->can_reach_self_not_through_another_induction) {
    //      view->induction_info.reset();
    //    }
  }

  // We didn't inject any new UNIONs :-) Now we can label all the merges
  // belonging to the same merge set, and make all the merges in a set know
  // about all the other merges in that set.
  auto group_id = 0u;

  for (auto &[view_, set] : merge_sets) {
    VIEW *const view = view_;
    InductionInfo *const info = view->induction_info.get();
    if (!info) {
      continue;
    }

    MergeSet *merge_set = set.FindAs<MergeSet>();
    if (!merge_set->related_merges) {
      merge_set->related_merges.reset(new WeakUseList<VIEW>(view));
      merge_set->merge_set_id = group_id;
      ++group_id;
    }

    info->merge_set_id = merge_set->merge_set_id;
    info->cyclic_views = merge_set->related_merges;
    merge_set->related_merges->AddUse(view);
  }

  const auto num_induction_groups = group_id;

  std::vector<std::vector<unsigned>> directly_reachable_from(
      num_induction_groups);
  std::vector<bool> group_is_reachable(num_induction_groups);
  std::vector<unsigned> group_to_label(num_induction_groups);

  // Now that we have a an assignment of inductions to IDs, we want to find
  // a partial order of inductions. That is, do the non-inductive outputs of
  // one induction lead to the non-inductive inputs of another induction. We
  // need to know this during control-flow IR generation, because we generally
  // group all inductions that are at the "frontier" of some set of views, e.g.
  // all inductions directly reachable from RECEIVEs. Some of these grouped
  // inductions might actually be reachable from the outputs of others, though.
  for (auto &[view_, set] : merge_sets) {
    VIEW *const view = view_;
    InductionInfo *const info = view->induction_info.get();
    if (!info) {
      continue;
    }

    const auto merge_id = info->merge_set_id;
    auto &reached_ids = directly_reachable_from[merge_id];

    seen.clear();
    frontier.clear();

    for (VIEW *succ_view : info->noninductive_successors) {
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
      } else if (auto frontier_info = frontier_view->induction_info.get()) {
        const auto frontier_merge_id = frontier_info->merge_set_id;

        if (merge_id != frontier_merge_id) {
          group_is_reachable[frontier_merge_id] = true;
          reached_ids.push_back(frontier_merge_id);
        }

      // We need to follow the frontier view's successors.
      } else {
        ForEachSuccessorOf(frontier_view, [&](VIEW *frontier_succ_view) {
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

  for (auto &[view_, set] : merge_sets) {
    VIEW *const view = view_;
    InductionInfo *const info = view->induction_info.get();
    if (!info) {
      continue;
    }

    const auto merge_id = info->merge_set_id;
    info->merge_depth = group_to_label[merge_id];
  }

  // Now do some error checking on if the inductions are even linearizable.
  // What we're looking for is an inductive group, where none of the inductions
  // have either proper non-inductive predecessors or successors, and thus
  // can't really said to be ordered anywhere.
  std::unordered_map<ParsedClause, std::vector<ParsedVariable>> bad_vars;

  for (auto &[view_, set] : merge_sets) {
    VIEW *const view = view_;
    if (!view->IsInductive()) {
      continue;
    }

    MergeSet *merge_set = set.FindAs<MergeSet>();
    if (merge_set->is_linearizable) {
      continue;
    }
    merge_set->is_linearizable = true;

    auto has_inputs = false;
    auto has_outputs = false;

    for (VIEW *related_view : *merge_set->related_merges) {
      assert(related_view->IsInductive());
      InductionInfo *const info = related_view->induction_info.get();
      if (info->noninductive_predecessors.Size()) {
        has_inputs = true;
      }
      if (info->noninductive_successors.Size()) {
        has_outputs = true;
      }
    }

    if (!has_inputs || !has_outputs) {
      const auto &related_views = *(merge_set->related_merges);
      for (VIEW *related_view : related_views) {
        for (auto col : related_view->columns) {
          if (col->var.has_value()) {
            auto clause = ParsedClause::Containing(*(col->var));
            bad_vars[clause].push_back(*(col->var));
          }
        }
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
  const_cast<const QueryImpl *>(this)->ForEachView([](VIEW *merge) {
    if (auto info = merge->induction_info.get()) {
      const auto merge_id = info->merge_set_id;
      const auto merge_depth = info->merge_depth;
      assert(0u < merge_depth);

      for (auto succ_view : info->inductive_successors) {
        auto succs = TransitiveSuccessorsOf(succ_view);
        assert(succs.count(merge));
      }

      for (auto succ_view : info->noninductive_successors) {
        auto succs = TransitiveSuccessorsOf(succ_view);
        assert(!succs.count(merge));

        for (auto reached_view : succs) {
          if (auto reached_info = reached_view->induction_info.get()) {
            assert(merge_id != reached_info->merge_set_id);
            assert(merge_depth < reached_info->merge_depth);
          }
        }
      }
    }
  });
#endif
}

}  // namespace hyde
