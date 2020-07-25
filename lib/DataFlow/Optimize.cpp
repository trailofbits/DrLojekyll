// Copyright 2019, Trail of Bits. All rights reserved.

#include "Optimize.h"

#include <drlojekyll/Util/EqualitySet.h>

#include "Query.h"

namespace hyde {
namespace {

using CandidateList = std::vector<VIEW *>;
using CandidateLists = std::unordered_map<uint64_t, CandidateList>;

// Perform common subexpression elimination, which will first identify
// candidate subexpressions for possible elimination using hashing, and
// then will perform recursive equality checks.
static bool CSE(CandidateList &all_views) {
  EqualitySet eq;
  CandidateLists candidate_groups;

  // NOTE(pag): We group by `HashInit` rather than `Hash` as `Hash` will force
  //            us to miss opportunities due to cycles in the dataflow graph.
  //            `HashInit` ends up being a good enough filter to restrict us to
  //            plausibly similar things.
  for (auto view : all_views) {
    candidate_groups[view->HashInit()].push_back(view);
  }

  auto changed = false;

  using CandidatePair = std::tuple<uint64_t, VIEW *, uint64_t, VIEW *>;
  std::vector<CandidatePair> to_replace;
  std::unordered_map<VIEW *, VIEW *> top_map;
  auto resolve = [&](VIEW *a) -> VIEW * {
    while (top_map.count(a)) {
      a = top_map[a];
    }
    return a;
  };

  for (auto &[hash, candidates] : candidate_groups) {
    (void) hash;

    std::sort(candidates.begin(), candidates.end());
    for (auto i = 0u; i < candidates.size(); ++i) {
      auto v1 = candidates[i];
      for (auto j = i + 1u; j < candidates.size(); ++j) {
        auto v2 = candidates[j];
        assert(v1 != v2);

        eq.Clear();
        if (v1->Equals(eq, v2)) {
          to_replace.emplace_back(v1->UpHash(1), v1, v2->UpHash(1), v2);
          top_map.emplace(v1, v2);
        }
      }
    }

    std::sort(to_replace.begin(), to_replace.end(),
              [](CandidatePair a, CandidatePair b) {
                const auto a_v1_uphash = std::get<0>(a);
                const auto a_v2_uphash = std::get<2>(a);

                const auto b_v1_uphash = std::get<0>(b);
                const auto b_v2_uphash = std::get<2>(b);

                int a_bad = a_v1_uphash != a_v2_uphash;
                int b_bad = b_v1_uphash != b_v2_uphash;

                if (a_bad != b_bad) {
                  return a_bad < b_bad;
                }

                const auto a_v1 = std::get<1>(a);
                const auto a_v2 = std::get<3>(a);

                const auto b_v1 = std::get<1>(b);
                const auto b_v2 = std::get<3>(b);

                return std::min(a_v1->Depth(), a_v2->Depth()) <
                       std::min(b_v1->Depth(), b_v2->Depth());
              });

    while (!to_replace.empty()) {
      auto [v1_uphash, v1, v2_uphash, v2] = to_replace.back();
      to_replace.pop_back();
      v2 = resolve(v2);

      (void) v1_uphash;
      (void) v2_uphash;

      eq.Clear();
      if (v1 != v2 && v1->IsUsed() && v2->IsUsed() && eq.Contains(v1, v2)) {
        v1->ReplaceAllUsesWith(v2);
        changed = true;
      }
    }
  }

  return changed;
}

template <typename T>
static void FillViews(T &def_list, CandidateList &views_out) {
  for (auto view : def_list) {
    if (view->IsUsed()) {
      views_out.push_back(view);
    }
  }
}

}  // namespace

// Relabel group IDs. This enables us to better optimize SELECTs. Our initial
// assignment of `group_id`s works well enough to start with, but isn't good
// enough to help us merge some SELECTs. The key idea is that if a given
// INSERT reaches two SELECTs, then those SELECTs cannot be merged.
void QueryImpl::RelabelGroupIDs(void) {

  // Clear out all `group_id` sets, and reset the depth counters.
  std::vector<COL *> sorted_cols;

  unsigned i = 1u;
  ForEachView([&](VIEW *view) {
    if (view->is_dead) {
      return;
    }

    view->depth = 0;
    view->hash = 0;
    view->group_ids.clear();

    if (view->AsJoin() || view->AsAggregate() || view->AsKVIndex()) {
      view->group_id = i++;
      view->group_ids.push_back(view->group_id);

    } else {
      view->group_id = 0;
    }

    for (auto col : view->columns) {
      sorted_cols.push_back(col);
    }
  });

  ForEachView([&](VIEW *view) {
    if (view->is_dead) {
      return;
    }

    (void) view->Depth();  // Calculate view depth.
  });

  // Sort it so that we process deeper views (closer to INSERTs) first.
  std::sort(sorted_cols.begin(), sorted_cols.end(),
            [](COL *a, COL *b) { return a->view->Depth() > b->view->Depth(); });

  // Propagate the group IDs down through the graph.
  for (auto changed = true; changed;) {
    changed = false;
    for (auto col : sorted_cols) {
      const auto view = col->view;

      const auto old_size = view->group_ids.size();

      // Look at the users of this column, e.g. joins, aggregates, tuples,
      // and copy their view's group ids back to this view.
      col->ForEachUser([=](VIEW *user) {
        // If the user if a JOIN, AGGREGATE, or KVINDEX, then take its group
        // ID.
        if (user->group_id) {
          view->group_ids.push_back(user->group_id);

        // Otherwise, take its set of group IDs.
        } else {
          view->group_ids.insert(view->group_ids.end(), user->group_ids.begin(),
                                 user->group_ids.end());
        }
      });

      std::sort(view->group_ids.begin(), view->group_ids.end());
      auto it = std::unique(view->group_ids.begin(), view->group_ids.end());
      view->group_ids.erase(it, view->group_ids.end());

      if (view->group_ids.size() > old_size) {
        changed = true;
      }
    }
  }
}

// Remove unused views.
bool QueryImpl::RemoveUnusedViews(void) {
  size_t ret = 0;
  size_t all_ret = 0;

  std::vector<VIEW *> views;

  ForEachViewInReverseDepthOrder([&](VIEW *view) { views.push_back(view); });

  for (auto changed = true; changed;) {
    changed = false;
    for (auto view : views) {
      if (!view->IsUsed()) {
        if (view->PrepareToDelete()) {
          changed = true;
        }
      }
    }
  }

  do {

    //    for (auto rel : relations) {
    //      if (rel->IsUsed()) {
    //        rel->ForEachUse<VIEW>([] (VIEW *v, REL *) {
    //          assert(!v->is_dead);
    //        });
    //      }
    //      rel->inserts.RemoveIf([] (VIEW *v) { return v->is_dead; });
    //    }

    ret = 0u;

    //    for (auto sel : selects) {
    //      sel->inserts.RemoveIf([] (VIEW *v) { return v->is_dead; });
    //    }

    ret |= selects.RemoveUnused() | tuples.RemoveUnused() |
           kv_indices.RemoveUnused() | joins.RemoveUnused() |
           maps.RemoveUnused() | aggregates.RemoveUnused() |
           merges.RemoveUnused() | constraints.RemoveUnused() |
           inserts.RemoveUnused();
    all_ret |= ret;
  } while (ret);

  all_ret |= relations.RemoveIf(
      [](REL *rel) { return rel->inserts.Empty() && rel->selects.Empty(); });

  all_ret |= ios.RemoveIf(
      [](IO *io) { return io->receives.Empty() && io->sends.Empty(); });

  return 0 != all_ret;
}

void QueryImpl::Simplify(const ErrorLog &log) {
  CandidateList views;

  // Start by applying CSE to the SELECTs only. This will improve
  // canonicalization of the initial TUPLEs and other things.
  FillViews(selects, views);
  CSE(views);

  views.clear();

  OptimizationContext opt(log);

  // Now canonicalize JOINs, which will eliminate columns of useless joins.
  for (auto join : joins) {
    join->Canonicalize(this, opt);
  }

  // Some of those useless JOINs are converted into TUPLEs, so canonicalize
  // those.
  for (auto tuple : tuples) {
    tuple->Canonicalize(this, opt);
  }

  RemoveUnusedViews();
  RelabelGroupIDs();
}

// Canonicalize the dataflow. This tries to put each node into its current
// "most optimal" form. Previously it was more about re-arranging columns
// to encourange better CSE results.
void QueryImpl::Canonicalize(const OptimizationContext &opt) {
  ForEachView([&](VIEW *view) { view->is_canonical = false; });

  // Canonicalize all views.
  for (auto non_local_changes = true; non_local_changes;) {
    non_local_changes = false;
    if (opt.bottom_up) {
      ForEachViewInDepthOrder([&](VIEW *view) {
        if (view->Canonicalize(this, opt)) {
          non_local_changes = true;
        }
      });
    } else {
      ForEachViewInReverseDepthOrder([&](VIEW *view) {
        if (view->Canonicalize(this, opt)) {
          non_local_changes = true;
        }
      });
    }
  }

  RemoveUnusedViews();
  RelabelGroupIDs();
}

// Sometimes we have a bunch of dumb condition patterns, roughly looking like
// a chain of constant input tuples, conditioned on the next one in the chain,
// and so we want to eliminate all the unnecessary intermediary tuples and
// conditions and shrink down to a more minimal form.
bool QueryImpl::ShrinkConditions(void) {
  std::vector<COND *> conds;
  ForEachView([&](VIEW *view) { view->depth = 0; });

  for (auto cond : conditions) {
    conds.push_back(cond);
  }

  std::sort(conds.begin(), conds.end(), [](COND *a, COND *b) {
    return QueryCondition(a).Depth() < QueryCondition(b).Depth();
  });

  for (auto cond : conds) {
    if (cond->setters.Size() != 1u) {
      continue;
    }

    VIEW *const setter = cond->setters[0];
    assert(setter->sets_condition.get() == cond);
    bool all_constant = true;
    for (auto in_col : setter->input_columns) {
      if (!in_col->IsConstant()) {
        all_constant = false;
        break;
      }
    }

    for (auto in_col : setter->attached_columns) {
      if (!in_col->IsConstant()) {
        all_constant = false;
        break;
      }
    }

    if (!all_constant) {
      continue;
    }

    if (TUPLE *tuple = setter->AsTuple(); tuple) {

      // Keep positive conditions the same
      for (auto pos_dep_cond : setter->positive_conditions) {
        for (auto user_view : cond->positive_users) {
          if (user_view) {
            user_view->positive_conditions.AddUse(pos_dep_cond);
            pos_dep_cond->positive_users.AddUse(user_view);
          }
        }
        for (auto user_view : cond->negative_users) {
          if (user_view) {
            user_view->negative_conditions.AddUse(pos_dep_cond);
            pos_dep_cond->negative_users.AddUse(user_view);
          }
        }
      }

      // Invert the negated conditions.
      for (auto neg_dep_cond : setter->negative_conditions) {
        for (auto user_view : cond->positive_users) {
          if (user_view) {
            user_view->negative_conditions.AddUse(neg_dep_cond);
            neg_dep_cond->negative_users.AddUse(user_view);
          }
        }
        for (auto user_view : cond->negative_users) {
          if (user_view) {
            user_view->positive_conditions.AddUse(neg_dep_cond);
            neg_dep_cond->positive_users.AddUse(user_view);
          }
        }
      }

      do {
        WeakUseRef<COND>().Swap(tuple->sets_condition);
      } while (false);

      cond->setters.Clear();
      cond->positive_users.Clear();
      cond->negative_users.Clear();

    } else if (CMP *cmp = setter->AsConstraint(); cmp) {
      (void) cmp;
    }
  }

  ForEachView([&](VIEW *view) {
    view->depth = 0;
    view->OrderConditions();
  });

  return conditions.RemoveIf([](COND *cond) { return cond->setters.Empty(); });
}

// Apply common subexpression elimination (CSE) to the dataflow.
void QueryImpl::Optimize(const ErrorLog &log) {
  CandidateList views;

  auto do_cse = [&](void) {
    views.clear();
    this->ForEachView([&views](VIEW *view) { views.push_back(view); });

    while (CSE(views)) {
      RemoveUnusedViews();
      RelabelGroupIDs();
      views.clear();
      this->ForEachView([&views](VIEW *view) { views.push_back(view); });
    }
  };

  do_cse();  // Apply CSE to all views before most canonicalization.
  OptimizationContext opt(log);
  Canonicalize(opt);
  do_cse();  // Apply CSE to all canonical views.

  do {

    // Now do a stronger form of canonicalization.
    opt.can_remove_unused_columns = true;
    opt.can_replace_inputs_with_constants = true;
    opt.bottom_up = false;
    Canonicalize(opt);

    if (ShrinkConditions()) {
      Canonicalize(opt);
    }

    RemoveUnusedViews();

  } while (EliminateDeadFlows());

  RemoveUnusedViews();
}

}  // namespace hyde
