// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {
namespace {

// Relabel group IDs. This enables us to better optimize SELECTs. Our initial
// assignment of `group_id`s works well enough to start with, but isn't good
// enough to help us merge some SELECTs. The key idea is that if a given
// INSERT reaches two SELECTs, then those SELECTs cannot be merged.
static void RelabelGroupIDs(QueryImpl *query) {

  // Clear out all `group_id` sets, and reset the depth counters.
  std::vector<COL *> sorted_cols;

  unsigned i = 0u;
  query->ForEachView([&] (VIEW *view) {
    if (view->is_dead) {
      return;
    }

    view->depth = 0;
    view->hash = 0;

    view->group_ids.clear();
    if (view->AsJoin() || view->AsAggregate() || view->AsKVIndex()) {
      view->group_ids.push_back(i++);

    } else {
      for (auto col : view->columns) {
        sorted_cols.push_back(col);
      }
    }
  });

  query->ForEachView([&] (VIEW *view) {
    if (view->is_dead) {
      return;
    }

    (void) view->Depth();  // Calculate view depth.
  });

  // Sort it so that we process deeper views (closer to INSERTs) first.
  std::sort(
      sorted_cols.begin(), sorted_cols.end(),
      [] (COL *a, COL *b) {
        return a->view->Depth() > b->view->Depth();
      });

  // Propagate the group IDs down through the graph.
  for (i = 0; i < 2; ++i) {
    for (auto col : sorted_cols) {
      const auto view = col->view;

      // Look at the users of this column, e.g. joins, aggregates, tuples,
      // and copy their view's group ids back to this view.
      col->ForEachUser([=] (VIEW *user) {
        view->group_ids.insert(
            view->group_ids.end(),
            user->group_ids.begin(),
            user->group_ids.end());
      });

      std::sort(view->group_ids.begin(), view->group_ids.end());
      auto it = std::unique(view->group_ids.begin(), view->group_ids.end());
      view->group_ids.erase(it, view->group_ids.end());
    }
  }
}

// Remove unused views.
static bool RemoveUnusedViews(QueryImpl *query) {
  size_t ret = 0;
  size_t all_ret = 0;
  do {
    ret = query->selects.RemoveUnused() |
          query->tuples.RemoveUnused() |
          query->kv_indices.RemoveUnused() |
          query->joins.RemoveUnused() |
          query->maps.RemoveUnused() |
          query->aggregates.RemoveUnused() |
          query->merges.RemoveUnused() |
          query->constraints.RemoveUnused() |
          query->inserts.RemoveUnused();
    all_ret |= ret;
  } while (ret);

  return 0 != all_ret;
}

using CandidateList = std::vector<VIEW *>;
using CandidateLists = std::unordered_map<uint64_t, CandidateList>;

// Perform common subexpression elimination, which will first identify
// candidate subexpressions for possible elimination using hashing, and
// then will perform recursive equality checks.
static bool CSE(CandidateList &all_views) {
  EqualitySet eq;
  CandidateLists candidate_groups;

  for (auto view : all_views) {
    candidate_groups[view->HashInit()].push_back(view);
  }

  auto changed = false;

  std::vector<std::pair<VIEW *, VIEW *>> to_replace;
  std::unordered_map<VIEW *, VIEW *> top_map;
  auto resolve = [&] (VIEW *a) {
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

        eq.Clear();
        if (v1->Equals(eq, v2)) {
          to_replace.emplace_back(v1, v2);
          top_map.emplace(v1, v2);
        }
      }
    }

    std::sort(to_replace.begin(), to_replace.end(),
              [] (std::pair<VIEW *, VIEW *> a, std::pair<VIEW *, VIEW *> b) {
      return std::max(a.first->Depth(), a.second->Depth()) <
             std::max(b.first->Depth(), b.second->Depth());
    });

    while (!to_replace.empty()) {
      auto [v1, v2] = to_replace.back();
      to_replace.pop_back();
      v2 = resolve(v2);

      eq.Clear();
      if (v1 != v2 &&
          v1->IsUsed() &&
          v2->IsUsed() &&
          QueryView(v1).ReplaceAllUsesWith(eq, QueryView(v2))) {
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

void QueryImpl::Simplify(void) {
  CandidateList views;

  // Start by applying CSE to the SELECTs only. This will improve
  // canonicalization of the initial TUPLEs and other things.
  FillViews(selects, views);
  CSE(views);

  views.clear();

  // Now canonicalize JOINs, which will eliminate columns of useless joins.
  for (auto join : joins) {
    join->Canonicalize(this);
  }

  // Some of those useless JOINs are converted into TUPLEs, so canonicalize
  // those.
  for (auto tuple : tuples) {
    tuple->Canonicalize(this);
  }

  RemoveUnusedViews(this);
  RelabelGroupIDs(this);
}


void QueryImpl::Optimize(void) {
  CandidateList views;

  auto do_cse = [&] (void) {
    views.clear();
    this->ForEachView([&views] (VIEW *view) {
      views.push_back(view);
    });

    while (CSE(views)) {
      RemoveUnusedViews(this);
      RelabelGroupIDs(this);
      views.clear();
      this->ForEachView([&views] (VIEW *view) {
        views.push_back(view);
      });
    }
  };

  RemoveUnusedViews(this);
  ForEachView([] (VIEW *view) {
    view->is_canonical = false;
    view->depth = 0;
    view->hash = 0;
  });

  auto max_depth = 2u;
  for (auto insert : inserts) {
    max_depth = std::max(max_depth, insert->Depth());
  }

  // Apply CSE to all views.
  do_cse();

  this->ForEachView([&] (VIEW *view) {
    view->is_canonical = false;
  });

  // Canonicalize all views.
  for (auto non_local_changes = true; non_local_changes; ) {
    non_local_changes = false;
    this->ForEachView([&] (VIEW *view) {
      non_local_changes = view->Canonicalize(this) || non_local_changes;
    });
  }

  RemoveUnusedViews(this);
  RelabelGroupIDs(this);

  // Apply CSE to all canonical views.
  do_cse();


//  ForEachView([=, &has_error] (VIEW *view) {
//    if (has_error) {
//      return;
//    }
//
//    if (view->Canonicalize(this)) {
//      RelabelGroupIDs(this);
//    }
//
//    if (view->valid != VIEW::kValid) {
//      has_error = true;
//      return;
//    }
//  });
//
//  if (has_error) {
//    assert(false);
//    return;
//  }

}

}  // namespace hyde
