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
    view->depth = 0;
    view->hash = 0;
    view->is_canonical = false;
    view->group_ids.clear();
    if (view->AsJoin() || view->AsAggregate()) {
      view->group_ids.push_back(i++);

    } else {
      for (auto col : view->columns) {
        sorted_cols.push_back(col);
      }
    }
  });

  query->ForEachView([&] (VIEW *view) {
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
  auto ret = query->selects.RemoveUnused() |
             query->tuples.RemoveUnused() |
             query->joins.RemoveUnused() |
             query->maps.RemoveUnused() |
             query->aggregates.RemoveUnused() |
             query->merges.RemoveUnused() |
             query->constraints.RemoveUnused() |
             query->inserts.RemoveUnused();
  return 0 != ret;
}

// Perform common subexpression elimination, which will first identify
// candidate subexpressions for possible elimination using hashing, and
// then will perform recursive equality checks.
static bool CSE(QueryImpl *query, unsigned max_depth) {

  using CandidateList = std::vector<VIEW *>;
  using CandidateLists = std::unordered_map<uint64_t, CandidateList>;

  auto apply_list = [=] (EqualitySet &eq, CandidateList &list,
                         CandidateList &ipr) -> bool {
    ipr.clear();
    std::sort(list.begin(), list.end(), [] (VIEW *a, VIEW *b) {
      return a->Sort() < b->Sort();
    });
    while (!list.empty()) {
      auto v1 = list.front();
      for (auto j = 1u; j < list.size(); ++j) {
        auto v2 = list[j];
        eq.Clear();
        if (QueryView(v1).ReplaceAllUsesWith(eq, QueryView(v2))) {
          v1 = v2;
        } else {
          ipr.push_back(v2);
        }
      }
      list.swap(ipr);
      ipr.clear();
    }
    return false;
  };

  auto changed = false;

  std::vector<VIEW *> in_progress;
  std::vector<VIEW *> ordered_views;
  EqualitySet equalities;
  CandidateLists candidates;

  // Repeatedly canonicalize until no more changes to the number of views
  // or number of columns.
  for (auto c = 0u; c < max_depth; ++c) {
    ordered_views.clear();

    query->ForEachView([&](VIEW *view) {
      if (view->IsUsed()) {
        ordered_views.push_back(view);
        view->is_canonical = false;
      }
    });

    // Sort the views so that we process the ones closer to the inputs
    // before the ones that are close to the outputs (inserts).
    std::sort(ordered_views.begin(), ordered_views.end(),
              [](VIEW *a, VIEW *b) {
                return a->Depth() < b->Depth();
              });

    bool made_progress = false;
    for (auto view : ordered_views) {
      if (view->Canonicalize(query)) {
        RelabelGroupIDs(query);
        made_progress = true;
      }
    }

    if (!made_progress) {
      break;
    }
  }

  RelabelGroupIDs(query);

  for (auto view : ordered_views) {
    candidates[view->Hash()].push_back(view);
  }

  // Apply CSE in reverse postorder.
  for (auto view : ordered_views) {
    if (!view->IsUsed()) {
      continue;  // We've replaced it.
    }

    auto &eq_views = candidates[view->Hash()];
    if (1 >= eq_views.size()) {
      continue;  // Doesn't look structurally equivalent to anything.
    }

    in_progress.clear();
    if (apply_list(equalities, eq_views, in_progress)) {
      changed = true;
    }
  }

  return changed;
}

}  // namespace

void QueryImpl::Optimize(void) {
  RelabelGroupIDs(this);

  ForEachView([=] (Node<QueryView> *view) {
    if (view->Canonicalize(this)) {
      RelabelGroupIDs(this);
    }
  });

  if (RemoveUnusedViews(this)) {
    RelabelGroupIDs(this);
  }

  auto max_depth = 2u;
  for (auto insert : inserts) {
    max_depth = std::max(max_depth, insert->Depth());
  }

  for (auto i = 0; i < max_depth; ++i) {
    if (CSE(this, max_depth)) {
      i = 0;
    }
    if (RemoveUnusedViews(this)) {
      RelabelGroupIDs(this);
      i = 0;
    }
  }
}

}  // namespace hyde
