// Copyright 2020, Trail of Bits. All rights reserved.

#include <algorithm>
#include <unordered_map>

#include "Query.h"

namespace hyde {

Node<QueryCondition>::~Node(void) {}

// After the query is built, we want to push down any condition annotations
// on nodes.
void QueryImpl::SinkConditions(void) const {
  std::unordered_map<QueryView, std::vector<QueryView>> to_from;
  std::unordered_map<QueryView, std::vector<QueryView>> from_to;

  ForEachView([&](QueryView view) {
    auto &from = from_to[view];
    view.ForEachUser([&](QueryView user_view) { from.push_back(user_view); });
  });

  for (auto changed = true; changed;) {
    changed = false;
    for (const auto &[input_view, user_views] : to_from) {
      const auto view = input_view.impl;

      auto old_size =
          view->positive_conditions.Size() + view->negative_conditions.Size();

      if (user_views.size() == 1) {
        auto user_view = user_views.front().impl;

        for (auto cond : user_view->positive_conditions) {
          view->positive_conditions.AddUse(cond);
          cond->positive_users.AddUse(view);
        }

        for (auto cond : user_view->negative_conditions) {
          view->negative_conditions.AddUse(cond);
          cond->negative_users.AddUse(view);
        }
      }

      view->OrderConditions();
      auto new_size =
          view->positive_conditions.Size() + view->negative_conditions.Size();
      changed = changed || new_size > old_size;
    }
  }
}

}  // namespace hyde
