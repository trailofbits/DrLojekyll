// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

Node<QueryCondition>::~Node(void) {}

// After the query is built, we want to push down any condition annotations
// on nodes.
//
// TODO(pag): We actually want to do the opposite of this; we want to raise
//            up conditions as far as possible.
void QueryImpl::SinkConditions(void) const {
  assert(false && "Don't use this");

//  std::unordered_map<QueryView, std::vector<QueryView>> to_from;
//  std::unordered_map<QueryView, std::vector<QueryView>> from_to;
//
//  ForEachView([&](QueryView view) {
//    auto &from = from_to[view];
//    view.ForEachUser([&](QueryView user_view) { from.push_back(user_view); });
//  });
//
//  for (auto changed = true; changed;) {
//    changed = false;
//    for (const auto &[input_view, user_views] : to_from) {
//      const auto view = input_view.impl;
//
//      auto old_size =
//          view->positive_conditions.Size() + view->negative_conditions.Size();
//
//      if (user_views.size() == 1) {
//        auto user_view = user_views.front().impl;
//
//        for (auto cond : user_view->positive_conditions) {
//          view->positive_conditions.AddUse(cond);
//          cond->positive_users.AddUse(view);
//        }
//
//        for (auto cond : user_view->negative_conditions) {
//          view->negative_conditions.AddUse(cond);
//          cond->negative_users.AddUse(view);
//        }
//      }
//
//      view->OrderConditions();
//      auto new_size =
//          view->positive_conditions.Size() + view->negative_conditions.Size();
//      changed = changed || new_size > old_size;
//    }
//  }
}

// Extract conditions from regular nodes and force them to belong to only
// tuple nodes. This simplifies things substantially for downstream users.
void QueryImpl::ExtractConditionsToTuples(void) {
  for (auto changed = true; changed; ) {
    changed = false;

    ForEachView([&](VIEW *view) {
      if (view->AsInsert()) {
        return;
      } else if (auto user = view->OnlyUser();
                 user && !user->AsMerge() && !user->AsJoin()) {
        if (view->sets_condition) {
          if (user->sets_condition) {
            user = view->GuardWithTuple(this, true);
            changed = true;

          } else {
            view->TransferSetConditionTo(user);
            changed = true;
          }
        } else if (!view->positive_conditions.Empty() ||
                   !view->negative_conditions.Empty()) {
          changed = true;
        }
        view->CopyTestedConditionsTo(user);
        view->DropTestedConditions();

      } else if (view->AsTuple()) {
        return;

      } else if (!view->positive_conditions.Empty() ||
                 !view->negative_conditions.Empty()) {
        auto user = view->GuardWithTuple(this, true);
        view->CopyTestedConditionsTo(user);
        view->DropTestedConditions();
        changed = true;

      } else if (view->sets_condition) {
        view->GuardWithTuple(this, true);
        changed = true;
      }
    });
  }

#ifndef NDEBUG
  ForEachView([&](VIEW *view) {
    if (view->AsTuple() || view->AsInsert()) {
      return;
    } else {
      assert(!view->sets_condition);
      assert(view->positive_conditions.Empty());
      assert(view->negative_conditions.Empty());
    }
  });
#endif
}

}  // namespace hyde
