// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

Node<QueryCondition>::~Node(void) {}

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

    // If any of the data leading into a join is conditional, then that makes
    // the join conditional, so try to move the conditions on a joined view
    // into the join itself.
    //
    // NOTE(pag): In practice, we want to lift conditions are far up the data
    //            flow itself (closer to message publications) so that not so
    //            much downstream stuff is conditional.
    for (auto join : joins) {
      for (auto joined_view : join->joined_views) {
        if (!joined_view->sets_condition &&
            (!joined_view->positive_conditions.Empty() ||
             !joined_view->negative_conditions.Empty()) &&
            joined_view->OnlyUser() == join) {

          joined_view->CopyTestedConditionsTo(join);
          joined_view->DropTestedConditions();
          changed = true;
        }
      }
    }
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
