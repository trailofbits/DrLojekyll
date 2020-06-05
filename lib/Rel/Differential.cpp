// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

// Go through and mark all views that can receive and produce differential
// updates.
void QueryImpl::TrackDifferentialUpdates(void) {
  for (auto changed = true; changed; ) {
    changed = false;
    ForEachView([&] (VIEW *view) {
      if (view->can_receive_deletions && !view->can_produce_deletions) {
        view->can_produce_deletions = true;
        changed = true;
      }

      if (!view->can_produce_deletions) {
        return;
      }

      for (auto col : view->columns) {
        col->ForEachUser([&] (VIEW *user_view) {
          if (!user_view->can_receive_deletions) {
            user_view->can_receive_deletions = true;
            changed = true;
          }
        });
      }
    });
  }
}

}  // namespace hyde
