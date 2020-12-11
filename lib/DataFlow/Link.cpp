// Copyright 2020, Trail of Bits. All rights reserved.

#include <vector>

#include "Query.h"

namespace hyde {

// Link together views in terms of predecessors and successors.
void QueryImpl::LinkViews(void) {
  for (auto view : selects) {
    for (auto incoming_view : view->inserts) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }


  // NOTE(pag): Process these before `tuples` because it might create tuples.
  for (auto view : negations) {
    if (auto incoming_view = VIEW::GetIncomingView(view->input_columns,
                                                   view->attached_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }

    if (!view->negated_view->AsTuple()) {
      view->negated_view->is_used_by_negation = false;
      view->negated_view.Emplace(
          view, view->negated_view->GuardWithTuple(this, true));
      view->negated_view->is_used_by_negation = true;
    }
  }

  // NOTE(pag): Process these before `tuples` because it might create tuples.
  for (auto view : merges) {
    auto has_non_tuples = false;
    for (auto incoming_view : view->merged_views) {
      if (!incoming_view->AsTuple() && !incoming_view->AsDelete()) {
        has_non_tuples = true;
        break;
      }
    }

    // TODO(pag): Other parts seem to want this invariant but I don't
    //            recall why.
    if (has_non_tuples) {
      UseList<VIEW> new_merged_views(view);
      for (auto incoming_view : view->merged_views) {
        if (!incoming_view->AsTuple() && !incoming_view->AsDelete()) {
          new_merged_views.AddUse(incoming_view->GuardWithTuple(this, true));
        } else {
          new_merged_views.AddUse(incoming_view);
        }
      }
      view->merged_views.Swap(new_merged_views);
    }

    for (auto incoming_view : view->merged_views) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : tuples) {
    if (auto incoming_view = VIEW::GetIncomingView(view->input_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : kv_indices) {
    if (auto incoming_view =
            VIEW::GetIncomingView(view->input_columns, view->attached_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : joins) {
    for (auto incoming_view : view->joined_views) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : maps) {
    if (auto incoming_view =
            VIEW::GetIncomingView(view->input_columns, view->attached_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : aggregates) {
    if (auto incoming_view =
            VIEW::GetIncomingView(view->group_by_columns, view->config_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }

    if (auto incoming_view = VIEW::GetIncomingView(view->aggregated_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : compares) {
    if (auto incoming_view =
            VIEW::GetIncomingView(view->input_columns, view->attached_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : inserts) {
    if (auto incoming_view = VIEW::GetIncomingView(view->input_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : deletes) {
    if (auto incoming_view = VIEW::GetIncomingView(view->input_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  ForEachView([=](VIEW *view) {
    view->predecessors.Unique();
    view->successors.Unique();
  });
}

}  // namespace hyde
