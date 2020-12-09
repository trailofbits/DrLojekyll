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

  for (auto view : merges) {
    for (auto incoming_view : view->merged_views) {
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
