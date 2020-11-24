// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

Node<QueryConstant>::~Node(void) {}

Node<QueryConstant> *Node<QueryConstant>::AsConstant(void) noexcept {
  return this;
}

const char *Node<QueryConstant>::KindName(void) const noexcept {
  return "CONST";
}

// Convert all views having constant inputs to depend upon tuple nodes, so
// that we have the invariant that the only type of view that can take all
// constants is a tuple. This simplifies lots of stuff later.
void QueryImpl::ConvertConstantInputsToTuples(void) {
  for (auto view : inserts) {
    const auto incoming_view = VIEW::GetIncomingView(view->input_columns);
    if (incoming_view) {
      continue;
    }
    ReplaceInputsWithTuple(this, view, &(view->input_columns));
  }

  for (auto view : compares) {
    const auto incoming_view =
        VIEW::GetIncomingView(view->input_columns, view->attached_columns);
    if (incoming_view) {
      continue;
    }
    ReplaceInputsWithTuple(this, view, &(view->input_columns),
                           &(view->attached_columns));
  }

  for (auto view : deletes) {
    const auto incoming_view = VIEW::GetIncomingView(view->input_columns);
    if (incoming_view) {
      continue;
    }
    ReplaceInputsWithTuple(this, view, &(view->input_columns));
  }

  for (auto view : maps) {
    const auto incoming_view =
        VIEW::GetIncomingView(view->input_columns, view->attached_columns);
    if (incoming_view) {
      continue;
    }
    ReplaceInputsWithTuple(this, view, &(view->input_columns),
                           &(view->attached_columns));
  }

  for (auto view : aggregates) {
    const auto incoming_view0 =
        VIEW::GetIncomingView(view->input_columns, view->config_columns);
    const auto incoming_view1 =
        VIEW::GetIncomingView(view->group_by_columns, view->aggregated_columns);
    if (incoming_view0 || incoming_view1) {
      continue;
    }
    ReplaceInputsWithTuple(this, view, &(view->input_columns),
                           &(view->attached_columns), &(view->group_by_columns),
                           &(view->config_columns),
                           &(view->aggregated_columns));
  }
}

}  // namespace hyde
