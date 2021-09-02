// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

QueryConstantImpl::~QueryConstantImpl(void) {}

QueryConstantImpl *QueryConstantImpl::AsConstant(void) noexcept {
  return this;
}

const char *QueryConstantImpl::KindName(void) const noexcept {
  return "CONST";
}

QueryTagImpl::~QueryTagImpl(void) {}

QueryTagImpl *QueryTagImpl::AsTag(void) noexcept {
  return this;
}

const char *QueryTagImpl::KindName(void) const noexcept {
  return "TAG";
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

  for (auto view : negations) {
    const auto incoming_view =
        VIEW::GetIncomingView(view->input_columns, view->attached_columns);
    if (incoming_view) {
      continue;
    }
    ReplaceInputsWithTuple(this, view, &(view->input_columns),
                           &(view->attached_columns));
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

// Track which views are constant after initialization.
// See `VIEW::is_const_after_init`.
void QueryImpl::TrackConstAfterInit(void) const {
  std::unordered_map<VIEW *, bool> conditional_views;
  this->ForEachView([&] (VIEW *view) {
    assert(!view->is_dead);
    view->is_const_after_init = VIEW::IsConditional(view, conditional_views);
  });
}

}  // namespace hyde
