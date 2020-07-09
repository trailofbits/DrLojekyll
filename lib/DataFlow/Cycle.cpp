// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <unordered_set>

#include <drlojekyll/Parse/ErrorLog.h>

namespace hyde {

void QueryImpl::BreakCycles(const ErrorLog &log) {
  (void) log;

  std::unordered_set<VIEW *> derived_from_input;

  for (SELECT *view : selects) {
    if (view->stream) {
      derived_from_input.insert(view);  // Inputs come from the outside world.
    }
  }

  for (TUPLE *view : tuples) {
    if (!VIEW::GetIncomingView(view->input_columns)) {
      derived_from_input.insert(view);  // All inputs are constants.
    }
  }

  ForEachViewInDepthOrder([] (VIEW *view) {

  });
}

}  // namespace hyde
