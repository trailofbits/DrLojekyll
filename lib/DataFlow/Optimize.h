// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class ErrorLog;

class OptimizationContext {
 public:
  // Are we allowed to replace input columns, marked as constant refs, with
  // their constants?
  bool can_replace_inputs_with_constants{false};

  // Are we allowed to remove unused columns?
  //
  // NOTE(pag): If there's an input column that's used two or more times, then
  //            it is always safe to remove and we don't consult
  //            `can_remove_unused_columns`.
  bool can_remove_unused_columns{false};

  // Can we sink unions?
  bool can_sink_unions{false};

  // Should optimization happen bottom-up or top-down?
  bool bottom_up{true};
};

}  // namespace hyde
