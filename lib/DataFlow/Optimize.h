// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class ErrorLog;

class OptimizationContext {
 public:
  explicit OptimizationContext(const ErrorLog &log_)
      : log(log_) {}

  const ErrorLog &log;

  // Are we allowed to replace output columns with constants when the inputs
  // are "true" constants, and not constant refs?
  bool can_replace_outputs_with_constants{false};

  // Are we allowed to remove unused columns?
  //
  // NOTE(pag): If there's an input column that's used two or more times, then
  //            it is always safe to remove and we don't consult
  //            `can_remove_unused_columns`.
  bool can_remove_unused_columns{false};

 private:
  OptimizationContext(void) = delete;
};

}  // namespace hyde
