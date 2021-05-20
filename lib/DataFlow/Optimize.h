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

  // Can we sink unions through tuples? This is basically always worth it and
  // composes nicely.
  bool can_sink_unions_through_tuples{true};

  // Can we sink unions through functor applications? This is basically always
  // worth it, as it enables better merging of nodes downstream, and composes
  // well with other optimizations as it doesn't rely on tag columns.
  bool can_sink_unions_through_maps{true};

  // Can we sink unions through negations?
  bool can_sink_unions_through_negations{true};

  // If we can sink them, then can we do it through JOINs? This generally
  // results in worse performance, and if there's a kind of tower of JOINs then
  // it prevents further sinking due to the introduction of the tagged tuples.
  bool can_sink_unions_through_joins{false};

  // Should optimization happen bottom-up or top-down?
  bool bottom_up{true};
};

}  // namespace hyde
