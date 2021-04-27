// Copyright 2020, Trail of Bits. All rights reserved.

#include <unordered_set>

#include "Query.h"

namespace hyde {

static bool IsTrivialCycle(TUPLE *tuple);

// Eliminate dead flows. This uses a taint-based approach and identifies a
// VIEW as dead if it is not derived directly or indirectly from input
// messages.
bool QueryImpl::EliminateDeadFlows(void) {

  std::unordered_set<void *> derived_from_input;
  std::vector<VIEW *> views;

  for (auto io : ios) {
    for (auto view : io->receives) {
      derived_from_input.insert(view);  // Inputs come from the outside world.
    }
  }

  for (SELECT *view : selects) {
    if (view->stream) {
      derived_from_input.insert(view);  // Inputs come from the outside world.
    }
  }

  // So that all constant inputs look like a view derived from input.
  derived_from_input.insert(nullptr);

  auto changed = true;

  auto should_check_view = [&](VIEW *view) {
    return !view->is_dead && !derived_from_input.count(view);
  };

  auto check_incoming_view = [&](VIEW *view, VIEW *incoming_view) {
    if (derived_from_input.count(incoming_view)) {
      changed = true;
      derived_from_input.insert(view);
    }
  };

  while (changed) {
    changed = false;

    for (SELECT *view : selects) {
      for (auto insert : view->inserts) {
        if (insert && !insert->is_dead && derived_from_input.count(insert)) {
          derived_from_input.insert(view);
          changed = true;
          break;
        }
      }
    }

    for (TUPLE *view : tuples) {
      if (should_check_view(view)) {
        check_incoming_view(view, VIEW::GetIncomingView(view->input_columns));
      }
    }

    for (INSERT *view : inserts) {
      if (should_check_view(view)) {
        check_incoming_view(view, VIEW::GetIncomingView(view->input_columns));
      }
    }

    for (CMP *view : compares) {
      if (should_check_view(view)) {
        check_incoming_view(
            view,
            VIEW::GetIncomingView(view->input_columns, view->attached_columns));
      }
    }

    for (MAP *view : maps) {
      if (should_check_view(view)) {
        check_incoming_view(
            view,
            VIEW::GetIncomingView(view->input_columns, view->attached_columns));
      }
    }

    for (KVINDEX *view : kv_indices) {
      if (should_check_view(view)) {
        check_incoming_view(
            view,
            VIEW::GetIncomingView(view->input_columns, view->attached_columns));
      }
    }

    for (AGG *view : aggregates) {
      if (should_check_view(view)) {
        auto iview0 = VIEW::GetIncomingView(view->aggregated_columns);
        auto iview1 =
            VIEW::GetIncomingView(view->group_by_columns, view->config_columns);

        if (iview0) {
          check_incoming_view(view, iview0);

        } else if (iview1) {
          check_incoming_view(view, iview1);

        // All constant inputs...
        } else {
          changed = true;
          derived_from_input.insert(view);
        }
      }
    }

    for (MERGE *view : merges) {
      if (should_check_view(view)) {
        for (auto merged_view : view->merged_views) {
          if (merged_view && !merged_view->is_dead &&
              derived_from_input.count(merged_view)) {
            derived_from_input.insert(view);
            changed = true;
            break;
          }
        }
      }
    }

    for (NEGATION *view : negations) {
      if (should_check_view(view)) {
        auto pred_view =
            VIEW::GetIncomingView(view->input_columns, view->attached_columns);
        if (derived_from_input.count(view->negated_view.get()) &&
            derived_from_input.count(pred_view)) {
          changed = true;
          derived_from_input.insert(view);
        }
      }
    }

    for (JOIN *view : joins) {
      if (should_check_view(view)) {
        auto all_tainted = true;
        for (auto joined_view : view->joined_views) {
          if (joined_view->is_dead || !derived_from_input.count(joined_view)) {
            all_tainted = false;
            break;
          }
        }

        if (all_tainted) {
          changed = true;
          derived_from_input.insert(view);
        }
      }
    }

    for (INSERT *view : inserts) {
      if (!view->is_dead && !VIEW::GetIncomingView(view->input_columns)) {
        derived_from_input.insert(view);  // All inputs are constants.
      }
    }
  }

  for (auto view : views) {
    if (!derived_from_input.count(view)) {
      view->PrepareToDelete();

    } else if (auto merge = view->AsMerge(); merge) {
      merge->merged_views.RemoveIf([&](VIEW *merged_view) {
        return !derived_from_input.count(merged_view);
      });
    } else if (auto tuple = view->AsTuple(); tuple && IsTrivialCycle(tuple)) {
      view->PrepareToDelete();
    }
  }

  for (auto changed = true; changed;) {
    changed = false;
    for (auto cond : conditions) {
      if (!cond->setters.Empty()) {
        continue;
      }

      // Negated uses of this (now dead) condition are fine, and so we can
      // remove the condition entirely.
      for (auto user_view : cond->negative_users) {
        if (user_view) {
          user_view->negative_conditions.RemoveIf(
              [=](COND *c) { return c == cond; });
        }
      }

      // Positive uses of the condition are unsatisfiable, and so we should
      // kill all positive users.
      for (auto user_view : cond->positive_users) {
        if (user_view && !user_view->is_dead) {
          user_view->PrepareToDelete();
          changed = true;
        }
      }

      cond->negative_users.Clear();
      cond->positive_users.Clear();
    }
  }

  return RemoveUnusedViews();
}

// Eliminate trivial cycles on unions
bool IsTrivialCycle(TUPLE *tuple) {
  if (!tuple) {
    return false;
  }

  auto incoming_view = VIEW::GetIncomingView(tuple->input_columns);

  // There is an incoming view and not all inputs are constant
  // There is only a single user view, which is the same as the incoming
  // view, meaning it's a cycle
  if (auto only_user = tuple->OnlyUser();
      only_user && incoming_view && only_user == incoming_view &&
      incoming_view->columns.Size() == tuple->columns.Size()) {
    for (auto i = 0u; i < tuple->columns.Size(); ++i) {
      auto *in_col = incoming_view->columns[i];
      auto *out_col = tuple->columns[i];
      if (in_col->Index() != out_col->Index()) {
        return false;
      }
    }

    // This TUPLE operates on a restriction of the set of nodes in the MERGE.
    // If the conditions are satisfied, then we set a separate condition, and
    // contribute back the record to the MERGE. Contributing back the data to
    // the MERGE is a no-op; however, setting the condition is not. Thus, we
    // can break the cyclic dependency between the TUPLE and the MERGE whilst
    // maintaining the TUPLE and its condition setting behavior.
    if (tuple->sets_condition && tuple->IntroducesControlDependency()) {
      if (auto merge = incoming_view->AsMerge(); merge) {
        merge->merged_views.RemoveIf([=](VIEW *v) { return v == tuple; });
      }

      return false;

    } else if (tuple->sets_condition) {
      tuple->TransferSetConditionTo(incoming_view);
      return true;

    // This TUPLE may or may not test any conditions. Any conditions tested are
    // irrelevant because they just send a subset of the MERGE's own data data
    // back into itself, which is a no-op.
    } else if (incoming_view->AsMerge()) {
      return true;
    }
  }

  return false;
}

}  // namespace hyde
