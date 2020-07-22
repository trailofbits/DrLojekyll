// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <unordered_set>

namespace hyde {

bool IsTrivialCycle(TUPLE *tuple);

// Eliminate dead flows. This uses a taint-based approach and identifies a
// VIEW as dead if it is not derived directly or indirectly from input
// messages.
bool QueryImpl::EliminateDeadFlows(void) {
  auto any_changed = false;
  auto outer_changed = true;
  auto kill_view = [&] (VIEW *view) {
    if (view->PrepareToDelete()) {
      outer_changed = true;
      any_changed = true;
    }
  };

  std::unordered_set<void *> derived_from_input;

  for (; outer_changed; ) {
    outer_changed = false;

    derived_from_input.clear();

    for (SELECT *view : selects) {
      if (view->stream) {
        derived_from_input.insert(view);  // Inputs come from the outside world.
      }
    }

    for (TUPLE *view : tuples) {
      if (!view->is_dead &&
          !VIEW::GetIncomingView(view->input_columns)) {
        derived_from_input.insert(view);  // All inputs are constants.
      }
    }

    for (CMP *view : constraints) {
      if (!view->is_dead &&
          !VIEW::GetIncomingView(view->input_columns, view->attached_columns)) {
        derived_from_input.insert(view);  // All inputs are constants.
      }
    }

    for (MAP *view : maps) {
      if (!view->is_dead &&
          !VIEW::GetIncomingView(view->input_columns, view->attached_columns)) {
        derived_from_input.insert(view);  // All inputs are constants.
      }
    }

    for (AGG *view : aggregates) {
      if (!view->is_dead &&
          !VIEW::GetIncomingView(view->aggregated_columns) &&
          !VIEW::GetIncomingView(view->group_by_columns, view->config_columns)) {
        derived_from_input.insert(view);  // All inputs are constants.
      }
    }

    for (auto changed = true; changed; ) {
      changed = false;
      ForEachViewInDepthOrder([&] (VIEW *view) {
        if (view->is_dead) {
          return;
        }
        if (derived_from_input.count(view)) {

          // Push to users of the column. The one condition is that we don't
          // push to JOINs.
          for (auto col : view->columns) {
            col->ForEachUser([&] (VIEW *user_view) {
              if (!user_view->is_dead &&
                  !derived_from_input.count(user_view) &&
                  !user_view->AsJoin()) {
                changed = true;
                derived_from_input.insert(user_view);
              }
            });
          }

        // Pull from the inserts.
        } else if (auto select = view->AsSelect(); select) {
          for (auto insert : select->inserts) {
            if (insert &&
                !insert->is_dead &&
                derived_from_input.count(insert)) {
              derived_from_input.insert(select);
              changed = true;
              break;
            }
          }

        // Pull from the merged views.
        } else if (auto merge = view->AsMerge(); merge) {
          for (auto merged_view : merge->merged_views) {
            if (merged_view &&
                !merged_view->is_dead &&
                derived_from_input.count(merged_view)) {
              derived_from_input.insert(merge);
              changed = true;
              break;
            }
          }

        // Require that all joined views be tainted in order for the join to
        // be tainted.
        } else if (auto join = view->AsJoin(); join) {
          auto all_tainted = true;
          for (auto joined_view : join->joined_views) {
            if (joined_view->is_dead || !derived_from_input.count(joined_view)) {
              all_tainted = false;
              break;
            }
          }

          if (all_tainted) {
            changed = true;
            derived_from_input.insert(join);
          }
        }
      });
    }

    ForEachView([&](VIEW *view) {
      if (!derived_from_input.count(view)) {
        kill_view(view);

      } else if (auto merge = view->AsMerge(); merge) {
        merge->merged_views.RemoveIf([&] (VIEW *merged_view) {
          return !derived_from_input.count(merged_view);
        });
      } else if (auto tuple = view->AsTuple(); tuple && IsTrivialCycle(tuple)) {
        kill_view(tuple);
      }
    });

    for (auto changed = true; changed; ) {
      changed = false;
      for (auto cond : conditions) {
        if (!cond->setters.Empty()) {
          continue;
        }

        // Negated uses of this (now dead) condition are fine, and so we can remove
        // the condition entirely.
        for (auto user_view : cond->negative_users) {
          if (user_view) {
            user_view->negative_conditions.RemoveIf([=] (COND *c) {
              return c == cond;
            });
          }
        }

        // Positive uses of the condition are unsatisfiable, and so we should kill
        // all positive users.
        for (auto user_view : cond->positive_users) {
          if (user_view && !user_view->is_dead) {
            kill_view(user_view);
            changed = true;
          }
        }

        cond->negative_users.Clear();
        cond->positive_users.Clear();
      }
    }
  }

  // Unlink any untested conditions from their setters.
  auto removed_conds = false;
  for (auto cond : conditions) {
    if (cond->positive_users.Empty() && cond->negative_users.Empty()) {
      for (auto setter : cond->setters) {
        if (setter) {
          removed_conds = true;
          WeakUseRef<COND>().Swap(setter->sets_condition);
        }
      }
      cond->setters.Clear();
    }
  }

  if (any_changed || removed_conds) {
    conditions.RemoveIf([] (COND *cond) {
      return cond->positive_users.Empty() && cond->negative_users.Empty();
    });
    RemoveUnusedViews();
    return true;

  } else {
    return false;
  }
}

// Eliminate trivial cycles on unions
bool IsTrivialCycle(TUPLE *tuple) {
  if (!tuple) {
    return false;
  }

  auto tests_condition = !tuple->positive_conditions.Empty() ||
                         !tuple->negative_conditions.Empty();

  // Keep the tuple around because it's contains important information about
  // conditionals
  if (tuple->sets_condition && tests_condition) {
    return false;
  }

  auto incoming_view = VIEW::GetIncomingView(tuple->input_columns);
  auto max_i = tuple->columns.Size();

  if (auto onlyUser = tuple->OnlyUser();
      // There is an incoming view and not all inputs are constant
      incoming_view &&
      // There is only a single user view, which is the same as the incoming
      // view, meaning it's a cycle
      onlyUser && onlyUser == incoming_view &&
      incoming_view->columns.Size() == tuple->columns.Size()) {
    bool ordered_cols = true;
    for (auto i = 0u; i < max_i; ++i) {
      auto *in_col = incoming_view->columns[i];
      auto *out_col = tuple->columns[i];
      if (in_col->Index() != out_col->Index()) {
        ordered_cols = false;
        break;
      }
    }

    if (ordered_cols) {
      // Handle forwarding of conditional properties before deletion
      if (tuple->sets_condition) {
        tuple->Node<QueryView>::TransferSetConditionTo(incoming_view);
      } else if (tests_condition) {
        tuple->Node<QueryView>::CopyTestedConditionsTo(incoming_view);
      }
      return true;
    }
  }
  
  return false;
}

}  // namespace hyde
