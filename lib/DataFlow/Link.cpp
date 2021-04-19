// Copyright 2020, Trail of Bits. All rights reserved.

#include <vector>

#include "Query.h"

namespace hyde {
namespace {

static VIEW *ProxyInsertWithTuple(QueryImpl *impl, INSERT *view,
                                 VIEW *incoming_view) {
  TUPLE * proxy = impl->tuples.Create();
  proxy->color = incoming_view->color;
  proxy->can_receive_deletions = incoming_view->can_produce_deletions;
  proxy->can_produce_deletions = proxy->can_receive_deletions;

  auto col_index = 0u;
  for (COL *col : view->input_columns) {
    COL * const tuple_col = proxy->columns.Create(
        col->var, proxy, col->id, col_index++);
    proxy->input_columns.AddUse(col);
    tuple_col->CopyConstantFrom(col);
  }

  view->input_columns.Clear();
  for (COL *col : proxy->columns) {
    view->input_columns.AddUse(col);
  }

  view->CopyTestedConditionsTo(proxy);
  view->DropTestedConditions();
  view->TransferSetConditionTo(proxy);
  view->DropSetConditions();
  return proxy;
}

// Proxy both the predecessor and the negated view of a negation, if they
// aren't already tuples.
static void ProxyNegatedViews(QueryImpl *impl, NEGATION *view) {

  // Make sure the negated view is a tuple.
  if (!view->negated_view->AsTuple()) {
    TUPLE * tuple = impl->tuples.Create();
    tuple->color = view->negated_view->color;
    tuple->can_receive_deletions = view->negated_view->can_produce_deletions;
    tuple->can_produce_deletions = tuple->can_receive_deletions;

    auto col_index = 0u;
    for (COL *col : view->negated_view->columns) {
      COL * const tuple_col = tuple->columns.Create(
          col->var, tuple, col->id, col_index++);
      tuple->input_columns.AddUse(col);
      tuple_col->CopyConstantFrom(col);
    }
    view->negated_view.Emplace(view, tuple);
  }

  // Force negations to take tuples as their main source so that
  // we can have induction vectors for negation inputs that won't ever
  // correspond with inductive join pivot vectors.
  if (auto incoming_view = VIEW::GetIncomingView(view->input_columns);
      incoming_view && !incoming_view->AsTuple()) {

    TUPLE * tuple = impl->tuples.Create();

    tuple->can_receive_deletions = view->can_produce_deletions;
    tuple->can_produce_deletions = tuple->can_receive_deletions;
    tuple->color = incoming_view->color;

    auto col_index = 0u;
    for (auto col : view->input_columns) {
      auto tuple_col = tuple->columns.Create(
          col->var, tuple, col->id, col_index++);
      tuple_col->CopyConstantFrom(col);
    }

    for (auto col : view->attached_columns) {
      auto tuple_col = tuple->columns.Create(
          col->var, tuple, col->id, col_index++);
      tuple_col->CopyConstantFrom(col);
    }

    UseList<COL> new_in_cols(view);
    col_index = 0u;
    for (auto in_col : view->input_columns) {
      tuple->input_columns.AddUse(in_col);
      new_in_cols.AddUse(tuple->columns[col_index++]);
    }
    view->input_columns.Swap(new_in_cols);
    new_in_cols.Clear();

    for (auto in_col : view->attached_columns) {
      tuple->input_columns.AddUse(in_col);
      new_in_cols.AddUse(tuple->columns[col_index++]);
    }
    view->attached_columns.Swap(new_in_cols);
  }
}

// Proxy each joined view with a tuple. We put the tuples in the order in which
// their data is accessed by the JOINs.
static void ProxyJoinedViews(QueryImpl *impl, JOIN *join) {
  WeakUseList<VIEW> new_joined_views(join);

  std::unordered_map<COL *, COL *> col_map;

  for (auto view : join->joined_views) {

    // We don't need to proxy this; it's already a tuple.
    if (view->AsTuple()) {
      new_joined_views.AddUse(view);
      for (auto col : view->columns) {
        col_map.emplace(col, col);
      }
      continue;
    }

    const auto tuple = impl->tuples.Create();
    new_joined_views.AddUse(tuple);

    tuple->can_receive_deletions = view->can_produce_deletions;
    tuple->can_produce_deletions = tuple->can_receive_deletions;
    tuple->color = view->color;

    // Copy the columns in order, so that if the predecessor has a table,
    // then we're more likely to share that table too.
    auto col_index = 0u;
    for (auto out_col : view->columns) {
      COL *tuple_col = tuple->columns.Create(
          out_col->var, tuple, out_col->id, col_index++);
      tuple_col->CopyConstantFrom(out_col);
      tuple->input_columns.AddUse(out_col);
      col_map.emplace(out_col, tuple_col);
    }
  }

  join->joined_views.Swap(new_joined_views);

  for (auto out_col : join->columns) {
    UseList<COL> new_in_cols(join);
    auto in_cols_it = join->out_to_in.find(out_col);
    for (auto in_col : in_cols_it->second) {
      if (in_col->IsConstant()) {
        new_in_cols.AddUse(in_col);
      } else {
        new_in_cols.AddUse(col_map[in_col]);
      }
    }
    new_in_cols.Swap(in_cols_it->second);
  }
}

static void ProxyMergedViews(QueryImpl *impl, MERGE *merge) {
  UseList<VIEW> new_merged_views(merge);
  for (auto view : merge->merged_views) {

    // We don't need to proxy this; it's already a tuple.
    if (view->AsTuple()) {
      new_merged_views.AddUse(view);
      continue;
    }

    const auto tuple = impl->tuples.Create();
    new_merged_views.AddUse(tuple);

    tuple->can_receive_deletions = view->can_produce_deletions;
    tuple->can_produce_deletions = tuple->can_receive_deletions;
    tuple->color = view->color;

    // Copy the columns in order, so that if the predecessor has a table,
    // then we're more likely to share that table too.
    auto col_index = 0u;
    for (auto out_col : view->columns) {
      COL *tuple_col = tuple->columns.Create(
          out_col->var, tuple, out_col->id, col_index++);
      tuple_col->CopyConstantFrom(out_col);
      tuple->input_columns.AddUse(out_col);
    }
  }

  merge->merged_views.Swap(new_merged_views);
}

}  // namespace

// Ensure that every INSERT view is preceded by a TUPLE. This makes a bunch
// of things easier downstream in the control-flow IR generation, because
// then the input column indices of an insert line up perfectly with the
// SELECTs and such.
void QueryImpl::ProxyInsertsWithTuples(void) {
  for (auto view : inserts) {
    auto incoming_view = VIEW::GetIncomingView(view->input_columns);
    (void) ProxyInsertWithTuple(this, view, incoming_view);
  }
}

// Link together views in terms of predecessors and successors.
void QueryImpl::LinkViews(bool recursive) {

  const_cast<const QueryImpl *>(this)->ForEachView([&] (VIEW *view) {
    view->successors.Clear();
    view->predecessors.Clear();
    view->is_used_by_merge = false;
    view->is_used_by_negation = false;
    view->is_used_by_join = false;
  });

  // NOTE(pag): Process these before `tuples` because it might create tuples.
  for (auto view : negations) {
    assert(!view->is_dead);
    ProxyNegatedViews(this, view);
  }

  // Force every input to a JOIN to be a TUPLE, so that we can't have JOIN0
  // be an input to JOIN1, and where JOIN0 and JOIN1 are both inductive, and
  // where we want to have an induction pivot vector for JOIN0, but also an
  // induction predecessor (for removals) for JOIN1 representing all columns
  // of JOIN0.
  for (auto view : joins) {
    assert(!view->is_dead);
    ProxyJoinedViews(this, view);
  }

  // Similarish reasons for proxying MERGEs.
  for (auto view : merges) {
    assert(!view->is_dead);
    ProxyMergedViews(this, view);
  }

  // Ensure that every INSERT view is preceded by a TUPLE. This makes a bunch
  // of things easier downstream in the control-flow IR generation, because
  // then the input column indices of an insert line up perfectly with the
  // SELECTs and such.
  //
  // NOTE(pag): Process these before `tuples` because it might create tuples.
  for (auto view : inserts) {
    assert(!view->is_dead);
    assert(view->columns.Empty());
    if (auto incoming_view = VIEW::GetIncomingView(view->input_columns);
        incoming_view && !incoming_view->AsTuple()) {
      ProxyInsertWithTuple(this, view, incoming_view);
    }
  }

  for (auto view : negations) {
    assert(!view->is_dead);
    if (auto incoming_view = VIEW::GetIncomingView(view->input_columns,
                                                   view->attached_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
    view->negated_view->is_used_by_negation = true;
  }

  for (auto view : merges) {
    assert(!view->is_dead);
    assert(view->input_columns.Empty());
    assert(view->attached_columns.Empty());

    for (auto incoming_view : view->merged_views) {
      incoming_view->is_used_by_merge = true;
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : selects) {
    assert(!view->is_dead);
    assert(view->input_columns.Empty());
    assert(view->attached_columns.Empty());

    for (auto incoming_view : view->inserts) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : tuples) {
    assert(!view->is_dead);
    assert(view->attached_columns.Empty());

    if (auto incoming_view = VIEW::GetIncomingView(view->input_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : kv_indices) {
    assert(!view->is_dead);
    if (auto incoming_view =
            VIEW::GetIncomingView(view->input_columns, view->attached_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : joins) {
    assert(!view->is_dead);
    for (auto incoming_view : view->joined_views) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
      incoming_view->is_used_by_join = true;
    }
  }

  for (auto view : maps) {
    assert(!view->is_dead);
    if (auto incoming_view =
            VIEW::GetIncomingView(view->input_columns, view->attached_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : aggregates) {
    assert(!view->is_dead);
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
    assert(!view->is_dead);
    if (auto incoming_view =
            VIEW::GetIncomingView(view->input_columns, view->attached_columns);
        incoming_view) {
      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
    }
  }

  for (auto view : inserts) {
    assert(!view->is_dead);
    assert(view->columns.Empty());
    if (auto incoming_view = VIEW::GetIncomingView(view->input_columns);
        incoming_view) {
      assert(incoming_view->AsTuple() != nullptr);
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
