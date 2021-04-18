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
void QueryImpl::LinkViews(void) {

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

    if (!view->negated_view->AsTuple()) {
      TUPLE * tuple = tuples.Create();
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
    view->negated_view->is_used_by_negation = true;
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
        incoming_view) {

      if (!incoming_view->AsTuple()) {
        incoming_view = ProxyInsertWithTuple(this, view, incoming_view);
      }

      view->predecessors.AddUse(incoming_view);
      incoming_view->successors.AddUse(view);
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
  }

  // NOTE(pag): Process these before `tuples` because it might create tuples.
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
