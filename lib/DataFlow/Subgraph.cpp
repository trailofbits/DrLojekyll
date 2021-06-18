// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <unordered_set>
#include <iostream>

namespace hyde {

Node<QuerySubgraph>::~Node(void) {}

Node<QuerySubgraph> *Node<QuerySubgraph>::AsSubgraph(void) noexcept {
  return this;
}

uint64_t Node<QuerySubgraph>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = HashInit();
  assert(hash != 0);

  auto local_hash = hash;

  // TODO(sonya): revisit this
  // Mix in the hashes of the input by columns; these are ordered.
  for (auto col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 33) * col->Hash();
  }

  hash = local_hash;
  return local_hash;


  hash = local_hash;
  return local_hash;

}

// TODO(sonya)
bool Node<QuerySubgraph>::Equals(EqualitySet &eq,
                                 Node<QueryView> *that) noexcept { return false; }
// TODO(sonya)
bool Node<QuerySubgraph>::Canonicalize(QueryImpl *query,
                                       const OptimizationContext &opt,
                                       const ErrorLog&) { return true; }

namespace {
static VIEW *ProxySubgraphs(QueryImpl *impl, VIEW *view, VIEW *incoming_view) {
  SUBGRAPH *proxy = impl->subgraphs.Create();
  if (incoming_view) {
    proxy->color = incoming_view->color;
    proxy->can_receive_deletions = incoming_view->can_produce_deletions;
  }
  proxy->can_produce_deletions = proxy->can_receive_deletions;

  auto col_index = 0u;
  for (COL *col : view->input_columns) {
    COL *const proxy_col =
        proxy->columns.Create(col->var, col->type, proxy, col->id, col_index++);
    proxy->input_columns.AddUse(col);
    proxy_col->CopyConstantFrom(col);
  }

  for (COL *col : view->attached_columns) {
    COL *const proxy_col =
        proxy->columns.Create(col->var, col->type, proxy, col->id, col_index++);
    proxy->input_columns.AddUse(col);
    proxy_col->CopyConstantFrom(col);
  }

  auto input_columns_size = view->input_columns.Size();
  view->input_columns.Clear();
  view->attached_columns.Clear();
  for (COL *col : proxy->columns) {
    if (input_columns_size > view->input_columns.Size()) {
      view->input_columns.AddUse(col);
    } else {
      view->attached_columns.AddUse(col);
    }
  }

  view->TransferSetConditionTo(proxy);
  view->TransferTestedConditionsTo(proxy);
  return proxy;
}
}  // namespace

// Identify sets of nodes that compose a subgraph and proxy the SubgraphSet
// with a SUBGRAPH node.
void QueryImpl::BuildSubgraphs(void) {
  std::unordered_set<VIEW *> possible_subgraphs;

  auto is_conditional = +[](QueryView view) {
    return view.SetCondition() || !view.PositiveConditions().empty() ||
           !view.NegativeConditions().empty();
  };

  ForEachView([&](QueryView view){
    //  1) find all nodes with only a single user
    //  2) make sure none of them are conditional (set a condition, or test a condition)
    //  3) make sure none of them are negations (eventually you can permit "never" negations)
        if (view.Successors().size() == 1 && !view.IsNegate()
            && !is_conditional(view)) {
          //  5) add them all to a set
          possible_subgraphs.insert(view.impl);
        }
  });

  //  6) make a new set, the candidate set, that is a copy of (5)
  std::unordered_set<VIEW *> subgraph_roots(possible_subgraphs);

  //  7) for each view in (5), if it has a single predecessor, and that
  //  predecessor that is also in (5), then remove it from (6)
  for (auto view : possible_subgraphs) {
    if (view->predecessors.Size() == 1
        && possible_subgraphs.find(view->predecessors[0])
            != possible_subgraphs.end()) {
      subgraph_roots.erase(view);
    }
  }


  //  now you have a candidate set of "roots" for your subgraphs, and you can grow them from there
  for (auto view : subgraph_roots) {
    //for (auto pred : view->predecessors) {
    if (view->predecessors.Size() == 1) {
      //auto incoming_view = VIEW::GetIncomingView(view->input_columns);
      (void) ProxySubgraphs(this, view, view->predecessors[0]);
      // (void) ProxySubgraphs(this, view, pred);
    } else if (!view->predecessors.Size()) {
      // TODO(ss): decide what to do with RECEIVEs in ProxySubgraphs
      //(void) ProxySubgraphs(this, view, NULL);
    } else {
      // TODO(ss): handle multiple predecessors in ProxySubgraphs
    }
  }
}
}  // namespace hyde
