// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

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
static VIEW *ProxySubgraphs(QueryImpl *impl, TUPLE *view,
                                  VIEW *incoming_view) {
  SUBGRAPH *proxy = impl->subgraphs.Create();
  proxy->color = incoming_view->color;
  proxy->can_receive_deletions = incoming_view->can_produce_deletions;
  proxy->can_produce_deletions = proxy->can_receive_deletions;

  auto col_index = 0u;
  for (COL *col : view->input_columns) {
    COL *const proxy_col =
        proxy->columns.Create(col->var, col->type, proxy, col->id, col_index++);
    proxy->input_columns.AddUse(col);
    proxy_col->CopyConstantFrom(col);
  }

  view->input_columns.Clear();
  for (COL *col : proxy->columns) {
    view->input_columns.AddUse(col);
  }

  view->TransferSetConditionTo(proxy);
  view->TransferTestedConditionsTo(proxy);
  return proxy;
}
}  // namespace

//
void QueryImpl::BuildSubgraphs(void) {
  for (auto view : tuples) {
    auto incoming_view = VIEW::GetIncomingView(view->input_columns);
    (void) ProxySubgraphs(this, view, incoming_view);
  }
}
}  // namespace hyde
