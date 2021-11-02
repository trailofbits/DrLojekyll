// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Query.h"

#include <unordered_set>
#include <iostream>

namespace hyde {

SubgraphInfo::SubgraphInfo(Node<QueryView> *root_, unsigned id_)
    : id(id_),
      root(root_, root_),
      tree(root_) { this->tree.AddUse(root_); }

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
// Equality over Subgraphs is structral to the set of subgraph nodes
bool Node<QuerySubgraph>::Equals(EqualitySet &eq,
                                 Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  return false;
}

// TODO(sonya)
bool Node<QuerySubgraph>::Canonicalize(QueryImpl *query,
                                       const OptimizationContext &opt,
                                       const ErrorLog&) {
  return true;
}

namespace {

static SUBGRAPH *ProxySubgraphs(QueryImpl *impl, VIEW *view, VIEW *incoming_view, unsigned id) {
  SUBGRAPH *subgraph = impl->subgraphs.Create();

  subgraph->subgraph_info.reset(new SubgraphInfo(subgraph, id));

  if (incoming_view) {
    subgraph->color = incoming_view->color;
    subgraph->can_receive_deletions = incoming_view->can_produce_deletions;
  }
  subgraph->can_produce_deletions = subgraph->can_receive_deletions;

  auto col_index = 0u;
  for (COL *col : view->input_columns) {
    COL *const proxy_col =
        subgraph->columns.Create(col->var, col->type, subgraph, col->id, col_index++);
    subgraph->input_columns.AddUse(col);
    proxy_col->CopyConstantFrom(col);
  }

  for (COL *col : view->attached_columns) {
    COL *const proxy_col =
        subgraph->columns.Create(col->var, col->type, subgraph, col->id, col_index++);
    subgraph->input_columns.AddUse(col);
    proxy_col->CopyConstantFrom(col);

  }

  auto input_columns_size = view->input_columns.Size();
  view->input_columns.Clear();
  view->attached_columns.Clear();
  for (COL *col : subgraph->columns) {
    if (input_columns_size > view->input_columns.Size()) {
      view->input_columns.AddUse(col);
    } else {
      view->attached_columns.AddUse(col);
    }
  }

  view->TransferSetConditionTo(subgraph);
  view->TransferTestedConditionsTo(subgraph);
  return subgraph;
}

}  // namespace

// IdentifyGraph sets of nodes that compose a subgraph and proxy the SubgraphSet
// with a SUBGRAPH node.
void QueryImpl::BuildSubgraphs(void) {

  // Link Views for easy access of predecessor + successors
  LinkViews();

  std::unordered_set<VIEW *> possible_subgraphs;

  auto is_conditional = +[](QueryView view) {
    return view.SetCondition() || !view.PositiveConditions().empty() ||
           !view.NegativeConditions().empty();
  };

  auto is_candidate_view_type  = +[](QueryView view) {
    return view.IsMap() || view.IsTuple() || view.IsCompare();
  };

  auto can_be_subgraph = [&](QueryView view) {
    return view.Successors().size() == 1 && !view.IsNegate()
        && view.Predecessors().size() == 1
        && !is_conditional(view) && is_candidate_view_type(view);
  };


  ForEachView([&](QueryView view){
    //  1) find all nodes with only a single user
    //  2) make sure none of them are conditional (set a condition, or test a condition)
    //  3) make sure none of them are negations (eventually you can permit "never" negations)
    //  4) Make sure they're a candidate view type (TUPLE, MAP/FILTER, COMPARE)
        if (can_be_subgraph(view) &&
            can_be_subgraph(view.Successors()[0]) &&
            (!view.Successors()[0].IsTuple() ||
                can_be_subgraph(view.Successors()[0].Successors()[0]))
                      ) {
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
  auto subgraph_id = 1u;
  for (auto view : subgraph_roots) {
    assert(view->predecessors.Size() == 1);
    (void) ProxySubgraphs(this, view, view->predecessors[0], subgraph_id++);
  }

  // Build Each Subgraph Tree
  auto can_be_child = [&](QueryView view) {
    return view.Successors().size() == 1 && !view.IsNegate()
        && !is_conditional(view) && is_candidate_view_type(view);
  };

  // Re-link since we inserted nodes
  LinkViews();

  for (auto subgraph : subgraphs) {
    auto info = subgraph->subgraph_info;

    for (auto child = subgraph->successors[0];
        can_be_child(child) && (!child->AsTuple() ||
            can_be_child(child->successors[0]));
        child = child->successors[0]) {
      child->subgraph_info = info;
      info.get()->tree.AddUse(child);
    }
  }

}
}  // namespace hyde
