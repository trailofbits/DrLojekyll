// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryView>::~Node(void) {}

Node<QuerySelect> *Node<QueryView>::AsSelect(void) noexcept {
  return nullptr;
}

Node<QueryTuple> *Node<QueryView>::AsTuple(void) noexcept {
  return nullptr;
}

Node<QueryJoin> *Node<QueryView>::AsJoin(void) noexcept {
  return nullptr;
}

Node<QueryMap> *Node<QueryView>::AsMap(void) noexcept {
  return nullptr;
}

Node<QueryAggregate> *Node<QueryView>::AsAggregate(void) noexcept {
  return nullptr;
}

Node<QueryMerge> *Node<QueryView>::AsMerge(void) noexcept {
  return nullptr;
}

Node<QueryConstraint> *Node<QueryView>::AsConstraint(void) noexcept {
  return nullptr;
}

Node<QueryInsert> *Node<QueryView>::AsInsert(void) noexcept {
  return nullptr;
}

// Return a number that can be used to help sort this node. The idea here
// is that we often want to try to merge together two different instances
// of the same underlying node when we can.
uint64_t Node<QueryView>::Sort(void) noexcept {
  return Depth();
}

// Returns `true` if this view is being used. This is defined in terms of
// whether or not the view is used in a merge, or whether or not any of its
// columns are used.
bool Node<QueryView>::IsUsed(void) const noexcept {
  if (is_used || this->Def<Node<QueryView>>::IsUsed()) {
    return true;
  }

  for (auto col : columns) {
    if (col->Def<Node<QueryColumn>>::IsUsed()) {
      return true;
    }
  }

  return false;
}

// Invoked any time time that any of the columns used by this view are
// modified.
void Node<QueryView>::Update(uint64_t next_timestamp) {
  if (timestamp >= next_timestamp) {
    return;
  }

  timestamp = next_timestamp;
  hash = 0;
  depth = 0;
  is_canonical = false;

  for (auto col : columns) {
    col->ForEachUse([=] (User *user, COL *) {
      user->Update(next_timestamp);
    });
  }

  // Update merges.
  ForEachUse([=] (User *user, Node<QueryView> *) {
    user->Update(next_timestamp);
  });
}

// Check to see if the attached columns are ordered and unique. If they're
// not unique then we can deduplicate them.
bool Node<QueryView>::AttachedColumnsAreCanonical(void) const noexcept {
  if (!attached_columns.Empty()) {
    if (attached_columns[0]->IsConstant()) {
      return false;
    }
  }
  for (auto i = 1u; i < attached_columns.Size(); ++i) {
    if (attached_columns[i - 1u] >= attached_columns[i] ||
        attached_columns[i]->IsConstant()) {
      return false;
    }
  }
  return true;
}

// Put this view into a canonical form.
bool Node<QueryView>::Canonicalize(QueryImpl *) {
  return false;
}

unsigned Node<QueryView>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.
    auto real = GetDepth(input_columns, 1u);
    real = GetDepth(attached_columns, real);
    depth = real + 1u;
  }
  return depth;
}

unsigned Node<QueryView>::GetDepth(const UseList<COL> &cols, unsigned depth) {
  for (const auto input_col : cols) {
    const auto input_depth = input_col->view->Depth();
    if (input_depth >= depth) {
      depth = input_depth;
    }
  }
  return depth;
}

// Returns `true` if we had to "guard" this view with a tuple so that we
// can put it into canonical form.
//
// If this view is used by a merge then we're not allowed to re-order the
// columns. Instead, what we can do is create a tuple that will maintain
// the ordering, and the canonicalize the join order below that tuple.
bool Node<QueryView>::GuardWithTuple(QueryImpl *query) {
  if (!this->Def<Node<QueryView>>::IsUsed()) {
    return false;
  }

  const auto tuple = query->tuples.Create();
  if (check_group_ids) {
    tuple->check_group_ids = true;
  }

  // Make any merges use the tuple.
  ReplaceAllUsesWith(tuple);

  for (auto col : columns) {
    const auto out_col = tuple->columns.Create(col->var, tuple, col->id);
    col->ReplaceAllUsesWith(out_col);
  }

  for (auto col : columns) {
    tuple->input_columns.AddUse(col);
  }
  return true;
}

// Utility for comparing use lists.
bool Node<QueryView>::ColumnsEq(
    const UseList<COL> &c1s, const UseList<COL> &c2s) {
  const auto num_cols = c1s.Size();
  if (num_cols != c2s.Size()) {
    return false;
  }
  for (auto i = 0u; i < num_cols; ++i) {
    if (c1s[i] != c2s[i]) {
      return false;
    }
  }
  return true;
}

// Check that all non-constant views in `cols1` and `cols2` match.
//
// NOTE(pag): This isn't a pairwise matching; instead it checks that all
//            columns in both of the lists independently reference the same
//            view.
bool Node<QueryView>::CheckAllViewsMatch(const UseList<COL> &cols1,
                                         const UseList<COL> &cols2) {
  VIEW *prev_view = nullptr;

  auto do_cols = [&prev_view] (const auto &cols) -> bool {
    for (auto col : cols) {
      if (!col->IsConstant() && !col->IsGenerator()) {
        if (prev_view) {
          if (prev_view != col->view) {
            return false;
          }
        } else {
          prev_view = col->view;
        }
      }
    }
    return true;
  };
  return do_cols(cols1) && do_cols(cols2);
}

// Check if teh `group_ids` of two views have any overlaps.
//
// Two selects in the same logical clause are not allowed to be merged,
// except in rare cases like constant streams. For example, consider the
// following:
//
//    node_pairs(A, B) : node(A), node(B).
//
// `node_pairs` is the cross-product of `node`. The two selects associated
// with each invocation of `node` are structurally the same, but cannot
// be merged because otherwise we would not get the cross product.
//
// NOTE(pag): The `group_ids` are sorted.
bool Node<QueryView>::InsertSetsOverlap(
    Node<QueryView> *a, Node<QueryView> *b) {
  if (!a->check_group_ids && !b->check_group_ids) {
    return false;
  }

  for (auto i = 0u, j = 0u;
       i < a->group_ids.size() && j < b->group_ids.size(); ) {

    if (a->group_ids[i] == b->group_ids[j]) {
      return true;

    } else if (a->group_ids[i] < b->group_ids[j]) {
      ++i;

    } else {
      ++j;
    }
  }

  return false;
}

}  // namespace hyde