// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryColumn>::~Node(void) {}

// Returns `true` if this column is a constant.
bool Node<QueryColumn>::IsConstant(void) const noexcept {
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsConstant()) {
      return true;
    }
  }
  return false;
}

// Returns `true` if this view is being used.
//
// NOTE(pag): Even if the column doesn't look used, it might be used indirectly
//            via a merge, and thus we want to capture this.
bool Node<QueryColumn>::IsUsed(void) const noexcept {
  if (this->Def<Node<QueryColumn>>::IsUsed()) {
    return true;
  }

  return view->Def<Node<QueryView>>::IsUsed();
}

// Return the index of this column inside of its view.
unsigned Node<QueryColumn>::Index(void) noexcept {
  if (index >= view->columns.Size() ||
      this != view->columns[index]) {
    auto i = 0u;
    for (auto col : view->columns) {
      if (col == this) {
        index = i;
        break;
      }
      ++i;
    }
  }
  return index;
}

uint64_t Node<QueryColumn>::Hash(void) noexcept {
  if (!hash) {
    const auto view_hash = view->Hash();
    hash = view_hash ^ __builtin_rotateright64(
        view_hash * 0xff51afd7ed558ccdull,
        (Index() + static_cast<unsigned>(type.Kind()) + 33u) % 64u);
  }
  return hash;
}

// Return a number that can be used to help sort this node. The idea here
// is that we often want to try to merge together two different instances
// of the same underlying node when we can.
uint64_t Node<QueryColumn>::Sort(void) noexcept {
  return Hash();
}

void Node<QueryColumn>::ReplaceAllUsesWith(Node<QueryColumn> *that) {
  this->Def<Node<QueryColumn>>::ReplaceAllUsesWith(that);
}

}  // namespace hyde
