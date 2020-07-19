// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryColumn>::~Node(void) {}

// Returns the real constant associated with this column if this column is
// a constant or constant reference. Otherwise it returns `nullptr`.
Node<QueryColumn> *Node<QueryColumn>::AsConstant(void) noexcept {
  if (referenced_constant) {
    return referenced_constant.get();
  }
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsConstant()) {
      return this;
    }
  }
  return nullptr;
}

// Returns `true` if will have a constant value at runtime.
bool Node<QueryColumn>::IsConstantRef(void) const noexcept {
  return referenced_constant;
}

// Returns `true` if this column is a constant.
bool Node<QueryColumn>::IsConstantOrConstantRef(void) const noexcept {
  if (referenced_constant) {
    return true;
  }
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsConstant()) {
      return true;
    }
  }
  return false;
}

// Returns `true` if this column is a constant.
bool Node<QueryColumn>::IsConstant(void) const noexcept {
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsConstant()) {
      assert(!referenced_constant);
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
    hash = view_hash ^ RotateRight64(
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

void Node<QueryColumn>::CopyConstant(Node<QueryColumn> *maybe_const_col) {
  if (auto const_col = maybe_const_col->AsConstant();
      const_col && !referenced_constant) {

    // We've done a kind of constant propagation, so mark the using views
    // as non-canonical.
    this->ForEachUse<VIEW>([] (VIEW *view, COL *) {
      view->is_canonical = false;
    });

    UseRef<COL> new_real_constant(const_col->CreateUse(this->view));
    referenced_constant.Swap(new_real_constant);
  }
}

void Node<QueryColumn>::ReplaceAllUsesWith(COL *that) {
//  if (that->referenced_constant) {
//    UseRef<COL> new_real_constant(that->referenced_constant->CreateUse(this->view));
//    referenced_constant.Swap(new_real_constant);
//
//  } else if (that->IsConstant()) {
//    UseRef<COL> new_real_constant(that->CreateUse(this->view));
//    referenced_constant.Swap(new_real_constant);
//
//  } else {
  if (referenced_constant && !that->IsConstantOrConstantRef()) {
    that->CopyConstant(referenced_constant.get());
  }
  this->Def<Node<QueryColumn>>::ReplaceAllUsesWith(that);
//  }
}

}  // namespace hyde
