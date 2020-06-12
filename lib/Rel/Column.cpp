// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

namespace {

static bool ColumnSetCompare(COL *a, COL *b) {
  const auto a_depth = a->view->Depth();
  const auto b_depth = b->view->Depth();

  // Deeper (from inputs/streams) columns are ordered first.
  if (a_depth > b_depth) {
    return true;

  } else if (a_depth < b_depth) {
    return false;
  }

  if (a->id != b->id) {
    return a->id < b->id;

  } else if (a->var != b->var) {
    return a->var.Order() < b->var.Order();

  } else {
    return a < b;
  }
}

}  // namespace

ColumnSet *ColumnSet::Find(void) {
  if (!parent) {
    return this;
  } else {
    const auto ret = parent->Find();
    if (ret != parent.get()) {
      parent = ret->shared_from_this();  // Path compression.
    }
    return ret;
  }
}

Node<QueryColumn> *ColumnSet::Leader(void) {
  const auto col_set = Find();
  if (!col_set->is_sorted) {
    std::sort(col_set->columns.begin(), col_set->columns.end(),
              ColumnSetCompare);
    col_set->is_sorted = true;
  }

  return col_set->columns[0];
}

bool ColumnSet::Contains(Node<QueryColumn> *search_col) const {
  for (auto col : columns) {
    if (col == search_col) {
      return true;
    }
  }
  return false;
}

Node<QueryColumn>::~Node(void) {
  if (equiv_columns) {
    std::shared_ptr<ColumnSet> our_equiv_columns;
    our_equiv_columns.swap(equiv_columns);

    const auto col_set = our_equiv_columns->Find();
    auto it = std::remove_if(
        col_set->columns.begin(), col_set->columns.end(),
        [=](COL *col) {
          return col == this;
        });
    col_set->columns.erase(it);

    our_equiv_columns->parent.reset();
  }
}

// Returns `true` if this column is a constant.
bool Node<QueryColumn>::IsConstant(void) const noexcept {
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsConstant()) {
      return true;
    }
  }
  return false;
}

// Returns `true` if this column is the output from a generator.
bool Node<QueryColumn>::IsGenerator(void) const noexcept {
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsGenerator()) {
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
  return __builtin_rotateright64(view->Hash(), 3u + Index()) *
         0xff51afd7ed558ccdull;
}

void Node<QueryColumn>::ReplaceAllUsesWith(Node<QueryColumn> *that) {
  this->Def<Node<QueryColumn>>::ReplaceAllUsesWith(that);
}

Node<QueryColumn> *Node<QueryColumn>::Find(void) {
  return equiv_columns->Leader();
}

void Node<QueryColumn>::Union(Node<QueryColumn> *a, Node<QueryColumn> *b) {
  if (a == b) {
    return;
  }

  auto a_set = a->equiv_columns->Find();
  auto b_set = b->equiv_columns->Find();

  if (a_set == b_set) {
    return;
  }

  if (a_set->columns.size() > b_set->columns.size()) {
    a_set->is_sorted = false;
    a_set->columns.insert(
        a_set->columns.end(),
        b_set->columns.begin(),
        b_set->columns.end());

    const auto set = a_set->shared_from_this();

    b_set->parent = set;
    b_set->columns.clear();

    a->equiv_columns = set;
    b->equiv_columns = set;

  } else {
    b_set->is_sorted = false;
    b_set->columns.insert(
        b_set->columns.end(),
        a_set->columns.begin(),
        a_set->columns.end());

    const auto set = b_set->shared_from_this();

    a_set->parent = set;
    a_set->columns.clear();

    a->equiv_columns = set;
    b->equiv_columns = set;
  }
}

}  // namespace hyde
