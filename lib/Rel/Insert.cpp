// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {

Node<QueryInsert>::~Node(void) {}

Node<QueryInsert> *Node<QueryInsert>::AsInsert(void) noexcept {
  return this;
}

uint64_t Node<QueryInsert>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = decl.Id();

  // Mix in the hashes of the input by columns; these are ordered.
  for (auto col : input_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ col->Hash();
  }

  hash <<= 4;
  hash |= 7;

  return hash;
}

bool Node<QueryInsert>::Canonicalize(QueryImpl *) {
  assert(CheckAllViewsMatch(input_columns, attached_columns));
  return false;
}

// Equality over inserts is structural.
bool Node<QueryInsert>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsInsert();
  return that &&
         decl.Id() == that->decl.Id() &&
         columns.Size() == that->columns.Size() &&
         ColumnsEq(input_columns, that->input_columns) &&
         is_used == that->is_used;
}

}  // namespace hyde
