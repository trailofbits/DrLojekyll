// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Query.h"

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
  hash = HashInit() ^ declaration.Id();

  auto local_hash = hash;

  // Mix in the hashes of the input by columns; these are ordered.
  for (auto col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 33) * col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

bool Node<QueryInsert>::Canonicalize(QueryImpl *, const OptimizationContext &) {
  is_canonical = true;
  if (valid == VIEW::kValid && !CheckIncomingViewsMatch(input_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
  }
  assert(attached_columns.Empty());
  return false;
}

// Equality over inserts is structural.
bool Node<QueryInsert>::Equals(EqualitySet &eq, VIEW *that_) noexcept {

  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsInsert();
  if (!that || is_insert != that->is_insert ||
      can_produce_deletions != that->can_produce_deletions ||
      declaration.Id() != that->declaration.Id() ||
      columns.Size() != that->columns.Size() ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions) {
    return false;
  }

  eq.Insert(this, that);
  if (!ColumnsEq(eq, input_columns, that->input_columns)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

}  // namespace hyde
