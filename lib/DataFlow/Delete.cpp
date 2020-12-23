// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Optimize.h"
#include "Query.h"

namespace hyde {

Node<QueryDelete>::~Node(void) {}

Node<QueryDelete> *Node<QueryDelete>::AsDelete(void) noexcept {
  return this;
}

uint64_t Node<QueryDelete>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = HashInit();
  assert(hash != 0);

  auto local_hash = hash;

  // Mix in the hashes of the input by columns; these are ordered.
  for (auto col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 33) * col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

bool Node<QueryDelete>::Canonicalize(
    QueryImpl *query, const OptimizationContext &opt, const ErrorLog &) {

  if (is_locked || is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  is_canonical = true;
  if (valid == VIEW::kValid && !CheckIncomingViewsMatch(input_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
  }

  assert(attached_columns.Empty());

  // NOTE(pag): This may update `is_canonical`.
  const auto incoming_view = PullDataFromBeyondTrivialTuples(
      GetIncomingView(input_columns), input_columns, attached_columns);

  const auto num_cols = columns.Size();
  is_canonical = true;  // Updated by `CanonicalizeColumn`.
  in_to_out.clear();  // Filled in by `CanonicalizeColumn`.
  Discoveries has = {};

  auto i = 0u;
  for (; i < num_cols; ++i) {

    // NOTE(pag): We treat all tuple columns as `is_attached=true` so that
    //            finding unused outputs changes `is_canonical`.
    has = CanonicalizeColumn(opt, input_columns[i], columns[i], true, has);
  }

  // Nothing changed.
  if (is_canonical) {
    return has.non_local_changes;
  }

  DefList<COL> new_columns(this);
  UseList<COL> new_input_columns(this);

  for (i = 0; i < num_cols; ++i) {
    const auto old_col = columns[i];
    if (old_col->IsUsed()) {
      const auto new_col = new_columns.Create(
          old_col->var, this, old_col->id, i);
      old_col->ReplaceAllUsesWith(new_col);
      new_input_columns.AddUse(input_columns[i]->TryResolveToConstant());
    } else {
      has.non_local_changes = true;
    }
  }

  // We dropped a reference to our predecessor; maintain it via a condition.
  if (incoming_view) {
    const auto new_incoming_view = GetIncomingView(new_input_columns);
    if (incoming_view != new_incoming_view) {
      CreateDependencyOnView(query, incoming_view);
      has.non_local_changes = true;
    }
  }

  columns.Swap(new_columns);
  input_columns.Swap(new_input_columns);

  // Restore the old columns.
  if (columns.Empty()) {
    columns.Swap(new_columns);
    input_columns.Swap(new_input_columns);
    is_locked = true;
  }

  hash = 0;
  is_canonical = true;
  return has.non_local_changes;

//  // It looks like at least one column is unused; go drop it.
//  const auto introduces_control_dep = IntroducesControlDependency();
//  if (opt.can_remove_unused_columns && !AllColumnsAreUsed() &&
//      !introduces_control_dep && !sets_condition) {
//
//    DefList<COL> new_columns;
//    UseList<COL> new_input_columns(this);
//
//    auto i = 0u;
//    for (auto out_col : columns) {
//      if (out_col->IsUsed()) {
//        new_input_columns.AddUse(input_columns[i]);
//        auto new_out_col = new_columns.Create(out_col->var, this, out_col->id);
//        new_out_col->CopyConstantFrom(out_col);
//        out_col->ReplaceAllUsesWith(new_out_col);
//      }
//      ++i;
//    }
//
//    columns.Swap(new_columns);
//    input_columns.Swap(new_input_columns);
//
//    return true;
//
//  } else {
//    return false;
//  }
}

// Equality over inserts is structural.
bool Node<QueryDelete>::Equals(EqualitySet &eq, VIEW *that_) noexcept {

  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsDelete();
  if (!that || positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
      can_receive_deletions != that->can_receive_deletions ||
      can_produce_deletions != that->can_produce_deletions ||
      columns.Size() != that->columns.Size() || InsertSetsOverlap(this, that)) {
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
