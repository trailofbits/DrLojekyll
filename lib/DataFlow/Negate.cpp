// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Optimize.h"
#include "Query.h"

namespace hyde {

Node<QueryNegate>::~Node(void) {}

Node<QueryNegate> *Node<QueryNegate>::AsNegate(void) noexcept {
  return this;
}

uint64_t Node<QueryNegate>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = HashInit();
  assert(hash != 0);

  auto local_hash = RotateRight64(hash, 17) ^ negated_view->Hash();

  // Mix in the hashes of the input by columns; these are ordered.
  for (auto col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 33) * col->Hash();
  }

  for (auto col : attached_columns) {
    local_hash ^= RotateRight64(local_hash, 53) * col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

bool Node<QueryNegate>::Canonicalize(
    QueryImpl *query, const OptimizationContext &opt, const ErrorLog &) {

  if (is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  if (valid == VIEW::kValid &&
      !CheckIncomingViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    return false;
  }

  const auto num_cols = columns.Size();
  const auto first_attached_col = input_columns.Size();
  is_canonical = true;  // Updated by `CanonicalizeColumn`.
  in_to_out.clear();  // Filled in by `CanonicalizeColumn`.
  Discoveries has = {};

  // NOTE(pag): This may update `is_canonical`.
  const auto incoming_view = PullDataFromBeyondTrivialTuples(
      GetIncomingView(input_columns, attached_columns),
      input_columns, attached_columns);

  auto i = 0u;
  for (; i < first_attached_col; ++i) {
    has = CanonicalizeColumn(opt, input_columns[i], columns[i], false, has);
  }

  // NOTE(pag): Mute this, as we always need to maintain the `input_columns`
  //            and so we don't want to infinitely rewrite this negation if
  //            there is a duplicate column in `input_columns`.
  has.duplicated_input_column = false;

  for (auto j = 0u; i < num_cols; ++i, ++j) {
    has = CanonicalizeColumn(opt, attached_columns[j], columns[i], true, has);
  }

  // Nothing changed.
  if (is_canonical) {
    return has.non_local_changes;
  }

  // There is at least one output of our negation that is a constant and that
  // can be guarded, or one duplicated column. Go create a tuple that will
  // only propagate forward the needed data.
  if (has.guardable_constant_output || has.duplicated_input_column) {
    if (!IsUsedDirectly() &&
        !(OnlyUser() && has.directly_used_column)) {
      GuardWithOptimizedTuple(query, first_attached_col, incoming_view);
      has.non_local_changes = true;
    }
  }

  DefList<COL> new_columns(this);
  UseList<COL> new_input_columns(this);
  UseList<COL> new_attached_columns(this);

  for (i = 0; i < first_attached_col; ++i) {
    const auto old_col = columns[i];
    const auto new_col = new_columns.Create(old_col->var, this, old_col->id, i);
    old_col->ReplaceAllUsesWith(new_col);
    new_input_columns.AddUse(input_columns[i]->TryResolveToConstant());
  }

  for (auto j = 0u; i < num_cols; ++i, ++j) {
    const auto old_col = columns[i];
    if (old_col->IsUsed()) {
      const auto new_col = new_columns.Create(old_col->var, this, old_col->id,
                                              new_columns.Size());
      old_col->ReplaceAllUsesWith(new_col);
      new_attached_columns.AddUse(attached_columns[j]->TryResolveToConstant());
    } else {
      has.non_local_changes = true;
    }
  }

  // We dropped a reference to our predecessor; maintain it via a condition.
  const auto new_incoming_view = GetIncomingView(new_input_columns,
                                                 new_attached_columns);
  if (incoming_view != new_incoming_view) {
    CreateDependencyOnView(query, incoming_view);
    has.non_local_changes = true;
  }

  columns.Swap(new_columns);
  input_columns.Swap(new_input_columns);
  attached_columns.Swap(new_attached_columns);

  hash = 0;
  is_canonical = true;

  if (!CheckIncomingViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  return has.non_local_changes;
}

// Equality over inserts is structural.
bool Node<QueryNegate>::Equals(EqualitySet &eq, VIEW *that_) noexcept {

  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsNegate();
  if (!that || can_produce_deletions != that->can_produce_deletions ||
      columns.Size() != that->columns.Size() ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions) {
    return false;
  }

  eq.Insert(this, that);
  if (!negated_view->Equals(eq, that->negated_view.get()) ||
      !ColumnsEq(eq, input_columns, that->input_columns) ||
      !ColumnsEq(eq, attached_columns, that->attached_columns)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

unsigned Node<QueryNegate>::Depth(void) noexcept {
  if (depth) {
    return depth;
  }

  auto estimate = EstimateDepth(input_columns, 1u);
  estimate = EstimateDepth(attached_columns, depth);
  estimate = EstimateDepth(positive_conditions, depth);
  estimate = EstimateDepth(negative_conditions, depth);
  depth = estimate + 1u;

  auto real = GetDepth(input_columns, negated_view->Depth());
  real = GetDepth(attached_columns, real);
  real = GetDepth(positive_conditions, real);
  real = GetDepth(negative_conditions, real);
  depth = real + 1u;

  return depth;
}

}  // namespace hyde
