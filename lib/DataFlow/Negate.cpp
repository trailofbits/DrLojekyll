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
    QueryImpl *, const OptimizationContext & opt) {
  is_canonical = true;
  if (valid == VIEW::kValid &&
      !CheckIncomingViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    return false;
  }

  const auto num_in_cols = input_columns.Size();
  bool non_local_changes = false;

  OptimizationContext attached_opt = opt;
  attached_opt.can_replace_outputs_with_constants = true;

  std::tie(is_canonical, non_local_changes) =
      CanonicalizeAttachedColumns(num_in_cols, attached_opt);

  // Propagate constants.
  for (auto i = 0u; i < num_in_cols; ++i) {
    auto out_col = columns[i];
    if (!out_col->IsConstantRef()) {
      auto in_col = input_columns[i];
      if (in_col->IsConstantOrConstantRef()) {
        out_col->CopyConstantFrom(in_col);
        non_local_changes = true;
      }
    }
  }

  if (!is_canonical) {

    in_to_out.clear();
    DefList<COL> new_columns(this);
    UseList<COL> new_attached_columns(this);

    // Make new output columns for the inputs.
    auto i = 0u;
    for (i = 0u; i < num_in_cols; ++i) {
      const auto out_col = columns[i];
      const auto new_out_col =
          new_columns.Create(out_col->var, this, out_col->id);

      new_out_col->CopyConstantFrom(out_col);
      out_col->ReplaceAllUsesWith(new_out_col);
      in_to_out.emplace(input_columns[i], new_out_col);
    }

    // Make the new attached columns.
    for (auto in_col : attached_columns) {
      const auto out_col = columns[i++];
      auto &prev_out_col = in_to_out[in_col];

      // This is either a re-use of one of the relation columns, or it's a
      // duplicate attached column; try to remove it.
      if (prev_out_col) {
        if (out_col->IsUsedIgnoreMerges()) {
          non_local_changes = true;
        }
        out_col->ReplaceAllUsesWith(prev_out_col);

        if (!out_col->IsUsed()) {
          in_col->view->is_canonical = false;
          continue;  // It's safe to remove.
        }
      }

      // This attached column isn't used; remove it by not adding it back in.
      if (opt.can_remove_unused_columns && !out_col->IsUsed()) {
        in_col->view->is_canonical = false;
        continue;
      }

      const auto new_out_col =
          new_columns.Create(out_col->var, this, out_col->id);

      new_out_col->CopyConstantFrom(out_col);
      out_col->ReplaceAllUsesWith(new_out_col);
      new_attached_columns.AddUse(in_col);

      if (!prev_out_col) {
        prev_out_col = new_out_col;
      }
    }

    columns.Swap(new_columns);
    attached_columns.Swap(new_attached_columns);
    non_local_changes = true;
  }

  if (!CheckIncomingViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  hash = 0;
  is_canonical = true;
  return non_local_changes;
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
