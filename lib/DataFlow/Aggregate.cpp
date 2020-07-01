// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {

Node<QueryAggregate>::~Node(void) {}

Node<QueryAggregate> *Node<QueryAggregate>::AsAggregate(void) noexcept {
  return this;
}

uint64_t Node<QueryAggregate>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Base case for recursion.
  hash = HashInit() ^ functor.Id();

  auto local_hash = hash;

  // Mix in the hashes of the group by columns.
  for (auto col : group_by_columns) {
    local_hash ^= __builtin_rotateright64(local_hash, 33) * col->Hash();
  }

  // Mix in the hashes of the configuration columns.
  for (auto col : config_columns) {
    local_hash ^= __builtin_rotateright64(local_hash, 23) * col->Hash();
  }

  // Mix in the hashes of the summarized columns.
  for (auto col : aggregated_columns) {
    local_hash ^= __builtin_rotateright64(local_hash, 13) * col->Hash();
  }

  hash = local_hash;
  return hash;
}

unsigned Node<QueryAggregate>::Depth(void) noexcept {
  if (depth) {
    return depth;
  }

  auto estimate = EstimateDepth(config_columns, 1u);
  estimate = EstimateDepth(group_by_columns, estimate);
  estimate = EstimateDepth(aggregated_columns, estimate);
  estimate = EstimateDepth(positive_conditions, estimate);
  estimate = EstimateDepth(negative_conditions, estimate);
  depth = estimate + 1u;

  auto real = GetDepth(config_columns, 1u);
  real = GetDepth(group_by_columns, real);
  real = GetDepth(aggregated_columns, real);
  real = GetDepth(positive_conditions, real);
  real = GetDepth(negative_conditions, real);
  depth = real + 1u;

  return depth;
}

// Put this aggregate into a canonical form, which will make comparisons and
// replacements easier.
bool Node<QueryAggregate>::Canonicalize(
    QueryImpl *query, bool sort, const ErrorLog &) {
  if (is_canonical) {
    return false;
  }

  if (is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  assert(!aggregated_columns.Empty());

  if (valid == VIEW::kValid &&
      (!CheckAllViewsMatch(group_by_columns, aggregated_columns) ||
       !CheckAllViewsMatch(config_columns, aggregated_columns))) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    is_canonical = true;
    return false;
  }

  assert(attached_columns.Empty());

  auto guard_tuple = GuardWithTuple(query);
  bool non_local_changes = guard_tuple != nullptr;
  is_canonical = true;

  in_to_out.clear();

  auto i = 0u;

  for (auto col : group_by_columns) {
    auto &prev_out_col = in_to_out[col];
    const auto out_col = columns[i];
    ++i;

    // Constants won't change the arity of the GROUP, so propagate and try to
    // remove them.
    if (col->IsConstant()) {
      const auto const_col = col->AsConstant();

      if (out_col->IsUsedIgnoreMerges()) {
        non_local_changes = true;
      }
      out_col->ReplaceAllUsesWith(const_col);
      out_col->CopyConstant(const_col);

      // Constant column that isn't used, including by merges. This won't
      // change the aggregate so we can remove it.
      if (!out_col->IsUsed()) {
        is_canonical = false;

      // Constant, and it's used directly or indirectly, e.g. by a MERGE, so
      // we'll force a guard tuple so that we can forward the constant and
      // eliminate it from the GROUP BY.
      } else if (!guard_tuple) {
        non_local_changes = true;
        guard_tuple = GuardWithTuple(query, true);
        non_local_changes = true;
        out_col->ReplaceAllUsesWith(const_col);  // Should replace one use.
      }

    // Similar to the above case, we can remove duplicate columns from GROUP BYs
    // as they won't change the arity of the grouped set.
    } else if (prev_out_col) {

      if (col->IsConstantRef()) {
        assert(prev_out_col->IsConstantRef());
        if (!out_col->IsConstantRef()) {
          non_local_changes = true;
        }
        out_col->CopyConstant(col);
      }

      if (out_col->IsUsedIgnoreMerges()) {
        non_local_changes = true;
      }

      out_col->ReplaceAllUsesWith(prev_out_col);

      // Previously used column that isn't used, including by merges. This won't
      // change the aggregate so we can remove it.
      if (!out_col->IsUsed()) {
        is_canonical = false;
        col->view->is_canonical = false;

      // Previously used, and it's used directly or indirectly, e.g. by a
      // MERGE, so we'll force a guard tuple so that we can forward the prior
      // used column and eliminate it from the GROUP BY.
      } else if (!guard_tuple) {
        non_local_changes = true;
        guard_tuple = GuardWithTuple(query, true);
        out_col->ReplaceAllUsesWith(prev_out_col);  // Should replace one use.
      }

    } else {
      if (col->IsConstantRef()) {
        if (!out_col->IsConstantRef()) {
          out_col->CopyConstant(col);
        }
        out_col->CopyConstant(col);
      }

      prev_out_col = out_col;
    }
  }

  // Nothing to do, all GROUP BY columns are unique and/or needed.
  if (is_canonical && in_to_out.size() == group_by_columns.Size()) {
    return non_local_changes;
  }

  hash = 0;

  DefList<COL> new_output_cols(this);
  UseList<COL> new_group_by_columns(this);

  assert(i == group_by_columns.Size());
  for (auto j = 0u; j < i; ++j) {
    const auto in_col = group_by_columns[j];
    if (const auto old_out_col = in_to_out[in_col]; old_out_col) {
      new_group_by_columns.AddUse(in_col);
    }
  }

  if (sort) {
    new_group_by_columns.Sort();
  }

  // Add in the new grouped columns, which may be in order, deduped, and used
  // by later flows.
  for (auto in_col : new_group_by_columns) {
    const auto old_out_col = in_to_out[in_col];
    const auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
    new_out_col->CopyConstant(old_out_col);
  }

  // Add back in the bound (configuration) and summarized columns.
  const auto num_cols = columns.Size();
  for (auto j = i; j < num_cols; ++j) {
    const auto old_out_col = columns[j];
    const auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
    new_out_col->CopyConstant(old_out_col);
  }

  group_by_columns.Swap(new_group_by_columns);
  columns.Swap(new_output_cols);

  if (!CheckAllViewsMatch(group_by_columns, aggregated_columns) ||
      !CheckAllViewsMatch(config_columns, aggregated_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  is_canonical = true;
  return non_local_changes;
}

// Equality over aggregates is structural.
bool Node<QueryAggregate>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {

  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsAggregate();
  if (!that ||
      functor != that->functor ||
      columns.Size() != that->columns.Size() ||
      can_receive_deletions != that->can_receive_deletions ||
      can_produce_deletions != that->can_produce_deletions ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
      InsertSetsOverlap(this, that)) {
    return false;
  }

  // In case of cycles, assume that these two aggregates are equivalent.
  eq.Insert(this, that);

  if (!ColumnsEq(eq, group_by_columns, that->group_by_columns) ||
      !ColumnsEq(eq, config_columns, that->config_columns) ||
      !ColumnsEq(eq, aggregated_columns, that->aggregated_columns)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

}  // namespace hyde
