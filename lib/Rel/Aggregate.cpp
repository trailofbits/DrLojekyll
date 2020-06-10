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

  // Mix in the hashes of the group by columns.
  uint64_t group_hash = 0;
  for (auto col : group_by_columns) {
    group_hash = __builtin_rotateright64(group_hash, 16) ^ col->Hash();
  }

  // Mix in the hashes of the configuration columns.
  uint64_t bound_hash = 0;
  for (auto col : config_columns) {
    bound_hash = __builtin_rotateright64(bound_hash, 16) ^ col->Hash();
  }

  // Mix in the hashes of the summarized columns.
  uint64_t summary_hash = 0;
  for (auto col : aggregated_columns) {
    summary_hash = __builtin_rotateright64(summary_hash, 16) ^ col->Hash();
  }

  hash = HashInit();
  hash ^= functor.Id() ^ group_hash ^ bound_hash ^ summary_hash;

  return hash;
}

unsigned Node<QueryAggregate>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.

    auto real = GetDepth(config_columns, 1u);
    real = GetDepth(group_by_columns, real);
    real = GetDepth(aggregated_columns, real);
    depth = real + 1u;
  }
  return depth;
}

// Put this aggregate into a canonical form, which will make comparisons and
// replacements easier.
bool Node<QueryAggregate>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  assert(attached_columns.Empty());

  const auto guard_tuple = GuardWithTuple(query);
  bool non_local_changes = guard_tuple != nullptr;
  is_canonical = true;

  std::unordered_map<COL *, COL *> in_to_out;
  auto i = 0u;
  COL *prev_col = nullptr;
  for (auto col : group_by_columns) {
    if (col <= prev_col) {
      is_canonical = false;  // Out of order, or there's a duplicate.
    }
    const auto out_col = columns[i++];

    // TODO(pag): Think about this, i.e. what it means to remove a group by
    //            column from an aggregate.
    if (!out_col->IsUsed()) {
      is_canonical = false;
    } else {
      in_to_out.emplace(col, out_col);
      prev_col = col;
    }
  }

  // There's a duplicate.
  if (in_to_out.size() != group_by_columns.Size()) {
    is_canonical = false;
  }

  // The group by columns are in order and unique.
  if (is_canonical) {
    assert(CheckAllViewsMatch(input_columns, attached_columns));
    return non_local_changes;
  }

  group_by_columns.Sort();

  DefList<COL> new_output_cols(this);
  UseList<COL> new_group_by_columns(this);

  for (auto j = 0u; j < i; ++j) {
    const auto old_out_col = columns[j];

    // If the output column is never used, then get rid of it.
    //
    // NOTE(pag): `IsUsed` on a column checks to see if its view is used
    //            in a merge, which would not show up in a normal def-use
    //            list.
    if (!old_out_col->IsUsed()) {
      non_local_changes = true;  // Shrinking the number of columns.
      continue;
    }

    const auto in_col = group_by_columns[j];

    // Constant propagation.
    //
    // TODO(pag): What does it mean to group by a constant?? Probably it means
    //            that all sources have already FILTERed by that constant, and
    //            so this constant node can be omitted from the group as all
    //            sources will have done the right thing.
    if (in_col->IsConstant() && old_out_col->IsUsedIgnoreMerges()) {
      old_out_col->ReplaceAllUsesWith(in_col);
      non_local_changes = true;
      continue;
    }

    auto &out_col = in_to_out[in_col];
    if (out_col) {
      non_local_changes = true;  // Shrinking the number of columns.

      if (out_col->NumUses() > old_out_col->NumUses()) {
        old_out_col->ReplaceAllUsesWith(out_col);
      } else {
        out_col->ReplaceAllUsesWith(old_out_col);
        out_col = old_out_col;
      }
    } else {
      out_col = old_out_col;
      new_group_by_columns.AddUse(in_col);
    }
  }

  new_group_by_columns.Sort();

  // Add in the new grouped columns, which are in order, deduped, and used
  // by later flows.
  for (auto in_col : new_group_by_columns) {
    const auto old_out_col = in_to_out[in_col];
    const auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
  }

  // Add back in the bound and summarized columns.
  const auto num_cols = columns.Size();
  for (auto j = i; j < num_cols; ++j) {
    const auto old_out_col = columns[j];
    const auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
  }

  columns.Swap(new_output_cols);


  assert(CheckAllViewsMatch(input_columns, attached_columns));
  is_canonical = true;
  return true;
}

// Equality over aggregates is structural.
bool Node<QueryAggregate>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsAggregate();
  if (!that ||
      functor != that->functor ||
      columns.Size() != that->columns.Size() ||
      can_receive_deletions != that->can_receive_deletions ||
      can_produce_deletions != that->can_produce_deletions ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  if (!ColumnsEq(group_by_columns, that->group_by_columns) ||
      !ColumnsEq(config_columns, that->config_columns) ||
      !ColumnsEq(aggregated_columns, that->aggregated_columns)) {
    return false;
  }

  // In case of cycles, assume that these two aggregates are equivalent.
  eq.Insert(this, that);

  return true;
}

}  // namespace hyde
