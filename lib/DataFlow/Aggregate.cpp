// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Optimize.h"
#include "Query.h"

namespace hyde {

QueryAggregateImpl::~QueryAggregateImpl(void) {}

QueryAggregateImpl::QueryAggregateImpl(ParsedFunctor functor_)
    : functor(functor_),
      group_by_columns(this),
      config_columns(this),
      aggregated_columns(this) {
  can_produce_deletions = true;
}

QueryAggregateImpl *QueryAggregateImpl::AsAggregate(void) noexcept {
  return this;
}

const char *QueryAggregateImpl::KindName(void) const noexcept {
  return "AGGREGATE";
}

uint64_t QueryAggregateImpl::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Base case for recursion.
  hash = HashInit() ^ functor.Id();
  assert(hash != 0);

  auto local_hash = hash;

  // Mix in the hashes of the group by columns.
  for (auto col : group_by_columns) {
    local_hash ^= RotateRight64(local_hash, 33) * col->Hash();
  }

  // Mix in the hashes of the configuration columns.
  for (auto col : config_columns) {
    local_hash ^= RotateRight64(local_hash, 23) * col->Hash();
  }

  // Mix in the hashes of the summarized columns.
  for (auto col : aggregated_columns) {
    local_hash ^= RotateRight64(local_hash, 13) * col->Hash();
  }

  hash = local_hash;
  return hash;
}

unsigned QueryAggregateImpl::Depth(void) noexcept {
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
bool QueryAggregateImpl::Canonicalize(QueryImpl *query,
                                        const OptimizationContext &opt,
                                        const ErrorLog &) {
  if (is_canonical) {
    return false;
  }

  if (is_dead || is_unsat || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  assert(!aggregated_columns.Empty());
  assert(attached_columns.Empty());

  if (valid == VIEW::kValid &&
      (!CheckIncomingViewsMatch(group_by_columns, aggregated_columns) ||
       !CheckIncomingViewsMatch(config_columns, aggregated_columns))) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    is_canonical = true;
    return false;
  }

  // If our predecessor is not satisfiable, then this flow is never reached.
  if (!is_unsat) {
    auto incoming_view0 = GetIncomingView(group_by_columns, aggregated_columns);
    auto incoming_view1 = GetIncomingView(config_columns, aggregated_columns);
    if ((incoming_view0 && incoming_view0->is_unsat) ||
        (incoming_view1 && incoming_view1->is_unsat)) {
      MarkAsUnsatisfiable();
      is_canonical = true;
      return true;
    }
  }

  VIEW *guard_tuple = nullptr;
  const auto is_used_in_merge = IsUsedDirectly();
  auto non_local_changes = false;
  is_canonical = true;

  in_to_out.clear();

  auto i = 0u;

  for (auto in_col : group_by_columns) {
    auto &prev_out_col = in_to_out[in_col];
    const auto out_col = columns[i];
    const auto in_col_is_const = in_col->IsConstant();
    ++i;

    const auto [changed, can_remove] =
        CanonicalizeColumnPair(in_col, out_col, opt);
    (void) can_remove;

    if (changed) {
      non_local_changes = true;
    }

    // Constants won't change the arity of the GROUP, so propagate and try to
    // remove them. Also, the same non-constant input column appearing multiple
    // times to a GROUP will also not change the arity, nor will its removal
    // affect control dependencies, so we can remove it too.
    if (in_col_is_const || prev_out_col) {
      if (is_used_in_merge && !guard_tuple) {
        guard_tuple = GuardWithTuple(query, true /* force */);
        non_local_changes = true;
      }

      if (in_col_is_const) {
        out_col->ReplaceAllUsesWith(in_col);
      } else {
        out_col->ReplaceAllUsesWith(prev_out_col);
      }

      is_canonical = false;
      continue;
    }

    prev_out_col = out_col;
  }

  assert(i == group_by_columns.Size());

  // Nothing to do, all GROUP columns are unique and/or needed.
  if (is_canonical) {
    return non_local_changes;
  }

  hash = 0;

  DefList<COL> new_columns(this);
  UseList<COL> new_group_by_columns(this);

  for (auto j = 0u; j < i; ++j) {
    const auto in_col = group_by_columns[j];
    if (const auto old_out_col = in_to_out[in_col]; old_out_col) {
      new_group_by_columns.AddUse(in_col);
      const auto new_out_col = new_columns.Create(
          old_out_col->var, old_out_col->type, this, old_out_col->id);
      old_out_col->ReplaceAllUsesWith(new_out_col);
      new_out_col->CopyConstantFrom(old_out_col);
    }
  }

  // Add back in the bound (configuration) and summarized columns.
  const auto num_cols = columns.Size();
  for (auto j = i; j < num_cols; ++j) {
    const auto old_out_col = columns[j];
    const auto new_out_col = new_columns.Create(
        old_out_col->var, old_out_col->type, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
    new_out_col->CopyConstantFrom(old_out_col);
  }

  group_by_columns.Swap(new_group_by_columns);
  columns.Swap(new_columns);

  if (!CheckIncomingViewsMatch(group_by_columns, aggregated_columns) ||
      !CheckIncomingViewsMatch(config_columns, aggregated_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  is_canonical = true;
  return non_local_changes;
}

// Equality over aggregates is structural.
bool QueryAggregateImpl::Equals(EqualitySet &eq,
                                  QueryViewImpl *that_) noexcept {

  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsAggregate();
  if (!that || functor != that->functor ||
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
