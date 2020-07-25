// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Optimize.h"
#include "Query.h"

namespace hyde {
namespace {

static bool MergeFunctorsEq(const std::vector<ParsedFunctor> &lhs,
                            const std::vector<ParsedFunctor> &rhs) {
  const auto size = lhs.size();
  if (size != rhs.size()) {
    return false;
  }

  for (size_t i = 0u; i < size; ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }

  return true;
}

}  // namespace

Node<QueryKVIndex>::~Node(void) {}

Node<QueryKVIndex> *Node<QueryKVIndex>::AsKVIndex(void) noexcept {
  return this;
}

uint64_t Node<QueryKVIndex>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = HashInit();
  auto local_hash = hash;

  // Mix in the hashes of the tuple by columns; these are ordered.
  for (auto col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 43) * col->Hash();
  }

  // Mix in the hashes of the tuple by columns; these are ordered.
  for (auto col : attached_columns) {
    local_hash ^= RotateRight64(local_hash, 33) * col->Hash();
  }

  for (auto functor : merge_functors) {
    local_hash ^= RotateRight64(local_hash, 23) * functor.Id();
  }

  hash = local_hash;
  return local_hash;
}

bool Node<QueryKVIndex>::Equals(EqualitySet &eq,
                                Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsKVIndex();
  if (!that || columns.Size() != that->columns.Size() ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
      !MergeFunctorsEq(merge_functors, that->merge_functors) ||
      InsertSetsOverlap(this, that)) {
    return false;
  }

  eq.Insert(this, that);

  if (!ColumnsEq(eq, input_columns, that->input_columns) ||
      !ColumnsEq(eq, attached_columns, that->attached_columns)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

// Put the KV index into a canonical form. The only real internal optimization
// that will happen is constant propagation of keys, but NOT values (as we can't
// predict how the merge functors will affect them).
bool Node<QueryKVIndex>::Canonicalize(QueryImpl *query,
                                      const OptimizationContext &opt) {

  if (is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  if (valid == VIEW::kValid && !CheckIncomingViewsMatch(input_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    is_canonical = true;
    return false;
  }

  is_canonical = true;
  auto non_local_changes = false;

  // NOTE(pag): We can't do the default canonicalization of attached columns
  //            here because they are our value columns, and we cannot eliminate
  //            them or we'll lose the association with the mutable functors.

  auto i = 0u;

  in_to_out.clear();

  // Check if the keys are canonical. What matters here is that they aren't
  // constants. If they aren't used then we still need to keep them, as they
  // might distinguish two values.
  for (auto in_col : input_columns) {
    const auto out_col = columns[i++];
    const auto [changed, can_remove] =
        CanonicalizeColumnPair(in_col, out_col, opt);

    auto &prev_out_col = in_to_out[in_col];
    const auto in_col_is_const =
        opt.can_replace_outputs_with_constants && in_col->IsConstant();

    non_local_changes = non_local_changes || changed;
    (void) can_remove;

    // A key is constant, or it is reused, so we will remove it.
    if (in_col_is_const || prev_out_col) {
      is_canonical = false;
    }

    prev_out_col = out_col;
  }

  // Make sure at least one value column is used. If none of the value columns
  // are used, then we can eliminate this K/V index.
  const auto num_cols = columns.Size();
  bool any_values_are_used = false;
  for (auto j = i; j < num_cols; ++j) {
    if (columns[j]->IsUsed()) {
      any_values_are_used = true;
      break;
    }
  }

  if (!any_values_are_used) {
    is_canonical = false;
  }

  if (is_canonical) {
    return non_local_changes;
  }

  // If none of the value columns are used then replace this K/V index with a
  // tuple.
  if (!any_values_are_used) {
    const auto tuple = query->tuples.Create();

#ifndef NDEBUG
    tuple->producer = "KVINDEX-UNUSED-VALS(" + producer + ")";
#endif

    for (auto col : columns) {
      tuple->columns.Create(col->var, tuple, col->id);
    }

    auto j = 0u;
    for (auto key_col : input_columns) {
      columns[j++]->CopyConstant(key_col);
      tuple->input_columns.AddUse(key_col);
    }

    // Forward them along, ignoring the mutable merge functors. They will get
    // eliminated by the tuple canonicalization.
    for (auto val_col : attached_columns) {
      tuple->input_columns.AddUse(val_col);
    }

    ReplaceAllUsesWith(tuple);
    return true;
  }

  TUPLE *guard_tuple = nullptr;
  const auto is_used_in_merge = this->Def<Node<QueryView>>::IsUsed();

  UseList<COL> new_input_columns(this);
  DefList<COL> new_output_columns(this);

  in_to_out.clear();

  // Make the new output columns for the keys that we're keeping.
  i = 0u;
  for (auto in_col : input_columns) {
    const auto in_col_is_const =
        opt.can_replace_outputs_with_constants && in_col->IsConstant();
    const auto out_col = columns[i++];
    auto &prev_out_col = in_to_out[in_col];

    // A constant key isn't going to affect the arity of the grouping needed
    // to implement this K/V Index. Similarly, a previously used input column
    // also won't affect the arity.
    //
    // NOTE(pag): We also know that if we're down here, then at least one of
    //            the values is used and so removing keys won't disappear the
    //            K/V index.
    if (in_col_is_const || prev_out_col) {
      if (is_used_in_merge && !guard_tuple) {
        non_local_changes = true;
        guard_tuple = GuardWithTuple(query, true);
      }

      if (in_col_is_const) {
        out_col->ReplaceAllUsesWith(in_col);
      } else {
        out_col->ReplaceAllUsesWith(prev_out_col);
      }

      continue;  // Remove the column.
    }

    const auto new_out_col =
        new_output_columns.Create(out_col->var, this, out_col->id);
    new_out_col->CopyConstant(out_col);
    out_col->ReplaceAllUsesWith(new_out_col);

    new_input_columns.AddUse(in_col);
    prev_out_col = new_out_col;
  }

  // Make the new output columns for the attached (mutable) columns. These
  // are all preserved.
  //
  // NOTE(pag): We cannot do constant propagation across these columns, and thus
  //            cannot invoke `CopyConstant` between old/new output attached
  //            columns because we don't actually know what value the merge
  //            functor applying the update will produce when combinging the old
  //            and proposed values.
  for (auto in_col : attached_columns) {
    (void) in_col;
    const auto old_out_col = columns[i++];
    const auto new_out_col =
        new_output_columns.Create(old_out_col->var, this, old_out_col->id);
    assert(!old_out_col->IsConstantRef());
    old_out_col->ReplaceAllUsesWith(new_out_col);
  }

  columns.Swap(new_output_columns);
  input_columns.Swap(new_input_columns);

  if (valid == VIEW::kValid && !CheckIncomingViewsMatch(input_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  hash = 0;
  is_canonical = true;
  return true;
}

}  // namespace hyde
