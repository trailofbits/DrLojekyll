// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

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
    local_hash ^= __builtin_rotateright64(local_hash, 43) * col->Hash();
  }

  // Mix in the hashes of the tuple by columns; these are ordered.
  for (auto col : attached_columns) {
    local_hash ^= __builtin_rotateright64(local_hash, 33) * col->Hash();
  }

  for (auto functor : merge_functors) {
    local_hash ^= __builtin_rotateright64(local_hash, 23) * functor.Id();
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
  if (!that ||
      columns.Size() != that->columns.Size() ||
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
bool Node<QueryKVIndex>::Canonicalize(
    QueryImpl *query, bool, const ErrorLog &) {

  if (is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  if (valid == VIEW::kValid && !CheckAllViewsMatch(input_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    is_canonical = true;
    return false;
  }

  is_canonical = true;

  auto i = 0u;

  // Check if the keys are canonical. What matters here is that they aren't
  // constants. If they aren't used then we still need to keep them, as they
  // might distinguish two values.
  for (auto col : input_columns) {

    // Input is a constant, forward it along.
    if (col->IsConstant()) {
      columns[i]->ReplaceAllUsesWith(col);
      hash = 0;
      is_canonical = false;

    } else if (col->IsConstantRef()) {
      columns[i]->CopyConstant(col);
    }
    ++i;
  }

  if (is_canonical) {
    return false;
  }

  (void) GuardWithTuple(query);

  UseList<COL> new_input_columns(this);
  DefList<COL> new_output_columns(this);

  // Make the new output columns for the keys that we're keeping.
  i = 0u;
  for (auto col : input_columns) {
    const auto old_out_col = columns[i];
    if (!col->IsConstant()) {
      const auto new_out_col = new_output_columns.Create(
          old_out_col->var, this, old_out_col->id);
      old_out_col->ReplaceAllUsesWith(new_out_col);
      new_out_col->CopyConstant(old_out_col);
    }
  }

  // Make the new output columns for the attached (mutable) columns. These
  // are all preserved.
  for (auto col : attached_columns) {
    (void) col;
    const auto old_out_col = columns[i++];
    const auto new_out_col = new_output_columns.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
    new_out_col->CopyConstant(old_out_col);
  }

  // Add uses for the new input columns.
  i = 0u;
  for (auto col : input_columns) {
    if (!col->IsConstant()) {
      new_input_columns.AddUse(col);
    }
  }

  columns.Swap(new_output_columns);
  input_columns.Swap(new_input_columns);

  if (valid == VIEW::kValid && !CheckAllViewsMatch(input_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  hash = 0;
  is_canonical = true;
  return true;
}

}  // namespace hyde
