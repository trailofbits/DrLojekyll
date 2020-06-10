// Copyright 2020, Trail of Bits. All rights reserved.

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

  // Mix in the hashes of the tuple by columns; these are ordered.
  for (auto col : input_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ col->Hash();
  }

  // Mix in the hashes of the tuple by columns; these are ordered.
  for (auto col : attached_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ col->Hash();
  }

  for (auto functor : merge_functors) {
    hash ^= __builtin_rotateright64(hash, 16) ^ functor.Hash();
  }

  return hash;
}

bool Node<QueryKVIndex>::Equals(EqualitySet &eq,
                                Node<QueryView> *that_) noexcept {
  const auto that = that_->AsKVIndex();
  return that &&
         columns.Size() == that->columns.Size() &&
         positive_conditions == that->positive_conditions &&
         negative_conditions == that->negative_conditions &&
         ColumnsEq(input_columns, that->input_columns) &&
         ColumnsEq(attached_columns, that->attached_columns) &&
         MergeFunctorsEq(merge_functors, that->merge_functors) &&
         !InsertSetsOverlap(this, that);
}

// Put the KV index into a canonical form. The only real internal optimization
// that will happen is constant propagation of keys, but NOT values (as we can't
// predict how the merge functors will affect them).
bool Node<QueryKVIndex>::Canonicalize(QueryImpl *query) {
  is_canonical = true;

  auto i = 0u;

  // Check if the keys are canonical. What matters here is that they aren't
  // constants. If they aren't used then we still need to keep them, as they
  // might distinguish two values.
  for (auto col : input_columns) {

    // Input is a constant, forward it along.
    if (col->IsConstant()) {
      columns[i]->ReplaceAllUsesWith(col);
      is_canonical = false;
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

  is_canonical = true;
  return true;
}

}  // namespace hyde
