// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {

Node<QueryConstraint>::~Node(void) {}

Node<QueryConstraint> *Node<QueryConstraint>::AsConstraint(void) noexcept {
  return this;
}

uint64_t Node<QueryConstraint>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = static_cast<unsigned>(op);
  for (auto col : input_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ col->Hash();
  }
  for (auto col : attached_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ col->Hash();
  }

  hash <<= 4;
  hash |= 2;

  return hash;
}

// Put this constraint into a canonical form, which will make comparisons and
// replacements easier. If this constraint's operator is unordered, then we
// sort the inputs to make comparisons trivial. We also need to put the
// "trailing" outputs into the proper order.
bool Node<QueryConstraint>::Canonicalize(QueryImpl *query) {
  is_canonical = AttachedColumnsAreCanonical();

  // Check to see if the input columns are ordered correctly. We can reorder
  // them only in the case of (in)equality comparisons.
  const auto is_unordered = ComparisonOperator::kEqual == op ||
      ComparisonOperator::kNotEqual == op;

  if ((is_unordered && input_columns[0] > input_columns[1]) ||
      input_columns[0]->IsConstant() || input_columns[1]->IsConstant()) {
    is_canonical = false;
  }

  if (is_canonical) {
    return false;
  }

  // If this view is used by a merge then we're not allowed to re-order the
  // columns. Instead, what we can do is create a tuple that will maintain
  // the ordering, and the canonicalize the join order below that tuple.
  bool non_local_changes = GuardWithTuple(query);

  // We need to re-order the input columns, and possibly also the output
  // columns to match the input ordering.

  std::unordered_map<COL *, COL *> in_to_out;
  DefList<COL> new_output_cols(this);

  auto i = 2u;

  // For equality, there's only one output `Def` for the two inputs, so we
  // can always sort the inputs.
  if (ComparisonOperator::kEqual == op) {
    i = 1u;

    input_columns.Sort();

    const auto lhs_col = input_columns[0];
    const auto rhs_col = input_columns[1];

    // This filter is no longer needed.
    if (lhs_col == rhs_col) {
      columns[0]->ReplaceAllUsesWith(lhs_col);

      for (auto j = 0u; j < attached_columns.Size(); ++j, ++i) {
        columns[i]->ReplaceAllUsesWith(attached_columns[j]);
      }

      input_columns.Clear();  // Remove this as taking inputs.
      attached_columns.Clear();
      return true;
    }

    auto new_out_col = new_output_cols.Create(
        columns[0]->var, this, columns[0]->id);
    columns[0]->ReplaceAllUsesWith(new_out_col);

    in_to_out.emplace(lhs_col, new_output_cols[0]);
    in_to_out.emplace(rhs_col, new_output_cols[0]);

    // Constant propagation.
    if (new_out_col->IsUsedIgnoreMerges()) {
      if (lhs_col->IsConstant()) {
        assert(!rhs_col->IsConstant());
        new_out_col->ReplaceAllUsesWith(lhs_col);
        non_local_changes = true;

      } else if (rhs_col->IsConstant()) {
        new_out_col->ReplaceAllUsesWith(rhs_col);
        non_local_changes = true;
      }
    }

    // For inequality, we can re-order the inputs, but must also re-order the
    // outputs.
  } else if (ComparisonOperator::kNotEqual == op &&
      input_columns[0] > input_columns[1]) {

    input_columns.Sort();

    const auto lhs_col = input_columns[0];
    const auto rhs_col = input_columns[1];
    assert(lhs_col != rhs_col);

    auto new_lhs_out = new_output_cols.Create(
        columns[1]->var, this, columns[1]->id);
    columns[1]->ReplaceAllUsesWith(new_lhs_out);

    const auto new_rhs_out = new_output_cols.Create(
        columns[0]->var, this, columns[0]->id);
    columns[0]->ReplaceAllUsesWith(new_rhs_out);

    in_to_out.emplace(lhs_col, new_output_cols[0]);
    in_to_out.emplace(rhs_col, new_output_cols[1]);

    // Constant propagation.
    if (lhs_col->IsConstant() && new_lhs_out->IsUsedIgnoreMerges()) {
      new_lhs_out->ReplaceAllUsesWith(lhs_col);
      non_local_changes = true;
    }

    if (rhs_col->IsConstant() && new_rhs_out->IsUsedIgnoreMerges()) {
      new_rhs_out->ReplaceAllUsesWith(rhs_col);
      non_local_changes = true;
    }

    // Preserve the column ordering for the output columns of other
    // comparisons.
  } else {
    const auto lhs_col = input_columns[0];
    const auto rhs_col = input_columns[1];
    assert(lhs_col != rhs_col);

    const auto new_lhs_out = new_output_cols.Create(
        columns[0]->var, this, columns[0]->id);
    columns[0]->ReplaceAllUsesWith(new_lhs_out);

    const auto new_rhs_out = new_output_cols.Create(
        columns[1]->var, this, columns[1]->id);
    columns[1]->ReplaceAllUsesWith(new_rhs_out);

    in_to_out.emplace(lhs_col, new_output_cols[0]);
    in_to_out.emplace(rhs_col, new_output_cols[1]);

    // Constant propagation.
    if (lhs_col->IsConstant() && new_lhs_out->IsUsedIgnoreMerges()) {
      new_lhs_out->ReplaceAllUsesWith(lhs_col);
      non_local_changes = true;
    }

    if (rhs_col->IsConstant() && new_rhs_out->IsUsedIgnoreMerges()) {
      new_rhs_out->ReplaceAllUsesWith(rhs_col);
      non_local_changes = true;
    }
  }

  const auto num_cols = columns.Size();
  assert((num_cols - i) == attached_columns.Size());

  UseList<COL> new_attached_cols(this);

  for (auto j = 0u; i < num_cols; ++i, ++j) {
    const auto old_out_col = columns[i];

    // If the output column is never used, then get rid of it.
    //
    // NOTE(pag): `IsUsed` on a column checks to see if its view is used
    //            in a merge, which would not show up in a normal def-use
    //            list.
    if (!old_out_col->IsUsed()) {
      continue;
    }

    const auto in_col = attached_columns[j];

    // If the old input column is a constant, then propagate it rather than
    // attach it.
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
      new_attached_cols.AddUse(in_col);
    }
  }

  new_attached_cols.Sort();

  for (auto in_col : new_attached_cols) {
    const auto old_out_col = in_to_out[in_col];
    const auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
  }

  attached_columns.Swap(new_attached_cols);
  columns.Swap(new_output_cols);

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over constraints is pointer-based.
bool Node<QueryConstraint>::Equals(
    EqualitySet &, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsConstraint();
  return that &&
         op == that->op &&
         columns.Size() == that_->columns.Size() &&
         ColumnsEq(input_columns, that->input_columns) &&
         ColumnsEq(attached_columns, that->attached_columns);
}

}  // namespace hyde
