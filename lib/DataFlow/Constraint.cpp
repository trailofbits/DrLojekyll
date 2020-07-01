// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Util/EqualitySet.h>

#include <iostream>
namespace hyde {

Node<QueryConstraint>::~Node(void) {}

Node<QueryConstraint> *Node<QueryConstraint>::AsConstraint(void) noexcept {
  return this;
}

uint64_t Node<QueryConstraint>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Base case for recursion.
  hash = HashInit() ^ static_cast<unsigned>(op);

  auto local_hash = hash;

  for (auto col : input_columns) {
    local_hash ^= __builtin_rotateright64(local_hash, 53) * col->Hash();
  }

  for (auto col : attached_columns) {
    local_hash ^= __builtin_rotateright64(local_hash, 43) * col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

// Put this constraint into a canonical form, which will make comparisons and
// replacements easier. If this constraint's operator is unordered, then we
// sort the inputs to make comparisons trivial. We also need to put the
// "trailing" outputs into the proper order.
bool Node<QueryConstraint>::Canonicalize(
    QueryImpl *query, bool sort, const ErrorLog &log) {
  if (is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  if (valid == VIEW::kValid &&
      !CheckAllViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    return false;
  }

  // Check to see if the input columns are ordered correctly. We can reorder
  // them only in the case of (in)equality comparisons.
  const auto is_equality = ComparisonOperator::kEqual == op;
  const auto is_unordered = is_equality || ComparisonOperator::kNotEqual == op;

  auto [is_canonical_, non_local_changes] = AttachedColumnsAreCanonical(
      (is_equality ? 1u : 2u), sort);
  is_canonical = is_canonical_;

  if (non_local_changes) {
    std::cerr << "!";
  }

  const auto lhs_col = input_columns[0];
  const auto rhs_col = input_columns[1];
  const auto lhs_sort = sort ? lhs_col->Sort() : lhs_col->Index();
  const auto rhs_sort = sort ? rhs_col->Sort() : rhs_col->Index();

  // Check if the result is used (ignoring merges).
  //
  // NOTE(pag): This should be checked before guarding with a TUPLE, otherwise
  //            if we end up guarding with a TUPLE then it will definitely look
  //            used.
  const auto result_col = columns[0];

  const auto lhs_is_const = lhs_col->IsConstantOrConstantRef();
  const auto rhs_is_const = rhs_col->IsConstantOrConstantRef();

  if ((is_unordered && sort && lhs_sort > rhs_sort) ||
      (lhs_col == rhs_col) ||
      (is_equality && (lhs_is_const != rhs_is_const))) {
    hash = 0;
    is_canonical = false;
  }

  if (is_canonical) {
    return non_local_changes;
  }

  // If this view is used by a merge then we're not allowed to re-order the
  // columns. Instead, what we can do is create a tuple that will maintain
  // the ordering, and the canonicalize the join order below that tuple.
//  (void) GuardWithTuple(query);

  // We need to re-order the input columns, and possibly also the output
  // columns to match the input ordering.

  in_to_out.clear();
  DefList<COL> new_output_cols(this);

  auto i = 2u;

  // For equality, there's only one output `Def` for the two inputs, so we
  // can always sort the inputs.
  if (ComparisonOperator::kEqual == op) {
    i = 1u;

    // This filter is no longer needed.
    if (lhs_col == rhs_col) {

      auto tuple = query->tuples.Create();
      for (auto col : columns) {
        tuple->columns.Create(col->var, tuple, col->id);
      }

      ReplaceAllUsesWith(tuple);

      tuple->input_columns.AddUse(lhs_col);
      tuple->columns[0]->CopyConstant(lhs_col);

      for (auto j = 0u; j < attached_columns.Size(); ++j, ++i) {
        tuple->input_columns.AddUse(attached_columns[j]);
        tuple->columns[i]->CopyConstant(attached_columns[j]);
      }

      input_columns.Clear();  // Remove this as taking inputs.
      attached_columns.Clear();
      hash = 0;
      is_used = false;
      is_dead = true;
      is_canonical = true;
      return true;
    }

    // This may or may not be a problem; we've found something like `0 = 1`, or
    // possibly something like `1 = 0x1`. This means that we can't propagate the
    // constants, lest the comparison actually fail.
    if (lhs_is_const && rhs_is_const) {
      if (!result_col->IsConstantRef()) {
        std::cerr << "A";
        non_local_changes = true;
      }
      result_col->CopyConstant(lhs_col);

    // Something like `0 = A`.
    } else if (lhs_is_const) {
      if (!result_col->IsConstantRef()) {
        std::cerr << "C";
        non_local_changes = true;
      }
      result_col->CopyConstant(lhs_col);

    // Something like `A = 0`.
    } else if (rhs_is_const) {
      if (!result_col->IsConstantRef()) {
        std::cerr << "E";
        non_local_changes = true;
      }
      result_col->CopyConstant(rhs_col);
    }

    // Input columns are out of order.
    if (sort && lhs_sort > rhs_sort) {
      UseList<COL> new_input_cols(this);
      new_input_cols.AddUse(rhs_col);
      new_input_cols.AddUse(lhs_col);
      input_columns.Swap(new_input_cols);
    }

    auto new_result_col = new_output_cols.Create(
        result_col->var, this, result_col->id);
    result_col->ReplaceAllUsesWith(new_result_col);
    in_to_out.emplace(lhs_col, new_result_col);
    in_to_out.emplace(rhs_col, new_result_col);

    new_result_col->CopyConstant(result_col);

  // For inequality, we can re-order the inputs, but must also re-order the
  // outputs.
  } else if (ComparisonOperator::kNotEqual == op && lhs_sort > rhs_sort) {

    const auto old_lhs_out = columns[0];
    const auto old_rhs_out = columns[1];

    // This is kind of bad but totally possible. We've proven that we can't
    // satisfy this particular constraint.
    if (lhs_col == rhs_col) {

      auto outer_range = spelling_range;
      if (outer_range.IsInvalid()) {
        outer_range = DisplayRange(columns[0]->var.SpellingRange().From(),
                                   columns[0]->var.SpellingRange().To());
      }

      auto err = log.Append(outer_range);
      err << "Unsatisfiable inequality between '" << columns[0]->var
          << "' and '" << columns[1]->var
          << "' ends up comparing the same values";

      err.Note(lhs_col->var.SpellingRange(), lhs_col->var.SpellingRange())
          << "Value comes from here";

      hash = 0;
      is_canonical = true;
      is_dead = true;
      return non_local_changes;
    }

    // Constant propagation of the LHS col.
    if (lhs_col->IsConstantOrConstantRef()) {
      if (!old_lhs_out->IsConstantRef()) {
        std::cerr << "G";
        non_local_changes = true;
      }
      old_lhs_out->CopyConstant(lhs_col);
    }

    // Constant propagation of the RHS col.
    if (rhs_col->IsConstantOrConstantRef()) {
      if (!old_rhs_out->IsConstantRef()) {
        std::cerr << "H";
        non_local_changes = true;
      }
      old_rhs_out->CopyConstant(rhs_col);
    }

    // The input columns were out of order, so put them in the right order.
    UseList<COL> new_input_cols(this);
    new_input_cols.AddUse(rhs_col);
    new_input_cols.AddUse(lhs_col);
    input_columns.Swap(new_input_cols);

    // Put the output columns into the right order.
    auto new_lhs_out = new_output_cols.Create(
        old_rhs_out->var, this, old_rhs_out->id);
    old_rhs_out->ReplaceAllUsesWith(new_lhs_out);

    const auto new_rhs_out = new_output_cols.Create(
        old_lhs_out->var, this, old_lhs_out->id);
    old_lhs_out->ReplaceAllUsesWith(new_rhs_out);

    new_lhs_out->CopyConstant(old_rhs_out);
    new_rhs_out->CopyConstant(old_lhs_out);

    in_to_out.emplace(rhs_col, new_lhs_out);
    in_to_out.emplace(lhs_col, new_rhs_out);

  // Preserve the column ordering for the output columns of other
  // comparisons.
  } else {

    // This is kind of bad but totally possible. We've proven that we can't
    // satisfy this particular constraint.
    if (lhs_col == rhs_col) {
      auto outer_range = spelling_range;
      if (outer_range.IsInvalid()) {
        outer_range = DisplayRange(columns[0]->var.SpellingRange().From(),
                                   columns[0]->var.SpellingRange().To());
      }

      auto err = log.Append(outer_range);
      err << "Unsatisfiable inequality between '" << columns[0]->var
          << "' and '" << columns[1]->var
          << "' ends up comparing the same values";

      err.Note(lhs_col->var.SpellingRange(), lhs_col->var.SpellingRange())
          << "Value comes from here";

      hash = 0;
      is_canonical = true;
      is_dead = true;
      return non_local_changes;
    }

    const auto old_lhs_out = columns[0];
    const auto old_rhs_out = columns[1];

    // Constant propagation of the LHS col.
    if (lhs_col->IsConstantOrConstantRef()) {
      if (!old_lhs_out->IsConstantRef()) {
        std::cerr << "I";
        non_local_changes = true;
      }
      old_lhs_out->CopyConstant(lhs_col);
    }

    // Constant propagation of the RHS col.
    if (rhs_col->IsConstantOrConstantRef()) {
      if (!old_rhs_out->IsConstantRef()) {
        std::cerr << "J";
        non_local_changes = true;
      }
      old_rhs_out->CopyConstant(rhs_col);
    }

    // We don't need to re-order anything, but to be uniform with the rest and
    // possible sorting of attached columns, we will create a new set of
    // output columns.
    const auto new_lhs_out = new_output_cols.Create(
        old_lhs_out->var, this, old_lhs_out->id);
    old_lhs_out->ReplaceAllUsesWith(new_lhs_out);

    const auto new_rhs_out = new_output_cols.Create(
        old_rhs_out->var, this, old_rhs_out->id);
    old_rhs_out->ReplaceAllUsesWith(new_rhs_out);

    new_lhs_out->CopyConstant(old_lhs_out);
    new_rhs_out->CopyConstant(old_rhs_out);

    in_to_out.emplace(lhs_col, new_lhs_out);
    in_to_out.emplace(rhs_col, new_rhs_out);
  }

  const auto num_cols = columns.Size();
  assert((num_cols - i) == attached_columns.Size());

  UseList<COL> new_attached_cols(this);

  for (auto j = 0u; i < num_cols; ++i, ++j) {

    const auto old_out_col = columns[i];
    const auto in_col = attached_columns[j];

    // If the output column is never used, then get rid of it.
    //
    // NOTE(pag): `IsUsed` on a column checks to see if its view is used
    //            in a merge, which would not show up in a normal def-use
    //            list.
    if (!old_out_col->IsUsed()) {
      std::cerr << "K";
      non_local_changes = true;
      in_col->view->is_canonical = false;
      continue;
    }

    // If the old input column is a constant, then propagate it rather than
    // attach it.
    if (in_col->IsConstant()) {

      old_out_col->CopyConstant(in_col);
      old_out_col->ReplaceAllUsesWith(in_col);

      if (old_out_col->IsUsedIgnoreMerges()) {
        std::cerr << "L";
        non_local_changes = true;
      }

      // If our view isn't used in a merge, then we can eliminate this
      // column.
      if (!old_out_col->IsUsed()) {
        continue;
      }

    } else if (in_col->IsConstantRef()) {
      if (!old_out_col->IsConstantRef()) {
        std::cerr << "M";
        non_local_changes = true;
      }
      old_out_col->CopyConstant(in_col);
    }

    auto &out_col = in_to_out[in_col];

    // There was at least one other attached column, or perhaps even a column
    // in the original comparison, that has already been processed, so we
    // can get rid of it.
    if (out_col) {

      if (old_out_col->IsUsedIgnoreMerges()) {
        std::cerr << "N";
        non_local_changes = true;
      }
      old_out_col->ReplaceAllUsesWith(out_col);
      out_col->CopyConstant(old_out_col);

      // Even though we've replaced the old output column, it ends up still
      // being used by a merge, so we need to keep it around.
      if (old_out_col->IsUsed()) {
        new_attached_cols.AddUse(in_col);

      // We're removing this input column, which might make the producer of this
      // column able to remove one of its outputs, so we'll mark it as non-
      // canonical so it can be updated by another pass.
      } else {
        std::cerr << "O";
        non_local_changes = true;
        in_col->view->is_canonical = false;
      }

    // Haven't seen this column yet, keep it around.
    } else {
      out_col = old_out_col;
      new_attached_cols.AddUse(in_col);
    }
  }

  if (sort) {
    new_attached_cols.Sort();
  }

  for (auto in_col : new_attached_cols) {
    const auto old_out_col = in_to_out[in_col];
    const auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
    new_out_col->CopyConstant(old_out_col);
  }

  attached_columns.Swap(new_attached_cols);
  columns.Swap(new_output_cols);

  hash = 0;
  is_canonical = true;

  if (!CheckAllViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  return non_local_changes;
}

// Equality over constraints is structural.
//
// NOTE(pag): The two inputs to the comparison being tested aren't always
//            ordered; however, equality testing here assumes ordering.
bool Node<QueryConstraint>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {

  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsConstraint();
  if (!that ||
      op != that->op ||
      can_receive_deletions != that->can_receive_deletions ||
      can_produce_deletions != that->can_produce_deletions ||
      columns.Size() != that_->columns.Size() ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
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

}  // namespace hyde
