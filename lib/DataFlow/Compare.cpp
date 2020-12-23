// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Util/EqualitySet.h>

#include "Optimize.h"
#include "Query.h"

namespace hyde {

Node<QueryCompare>::~Node(void) {}

Node<QueryCompare> *Node<QueryCompare>::AsCompare(void) noexcept {
  return this;
}

uint64_t Node<QueryCompare>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Base case for recursion.
  hash = HashInit() ^ static_cast<unsigned>(op);
  assert(hash != 0);

  auto local_hash = hash;

  for (auto col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 53) * col->Hash();
  }

  for (auto col : attached_columns) {
    local_hash ^= RotateRight64(local_hash, 43) * col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

// Put this constraint into a canonical form, which will make comparisons and
// replacements easier. If this constraint's operator is unordered, then we
// sort the inputs to make comparisons trivial. We also need to put the
// "trailing" outputs into the proper order.
bool Node<QueryCompare>::Canonicalize(
    QueryImpl *query, const OptimizationContext &opt, const ErrorLog &log) {

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
  auto first_attached_col = 1u;

  is_canonical = true;  // Updated by `CanonicalizeColumn`.
  in_to_out.clear();  // Filled in by `CanonicalizeColumn`.
  Discoveries has = {};

  // NOTE(pag): This may update `is_canonical`.
  const auto incoming_view = PullDataFromBeyondTrivialTuples(
      GetIncomingView(input_columns, attached_columns),
      input_columns, attached_columns);

  // Equality comparisons are merged into a single output.
  if (op == ComparisonOperator::kEqual) {
    has = CanonicalizeColumn(opt, input_columns[0], columns[0], false, has);
    has = CanonicalizeColumn(opt, input_columns[1], columns[0], false, has);

    // This is trivially satisfiable, create a tuple that forwards all of
    // the columns. We'll defer to the tuple's canonicalizer to continue
    // constant propagation.
    if (input_columns[0] == input_columns[1]) {
      auto tuple = query->tuples.Create();
      tuple->color = color;
#ifndef NDEBUG
      tuple->producer = "TRIVIAL-CMP";
#endif
      tuple->columns.Create(columns[0]->var, tuple, columns[0]->id, 0u);
      tuple->input_columns.AddUse(input_columns[0]);
      for (auto i = 1u; i < num_cols; ++i) {
        tuple->columns.Create(columns[i]->var, tuple, columns[i]->id, i);
        tuple->input_columns.AddUse(attached_columns[i - 1u]);
      }

      // NOTE(pag): This will transfer/fixup conditions.
      this->ReplaceAllUsesWith(tuple);
      return true;
    }

  // Inequality comparisons go to separate outputs.
  } else {
    has = CanonicalizeColumn(opt, input_columns[0], columns[0], false, has);
    has = CanonicalizeColumn(opt, input_columns[1], columns[1], false, has);
    first_attached_col = 2u;

    // This condition is unsatisfiable.
    if (input_columns[0] == input_columns[1]) {
      auto err = log.Append(spelling_range);
      err << "Unsatisfiable comparison between '" << columns[0]->var
          << "' and '" << columns[1]->var
          << "' ends up comparing the same values";

      err.Note(input_columns[0]->var.SpellingRange(),
               input_columns[0]->var.SpellingRange())
          << "Left-hand side value comes from here";

      err.Note(input_columns[1]->var.SpellingRange(),
               input_columns[1]->var.SpellingRange())
          << "Right-hand side value comes from here";

      valid = VIEW::kInvalidBeforeCanonicalize;
      invalid_var = input_columns[0]->var;
      return false;
    }
  }

  // Do constant propagation on the attached columns.
  for (auto i = first_attached_col, j = 0u; i < num_cols; ++i, ++j) {
    has = CanonicalizeColumn(opt, attached_columns[j], columns[i], true, has);
  }

  // Nothing changed.
  if (is_canonical) {
    return has.non_local_changes;
  }

  // There is at least one output of our compare that is a constant and that
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

  COL *new_lhs_out = nullptr;
  COL *new_rhs_out = nullptr;

  // Create and keep the new versions of the output columns.
  if (op == ComparisonOperator::kEqual) {
    new_lhs_out = new_columns.Create(columns[0]->var, this, columns[0]->id, 0u);
    new_rhs_out = new_lhs_out;

    columns[0]->ReplaceAllUsesWith(new_lhs_out);
  } else {
    new_lhs_out = new_columns.Create(columns[0]->var, this, columns[0]->id, 0u);
    new_rhs_out = new_columns.Create(columns[1]->var, this, columns[1]->id, 1u);

    columns[0]->ReplaceAllUsesWith(new_lhs_out);
    columns[1]->ReplaceAllUsesWith(new_rhs_out);
  }

  new_input_columns.AddUse(input_columns[0]->TryResolveToConstant());
  new_input_columns.AddUse(input_columns[1]->TryResolveToConstant());

  // Now bring in the attached columns, and only those that we need.
  for (auto j = first_attached_col, i = 0u; j < num_cols; ++j, ++i) {
    const auto col = columns[j];
    if (col->IsUsed()) {
      const auto new_col = new_columns.Create(
          col->var, this, col->id, new_columns.Size());
      col->ReplaceAllUsesWith(new_col);
      new_attached_columns.AddUse(attached_columns[i]->TryResolveToConstant());

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

// Equality over compares is structural.
//
// NOTE(pag): The two inputs to the comparison being tested aren't always
//            ordered; however, equality testing here assumes ordering.
bool Node<QueryCompare>::Equals(EqualitySet &eq,
                                Node<QueryView> *that_) noexcept {

  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsCompare();
  if (!that || op != that->op ||
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
