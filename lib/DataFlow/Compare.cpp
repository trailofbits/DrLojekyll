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
bool Node<QueryCompare>::Canonicalize(QueryImpl *query,
                                      const OptimizationContext &opt) {
  if (is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  if (valid == VIEW::kValid &&
      !CheckIncomingViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    return false;
  }

  // Check to see if the input columns are ordered correctly. We can reorder
  // them only in the case of (in)equality comparisons.
  const auto is_equality = ComparisonOperator::kEqual == op;

  auto non_local_changes = false;
  std::tie(is_canonical, non_local_changes) =
      CanonicalizeAttachedColumns((is_equality ? 1u : 2u), opt);

  // Check if the result is used (ignoring merges).
  //
  // NOTE(pag): This should be checked before guarding with a TUPLE, otherwise
  //            if we end up guarding with a TUPLE then it will definitely look
  //            used.
  const auto lhs_out_col = columns[0u];
  const auto rhs_out_col = columns[is_equality ? 0u : 1u];
  const auto lhs_col = input_columns[0u];
  const auto rhs_col = input_columns[1u];
  const auto lhs_out_is_const = lhs_out_col->IsConstantRef();
  const auto rhs_out_is_const = lhs_out_col->IsConstantRef();
  const auto lhs_is_const = lhs_col->IsConstantOrConstantRef();
  const auto rhs_is_const = rhs_col->IsConstantOrConstantRef();

  if ((lhs_col == rhs_col) ||
      (is_equality && lhs_out_is_const != (lhs_is_const || rhs_is_const)) ||
      (!is_equality && (lhs_out_is_const != lhs_is_const ||
                        rhs_out_is_const != rhs_is_const)) ||
      (opt.can_replace_inputs_with_constants &&
       (lhs_is_const || rhs_is_const))) {
    hash = 0;
    is_canonical = false;
  }

  // Check to see if this comparison is redundant, i.e. it does the same
  // thing as its predecessor. This can be an artifact of the way we apply
  // inequality comparisons in the builder.
  //
  // TODO(pag): We should probably re-think the repeated application of
  //            inequality comparisons as they might introduce extra data
  //            dependencies.
  const auto incoming_view = GetIncomingView(input_columns, attached_columns);
  CMP * const incoming_cmp = incoming_view ? incoming_view->AsCompare() : nullptr;
  const auto num_cols = columns.Size();
  if (incoming_cmp && incoming_cmp->op == this->op &&
      incoming_cmp->columns.Size() == num_cols &&
      !sets_condition && positive_conditions.Empty() &&
      negative_conditions.Empty()) {

    bool is_redundant = true;
    auto i = 0u;
    for (auto max_i = input_columns.Size(); i < max_i; ++i) {
      if (input_columns[i] != incoming_cmp->columns[i]) {
        is_redundant = false;
        break;
      }
    }
    for (auto a = 0u; is_redundant && i < num_cols; ++a, ++i) {
      if (attached_columns[a] != incoming_cmp->columns[i]) {
        is_redundant = false;
        break;
      }
    }

    // This comparison is redundant; it does exactly the same thing as its
    // predecessor.
    if (is_redundant) {
      ReplaceAllUsesWith(incoming_cmp);
      PrepareToDelete();
      return true;
    }
  }

  if (is_canonical) {
    return non_local_changes;
  }

  // If this view is used by a merge then we're not allowed to re-order/remove
  // the columns. Instead, what we can do is create a tuple that will maintain
  // the ordering, and the canonicalize the join order below that tuple.
  //  (void) GuardWithTuple(query);

  in_to_out.clear();
  auto [changed_lhs, can_remove_lhs] =
      CanonicalizeColumnPair(lhs_col, lhs_out_col, opt);

  auto [changed_rhs, can_remove_rhs] =
      CanonicalizeColumnPair(rhs_col, rhs_out_col, opt);

  (void) can_remove_lhs;  // Can't remove these.
  (void) can_remove_rhs;

  if (changed_lhs || changed_rhs) {
    non_local_changes = true;
  }

  DefList<COL> new_columns(this);

  auto i = 2u;

  // For equality, there's only one output `Def` for the two inputs, so we
  // can always sort the inputs.
  if (is_equality) {
    i = 1u;

    // This comparison is no longer needed.
    if (lhs_col == rhs_col) {
      auto tuple = query->tuples.Create();
#ifndef NDEBUG
      tuple->producer = "DEAD-CMP(" + producer + ")";
#endif

      for (auto col : columns) {
        tuple->columns.Create(col->var, tuple, col->id);
      }

      // NOTE(pag): Don't apply `input_col` to `lhs_col`; we'll let the tuple
      //            canonicalization do any simplification for us.
      tuple->input_columns.AddUse(lhs_col);
      tuple->columns[0]->CopyConstant(lhs_col);

      for (auto j = 0u; j < attached_columns.Size(); ++j, ++i) {
        tuple->input_columns.AddUse(attached_columns[j]);
        tuple->columns[i]->CopyConstant(attached_columns[j]);
      }

      ReplaceAllUsesWith(tuple);
      return true;
    }

    const auto new_lhs_out =
        new_columns.Create(lhs_out_col->var, this, lhs_out_col->id);

    in_to_out.emplace(lhs_col, new_lhs_out);
    in_to_out.emplace(rhs_col, new_lhs_out);
    new_lhs_out->CopyConstant(lhs_out_col);
    lhs_out_col->ReplaceAllUsesWith(new_lhs_out);

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

      auto err = opt.log.Append(outer_range);
      err << "Unsatisfiable inequality between '" << columns[0]->var
          << "' and '" << columns[1]->var
          << "' ends up comparing the same values";

      err.Note(lhs_col->var.SpellingRange(), lhs_col->var.SpellingRange())
          << "Value comes from here";

      valid = VIEW::kInvalidBeforeCanonicalize;
      invalid_var = lhs_col->var;
      return non_local_changes;
    }

    // We don't need to re-order anything, but to be uniform with the rest and
    // possible sorting of attached columns, we will create a new set of
    // output columns.
    const auto new_lhs_out =
        new_columns.Create(lhs_out_col->var, this, lhs_out_col->id);

    const auto new_rhs_out =
        new_columns.Create(rhs_out_col->var, this, rhs_out_col->id);

    new_lhs_out->CopyConstant(lhs_out_col);
    new_rhs_out->CopyConstant(rhs_out_col);

    in_to_out.emplace(lhs_col, new_lhs_out);
    in_to_out.emplace(rhs_col, new_rhs_out);
    lhs_out_col->ReplaceAllUsesWith(new_lhs_out);
    rhs_out_col->ReplaceAllUsesWith(new_rhs_out);
  }

  assert(i == 1u || i == 2u);
  assert((num_cols - i) == attached_columns.Size());

  // Little functor that gets us the normal input column, or the constant
  // propagated one.
  auto did_constprop = false;
  auto constprop_col = [&](COL *in_col) -> COL * {
    if (auto as_const = in_col->AsConstant();
        as_const && opt.can_replace_inputs_with_constants) {
      if (!in_col->IsConstant()) {
        did_constprop = true;
        non_local_changes = true;
        in_col->view->is_canonical = false;
      }
      return as_const;
    } else {
      return in_col;
    }
  };

  UseList<COL> new_input_columns(this);
  UseList<COL> new_attached_columns(this);

  new_input_columns.AddUse(constprop_col(lhs_col));
  new_input_columns.AddUse(constprop_col(rhs_col));

  for (auto j = 0u; i < num_cols; ++i, ++j) {

    const auto out_col = columns[i];
    const auto out_col_is_used = out_col->IsUsed();
    const auto in_col = attached_columns[j];
    auto &prev_out_col = in_to_out[in_col];

    // There was at least one other attached column, or perhaps even a column
    // in the original comparison, that has already been processed, so we
    // can get rid of it.
    if (prev_out_col) {
      if (out_col->IsUsedIgnoreMerges()) {
        non_local_changes = true;
      }

      out_col->ReplaceAllUsesWith(prev_out_col);

      // It's safe to remove this column without consulting
      // `opt.can_remove_unused_columns` because any control dependencies
      // induced by `in_col` will already be present due to its prior use.
      if (!out_col_is_used) {
        non_local_changes = true;
        in_col->view->is_canonical = false;
        continue;
      }
    }

    // If the output column is never used, and if we're allowed to remove
    // unused columns, then get rid of it.
    //
    // NOTE(pag): `IsUsed` on a column checks to see if its view is used
    //            in a merge, which would not show up in a normal def-use
    //            list.
    if (opt.can_remove_unused_columns && !out_col_is_used) {
      non_local_changes = true;
      in_col->view->is_canonical = false;
      continue;
    }

    const auto new_out_col =
        new_columns.Create(out_col->var, this, out_col->id);

    new_out_col->CopyConstant(out_col);
    out_col->ReplaceAllUsesWith(new_out_col);

    if (!prev_out_col) {
      prev_out_col = new_out_col;
    }

    new_attached_columns.AddUse(constprop_col(in_col));
  }

  input_columns.Swap(new_input_columns);
  attached_columns.Swap(new_attached_columns);
  columns.Swap(new_columns);

  // We may have just broken a dependency.
  if (did_constprop) {
    auto prev_incoming_view =
        GetIncomingView(new_input_columns, new_attached_columns);
    auto curr_incoming_view = GetIncomingView(input_columns, attached_columns);

    if (prev_incoming_view != curr_incoming_view) {
      assert(prev_incoming_view != nullptr);
      assert(!curr_incoming_view);

      COND *cond = nullptr;

      // Preferentially use only the old input columns (in `new_input_columns`)
      // for the condtion.
      if (new_attached_columns.Empty() ||
          prev_incoming_view == GetIncomingView(new_input_columns)) {
        cond = CreateOrInheritConditionOnView(query, prev_incoming_view,
                                              std::move(new_input_columns));

      // Otherwise use the attached columns.
      } else if (new_input_columns.Empty() ||
                 prev_incoming_view == GetIncomingView(new_attached_columns)) {
        cond = CreateOrInheritConditionOnView(query, prev_incoming_view,
                                              std::move(new_attached_columns));

      // Oof, maybe both are needed :-(
      } else {
        UseList<COL> combined_cols(this);
        for (auto col : new_input_columns) {
          if (!col->IsConstant()) {
            combined_cols.AddUse(col);
          }
        }
        assert(!combined_cols.Empty());
        cond = CreateOrInheritConditionOnView(query, prev_incoming_view,
                                              std::move(combined_cols));
      }

      assert(cond);
      positive_conditions.AddUse(cond);
      cond->positive_users.AddUse(this);

      is_canonical = false;
      return Canonicalize(query, opt);  // Recursively apply.
    }
  }

  hash = 0;
  is_canonical = true;

  if (!CheckIncomingViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  return non_local_changes;
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
