// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

#include "Optimize.h"

namespace hyde {

Node<QueryTuple>::~Node(void) {}

Node<QueryTuple> *Node<QueryTuple>::AsTuple(void) noexcept {
  return this;
}

uint64_t Node<QueryTuple>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = HashInit();
  auto local_hash = hash;

  // Mix in the hashes of the tuple by columns; these are ordered.
  for (auto col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 33) * col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

// Put this tuple into a canonical form, which will make comparisons and
// replacements easier. Because comparisons are mostly pointer-based, the
// canonical form of this tuple is one where all input columns are sorted,
// deduplicated, and where all output columns are guaranteed to be used.
bool Node<QueryTuple>::Canonicalize(
    QueryImpl *query, const OptimizationContext &opt) {

  if (is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  bool has_replaceable_constref_inputs = false;
  if (is_canonical && opt.can_replace_inputs_with_constants) {

    for (auto in_col : input_columns) {
      if (in_col->IsConstantRef()) {
        is_canonical = false;
        has_replaceable_constref_inputs = true;
        break;
      }
    }
  }

  assert(attached_columns.Empty());
  if (valid == VIEW::kValid && !CheckIncomingViewsMatch(input_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    is_canonical = true;
    return false;
  }

  const auto is_used_in_merge = this->Def<Node<QueryView>>::IsUsed();
  const auto incoming_view = GetIncomingView(input_columns);
  const auto introduces_control_dep = IntroducesControlDependency();
  const auto max_i = columns.Size();

  // This tuple forwards all its data to something else; merge upward. The one
  // case where we can't forward our data along is when the outgoing view is
  // a JOIN, because otherwise we might have a diamond pattern where there the
  // tuples exist to introduce two separate flows into a JOIN, e.g. as in
  // `transitive_closure.dr`.
  if (!is_used_in_merge &&
      !introduces_control_dep &&
      !sets_condition &&
      AllColumnsAreUsed()) {
    if (auto outgoing_view = OnlyUser();
        outgoing_view && !outgoing_view->AsJoin()) {
      assert(!outgoing_view->AsMerge());

      for (auto i = 0u; i < max_i; ++i) {
        const auto in_col = input_columns[i];
        const auto out_col = columns[i];
        out_col->ReplaceAllUsesWith(in_col);
        in_col->view->is_canonical = false;
      }

      PrepareToDelete();
      return true;
    }
  }

  if (is_canonical) {
    return false;
  }

  bool non_local_changes = false;
  bool has_unused_columns = false;

  in_to_out.clear();
  for (auto i = 0u; i < max_i; ++i) {
    const auto in_col = input_columns[i];
    const auto out_col = columns[i];
    const auto out_col_is_directly_used = out_col->IsUsedIgnoreMerges();
    auto &prev_out_col = in_to_out[in_col];

    if (prev_out_col) {
      if (out_col_is_directly_used) {
        non_local_changes = true;
      }

      out_col->ReplaceAllUsesWith(prev_out_col);

      // If the input column is represented more than once, then we can remove
      // any secondary representations safely.
      if (opt.can_remove_unused_columns && !out_col->IsUsed()) {
        has_unused_columns = true;
      }

    } else {
      prev_out_col = out_col;
    }

    const auto [changed, can_remove] = CanonicalizeColumnPair(
        in_col, out_col, opt);

    if (opt.can_replace_inputs_with_constants && in_col->IsConstantRef()) {
      has_replaceable_constref_inputs = true;
    }

    non_local_changes = non_local_changes || changed;
    has_unused_columns = has_unused_columns || can_remove;
  }

  // We can replace the view with `incoming_view` if `incoming_view` only has
  // one use (this TUPLE), if order of columns doesn't matter, and if all
  // columns are preserved.
  if (incoming_view &&
      incoming_view->NumUses() == 1 &&
      incoming_view->columns.Size() == columns.Size() &&
      !introduces_control_dep) {

    assert(incoming_view != this);

    // If this view is used in a MERGE then it's only safe to replace it with
    // `incoming_view` if the columns are in the same order.
    if (is_used_in_merge) {
      if (incoming_view->columns.Size() == columns.Size()) {

        auto i = 0u;
        auto all_in_order = true;
        for (auto in_col : input_columns) {
          if (in_col->IsConstant() || in_col->Index() != i) {
            all_in_order = false;
            break;
          }
          ++i;
        }

        if (all_in_order) {
          ReplaceAllUsesWith(incoming_view);
          return true;
        }
      }

    // `this` is not used in a MERGE, so there are no ordering constraints
    // on `incoming_view`.
    } else {
      CopyTestedConditionsTo(incoming_view);
      TransferSetConditionTo(incoming_view);

      for (auto [in_col, out_col] : in_to_out) {
        out_col->ReplaceAllUsesWith(in_col);
      }

      // NOTE(pag): There might be weak uses of `this`, i.e. in a JOIN.
      this->Def<Node<QueryView>>::ReplaceAllUsesWith(incoming_view);
      PrepareToDelete();
      return true;
    }
  }

  if (has_unused_columns || has_replaceable_constref_inputs) {

    DefList<COL> new_columns(this);
    UseList<COL> new_input_columns(this);

    in_to_out.clear();

    for (auto i = 0u; i < max_i; ++i) {
      const auto in_col = input_columns[i];
      const auto out_col = columns[i];
      const auto out_col_is_used = out_col->IsUsed();
      auto &prev_out_col = in_to_out[in_col];

      if (prev_out_col && !out_col_is_used) {
        non_local_changes = true;
        continue;  // Always safe to remove; it's already represented.
      }

      // Remove it.
      if (opt.can_remove_unused_columns && !out_col_is_used) {
        in_col->view->is_canonical = false;
        non_local_changes = true;
        continue;
      }

      auto new_out_col = new_columns.Create(out_col->var, this, out_col->id);
      out_col->ReplaceAllUsesWith(new_out_col);
      new_out_col->CopyConstant(out_col);

      if (opt.can_replace_inputs_with_constants &&
          in_col->IsConstantRef()) {

        new_input_columns.AddUse(in_col->AsConstant());
        in_col->view->is_canonical = false;
        non_local_changes = true;

      } else {
        new_input_columns.AddUse(in_col);
      }

      if (!prev_out_col) {
        prev_out_col = new_out_col;
      }
    }

    input_columns.Swap(new_input_columns);
    columns.Swap(new_columns);

    if (auto new_incoming_view = GetIncomingView(input_columns);
        new_incoming_view != incoming_view) {
      assert(incoming_view);
      assert(!new_incoming_view);
      COND *condition = CreateOrInheritConditionOnView(
          query, incoming_view, std::move(new_input_columns));
      positive_conditions.AddUse(condition);
      condition->positive_users.AddUse(this);
    }
  }

  if (!CheckIncomingViewsMatch(input_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  // **** Eliminate trivial cycles on unions ****
  if (auto onlyUser = this->OnlyUser();
      // There is an incoming view (not all inputs are constant)
      incoming_view &&
      // There is only a single user view, which is the same as the incoming
      // view
      onlyUser && onlyUser == incoming_view &&
      // The number of columns in the incoming view matches the number of
      // columns in the tuple
      incoming_view->columns.Size() == this->columns.Size()) {
    // The order of the input columns matches the output column order
    // for all columns
    bool ordered_cols = true;
    for (auto i = 0u; i < max_i; ++i) {
      auto *in_col = incoming_view->columns[i];
      auto *out_col = this->columns[i];
      if (in_col->Index() != out_col->Index()) {
        ordered_cols = false;
        break;
      }
    }
    if (ordered_cols) {
      CopyTestedConditionsTo(incoming_view);
      TransferSetConditionTo(incoming_view);

      for (auto [in_col, out_col] : in_to_out) {
        out_col->ReplaceAllUsesWith(in_col);
      }

      // NOTE(pag): There might be weak uses of `this`, i.e. in a JOIN.
      this->Def<Node<QueryView>>::ReplaceAllUsesWith(incoming_view);
      this->PrepareToDelete();
      return true;
    }
  }

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over tuples is structural.
bool Node<QueryTuple>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsTuple();
  if (!that ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
      can_receive_deletions != that->can_receive_deletions ||
      can_produce_deletions != that->can_produce_deletions ||
      columns.Size() != that->columns.Size() ||
      InsertSetsOverlap(this, that)) {
    return false;
  }

  eq.Insert(this, that);
  if (!ColumnsEq(eq, input_columns, that->input_columns)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

}  // namespace hyde
