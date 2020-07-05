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
    local_hash ^= __builtin_rotateright64(local_hash, 33) * col->Hash();
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

  if (is_canonical) {
    return false;
  }

  if (valid == VIEW::kValid && !CheckAllViewsMatch(input_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    is_canonical = true;
    return false;
  }

  assert(attached_columns.Empty());

  // If this tuple is not used in a MERGE, then try to figure out if we can
  // eliminate this tuple altogether by seeing who uses it. If none of the
  // users are a JOIN, then we can eliminate this tuple.
  //
  // The check for JOINs is important because if we eliminated the TUPLE, then
  // the INPUT would flow into the same JOIN pivot columns multiple times, and
  // then the JOIN would be canonicalized away into nothing.
  //
  //                 |
  //         .-<-- INPUT -->--.
  //        /                 |
  //    JOIN --<-- TUPLE --<--'
  //
  // NOTE(pag): It's possible that an unused output column holds onto a
  //            critical input column whose condition might decode whether
  //            or not anything will ever actually flow. Thus, we can only
  //            eliminate this TUPLE if all of its outputs are used.
  const auto used_in_merge = this->Def<Node<QueryView>>::IsUsed();
  if (!used_in_merge && !IntroducesControlDependency() && AllColumnsAreUsed()) {

    auto any_user_is_join = false;
    for (auto col : columns) {
      col->ForEachUse<VIEW>([&] (VIEW *user, COL *) {
        if (user->AsJoin()) {
          any_user_is_join = true;
        }
      });

      if (any_user_is_join) {
        break;
      }
    }

    // We can eliminate this tuple as none of the users are JOINs, and this
    // tuple doesn't feed into a MERGE.
    if (!any_user_is_join) {
      for (auto i = 0u, max_i = columns.Size(); i < max_i; ++i) {
        columns[i]->ReplaceAllUsesWith(input_columns[i]);
        input_columns[i]->view->is_canonical = false;
      }

      hash = 0;
      is_canonical = true;
      is_used = false;
      is_dead = true;
      input_columns.Clear();
      return true;  // Non-local changes.
    }
  }

  bool non_local_changes = false;
  bool has_unused_columns = false;

  in_to_out.clear();

  VIEW *incoming_view = nullptr;

  const auto max_i = columns.Size();
  for (auto i = 0u; i < max_i; ++i) {
    const auto in_col = input_columns[i];
    const auto out_col = columns[i];
    const auto [changed, can_remove] = CanonicalizeColumnPair(
        in_col, out_col, opt, true  /* update_in_to_out */);

    non_local_changes = non_local_changes || changed;
    has_unused_columns = has_unused_columns || can_remove;

    if (!in_col->IsConstant()) {
      incoming_view = in_col->view;
    }
  }

  // We can replace the view with `incoming_view` if `incoming_view` only has
  // one use (this TUPLE), if order of columns doesn't matter, and if all
  // columns are preserved.
  if (incoming_view &&
      incoming_view->NumUses() == 1 &&
      (opt.can_remove_unused_columns ||
       incoming_view->columns.Size() == columns.Size())) {

    assert(incoming_view != this);

    // If this view is used in a MERGE then it's only safe to replace it with
    // `incoming_view` if the columns are in the same order.
    if (this->Def<Node<QueryView>>::IsUsed()) {
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
          input_columns.Clear();
          incoming_view->CopyConditions(this);
          incoming_view->OrderConditions();
          incoming_view->OrderConditions();
          ReplaceAllUsesWith(incoming_view);
          is_dead = true;
          return true;
        }
      }

    // `this` is not used in a MERGE, so there are no ordering constraints
    // on `incoming_view`.
    } else {
      input_columns.Clear();
      incoming_view->CopyConditions(this);
      incoming_view->OrderConditions();
      incoming_view->OrderConditions();

      for (auto [in_col, out_col] : in_to_out) {
        out_col->ReplaceAllUsesWith(in_col);
      }

      // NOTE(pag): There might be weak uses of `this`, i.e. in a JOIN.
      ReplaceAllUsesWith(incoming_view);

      is_dead = true;
      return true;
    }
  }

  if (has_unused_columns) {

    DefList<COL> new_output_cols(this);
    UseList<COL> new_input_cols(this);

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

      auto new_out_col = new_output_cols.Create(out_col->var, this, out_col->id);
      out_col->ReplaceAllUsesWith(new_out_col);
      new_out_col->CopyConstant(out_col);
      new_input_cols.AddUse(in_col);

      if (!prev_out_col) {
        prev_out_col = new_out_col;
      }
    }

    input_columns.Swap(new_input_cols);
    columns.Swap(new_output_cols);
  }

  if (!CheckAllViewsMatch(input_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  hash = 0;
  is_canonical = true;
  return non_local_changes;





//  // If this tuple is forwarding the values of something else along, and if it
//  // is the only user of that other thing, then forward those values along,
//  // otherwise we'll depend on CSE to try to merge this tuple with any other
//  // equivalent tuples.
//  //
//  // The check for the number of uses on the last view helps us avoid the
//  // situation where we have the following triangle dataflow pattern leading
//  // into a JOIN.
//  auto all_from_same_view = last_view && 1 == last_view->NumUses();
//
//  // All inputs are constants, or their corresponding outputs are not used,
//  // or both.
//  if (!last_view) {
//    all_from_same_view = false;
//  }
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
