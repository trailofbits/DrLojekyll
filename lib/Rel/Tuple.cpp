// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

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
bool Node<QueryTuple>::Canonicalize(QueryImpl *query, bool sort) {
  if (is_dead) {
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

  const auto used_in_merge = this->Def<Node<QueryView>>::IsUsed();

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
  if (!used_in_merge &&
      positive_conditions.Empty() &&
      negative_conditions.Empty()) {

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

  in_to_out.clear();

  VIEW *last_view = nullptr;

  // TODO(pag): Come up with a better variant of this, perhaps by putting all
  //            VIEWs leading into INSERTs into a use list, kind of like with
  //            MERGEs.
  //
  //            The issue comes up in `prove_constant.dr` example, where a
  //            CONDitioned TUPLE with only constant values flows into an
  //            INSERT, and then the TUPLE self-eliminates. The CONDitions on
  //            the TUPLE are thus lost.
  auto can_constprop = positive_conditions.Empty() &&
                       negative_conditions.Empty();

  const auto max_i = columns.Size();
  for (auto i = 0u; i < max_i; ++i) {
    const auto in_col = input_columns[i];
    const auto out_col = columns[i];

    if (in_col->IsConstant()) {
      if (can_constprop) {
        if (out_col->IsUsedIgnoreMerges()) {
          non_local_changes = true;
        }
        out_col->ReplaceAllUsesWith(in_col);
      }

    // Make sure all non-constant inputs come from the same VIEW.
    } else if (last_view) {
      assert(last_view == in_col->view);

    // Find the incoming VIEW.
    } else {
      last_view = in_col->view;
    }

    if (!out_col->IsUsed()) {
      is_canonical = false;
      continue;  // We can remove this column.
    }

    auto &prev_out_col = in_to_out[in_col];
    if (prev_out_col) {
      if (out_col->IsUsedIgnoreMerges()) {
        non_local_changes = true;
      }
      out_col->ReplaceAllUsesWith(prev_out_col);

      if (!out_col->IsUsed()) {
        is_canonical = false;
        continue;  // Removing this column.
      }

    } else {
      prev_out_col = out_col;
    }
  }

  // If this tuple is forwarding the values of something else along, and if it
  // is the only user of that other thing, then forward those values along,
  // otherwise we'll depend on CSE to try to merge this tuple with any other
  // equivalent tuples.
  //
  // The check for the number of uses on the last view helps us avoid the
  // situation where we have the following triangle dataflow pattern leading
  // into a JOIN.
  auto all_from_same_view = last_view && 1 == last_view->NumUses();

  // All inputs are constants, or their corresponding outputs are not used,
  // or both.
  if (!last_view) {
    all_from_same_view = false;
  }

  // We can "merge" with the source tuple. Because it only has one use, i.e.
  // this TUPLE, we can give our conditions to it.
  if (all_from_same_view) {
    if (!positive_conditions.Empty()) {
      for (auto cond : positive_conditions) {
        last_view->positive_conditions.AddUse(cond);
      }
    }

    if (!negative_conditions.Empty()) {
      for (auto cond : negative_conditions) {
        last_view->negative_conditions.AddUse(cond);
      }
    }

    last_view->OrderConditions();

    non_local_changes = true;
    for (auto [in_col, out_col] : in_to_out) {
      out_col->ReplaceAllUsesWith(in_col);
    }

    if (last_view->columns.Size() == columns.Size()) {
      this->Def<Node<QueryView>>::ReplaceAllUsesWith(last_view);
    }

    is_dead = true;
    is_canonical = true;
    hash = 0;
    return true;  // We made changes, i.e. we deleted ourself.
  }

  // This is used by a MERGE, leave it as-is.
  if (used_in_merge || in_to_out.size() == columns.Size()) {
    is_canonical = true;
    hash = 0;
    return non_local_changes;
  }

  DefList<COL> new_output_cols(this);
  UseList<COL> new_input_cols(this);

  for (auto col : input_columns) {
    if (auto out_col = in_to_out[col]; out_col) {
      new_input_cols.AddUse(col);
    }
  }

  if (sort) {
    new_input_cols.Sort();
  }

  for (auto col : new_input_cols) {
    auto old_out_col = in_to_out[col];
    auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
  }

  input_columns.Swap(new_input_cols);
  columns.Swap(new_output_cols);

  if (!CheckAllViewsMatch(input_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
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
