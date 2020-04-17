// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryTuple>::~Node(void) {}

Node<QueryTuple> *Node<QueryTuple>::AsTuple(void) noexcept {
  return this;
}

uint64_t Node<QueryTuple>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = columns.Size();

  // Mix in the hashes of the tuple by columns; these are ordered.
  for (auto col : input_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ col->Hash();
  }

  hash <<= 4;
  hash |= 8;
  return hash;
}

// Put this tuple into a canonical form, which will make comparisons and
// replacements easier. Because comparisons are mostly pointer-based, the
// canonical form of this tuple is one where all input columns are sorted,
// deduplicated, and where all output columns are guaranteed to be used.
bool Node<QueryTuple>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  bool non_local_changes = false;

  std::unordered_map<COL *, COL *> in_to_out;
  VIEW *last_view = nullptr;
  bool all_from_same_view = true;

  const auto max_i = columns.Size();
  for (auto i = 0u; i < max_i; ++i) {
    const auto in_col = input_columns[i];
    const auto out_col = columns[i];
    const auto out_col_is_used_not_in_merge =
        out_col->Def<Node<QueryColumn>>::IsUsed();

    // Constant propagation.
    if (in_col->IsConstant()) {
      if (out_col_is_used_not_in_merge) {
        out_col->ReplaceAllUsesWith(in_col);
        non_local_changes = true;
      }
      continue;
    }

    if (!out_col->IsUsed()) {
      continue;  // Shrinking the number of columns.
    }

    // Keep track if every non-constant, used column comes from the same view,
    // and if so, we'll just push those inputs forward.
    if (!last_view) {
      last_view = in_col->view;

    } else if (in_col->view != last_view) {
      all_from_same_view = false;
    }

    auto &prev_out_col = in_to_out[in_col];
    if (prev_out_col) {
      non_local_changes = true;  // Shrinking the number of columns.

      if (out_col->NumUses() > prev_out_col->NumUses()) {
        prev_out_col->ReplaceAllUsesWith(out_col);
        prev_out_col = out_col;

      } else {
        out_col->ReplaceAllUsesWith(prev_out_col);
      }
    } else {
      prev_out_col = out_col;
    }
  }

  // If we have `check_group_ids`, then this TUPLE might be taking the place
  // of a SELECT, so it needs to be preserved. However, if the incoming view
  // is also a TUPLE that also has `check_group_ids`, then we can safely merge
  // the two.
  if (all_from_same_view && check_group_ids) {
    if (auto from_tuple = last_view->AsTuple();
        !from_tuple || !from_tuple->check_group_ids) {
      all_from_same_view = false;
    }
  }

  if (all_from_same_view) {
    non_local_changes = true;
    for (auto [in_col, out_col] : in_to_out) {
      out_col->ReplaceAllUsesWith(in_col);
    }
  }

  // This is used by a `merge`, leave it as-is, or we just replaced every use
  // of an output column, and so we don't really care about doing any more work.
  if (this->Def<Node<QueryView>>::IsUsed() || all_from_same_view) {
    is_canonical = true;
    hash = 0;
    return non_local_changes;
  }

  input_columns.Sort();

  // Shrinking the number of columns.
  if (max_i > in_to_out.size()) {

    DefList<COL> new_output_cols(this);
    for (auto in_col : input_columns) {
      if (const auto old_out_col = in_to_out[in_col]) {
        const auto new_out_col = new_output_cols.Create(
            old_out_col->var, this, old_out_col->id);
        old_out_col->ReplaceAllUsesWith(new_out_col);
      }
    }

    UseList<COL> new_input_cols(this);
    for (auto in_col : input_columns) {
      if (in_to_out[in_col]) {
        new_input_cols.AddUse(in_col);
      }
    }

    columns.Swap(new_output_cols);
    input_columns.Swap(new_input_cols);

  // Not shrinking, just re-ordering. Note that `input_columns` is sorted
  // already.
  } else {
    std::unordered_map<COL *, unsigned> out_to_order;
    auto i = 0u;
    for (auto in_col : input_columns) {
      out_to_order[in_to_out[in_col]] = i++;
    }
    columns.Sort([&out_to_order] (COL *a, COL *b) {
      return out_to_order[a] < out_to_order[b];
    });
  }

  assert(CheckAllViewsMatch(input_columns, attached_columns));

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over tuples are pointer-based.
bool Node<QueryTuple>::Equals(EqualitySet &, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsTuple();
  return that &&
         columns.Size() == that->columns.Size() &&
         ColumnsEq(input_columns, that->input_columns) &&
         !InsertSetsOverlap(this, that);
}

}  // namespace hyde
