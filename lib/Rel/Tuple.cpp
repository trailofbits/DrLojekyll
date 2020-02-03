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

// Used to tell if a tuple looks canonical. While canonicalizing a tuple, we
// may affect the query graph and accidentally convert it back into a non-
// canonical form.
bool Node<QueryTuple>::LooksCanonical(void) const {

  // Check if the input columns are in sorted order, and that they are all
  // unique. If they are, then this tuple is in a canonical form already.
  COL *prev_col = nullptr;
  for (auto col : input_columns) {

    // Out-of-order (sorted order).
    if (prev_col > col) {
      return false;

      // If there's a redundant column, then it's only non-canonical if this
      // tuple isn't being used by a merge.
    } else if (prev_col == col && !this->Def<Node<QueryView>>::IsUsed()) {
      return false;
    }

    prev_col = col;
  }

  // Check that all output columns are used. If they aren't used, then get
  // rid of them.
  for (auto col : columns) {
    if (!col->IsUsed()) {
      return false;
    }
  }

  return true;
}

// Put this tuple into a canonical form, which will make comparisons and
// replacements easier. Because comparisons are mostly pointer-based, the
// canonical form of this tuple is one where all input columns are sorted,
// deduplicated, and where all output columns are guaranteed to be used.
bool Node<QueryTuple>::Canonicalize(QueryImpl *query) {
  std::unordered_map<COL *, COL *> in_to_out;
  auto non_local_changes = false;

  // It's feasible that there's a cycle in the graph which would have triggered
  // an update to an input, thus requiring us to try again.
  if (!LooksCanonical()) {
    non_local_changes = true;

    in_to_out.clear();

    DefList<COL> new_output_cols(this);
    UseList<COL> new_input_cols(this);

    for (auto i = 0u; i < columns.Size(); ++i) {
      const auto old_out_col = columns[i];

      // Constant propagation first, as the later `IsUsed` check on
      // `old_out_col` is sensitive to merges.
      const auto in_col = input_columns[i];
      if (in_col->IsConstant() && old_out_col->IsUsedIgnoreMerges()) {
        old_out_col->ReplaceAllUsesWith(in_col);
      }

      // If the output column is never used, then get rid of it.
      //
      // NOTE(pag): `IsUsed` on a column checks to see if its view is used
      //            in a merge, which would not show up in a normal def-use
      //            list.
      if (!old_out_col->IsUsed()) {
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
        new_input_cols.AddUse(in_col);
      }
    }

    new_input_cols.Sort();

    for (auto in_col : new_input_cols) {
      const auto old_out_col = in_to_out[in_col];
      const auto new_out_col = new_output_cols.Create(
          old_out_col->var, this, old_out_col->id);
      old_out_col->ReplaceAllUsesWith(new_out_col);
    }

    input_columns.Swap(new_input_cols);
    columns.Swap(new_output_cols);
  }

  if (non_local_changes) {
    hash = 0;
    depth = 0;
  }

  assert(CheckAllViewsMatch(input_columns, attached_columns));

  is_canonical = true;
  return non_local_changes;
}

// Equality over tuples are pointer-based.
bool Node<QueryTuple>::Equals(EqualitySet &, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsTuple();
  return that &&
         columns.Size() == that->columns.Size() &&
         ColumnsEq(input_columns, that->input_columns);
}

}  // namespace hyde
