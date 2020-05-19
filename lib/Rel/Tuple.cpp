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
  hash |= query::kTupleId;
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

  // Figure out how many input columns are really needed. A single input
  // columns might be able to multiple output column, thus leading to
  // `in_to_out.size()` matching `columns.Size()`.
  std::vector<COL *> needed_inputs;
  for (auto [in_col, out_col] : in_to_out) {
    needed_inputs.push_back(in_col);
    (void) out_col;
  }
  std::sort(needed_inputs.begin(), needed_inputs.end());
  auto needed_inputs_it = std::unique(needed_inputs.begin(), needed_inputs.end());
  needed_inputs.erase(needed_inputs_it, needed_inputs.end());

  // If this tuple is forwarding the values of something else along, and if it
  // is the only user of that other hting, then forward those values along,
  // otherwise we'll depend on CSE to try to merge this tuple with any other
  // equivalent tuples.
  if (all_from_same_view && last_view) {
    all_from_same_view = 1 == last_view->NumUses();
  }

  // All inputs are constants, or their corresponding outputs are not used,
  // or both.s
  if (!last_view) {
    is_used = this->Def<Node<QueryView>>::IsUsed();
    all_from_same_view = false;
  }

  if (all_from_same_view) {
    non_local_changes = true;
    for (auto [in_col, out_col] : in_to_out) {
      out_col->ReplaceAllUsesWith(in_col);
    }

    if (last_view->columns.Size() == columns.Size()) {
      this->Def<Node<QueryView>>::ReplaceAllUsesWith(last_view);
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
  if (max_i > needed_inputs.size()) {

    DefList<COL> new_output_cols(this);
    for (auto in_col : needed_inputs) {
      if (const auto old_out_col = in_to_out[in_col]; old_out_col) {
        const auto new_out_col = new_output_cols.Create(
            old_out_col->var, this, old_out_col->id);
        old_out_col->ReplaceAllUsesWith(new_out_col);
      }
    }

    UseList<COL> new_input_cols(this);
    for (auto in_col : needed_inputs) {
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
