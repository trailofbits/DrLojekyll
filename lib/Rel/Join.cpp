// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {

Node<QueryJoin>::~Node(void) {}

Node<QueryJoin> *Node<QueryJoin>::AsJoin(void) noexcept {
  return this;
}

uint64_t Node<QueryJoin>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  if (out_to_in.empty()) {
    return 0;
  }

  assert(input_columns.Size() == 0);

  for (auto col : columns) {
    auto in_set = out_to_in.find(col);
    assert(in_set != out_to_in.end());
    uint64_t col_set_hash = 0;
    for (auto in_col : in_set->second) {
      col_set_hash = __builtin_rotateright64(col_set_hash, 16) ^
                     in_col->Hash();
    }
    hash = __builtin_rotateleft64(hash, 13) ^ col_set_hash;
  }

  hash <<= 4;
  hash |= 6;

  return hash;
}

unsigned Node<QueryJoin>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.

    auto real = 1u;
    for (const auto &[out_col, in_cols] : out_to_in) {
      real = GetDepth(in_cols, real);
    }
    depth = real + 1u;
  }
  return depth;
}

// Verify that all pivot sets cover the same views.
//
// TODO(pag): Re-implement to work in the case of constant propagations.
void Node<QueryJoin>::VerifyPivots(void) {
  return;
//  if (!num_pivots) {
//    return;
//  }
//
//  pivot_views.clear();
//
//  auto pivot_col = columns[0];
//  auto in_col_set = out_to_in.find(pivot_col);
//  assert(in_col_set != out_to_in.end());
//  assert(1 < in_col_set->second.Size());
//  for (auto in_pivot_col : in_col_set->second) {
//    pivot_views.push_back(in_pivot_col->view);
//  }
//
//  std::sort(pivot_views.begin(), pivot_views.end());
//  auto it = std::unique(pivot_views.begin(), pivot_views.end());
//  pivot_views.erase(it, pivot_views.end());
//
//  const auto num_pivot_views = pivot_views.size();
//
//  for (auto i = 1u; i < num_pivots; ++i) {
//    next_pivot_views.clear();
//
//    pivot_col = columns[i];
//    in_col_set = out_to_in.find(pivot_col);
//    assert(in_col_set != out_to_in.end());
//    assert(1 < in_col_set->second.Size());
//    for (auto in_pivot_col : in_col_set->second) {
//      next_pivot_views.push_back(in_pivot_col->view);
//    }
//
//    std::sort(next_pivot_views.begin(), next_pivot_views.end());
//    it = std::unique(next_pivot_views.begin(), next_pivot_views.end());
//    next_pivot_views.erase(it, next_pivot_views.end());
//
//    assert(num_pivot_views == next_pivot_views.size());
//    for (auto j = 0u; j < num_pivot_views; ++j) {
//      assert(pivot_views[j] == next_pivot_views[j]);
//    }
//  }
}

// Put this join into a canonical form, which will make comparisons and
// replacements easier. The approach taken is to sort the incoming columns, and
// to ensure that the iteration order of `out_to_in` matches `columns`.
//
// TODO(pag): If *all* incoming columns for a pivot column are the same, then
//            it no longer needs to be a pivot column.
//
// TODO(pag): If we make the above transform, then a join could devolve into
//            a merge.
bool Node<QueryJoin>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  if (out_to_in.empty()) {
    is_canonical = true;
    return false;
  }

  assert(num_pivots <= columns.Size());
  assert(out_to_in.size() == columns.Size());

  VerifyPivots();

  // If this view is used by a merge then we're not allowed to re-order the
  // columns. Instead, what we can do is create a tuple that will maintain
  // the ordering, and the canonicalize the join order below that tuple.
  bool non_local_changes = GuardWithTuple(query);

  std::unordered_map<COL *, COL *> in_to_out;

  VIEW *incoming_view = nullptr;
  auto joins_at_least_two_views = false;
  bool all_views_are_selects = true;

  for (auto &[out_col, input_cols] : out_to_in) {
    const auto max_i = input_cols.Size();
    assert(1u <= max_i);

    // Sort the input columns within this pivot set. We use lexicographic
    // ordering later as part of the stage that re-orders joins.
    input_cols.Sort();

    // Try to figure out if this JOIN actually joins together more than
    // one view.
    COL *constant_col = nullptr;
    bool all_are_constant = true;

    for (auto in_col : input_cols) {
      if (in_col->IsConstant()) {
        if (constant_col) {
          assert(in_col == constant_col);
        } else {
          constant_col = in_col;
        }
      } else {
        all_are_constant = false;

        if (!in_col->view->AsSelect()) {
          all_views_are_selects = false;
        }

        if (!incoming_view) {
          incoming_view = in_col->view;
        } else if (in_col->view != incoming_view) {
          joins_at_least_two_views = true;
        }
      }
    }

    // Deduplicate non-pivot columns.
    if (1 == max_i) {
      auto &prev_out = in_to_out[input_cols[0]];
      if (prev_out) {
        non_local_changes = true;  // Changing number of columns.

        if (prev_out->NumUses() > out_col->NumUses()) {
          out_col->ReplaceAllUsesWith(prev_out);
        } else {
          prev_out->ReplaceAllUsesWith(out_col);
          prev_out = out_col;
        }
      } else {
        prev_out = out_col;
      }

    // `input_cols` is a pivot set, one of the pivots is a constant, but not
    // all of the pivots are constant. What we'll do is introduce a filter
    // before the incoming views to constrain them to have the particular
    // column as a pivot.
    } else if (constant_col && !all_are_constant) {
      // TODO(pag): Implement this.
    }

    // There's a constant in the pivot set. Perform constant propagation.
    if (constant_col && out_col->IsUsedIgnoreMerges()) {
      out_col->ReplaceAllUsesWith(constant_col);
      non_local_changes = true;
    }

    // The entire pivot is unnecessary.
    if (1 < max_i && constant_col && all_are_constant) {
      // TODO(pag): Do something.
    }
  }

  // This join isn't needed.
  //
  // TODO(pag): Double check how this code behaves in the presence of
  //            constant propagation. I think the check that leads to
  //            `goto skip_remove;` should be sufficient to prevent anything
  //            unsafe.
  if (!joins_at_least_two_views) {
    for (auto &[out_col, input_cols] : out_to_in) {
      const auto num_input_cols = input_cols.Size();
      if (1 == num_input_cols) {
        continue;
      }

      // Go look to see if the incoming pivots are unique or not. If they
      // aren't, then we're not going to eliminate this JOIN. The hope is that
      // later optimizations and canonicalizations will result in the pivot
      // sets losing their uniqueness, thus enabling replacement.
      for (auto i = 1u; i < num_input_cols; ++i) {
        const auto lhs_col = input_cols[i - 1u];
        const auto rhs_col = input_cols[i];
        if (lhs_col != rhs_col) {
          goto skip_remove;
        }
      }
    }

    // Create a tuple that forwards along the inputs to this join.
    auto tuple = query->tuples.Create();
    auto j = 0u;
    for (auto col : columns) {
      const auto new_out_col = tuple->columns.Create(col->var, tuple, col->id);
      columns[j++]->ReplaceAllUsesWith(new_out_col);
    }

    for (auto col : columns) {
      auto in_set_it = out_to_in.find(col);
      assert(in_set_it != out_to_in.end());

      // If any of the columns in the pivot set is a constant, then forward
      // that along.
      for (auto in_col : in_set_it->second) {
        if (in_col->IsConstant()) {
          tuple->input_columns.AddUse(in_col);
          goto next;
        }
      }

      // Otherwise forward along the first one. This should end up being
      // the smallest-values pointer in the pivot set due to earlier sorting.
      tuple->input_columns.AddUse(in_set_it->second[0]);

    next:
      continue;
    }

    ReplaceAllUsesWith(tuple);
    out_to_in.clear();
    is_used = false;
    return true;
  }
skip_remove:

  // Find unused output columns that aren't themselves pivots. Otherwise,
  // mark pivot output columns for keeping.
  std::vector<COL *> keep_cols;
  for (auto &[out_col, input_cols] : out_to_in) {
    if (1 == input_cols.Size() && !out_col->NumUses()) {
      non_local_changes = true;
    } else {
      keep_cols.push_back(out_col);
    }
  }

  // Keep only the output columns that are needed, and that correspond with
  // unique (non-pivot) incoming columns.
  if (keep_cols.size() < columns.Size()) {
    DefList<COL> new_output_columns(this);
    std::unordered_map<COL *, UseList<COL>> new_out_to_in;

    for (auto old_out_col : keep_cols) {
      const auto new_out_col = new_output_columns.Create(
          old_out_col->var, this, old_out_col->id, new_output_columns.Size());
      old_out_col->ReplaceAllUsesWith(new_out_col);

      new_out_to_in.emplace(new_out_col, this);
      auto new_input_cols = new_out_to_in.find(new_out_col);
      auto old_input_cols = out_to_in.find(old_out_col);
      new_input_cols->second.Swap(old_input_cols->second);
    }

    non_local_changes = true;
    out_to_in.swap(new_out_to_in);
    columns.Swap(new_output_columns);
  }

  hash = 0;  // Sorting the columns changes the hash.

  // We'll order them in terms of:
  //    - Largest pivot set first.
  //    - Lexicographic order of pivot sets.
  //    - Pointer ordering.
  const auto cmp = [=] (COL *a, COL *b) {
    if (a == b) {
      return false;
    }

    const auto a_cols = out_to_in.find(a);
    const auto b_cols = out_to_in.find(b);

    assert(a_cols != out_to_in.end());
    assert(b_cols != out_to_in.end());

    const auto a_size = a_cols->second.Size();
    const auto b_size = b_cols->second.Size();

    if (a_size > b_size) {
      return true;
    } else if (a_size < b_size) {
      return false;
    }

    // Pivot sets are same size. Lexicographically order them.
    for (auto i = 0u; i < a_size; ++i) {
      if (a_cols->second[i] < b_cols->second[i]) {
        return true;
      } else if (a_cols->second[i] > b_cols->second[i]) {
        return false;
      } else {
        continue;
      }
    }

    // Pivot sets are identical... this is interesting, order no longer
    // matters.
    //
    // TODO(pag): Remove duplicate pivot set?
    return a < b;
  };

  columns.Sort(cmp);

  auto i = 0u;
  for (auto col : columns) {
    col->index = i++;  // Fixup the indices now that we've sorted things.
  }

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over joins is pointer-based.
bool Node<QueryJoin>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsJoin();
  if (!that ||
      columns.Size() != that->columns.Size() ||
      num_pivots != that->num_pivots ||
      out_to_in.empty() ||
      that->out_to_in.empty() ||
      out_to_in.size() != that->out_to_in.size()) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  auto i = 0u;
  for (const auto j1_out_col : columns) {
    assert(j1_out_col->index == i);

    const auto j2_out_col = that->columns[i];
    assert(j2_out_col->index == i);
    ++i;

    const auto j1_in_cols = out_to_in.find(j1_out_col);
    const auto j2_in_cols = that->out_to_in.find(j2_out_col);
    assert(j1_in_cols != out_to_in.end());
    assert(j2_in_cols != that->out_to_in.end());
    if (!ColumnsEq(j1_in_cols->second, j2_in_cols->second)) {
      return false;
    }
  }

  eq.Insert(this, that);
  return true;
}

}  // namespace hyde
