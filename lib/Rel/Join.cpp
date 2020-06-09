// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <unordered_set>

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
  hash |= query::kJoinId;

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
  if (!num_pivots) {
    return;
  }

  pivot_views.clear();

  auto pivot_col = columns[0];
  auto in_col_set = out_to_in.find(pivot_col);
  assert(in_col_set != out_to_in.end());
  assert(1 < in_col_set->second.Size());
  for (auto in_pivot_col : in_col_set->second) {
    pivot_views.push_back(in_pivot_col->view);
  }

  std::sort(pivot_views.begin(), pivot_views.end());
  auto it = std::unique(pivot_views.begin(), pivot_views.end());
  pivot_views.erase(it, pivot_views.end());

  public_pivot_views.clear();
  for (auto v : pivot_views) {
    public_pivot_views.emplace_back(v);
  }

  const auto num_pivot_views = pivot_views.size();

  // TODO(pag): I forgot what this does or why I started the loop at `1`.
  for (auto i = 1u; i < num_pivots; ++i) {
    next_pivot_views.clear();

    pivot_col = columns[i];
    in_col_set = out_to_in.find(pivot_col);
    assert(in_col_set != out_to_in.end());
    assert(1 < in_col_set->second.Size());
    for (auto in_pivot_col : in_col_set->second) {
      next_pivot_views.push_back(in_pivot_col->view);
    }

    std::sort(next_pivot_views.begin(), next_pivot_views.end());
    it = std::unique(next_pivot_views.begin(), next_pivot_views.end());
    next_pivot_views.erase(it, next_pivot_views.end());

    assert(num_pivot_views == next_pivot_views.size());
    for (auto j = 0u; j < num_pivot_views; ++j) {
      assert(pivot_views[j] == next_pivot_views[j]);
    }
  }
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
    VerifyPivots();
    return false;
  }

  if (out_to_in.empty()) {
    is_canonical = true;
    return false;
  }

  assert(num_pivots <= columns.Size());
  assert(out_to_in.size() == columns.Size());

  // Maps incoming VIEWs to the pairs of `(out_col, in_col)`, where `out_col`
  // is the output column associated with `in_col`, and `in_col` belongs to
  // the mapped VIEW.
  std::unordered_map<VIEW *, std::vector<std::pair<COL *, COL *>>>
      in_view_to_inout_cols;

  // Maps output columns to constant inputs.
  std::unordered_map<COL *, COL *> out_to_constant_in;

  // If any input column is a constant, then we're going to force this join to
  // be guarded by a tuple. That simplifies const prop and downward
  // restructuring.
  for (auto &[out_col, input_cols] : out_to_in) {
    for (COL *in_col : input_cols) {
      if (in_col->IsConstant()) {
        out_to_constant_in.emplace(out_col, in_col);
      } else {
        in_view_to_inout_cols[in_col->view].emplace_back(out_col, in_col);
      }
    }
  }

  // If this view is used by a merge then we're not allowed to re-order the
  // columns. Instead, what we can do is create a tuple that will maintain
  // the ordering, and the canonicalize the join order below that tuple.
  const auto guard_tuple = GuardWithTuple(query, !out_to_constant_in.empty());
  bool non_local_changes = !!guard_tuple;

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

    for (COL *in_col : input_cols) {
      if (in_col->IsConstant()) {
        if (constant_col) {
          assert(in_col == constant_col);
        } else {
          constant_col = in_col;
        }
      } else {

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
    }

    // There's a constant, either in the pivot set or just a normal incoming
    // column. Perform constant propagation.
    //
    // NOTE(pag): This JOIN is guaranteed to be guarded by a tuple.
    if (constant_col) {
      out_col->ReplaceAllUsesWith(constant_col);
      non_local_changes = true;
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
    tuple->can_receive_deletions = can_receive_deletions;
    tuple->can_produce_deletions = can_produce_deletions;

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
    num_pivots = 0;
    is_used = false;
    VerifyPivots();
    return true;
  }
skip_remove:

  // At least one of the pivot columns is a constant. We want to generate
  // FILTERs for all of the incoming views.
  if (!out_to_constant_in.empty()) {
    for (auto &[view, inout_cols] : in_view_to_inout_cols) {
      std::sort(inout_cols.begin(), inout_cols.end(),
                [] (std::pair<COL *, COL *> a, std::pair<COL *, COL *> b) {
                  return a.first->index < b.first->index;
                });

      for (auto &[out_col, in_col] : inout_cols) {
        const auto const_col_it = out_to_constant_in.find(out_col);
        if (const_col_it == out_to_constant_in.end()) {
          continue;
        }

        COL * const const_col = const_col_it->second;
        const auto filter = query->constraints.Create(ComparisonOperator::kEqual);
        const VIEW *in_view = in_col->view;
        filter->can_receive_deletions = in_view->can_produce_deletions;
        filter->can_produce_deletions = filter->can_receive_deletions;
        filter->input_columns.AddUse(in_col);
        filter->input_columns.AddUse(const_col);

        const auto new_in_col = filter->columns.Create(
            in_col->var, filter, in_col->id);

        for (auto &[out_col2, in_col2] : inout_cols) {
          if (out_col != out_col2) {
            filter->attached_columns.AddUse(in_col2);
            in_col2 = filter->columns.Create(in_col2->var, filter, in_col2->id);
          }
        }

        in_col = new_in_col;
      }
    }
  }

  // Find unused output columns that aren't themselves pivots. Otherwise,
  // mark pivot output columns for keeping. We can skip over constant outputs,
  // even pivots, because the JOIN will be guarded by a TUPLE. We still have
  // to inject FILTERs on the sources, though.
  std::vector<COL *> keep_cols;
  for (const auto &[out_col, input_cols] : out_to_in) {

    // We don't need to keep constant pivots because they will have been
    // filtered before coming in, and we don't need to keep constant outputs
    // because we will have propagated them.
    if (out_to_constant_in.count(out_col)) {
      continue;

    // We don't want to keep unused non-PIVOTs, as they don't matter.
    } else if (1 == input_cols.Size() && !out_col->NumUses()) {
      non_local_changes = true;

    // Either it's a non-constant pivot, which may or may not be used, but must
    // be kept to preserve the higher-level semantics, or it's a used non-pivot,
    // non-constant column.
    } else {
      keep_cols.push_back(out_col);
    }
  }

  // Keep only the output columns that are needed, and that correspond with
  // unique (non-pivot) incoming columns.
  if (!out_to_constant_in.empty() || keep_cols.size() < columns.Size()) {
    DefList<COL> new_output_columns(this);
    std::unordered_map<COL *, UseList<COL>> new_out_to_in;

    for (auto old_out_col : keep_cols) {
      assert(!out_to_constant_in.count(old_out_col));

      const auto new_out_col = new_output_columns.Create(
          old_out_col->var, this, old_out_col->id, new_output_columns.Size());
      old_out_col->ReplaceAllUsesWith(new_out_col);

      new_out_to_in.emplace(new_out_col, this);
      auto new_input_cols = new_out_to_in.find(new_out_col);

      // We can copy the old input column sets over.
      if (out_to_constant_in.empty()) {
        auto old_input_cols = out_to_in.find(old_out_col);
        new_input_cols->second.Swap(old_input_cols->second);

      // We need to rebuild the incoming column sets with any FILTERed inputs.
      } else {
        for (auto &[old_in_view, inout_cols] : in_view_to_inout_cols) {
          for (auto [old_out_col2, filtered_in_col] : inout_cols) {
            if (old_out_col2 == old_out_col) {
              new_input_cols->second.AddUse(filtered_in_col);
              break;
            }
          }
        }
      }

      assert(!new_input_cols->second.Empty());
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

  // Recalculate the number of pivot sets.
  num_pivots = 0;
  for (auto &[out_col, in_cols] : out_to_in) {
    if (1 < in_cols.Size()) {
      ++num_pivots;
    }
  }

  auto i = 0u;
  for (auto col : columns) {
    col->index = i++;  // Fixup the indices now that we've sorted things.
  }

  hash = 0;
  is_canonical = true;
  VerifyPivots();
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
