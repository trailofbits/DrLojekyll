// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {

Node<QueryMap>::~Node(void) {}

Node<QueryMap> *Node<QueryMap>::AsMap(void) noexcept {
  return this;
}

uint64_t Node<QueryMap>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = functor.Id();

  // Mix in the hashes of the merged views and columns;
  for (auto input_col : input_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ input_col->Hash();
  }

  for (auto input_col : attached_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ input_col->Hash();
  }

  hash <<= 4;
  hash |= 4;

  return hash;
}

// Put this map into a canonical form, which will make comparisons and
// replacements easier. We also need to put the "attached" outputs into the
// proper order.
bool Node<QueryMap>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  is_canonical = AttachedColumnsAreCanonical();

  // If this view is used by a merge then we're not allowed to re-order the
  // columns. Instead, what we can do is create a tuple that will maintain
  // the ordering, and the canonicalize the join order below that tuple.
  bool non_local_changes = GuardWithTuple(query);

  // We need to re-order the input columns, and possibly also the output
  // columns to match the input ordering.

  std::unordered_map<COL *, COL *> in_to_out;
  DefList<COL> new_output_cols(this);

  auto i = columns.Size() - attached_columns.Size();
  assert(i == functor.Arity());

  // The first few columns, which are the official outputs of the MAP, must
  // remain the same and in their original order.
  for (auto j = 0u; j < i; ++j) {
    auto new_out_col = new_output_cols.Create(
        columns[j]->var, this, columns[j]->id);
    columns[j]->ReplaceAllUsesWith(new_out_col);
  }

  // Map the `bound`-attributed input columns to the output columns of the
  // map, just in case any of the attached columns end up being redundant
  // w.r.t. these bound columns.
  for (auto j = 0u, k = 0u; j < i; ++j) {
    const auto param = functor.NthParameter(j);
    if (ParameterBinding::kBound == param.Binding()) {
      const auto input_col = input_columns[k++];
      const auto new_output_col = new_output_cols[j];
      in_to_out.emplace(input_col, new_output_col);

      // Constant propagation on the bound columns.
      if (input_col->IsConstant() && new_output_col->IsUsedIgnoreMerges()) {
        new_output_col->ReplaceAllUsesWith(input_col);
        non_local_changes = true;
      }
    }
  }

  const auto num_cols = columns.Size();

  UseList<COL> new_attached_cols(this);

  for (auto j = 0u; i < num_cols; ++i, ++j) {
    const auto old_out_col = columns[i];

    // If the output column is never used, then get rid of it.
    //
    // NOTE(pag): `IsUsed` on a column checks to see if its view is used
    //            in a merge, which would not show up in a normal def-use
    //            list.
    if (!old_out_col->IsUsed()) {
      non_local_changes = true;  // Shrinking the number of columns.
      continue;
    }

    const auto in_col = attached_columns[j];

    // Constant propagation.
    if (in_col->IsConstant() && old_out_col->IsUsedIgnoreMerges()) {
      old_out_col->ReplaceAllUsesWith(in_col);
      non_local_changes = true;
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
      new_attached_cols.AddUse(in_col);
    }
  }

  new_attached_cols.Sort();

  for (auto in_col : new_attached_cols) {
    const auto old_out_col = in_to_out[in_col];
    const auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
  }

  attached_columns.Swap(new_attached_cols);
  columns.Swap(new_output_cols);

  assert(CheckAllViewsMatch(input_columns, attached_columns));

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over maps is pointer-based.
bool Node<QueryMap>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsMap();
  if (!that || columns.Size() != that->columns.Size() ||
      attached_columns.Size() != that->attached_columns.Size()) {
    return false;
  }

  if (functor != that->functor) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  if (!ColumnsEq(input_columns, that->input_columns) ||
      !ColumnsEq(attached_columns, that->attached_columns)) {
    return false;
  }

  eq.Insert(this, that);

  return true;
}

}  // namespace hyde
