// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {

Node<QueryMap>::~Node(void) {}

Node<QueryMap> *Node<QueryMap>::AsMap(void) noexcept {
  return this;
}

uint64_t Node<QueryMap>::Sort(void) noexcept {
  return position.Index();
}

uint64_t Node<QueryMap>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = HashInit() ^ functor.Id();
  auto local_hash = hash;

  // Mix in the hashes of the merged views and columns;
  for (auto input_col : input_columns) {
    local_hash = __builtin_rotateright64(local_hash, 16) ^ input_col->Hash();
  }

  for (auto input_col : attached_columns) {
    local_hash = __builtin_rotateright64(local_hash, 16) ^ input_col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

// Put this map into a canonical form, which will make comparisons and
// replacements easier. Maps correspond to functors with inputs, whereas
// functors without inputs are called generators. If the functor has inputs,
// then some of them might be specified to belong to an `unordered` set, which
// means that they can be re-ordered during canonicalization for the sake of
// helping deduplicate common subexpressions. We also need to put the "attached"
// outputs into the proper order.
bool Node<QueryMap>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  is_canonical = AttachedColumnsAreCanonical();

  if (valid == VIEW::kValid &&
      !CheckAllViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
  }

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

  struct Param {
    COL *output;
    COL *input;
    unsigned orig_index;
    unsigned min_index;
    bool can_reorder;
  };

  std::vector<unsigned> min_index;
  min_index.reserve(i);

  for (auto param : functor.Parameters()) {
    min_index.push_back(param.Index());
  }

  // Go find the minimum index associated with each parameter.
  const auto num_usets = functor.NumUnorderedParameterSets();
  for (auto u = 0u; u < num_usets; ++u) {
    auto found_min_index = ~0u;
    for (auto param : functor.NthUnorderedSet(u)) {
      found_min_index = std::min(found_min_index, param.Index());
    }
    for (auto param : functor.NthUnorderedSet(u)) {
      min_index[param.Index()] = found_min_index;
    }
  }

  std::vector<Param> params;
  params.reserve(i);

  // The first few columns, which are the official outputs of the MAP, must
  // remain the same and in their original order.
  for (auto j = 0u, k = 0u; j < i; ++j) {
    const auto param = functor.NthParameter(j);
    assert(param.Index() == j);

    COL *input = nullptr;

    if (ParameterBinding::kBound == param.Binding()) {
      input = input_columns[k++];
    }

    params.push_back({columns[j], input,
                      j, min_index[j], param.CanBeReordered()});
  }

  // Sort the parameters, allowing us to re-sort inputs within the unordered
  // sets. This lets us convert `add_i32(A, B, Sum)` into `add_i32(B, A, Sum)`
  // if `A` and `B` belong to the same `unordered` set for `add_i32`. This
  // can help CSE to merge two equivalent MAPs.
  std::sort(
      params.begin(), params.end(),
      [] (const Param &a, const Param &b) {
        if (a.min_index != b.min_index) {
          return a.orig_index < b.orig_index;
        }

        assert(a.can_reorder && b.can_reorder);
        assert(a.input && b.input);

        if (a.input < b.input) {
          return true;

        } else if (a.input > b.input) {
          return false;

        } else {
          return a.orig_index < b.orig_index;
        }
      });

  // Create the new output columns.
  for (auto j = 0u; j < i; ++j) {
    const auto old_out_col = params[j].output;
    auto new_out_col = new_output_cols.Create(
        old_out_col->var, this, old_out_col->id);
    old_out_col->ReplaceAllUsesWith(new_out_col);
  }

  UseList<COL> new_input_cols(this);

  // Map the `bound`-attributed input columns to the output columns of the
  // map, just in case any of the attached columns end up being redundant
  // w.r.t. these bound columns.
  for (auto j = 0u; j < i; ++j) {
    const auto &param = params[j];
    if (!param.input) {
      continue;
    }

    const auto input_col = param.input;
    const auto new_output_col = new_output_cols[j];
    in_to_out.emplace(input_col, new_output_col);

    // Constant propagation on the bound columns.
    if (input_col->IsConstant() && new_output_col->IsUsedIgnoreMerges()) {
      new_output_col->ReplaceAllUsesWith(input_col);
      non_local_changes = true;
    }

    new_input_cols.AddUse(input_col);
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

  input_columns.Swap(new_input_cols);
  attached_columns.Swap(new_attached_cols);
  columns.Swap(new_output_cols);

  if (!CheckAllViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over maps is pointer-based.
bool Node<QueryMap>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsMap();
  if (!that ||
      columns.Size() != that->columns.Size() ||
      attached_columns.Size() != that->attached_columns.Size() ||
      functor != that->functor ||
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
