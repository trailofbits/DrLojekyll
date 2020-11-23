// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Optimize.h"
#include "Query.h"

namespace hyde {

Node<QueryMap>::~Node(void) {}

Node<QueryMap> *Node<QueryMap>::AsMap(void) noexcept {
  return this;
}

uint64_t Node<QueryMap>::Sort(void) noexcept {
  return position.Index();
}

static const std::hash<std::string_view> kStringViewHasher;

uint64_t Node<QueryMap>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  const auto binding_pattern = ParsedDeclaration(functor).BindingPattern();

  hash ^= RotateRight64(HashInit(), 43) * functor.Id();
  hash ^= RotateRight64(hash, 33) * kStringViewHasher(binding_pattern);

  auto local_hash = hash;

  // Mix in the hashes of the merged views and columns;
  for (auto input_col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 23) * input_col->Hash();
  }

  for (auto input_col : attached_columns) {
    local_hash ^= RotateRight64(local_hash, 13) * input_col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

// Put this map into a canonical form, which will make comparisons and
// replacements easier. Maps correspond to functors with inputs. Some of a
// functor's inputs might be specified to belong to an `unordered` set, which
// means that they can be re-ordered during canonicalization for the sake of
// helping deduplicate common subexpressions. We also need to put the "attached"
// outputs into the proper order.
bool Node<QueryMap>::Canonicalize(QueryImpl *query,
                                  const OptimizationContext &opt) {
  if (is_canonical) {
    return false;
  }

  if (is_dead || valid != VIEW::kValid) {
    is_canonical = true;
    return false;
  }

  if (valid == VIEW::kValid &&
      !CheckIncomingViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
    is_canonical = true;
    return false;
  }

  auto i = columns.Size() - attached_columns.Size();
  assert(i == functor.Arity());

  bool non_local_changes = false;
  std::tie(is_canonical, non_local_changes) =
      CanonicalizeAttachedColumns(i, opt);

  i = 0u;
  for (auto j = 0u; i < functor.Arity(); ++i) {
    if (functor.NthParameter(i).Binding() == ParameterBinding::kFree) {
      continue;  // It's an output column.
    }

    const auto out_col = columns[i];
    const auto in_col = input_columns[j++];
    const auto [changed, can_remove] =
        CanonicalizeColumnPair(in_col, out_col, opt);

    non_local_changes = non_local_changes || changed;
    (void) can_remove;  // Can't remove these.
  }

  if (!is_canonical) {

    in_to_out.clear();
    DefList<COL> new_columns(this);
    UseList<COL> new_attached_columns(this);

    // Make new output columns for the inputs.
    auto j = 0u;
    for (i = 0u; i < functor.Arity(); ++i) {
      const auto out_col = columns[i];
      const auto new_out_col =
          new_columns.Create(out_col->var, this, out_col->id);

      new_out_col->CopyConstantFrom(out_col);
      out_col->ReplaceAllUsesWith(new_out_col);

      if (functor.NthParameter(i).Binding() != ParameterBinding::kFree) {
        in_to_out.emplace(input_columns[j++], new_out_col);
      }
    }

    // Make the new attached columns.
    for (auto in_col : attached_columns) {
      const auto out_col = columns[i++];
      auto &prev_out_col = in_to_out[in_col];

      // This is either a re-use of one of the functor parameters, or it's a
      // duplicate attached column; try to remove it.
      if (prev_out_col) {
        if (out_col->IsUsedIgnoreMerges()) {
          non_local_changes = true;
        }
        out_col->ReplaceAllUsesWith(prev_out_col);

        if (!out_col->IsUsed()) {
          in_col->view->is_canonical = false;
          continue;  // It's safe to remove.
        }
      }

      // This attached column isn't used; remove it by not adding it back in.
      if (opt.can_remove_unused_columns && !out_col->IsUsed()) {
        in_col->view->is_canonical = false;
        continue;
      }

      const auto new_out_col =
          new_columns.Create(out_col->var, this, out_col->id);

      new_out_col->CopyConstantFrom(out_col);
      out_col->ReplaceAllUsesWith(new_out_col);
      new_attached_columns.AddUse(in_col);

      if (!prev_out_col) {
        prev_out_col = new_out_col;
      }
    }

    columns.Swap(new_columns);
    attached_columns.Swap(new_attached_columns);
    non_local_changes = true;
  }

  if (!CheckIncomingViewsMatch(input_columns, attached_columns)) {
    valid = VIEW::kInvalidAfterCanonicalize;
  }

  hash = 0;
  is_canonical = true;
  return non_local_changes;

  //  // We need to re-order the input columns, and possibly also the output
  //  // columns to match the input ordering.
  //
  //  std::unordered_map<COL *, COL *> in_to_out;
  //  DefList<COL> new_output_cols(this);
  //
  //  struct Param {
  //    COL *output;
  //    COL *input;
  //    unsigned orig_index;
  //    unsigned min_index;
  //    bool can_reorder;
  //  };
  //
  //  std::vector<unsigned> min_index;
  //  min_index.reserve(i);
  //
  //  for (auto param : functor.Parameters()) {
  //    min_index.push_back(param.Index());
  //  }
  //
  //  // Go find the minimum index associated with each parameter.
  //  const auto num_usets = functor.NumUnorderedParameterSets();
  //  for (auto u = 0u; u < num_usets; ++u) {
  //    auto found_min_index = ~0u;
  //    for (auto param : functor.NthUnorderedSet(u)) {
  //      found_min_index = std::min(found_min_index, param.Index());
  //    }
  //    for (auto param : functor.NthUnorderedSet(u)) {
  //      min_index[param.Index()] = found_min_index;
  //    }
  //  }
  //
  //  std::vector<Param> params;
  //  params.reserve(i);
  //
  //  // The first few columns, which are the official outputs of the MAP, must
  //  // remain the same and in their original order.
  //  for (auto j = 0u, k = 0u; j < i; ++j) {
  //    const auto param = functor.NthParameter(j);
  //    assert(param.Index() == j);
  //
  //    COL *input = nullptr;
  //
  //    if (ParameterBinding::kBound == param.Binding()) {
  //      input = input_columns[k++];
  //    }
  //
  //    params.push_back({columns[j], input,
  //                      j, min_index[j], param.CanBeReordered()});
  //  }
  //
  //  // Sort the parameters, allowing us to re-sort inputs within the unordered
  //  // sets. This lets us convert `add_i32(A, B, Sum)` into `add_i32(B, A, Sum)`
  //  // if `A` and `B` belong to the same `unordered` set for `add_i32`. This
  //  // can help CSE to merge two equivalent MAPs.
  //  std::sort(
  //      params.begin(), params.end(),
  //      [] (const Param &a, const Param &b) {
  //        if (a.min_index != b.min_index) {
  //          return a.orig_index < b.orig_index;
  //        }
  //
  //        assert(a.can_reorder && b.can_reorder);
  //        assert(a.input && b.input);
  //
  //        const auto a_sort = a.input->Sort();
  //        const auto b_sort = b.input->Sort();
  //        if (a_sort < b_sort) {
  //          return true;
  //
  //        } else if (a_sort > b_sort) {
  //          return false;
  //
  //        } else {
  //          return a.orig_index < b.orig_index;
  //        }
  //      });
  //
  //  // Create the new output columns.
  //  for (auto j = 0u; j < i; ++j) {
  //    const auto old_out_col = params[j].output;
  //    auto new_out_col = new_output_cols.Create(
  //        old_out_col->var, this, old_out_col->id);
  //    old_out_col->ReplaceAllUsesWith(new_out_col);
  //    new_out_col->CopyConstant(old_out_col);
  //  }
  //
  //  UseList<COL> new_input_cols(this);
  //
  //  // Map the `bound`-attributed input columns to the output columns of the
  //  // map, just in case any of the attached columns end up being redundant
  //  // w.r.t. these bound columns.
  //  for (auto j = 0u; j < i; ++j) {
  //    const auto &param = params[j];
  //    if (!param.input) {
  //      continue;
  //    }
  //
  //    const auto input_col = param.input;
  //    const auto new_output_col = new_output_cols[j];
  //    in_to_out.emplace(input_col, new_output_col);
  //
  //    // Constant propagation on the bound columns.
  //    if (input_col->IsConstant()) {
  //      if (new_output_col->IsUsedIgnoreMerges()) {
  //        non_local_changes = true;
  //      }
  //
  //      new_output_col->CopyConstant(input_col);
  //      new_output_col->ReplaceAllUsesWith(input_col);
  //
  //    } else if (input_col->IsConstantRef()) {
  //      if (!new_output_col->IsConstantRef()) {
  //        new_output_col->CopyConstant(input_col);
  //        non_local_changes = true;
  //      }
  //    }
  //
  //    new_input_cols.AddUse(input_col);
  //  }
  //
  //  const auto num_cols = columns.Size();
  //
  //  UseList<COL> new_attached_cols(this);
  //
  //  assert(i == (columns.Size() - attached_columns.Size()));
  //  for (auto j = 0u; i < num_cols; ++i, ++j) {
  //    const auto old_out_col = columns[i];
  //    const auto in_col = attached_columns[j];
  //
  //    // If the output column is never used, then get rid of it.
  //    //
  //    // NOTE(pag): `IsUsed` on a column checks to see if its view is used
  //    //            in a merge, which would not show up in a normal def-use
  //    //            list.
  //    if (!old_out_col->IsUsed()) {
  //      in_col->view->is_canonical = false;
  //      non_local_changes = true;  // Shrinking the number of columns.
  //      continue;
  //    }
  //
  //    // Constant propagation.
  //    if (in_col->IsConstant()) {
  //      if (old_out_col->IsUsedIgnoreMerges()) {
  //        non_local_changes = true;
  //      }
  //      old_out_col->CopyConstant(in_col);
  //      old_out_col->ReplaceAllUsesWith(in_col);
  //      if (!old_out_col->IsUsed()) {
  //        continue;
  //      }
  //
  //    } else if (in_col->IsConstantRef()) {
  //      if (!old_out_col->IsConstantRef()) {
  //        non_local_changes = true;
  //      }
  //      old_out_col->CopyConstant(in_col);
  //    }
  //
  //    auto &out_col = in_to_out[in_col];
  //    if (out_col) {
  //      in_col->view->is_canonical = false;
  //      non_local_changes = true;  // Shrinking the number of columns.
  //      old_out_col->ReplaceAllUsesWith(out_col);
  //
  //    } else {
  //      out_col = old_out_col;
  //      new_attached_cols.AddUse(in_col);
  //    }
  //  }
  //
  //  if (sort) {
  //    new_attached_cols.Sort();
  //  }
  //
  //  for (auto in_col : new_attached_cols) {
  //    const auto old_out_col = in_to_out[in_col];
  //    const auto new_out_col = new_output_cols.Create(
  //        old_out_col->var, this, old_out_col->id);
  //    old_out_col->ReplaceAllUsesWith(new_out_col);
  //    new_out_col->CopyConstant(old_out_col);
  //  }
  //
  //  input_columns.Swap(new_input_cols);
  //  attached_columns.Swap(new_attached_cols);
  //  columns.Swap(new_output_cols);
}

// Equality over maps is pointer-based.
bool Node<QueryMap>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsMap();
  if (!that || columns.Size() != that->columns.Size() ||
      attached_columns.Size() != that->attached_columns.Size() ||
      functor != that->functor ||
      (ParsedDeclaration(functor).BindingPattern() !=
       ParsedDeclaration(that->functor).BindingPattern()) ||
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
