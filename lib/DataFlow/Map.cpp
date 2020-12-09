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
  return range.From().Index();
}

static const std::hash<std::string_view> kStringViewHasher;

uint64_t Node<QueryMap>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  const auto binding_pattern = ParsedDeclaration(functor).BindingPattern();

  hash = RotateRight64(HashInit(), 43) ^ functor.Id();
  assert(hash != 0);

  if (!is_positive) {
    hash = ~hash;
  }
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
}

// Equality over maps is pointer-based.
bool Node<QueryMap>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsMap();
  if (!that ||
      is_positive != that->is_positive ||
      num_free_params != that->num_free_params ||
      columns.Size() != that->columns.Size() ||
      attached_columns.Size() != that->attached_columns.Size() ||
      functor.Id() != that->functor.Id() ||
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
