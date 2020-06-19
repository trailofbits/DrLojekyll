// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {

Node<QueryMerge>::~Node(void) {}

Node<QueryMerge> *Node<QueryMerge>::AsMerge(void) noexcept {
  return this;
}

uint64_t Node<QueryMerge>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  //
  // NOTE(pag): We don't include the number of merged views, as there may
  //            be redundancies in them after.
  hash = HashInit();

  if (merged_views.Empty()) {
    return hash;
  }

  auto merged_hashes = hash;

  // Mix in the hashes of the merged views. Don't double-mix an already seen
  // hash, otherwise it will remove its effect.
  for (auto view : merged_views) {
    merged_hashes = __builtin_rotateleft64(merged_hashes, 16) ^ view->Hash();
  }

  hash = merged_hashes;

  return hash;
}

unsigned Node<QueryMerge>::Depth(void) noexcept {
  if (depth) {
    return depth;
  }
  auto estimate = EstimateDepth(positive_conditions, 1u);
  estimate = EstimateDepth(negative_conditions, estimate);
  for (auto merged_view : merged_views) {
    estimate = std::max(estimate, merged_view->depth);
  }

  depth = estimate + 1u;

  auto real = 1u;
  for (auto merged_view : merged_views) {
    real = std::max(real, merged_view->Depth());
  }

  real = GetDepth(positive_conditions, real);
  real = GetDepth(negative_conditions, real);

  depth = real + 1u;

  return depth;
}

// Put this merge into a canonical form, which will make comparisons and
// replacements easier. For example, after optimizations, some of the merged
// views might be the same.
//
// NOTE(pag): If a merge directly merges with itself then we filter it out.
bool Node<QueryMerge>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  bool non_local_changes = false;

  UseList<VIEW> next_merged_views(this);

  std::vector<VIEW *> seen_merges;
  seen_merges.insert(seen_merges.end(), this);

  merged_views.Sort();

  VIEW *prev_view = nullptr;
  for (auto view : merged_views) {

    // Already added this view in.
    if (view == prev_view) {
      continue;
    }

    prev_view = view;

    // Don't let a merge be its own source, and don't double-merge any
    // sub-merges.
    if (std::find(seen_merges.begin(), seen_merges.end(), view) !=
        seen_merges.end()) {
      continue;
    }

    seen_merges.push_back(view);

    // If we're merging a merge, then copy the lower merge into this one.
    if (auto incoming_merge = view->AsMerge()) {
      incoming_merge->merged_views.Sort();
      incoming_merge->hash = 0;
      incoming_merge->is_canonical = false;

      for (auto sub_view : incoming_merge->merged_views) {
        if (std::find(seen_merges.begin(), seen_merges.end(), sub_view) ==
            seen_merges.end()) {
          next_merged_views.AddUse(sub_view);
          seen_merges.push_back(sub_view);
        }
      }

      // This is a unique view we're adding in.
    } else {
      next_merged_views.AddUse(view);
    }
  }

  merged_views.Swap(next_merged_views);

  // This merged view only merges other things.
  if (merged_views.Size() == 1) {
    const auto merged_view = merged_views[0];
#ifndef NDEBUG
    const auto num_cols = columns.Size();
    assert(merged_view->columns.Size() == num_cols);
#endif

//    // This merge view requires certain uniqueness properties, so we need
//    // to go and maintain those, but the incoming view does not respect those
//    // properties, so go and introduce them.
//    if (check_group_ids && !merged_view->check_group_ids) {
//      auto tuple = query->tuples.Create();
//      tuple->check_group_ids = true;
//      auto i = 0u;
//      for (auto input_col : merged_view->columns) {
//        tuple->input_columns.AddUse(input_col);
//        const auto output_col = tuple->columns.Create(
//            input_col->var, tuple, input_col->id);
//        columns[i++]->ReplaceAllUsesWith(output_col);
//      }
//
//    // Forward the columns directly along.
//    } else {
    auto i = 0u;
    for (auto input_col : merged_view->columns) {
      columns[i++]->ReplaceAllUsesWith(input_col);
    }
//    }

    merged_views.Clear();  // Clear it out.
    hash = 0;
    is_canonical = true;
    is_dead = true;
    return true;
  }

  // Check to see if any of the output columns are unused. If they are, project
  // a tuple down to the incoming views to restrict what goes into the merge.
  //
  // TODO(pag): Implement this.

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over merge is pointer-based.
bool Node<QueryMerge>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsMerge();
  const auto num_views = merged_views.Size();
  if (!that ||
      columns.Size() != that->columns.Size() ||
      num_views != that->merged_views.Size() ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
      InsertSetsOverlap(this, that)) {
    return false;
  }

  eq.Insert(this, that);  // Base case in case of cycles.

  for (auto i = 0u; i < num_views; ++i) {
    if (!merged_views[i]->Equals(eq, that->merged_views[i])) {
      eq.Remove(this, that);
      return false;
    }
  }

  return true;
}

}  // namespace hyde
