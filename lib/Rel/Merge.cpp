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
  if (!merged_views.Empty()) {
    hash = merged_views[0]->columns.Size();
    hash <<= 4;
    hash |= 3;
  }

  auto merged_hashes = hash;

  // Mix in the hashes of the merged views. Don't double-mix an already seen
  // hash, otherwise it will remove its effect.
  for (auto view : merged_views) {
    merged_hashes = __builtin_rotateleft64(merged_hashes, 16) ^ view->Hash();
  }

  hash = merged_hashes;
  hash <<= 4;
  hash |= 3;

  return hash;
}

unsigned Node<QueryMerge>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.

    auto real = 1u;
    for (auto merged_view : merged_views) {
      real = std::max(real, merged_view->Depth());
    }
    depth = real + 1u;
  }
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

  VIEW *prev_view = nullptr;
  for (auto retry = true; retry; ) {

    // Check if the merged views are sorted, and if not, reset the hash.
    const auto max_i = merged_views.Size();
    for (auto i = 1u; i < max_i; ++i) {
      if (merged_views[i - 1] > merged_views[i]) {
        non_local_changes = true;
      }
    }

    merged_views.Sort();
    retry = false;

    for (auto view : merged_views) {

      // Don't let a merge be its own source, and don't double-merge any
      // sub-merges.
      if (std::find(seen_merges.begin(), seen_merges.end(), view) ==
          seen_merges.end()) {
        prev_view = this;
        continue;
      }

      // Already added this view in.
      if (view == prev_view) {
        continue;
      }

      prev_view = view;

      // If we're merging a merge, then copy the lower merge into this one.
      if (auto incoming_merge = view->AsMerge()) {
        seen_merges.push_back(incoming_merge);

        incoming_merge->merged_views.Sort();
        incoming_merge->hash = 0;
        incoming_merge->is_canonical = false;
        non_local_changes = true;

        for (auto sub_view : incoming_merge->merged_views) {
          if (sub_view != this && sub_view != incoming_merge) {
            next_merged_views.AddUse(sub_view);
            retry = true;
          }
        }

        // This is a unique view we're adding in.
      } else {
        next_merged_views.AddUse(view);
      }
    }

    merged_views.Swap(next_merged_views);
    next_merged_views.Clear();
  }

  // This merged view only merges other things.
  if (merged_views.Size() == 1) {

    const auto num_cols = columns.Size();
    assert(merged_views[0]->columns.Size() == num_cols);

    // Forward the columns directly along.
    auto i = 0u;
    for (auto input_col : merged_views[0]->columns) {
      columns[i++]->ReplaceAllUsesWith(input_col);
    }

    merged_views.Clear();  // Clear it out.
    non_local_changes = true;
  }

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over merge is pointer-based.
bool Node<QueryMerge>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsMerge();
  if (!that || columns.Size() != that->columns.Size()) {
    return false;
  }

  const auto num_views = merged_views.Size();
  if (num_views != that->merged_views.Size()) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  for (auto i = 0u; i < num_views; ++i) {
    if (merged_views[i] != that->merged_views[i]) {
      return false;
    }
  }

  eq.Insert(this, that);  // Base case in case of cycles.
  return true;
}

}  // namespace hyde
