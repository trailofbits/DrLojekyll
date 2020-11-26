// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Optimize.h"
#include "Query.h"

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
  for (auto merged_view : merged_views) {
    merged_hashes ^= merged_view->Hash();
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
bool Node<QueryMerge>::Canonicalize(QueryImpl *query,
                                    const OptimizationContext &opt) {
  if (is_dead) {
    is_canonical = true;
    return false;
  }

  // If one of the columns of the merged views isn't needed, then mark this as
  // non-canonical.
  auto has_unused_col = false;
  for (auto col : columns) {
    if (!col->IsUsed() && opt.can_remove_unused_columns) {
      is_canonical = false;
      has_unused_col = true;
    }
  }

  if (is_canonical) {
    return false;
  }

  bool non_local_changes = false;
  is_canonical = true;

  std::vector<VIEW *> unique_merged_views;
  std::vector<VIEW *> seen;
  std::vector<VIEW *> work_list;

  for (auto i = merged_views.Size(); i--;) {
    work_list.push_back(merged_views[i]);
  }

  const auto num_cols = columns.Size();
  while (!work_list.empty()) {

    const auto view = work_list.back();
    assert(view->columns.Size() == num_cols);
    work_list.pop_back();

    // Don't let a merge be its own source.
    if (view == this) {
      continue;
    }

    // Don't double-process any reached views.
    const auto end = seen.end();
    if (std::find(seen.begin(), end, view) != end) {
      continue;
    }
    seen.push_back(view);

    // If we're merging a merge, then copy the lower merge into this one.
    if (auto incoming_merge = view->AsMerge();
        incoming_merge &&
        !incoming_merge->IntroducesControlDependency()) {

      non_local_changes = true;
      is_canonical = false;

      for (auto i = incoming_merge->merged_views.Size(); i--;) {
        work_list.push_back(incoming_merge->merged_views[i]);
      }

    // This is a unique view we're adding in.
    } else {
      unique_merged_views.push_back(view);
    }
  }

  // This MERGE isn't needed anymore.
  if (1 == unique_merged_views.size()) {

    // Create a forwarding tuple.
    const auto tuple = query->tuples.Create();
    const auto source_view = unique_merged_views[0];
    auto i = 0u;
    for (auto out_col : columns) {
      (void) tuple->columns.Create(out_col->var, tuple, out_col->id);
      tuple->input_columns.AddUse(source_view->columns[i++]);
    }

    assert(source_view->columns.Size() == i);

    ReplaceAllUsesWith(tuple);
    return true;  // Definitely made non-local changes.
  }

  // Nothing to do; it's already canonical.
  if (is_canonical && !has_unused_col) {
    is_canonical = true;
    hash = 0;
    return non_local_changes;
  }

  // Path compression.
  if (non_local_changes) {
    UseList<VIEW> new_merged_views(this);
    for (auto view : unique_merged_views) {
      assert(view->columns.Size() == num_cols);
      new_merged_views.AddUse(view);
    }
    merged_views.Swap(new_merged_views);
  }

  // There's an unused column; go and guard the incoming views with TUPLEs that
  // don't use that column.
  if (has_unused_col && opt.can_remove_unused_columns) {

    UseList<VIEW> new_merged_views(this);

    for (auto view : unique_merged_views) {
      assert(view->columns.Size() == num_cols);

      TUPLE *const guarded_view = query->tuples.Create();
#ifndef NDEBUG
      guarded_view->producer = "MERGE-GUARD";
#endif

      guarded_view->is_canonical = false;

      for (auto i = 0u; i < num_cols; ++i) {
        if (columns[i]->IsUsed()) {
          const auto out_col = view->columns[i];
          auto guard_out_col = guarded_view->columns.Create(
              out_col->var, guarded_view, out_col->id);
          guarded_view->input_columns.AddUse(out_col);
          guard_out_col->CopyConstantFrom(out_col);
        }
      }

      assert(num_cols > guarded_view->columns.Size());
      new_merged_views.AddUse(guarded_view);
    }

    DefList<COL> new_columns;

    // Create the new columns and replace all uses of old columns with the
    // new columns.
    for (auto i = 0u; i < num_cols; ++i) {
      const auto out_col = columns[i];
      if (out_col->IsUsed()) {
        auto new_out_col = new_columns.Create(out_col->var, this, out_col->id);
        new_out_col->CopyConstantFrom(out_col);
        out_col->ReplaceAllUsesWith(new_out_col);
      }
    }

    assert(new_columns.Size() < num_cols);

    non_local_changes = true;
    merged_views.Swap(new_merged_views);
    columns.Swap(new_columns);
  }

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Equality over merge is structural.
bool Node<QueryMerge>::Equals(EqualitySet &eq,
                              Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsMerge();
  const auto num_views = merged_views.Size();
  if (!that || columns.Size() != that->columns.Size() ||
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
