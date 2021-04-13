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
  assert(hash != 0);

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
                                    const OptimizationContext &opt,
                                    const ErrorLog &) {

  if (is_dead) {
    is_canonical = true;
    return false;
  }

  // If one of the columns of the merged views isn't needed, then mark this as
  // non-canonical.
  auto has_unused_col = false;
  if (opt.can_remove_unused_columns && !IsUsedDirectly()) {
    for (auto col : columns) {
      if (!col->Def<COL>::IsUsed()) {
        is_canonical = false;
        has_unused_col = true;
      }
    }
  }

  //  if (is_canonical) {
  //    return false;
  //  }

  bool non_local_changes = false;
  is_canonical = true;

  std::vector<VIEW *> unique_merged_views;
  std::vector<VIEW *> seen;
  std::vector<VIEW *> work_list;

  for (auto i = merged_views.Size(); i;) {
    work_list.push_back(merged_views[--i]);
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

    // Don't double-process any reached views. If the merge wasn't unique
    // before then that could affect `VIEW::OnlyUser()`.
    const auto end = seen.end();
    if (std::find(seen.begin(), end, view) != end) {
      is_canonical = false;
      non_local_changes = true;
      continue;
    }
    seen.push_back(view);

    // Try to pull data through tuples. We can do this if the tuple isn't used
    // by anything else, and if it forwards its incoming view perfectly.
    if (auto tuple = view->AsTuple(); tuple && tuple->OnlyUser()) {
      auto tuple_source = VIEW::GetIncomingView(tuple->input_columns);

      // NOTE(pag): `ForwardsAllInputsAsIs` checks conditions.
      if (tuple->ForwardsAllInputsAsIs(tuple_source)) {
        non_local_changes = true;
        is_canonical = false;
        work_list.push_back(tuple_source);

      } else {
        unique_merged_views.push_back(tuple);
      }

    // If we've found a merge, and it is only used by us, and if it doesn't
    // set or test conditions, then we can flatten this merge into our merge.
    // Otherwise, we can't, as we'd risk breaking inductive cycles.
    } else if (auto merge = view->AsMerge();
               merge && !merge->sets_condition &&
               merge->positive_conditions.Empty() &&
               merge->negative_conditions.Empty() && merge->OnlyUser()) {

      is_canonical = false;
      non_local_changes = true;
      for (auto i = merge->merged_views.Size(); i;) {
        work_list.push_back(merge->merged_views[--i]);
      }

    // This is a unique view we're adding in.
    } else {
      unique_merged_views.push_back(view);
    }
  }

  // This MERGE isn't needed anymore.
  if (1 == unique_merged_views.size()) {

    // Create a forwarding tuple.
    const auto source_view = unique_merged_views[0];
    const auto tuple = query->tuples.Create();
    tuple->color = color ? color : source_view->color;

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
  if (is_canonical && !has_unused_col && !non_local_changes &&
      !opt.can_sink_unions) {
    is_canonical = true;
    hash = 0;
    return false;
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
  if (has_unused_col) {

    UseList<VIEW> new_merged_views(this);
    for (auto view : merged_views) {
      assert(view->columns.Size() == num_cols);

      TUPLE *const guarded_view = query->tuples.Create();
      guarded_view->color = color;
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

  // Lets go see if we can sink merges/unions deeper into
  // the dataflow. The idea here is that sometimes you have two or more
  // superficially similar inputs flowing into a MERGE, and you'd like
  // to instead merge those two inputs together.
  if (opt.can_sink_unions) {
    if (sink_penalty) {
      sink_penalty -= 1u;
      goto done;
    }

    std::unordered_map<const char *, std::vector<VIEW *>> grouped_views;
    bool has_opportunity = false;

    for (auto merged_view : merged_views) {

      // Don't even try to sink through conditions.
      if (merged_view->positive_conditions.Size() ||
          merged_view->negative_conditions.Size()) {
        goto done;
      }

      auto &similar_views = grouped_views[merged_view->KindName()];
      if (!similar_views.empty()) {
        has_opportunity = true;
      }

      similar_views.push_back(merged_view);
    }

    if (!has_opportunity) {
      goto done;
    }

    UseList<VIEW> new_merged_views(this);
    bool changed = false;

    // We have at least one set of views that look similar are thus might
    // be mergeable.
    for (auto &[kind_name, similar_views] : grouped_views) {
      VIEW *const first_view = similar_views[0];
      if (first_view->AsTuple()) {
        if (SinkThroughTuples(query, similar_views)) {
          changed = true;
        }
      } else if (first_view->AsMap()) {
        if (SinkThroughMaps(query, similar_views)) {
          changed = true;
        }
      }

      // `similar_views` may have been updated in place by a sinking function,
      // and some of its entries nulled out.
      for (VIEW *old_merged_view : similar_views) {
        if (old_merged_view) {
          new_merged_views.AddUse(old_merged_view);
        }
      }
    }

    if (changed) {
      sink_penalty = Depth() + 1u;
      non_local_changes = true;
      merged_views.Swap(new_merged_views);
    }
  }

done:

  hash = 0;
  is_canonical = true;
  return non_local_changes;
}

// Convert two or more tuples into a single tuple that reads its data from
// a union, where that union reads its data from the sources of the two
// tuples. The returned tuple is likely superficial but serves to prevent
// the union of the tuple's sources from being merged upward. What we are
// looking for are cases where the tuples leading into the union have
// similarly shaped inputs. We want a union over those inputs (which may be
// wider than the tuple itself, hence the returned tuple).
// Returns `true` if successful, and updates `tuples` in place with the
// new merged entries.
bool Node<QueryMerge>::SinkThroughTuples(QueryImpl *impl,
                                         std::vector<VIEW *> &inout_views) {

  VIEW *first_tuple_pred = nullptr;
  TUPLE *first_tuple = nullptr;
  TUPLE *merged_tuple = nullptr;
  MERGE *sunk_merge = nullptr;
  unsigned first_tuple_index = 0u;
  unsigned num_cols = 0u;
  unsigned num_pred_cols = 0u;
  unsigned num_failed = 0u;

  bool changed_rec = false;

  for (auto &inout_view : inout_views) {
    if (!inout_view) {
      first_tuple_index++;
      continue;

    // We've found the first non-null tuple; we'll try to match the remaining
    // non-`nullptr` entries in `inout_views` against it.
    } else if (!first_tuple) {
      first_tuple = inout_view->AsTuple();
      inout_view = nullptr;

      // `first_tuple` has a constant input, so we can't do anything with it.
      // So we'll proceed on the rest of the list recursively.
      for (auto first_tuple_input_col : first_tuple->input_columns) {
        if (first_tuple_input_col->IsConstant()) {
          changed_rec = SinkThroughTuples(impl, inout_views);
          goto done;
        } else {
          assert(!first_tuple_pred ||
                 first_tuple_pred == first_tuple_input_col->view);
          first_tuple_pred = first_tuple_input_col->view;
        }
      }

      assert(first_tuple_pred);
      num_cols = first_tuple->columns.Size();
      num_pred_cols = first_tuple_pred->columns.Size();
      continue;
    }

    auto next_tuple = inout_view->AsTuple();

    // The next tuple isn't allowed to have any constant inputs because we
    // can't feed those into a merge.
    VIEW *next_tuple_pred = nullptr;
    for (auto next_tuple_input_col : next_tuple->input_columns) {
      if (next_tuple_input_col->IsConstant()) {
        goto skip_to_next_tuple;
      } else {
        assert(!next_tuple_pred ||
               next_tuple_pred == next_tuple_input_col->view);
        next_tuple_pred = next_tuple_input_col->view;
      }
    }

    // Predecessors have a different shape of their outputs.
    if (num_pred_cols != next_tuple_pred->columns.Size()) {
      goto count_and_skip_to_next_tuple;
    }

    // Make sure that all columns of each tuple's predecessor have the same
    // type, so that we can make a merge that is compatible with all of them.
    for (auto j = 0u; j < num_pred_cols; ++j) {
      const auto first_pred_tuple_col = first_tuple_pred->columns[j];
      const auto next_pred_tuple_col = next_tuple_pred->columns[j];
      if (first_pred_tuple_col->type != next_pred_tuple_col->type) {
        goto count_and_skip_to_next_tuple;
      }
    }

    // Make sure that `first_tuple` and `next_tuple` take their inputs from
    // the same indexed columns of their predecessors.
    for (auto j = 0u; j < num_cols; ++j) {
      COL *const first_tuple_col = first_tuple->input_columns[j];
      COL *const next_tuple_col = next_tuple->input_columns[j];

      if (first_tuple_col->Index() != next_tuple_col->Index()) {
        goto count_and_skip_to_next_tuple;
      }
    }

    // We've made it down here, which means that the predecessors to
    // `first_tuple` and `next_tuple` "match" and we can sink the union.

    if (!merged_tuple) {
      first_tuple->is_canonical = false;

      // Create the sunken merge. It might have more columns than `num_cols`.
      sunk_merge = impl->merges.Create();
      sunk_merge->color = color;
      for (auto j = 0u; j < num_pred_cols; ++j) {
        const auto first_tuple_pred_col = first_tuple_pred->columns[j];
        sunk_merge->columns.Create(first_tuple_pred_col->var, sunk_merge,
                                   first_tuple_pred_col->id, j);
      }

      // Now create the merged tuple that will select only the columns of
      // interest from the sunken merge.
      merged_tuple = impl->tuples.Create();
      merged_tuple->color = color;
      for (auto j = 0u; j < num_cols; ++j) {
        const auto first_tuple_col = first_tuple->columns[j];
        const auto first_tuple_pred_col = first_tuple->input_columns[j];
        (void) merged_tuple->columns.Create(first_tuple_col->var, merged_tuple,
                                            first_tuple_col->id, j);

        // Select in the correct column from the sunken merge to pass into
        // the final tuple.
        merged_tuple->input_columns.AddUse(
            sunk_merge->columns[first_tuple_pred_col->Index()]);
      }

      sunk_merge->merged_views.AddUse(first_tuple_pred);
    }

    inout_view->is_canonical = false;
    inout_view = nullptr;
    sunk_merge->merged_views.AddUse(next_tuple_pred);
    continue;

  count_and_skip_to_next_tuple:
    ++num_failed;
  skip_to_next_tuple:
    continue;
  }

  // It's worth it to try recursion.
  if (num_failed >= 2u) {
    changed_rec = SinkThroughTuples(impl, inout_views);
  }

done:

  // We've bottomed out with a list of `nullptr` tuples, i.e. we've tried to
  // recursively process stuff in the list and there's nothing more to do.
  if (!first_tuple) {
    return false;

  // We failed to match `first_tuple` against anything else. It could be that
  // `first_tuple` takes some constant inputs, or that its predecessor's
  // shape or its usage of its predecessor doesn't match anything else. We'll
  // assume we did a recursive call and return the result of that.
  } else if (!merged_tuple) {
    inout_views[first_tuple_index] = first_tuple;
    return changed_rec;

  // We've successfully merged at least two tuples into one tuple taking a
  // union of the predecessors of the merged tuples.
  } else {
    assert(merged_tuple != nullptr);
    inout_views[first_tuple_index] = merged_tuple;
    sunk_merge->sink_penalty = sunk_merge->Depth();
    return true;
  }
}

// Convert two or more MAPs into a single MAP that reads its data from
// a union, where that union reads its data from the sources of the two
// MAPs.
bool Node<QueryMerge>::SinkThroughMaps(QueryImpl *impl,
                                       std::vector<VIEW *> &inout_views) {

  VIEW *first_map_pred = nullptr;
  MAP *first_map = nullptr;
  MAP *merged_map = nullptr;
  MERGE *sunk_merge = nullptr;
  unsigned first_map_index = 0u;
  unsigned num_cols = 0u;
  unsigned num_input_cols = 0u;
  unsigned num_attached_cols = 0u;
  unsigned num_failed = 0u;

  bool changed_rec = false;

  // Create a TUPLE that will package up the inputs and attached columns to
  // `map`.
  auto proxy_inputs = [&](MAP *map) -> TUPLE * {
    const auto input_tuple_for_map = impl->tuples.Create();
    input_tuple_for_map->color = color;
    auto col_index = 0u;

    for (auto j = 0u; j < num_input_cols; ++j) {
      const auto orig_input_col = map->input_columns[j];
      input_tuple_for_map->columns.Create(orig_input_col->var,
                                          input_tuple_for_map,
                                          orig_input_col->id, col_index++);
      input_tuple_for_map->input_columns.AddUse(orig_input_col);
    }

    for (auto j = 0u; j < num_attached_cols; ++j) {
      const auto orig_attached_col = map->attached_columns[j];
      input_tuple_for_map->columns.Create(orig_attached_col->var,
                                          input_tuple_for_map,
                                          orig_attached_col->id, col_index++);
      input_tuple_for_map->input_columns.AddUse(orig_attached_col);
    }

    return input_tuple_for_map;
  };

  for (auto &inout_view : inout_views) {
    if (!inout_view) {
      first_map_index++;
      continue;

    // We've found the first non-null map; we'll try to match the remaining
    // non-`nullptr` entries in `inout_views` against it.
    } else if (!first_map) {
      first_map = inout_view->AsMap();
      inout_view = nullptr;
      first_map_pred = VIEW::GetIncomingView(first_map->input_columns,
                                             first_map->attached_columns);
      num_cols = first_map->columns.Size();
      num_input_cols = first_map_pred->input_columns.Size();
      num_attached_cols = first_map_pred->attached_columns.Size();
      continue;
    }

    // If their functors don't match then we can't merge. If they do, then
    // their usage in a merge implies that they have the same number of attached
    // columns and input columns. We don't know if their attached/input columns
    // are constants, but we'll mitigate this possibility by injecting in tuples
    // to feed into the sunken merge.
    const auto next_map = inout_view->AsMap();
    if (first_map->functor != next_map->functor ||
        first_map->is_positive != next_map->is_positive ||
        first_map->num_free_params != next_map->num_free_params ||
        num_input_cols != next_map->input_columns.Size() ||
        num_attached_cols != next_map->attached_columns.Size()) {
      ++num_failed;
      continue;
    }

    assert(ParsedDeclaration(first_map->functor).BindingPattern() ==
           ParsedDeclaration(next_map->functor).BindingPattern());

    if (!merged_map) {
      first_map->is_canonical = false;
      const auto first_map_input_tuple = proxy_inputs(first_map);

      // Create the sunken merge.
      sunk_merge = impl->merges.Create();
      sunk_merge->color = color;
      for (auto j = 0u; j < (num_input_cols + num_attached_cols); ++j) {
        const auto first_tuple_pred_col = first_map_input_tuple->columns[j];
        sunk_merge->columns.Create(first_tuple_pred_col->var, sunk_merge,
                                   first_tuple_pred_col->id, j);
      }

      // Now create the merged MAP that will operate on the sunken MERGE.
      merged_map = impl->maps.Create(first_map->functor, first_map->range,
                                     first_map->is_positive);
      merged_map->color = color;
      for (auto j = 0u; j < num_cols; ++j) {
        const auto first_map_col = first_map->columns[j];
        (void) merged_map->columns.Create(first_map_col->var, merged_map,
                                          first_map_col->id, j);
      }

      // The sunken merge's first set of columns are the input columns to the
      // map.
      for (auto j = 0u; j < num_input_cols; ++j) {
        merged_map->input_columns.AddUse(sunk_merge->columns[j]);
      }

      // The sunken merge's second set of columns are the attached columns
      // to the map.
      for (auto j = 0u; j < num_attached_cols; ++j) {
        merged_map->attached_columns.AddUse(
            sunk_merge->columns[num_input_cols + j]);
      }

      sunk_merge->merged_views.AddUse(first_map_input_tuple);
    }

    inout_view->is_canonical = false;
    inout_view = nullptr;
    sunk_merge->merged_views.AddUse(proxy_inputs(next_map));
  }

  // It's worth it to try recursion.
  if (num_failed >= 2u) {
    changed_rec = SinkThroughMaps(impl, inout_views);
  }

  // We've bottomed out with a list of `nullptr` tuples, i.e. we've tried to
  // recursively process stuff in the list and there's nothing more to do.
  if (!first_map) {
    return false;

  // We failed to match `first_tuple` against anything else. It could be that
  // `first_tuple` takes some constant inputs, or that its predecessor's
  // shape or its usage of its predecessor doesn't match anything else. We'll
  // assume we did a recursive call and return the result of that.
  } else if (!merged_map) {
    inout_views[first_map_index] = first_map;
    return changed_rec;

  // We've successfully merged at least two tuples into one tuple taking a
  // union of the predecessors of the merged tuples.
  } else {
    assert(merged_map != nullptr);
    inout_views[first_map_index] = merged_map;
    sunk_merge->sink_penalty = sunk_merge->Depth();
    return true;
  }
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
