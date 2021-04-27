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
bool Node<QueryMerge>::Canonicalize(
    QueryImpl *query, const OptimizationContext &opt, const ErrorLog &) {

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
      VIEW *tuple_source = VIEW::GetIncomingView(tuple->input_columns);

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
               merge->negative_conditions.Empty() &&
               merge->OnlyUser()) {

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
      (void) tuple->columns.Create(
          out_col->var, out_col->type, tuple, out_col->id);
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
              out_col->var, out_col->type, guarded_view, out_col->id);
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
        auto new_out_col = new_columns.Create(
            out_col->var, out_col->type, this, out_col->id);
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

//    // Can't sink yet.
//    if (sink_penalty) {
////      --sink_penalty;
//      goto done;
//    }

    std::unordered_map<const char *, std::vector<VIEW *>> grouped_views;
    bool has_opportunity = false;

//    // Wait for all things being merged to not have a sinking penalty.
//    auto is_penalized = false;
//    for (auto merged_view : merged_views) {
//      if (merged_view->sink_penalty) {
////        --merged_view->sink_penalty;
//        is_penalized = true;
//        break;
//      }
//    }
//
//    if (is_penalized) {
//      goto done;
//    }

    for (auto merged_view : merged_views) {

      // Don't even try to sink through conditions.
      if (merged_view->positive_conditions.Size() ||
          merged_view->negative_conditions.Size() ||
          !!merged_view->sets_condition) {
        goto done;
      }

//      while (auto tuple = merged_view->AsTuple()) {
//        auto incoming_view = VIEW::GetIncomingView(tuple->input_columns);
//        if (tuple->ForwardsAllInputsAsIs(incoming_view)) {
//          merged_view = incoming_view;
//          assert(false);
//        } else {
//          break;
//        }
//      }

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
      VIEW * const first_view = similar_views[0];
      if (first_view->AsTuple()) {
        if (SinkThroughTuples(query, similar_views)) {
          changed = true;
        }
      } else if (first_view->AsMap()) {
        if (SinkThroughMaps(query, similar_views)) {
          changed = true;
        }
      } else if (first_view->AsNegate()) {
        if (SinkThroughNegations(query, similar_views)) {
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
      non_local_changes = true;
      merged_views.Swap(new_merged_views);
    }

//    sink_penalty = Depth() + 1u;
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
bool Node<QueryMerge>::SinkThroughTuples(
    QueryImpl *impl, std::vector<VIEW *> &inout_views) {
  VIEW *first_tuple_pred = nullptr;
  TUPLE *first_tuple = nullptr;
  TUPLE *merged_tuple = nullptr;
  MERGE *sunk_merge = nullptr;
  unsigned first_tuple_index = 0u;
  unsigned num_cols = 0u;
  unsigned num_pred_cols = 0u;

  for (auto &inout_view : inout_views) {
    TUPLE *next_tuple = nullptr;
    VIEW *next_tuple_pred = nullptr;

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
          goto done;
        } else {
          assert(!first_tuple_pred ||
                 first_tuple_pred == first_tuple_input_col->view);
          first_tuple_pred = first_tuple_input_col->view;
        }
      }

      // Tuple taking only constants; can't sink through it.
      if (!first_tuple_pred) {
        goto done;
      }

      assert(first_tuple_pred);
      num_cols = first_tuple->columns.Size();
      num_pred_cols = first_tuple_pred->columns.Size();
      continue;
    }

    next_tuple = inout_view->AsTuple();

    // The next tuple isn't allowed to have any constant inputs because we
    // can't feed those into a merge.
    for (auto next_tuple_input_col : next_tuple->input_columns) {
      if (next_tuple_input_col->IsConstant()) {
        goto skip;
      } else {
        assert(!next_tuple_pred || next_tuple_pred == next_tuple_input_col->view);
        next_tuple_pred = next_tuple_input_col->view;
      }
    }

    // Predecessors have a different shape of their outputs.
    if (num_pred_cols != next_tuple_pred->columns.Size()) {
      goto skip;
    }

    // Make sure that all columns of each tuple's predecessor have the same
    // type, so that we can make a merge that is compatible with all of them.
    for (auto j = 0u; j < num_pred_cols; ++j) {
      const auto first_pred_tuple_col = first_tuple_pred->columns[j];
      const auto next_pred_tuple_col = next_tuple_pred->columns[j];
      if (first_pred_tuple_col->type != next_pred_tuple_col->type) {
        goto skip;
      }
    }

    // Make sure that `first_tuple` and `next_tuple` take their inputs from
    // the same indexed columns of their predecessors.
    for (auto j = 0u; j < num_cols; ++j) {
      COL * const first_tuple_col = first_tuple->input_columns[j];
      COL * const next_tuple_col = next_tuple->input_columns[j];

      if (first_tuple_col->Index() != next_tuple_col->Index()) {
        goto skip;
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
        (void) sunk_merge->columns.Create(
            first_tuple_pred_col->var, first_tuple_pred_col->type,
            sunk_merge, first_tuple_pred_col->id, j);
      }

      // Now create the merged tuple that will select only the columns of
      // interest from the sunken merge.
      merged_tuple = impl->tuples.Create();
      merged_tuple->color = color;
      for (auto j = 0u; j < num_cols; ++j) {
        const auto first_tuple_col = first_tuple->columns[j];
        const auto first_tuple_pred_col = first_tuple->input_columns[j];
        (void) merged_tuple->columns.Create(
            first_tuple_col->var, first_tuple_col->type,
            merged_tuple, first_tuple_col->id, j);

        // Select in the correct column from the sunken merge to pass into
        // the final tuple.
        merged_tuple->input_columns.AddUse(
            sunk_merge->columns[first_tuple_pred_col->Index()]);
      }

      sunk_merge->merged_views.AddUse(first_tuple_pred);
    }

    next_tuple->is_canonical = false;
    inout_view = nullptr;
    sunk_merge->merged_views.AddUse(next_tuple_pred);

  skip:
    continue;
  }

done:

  // We've bottomed out with a list of `nullptr` tuples, i.e. we've tried to
  // recursively process stuff in the list and there's nothing more to do.
  if (!first_tuple) {
    return false;
  }

  // It's worth it to try recursion.
  auto changed_rec = SinkThroughTuples(impl, inout_views);

  // We failed to match `first_tuple` against anything else. It could be that
  // `first_tuple` takes some constant inputs, or that its predecessor's
  // shape or its usage of its predecessor doesn't match anything else. We'll
  // assume we did a recursive call and return the result of that.
  if (!merged_tuple) {
    inout_views[first_tuple_index] = first_tuple;
    return changed_rec;

  // We've successfully merged at least two tuples into one tuple taking a
  // union of the predecessors of the merged tuples.
  } else {
    assert(merged_tuple != nullptr);
    inout_views[first_tuple_index] = merged_tuple;
    merged_tuple->sink_penalty = merged_tuple->Depth() + 1u;
    return true;
  }
}

namespace {

// Make a tuple that will take the place of a negation that will be merged
// with other negations.
static TUPLE *MakeSameShapedTuple(QueryImpl *impl, MAP *map) {
//  const auto input_tuple_for_map = impl->tuples.Create();
//  input_tuple_for_map->color = color;
//  auto col_index = 0u;
//
//  for (auto j = 0u; j < num_input_cols; ++j) {
//    const auto orig_input_col = map->input_columns[j];
//    input_tuple_for_map->columns.Create(
//        orig_input_col->var, orig_input_col->type, input_tuple_for_map,
//        orig_input_col->id, col_index++);
//    input_tuple_for_map->input_columns.AddUse(orig_input_col);
//  }
//
//  for (auto j = 0u; j < num_attached_cols; ++j) {
//    const auto orig_attached_col = map->attached_columns[j];
//    input_tuple_for_map->columns.Create(
//        orig_attached_col->var, orig_attached_col->type,
//        input_tuple_for_map, orig_attached_col->id, col_index++);
//    input_tuple_for_map->input_columns.AddUse(orig_attached_col);
//  }
//
//  return input_tuple_for_map;


  TUPLE *tuple = impl->tuples.Create();

  auto col_index = 0u;
  for (auto in_col : map->input_columns) {
    (void) in_col;
    auto out_col = map->columns[col_index];
    (void) tuple->columns.Create(
        out_col->var, out_col->type, tuple, out_col->id, col_index++);
  }
  for (auto in_col : map->attached_columns) {
    (void) in_col;
    auto out_col = map->columns[col_index];
    (void) tuple->columns.Create(
        out_col->var, out_col->type, tuple, out_col->id, col_index++);
  }

  for (auto col : map->input_columns) {
    tuple->input_columns.AddUse(col);
  }

  for (auto col : map->attached_columns) {
    tuple->input_columns.AddUse(col);
  }

  map->CopyTestedConditionsTo(tuple);
  map->CopyDifferentialAndGroupIdsTo(tuple);
  assert(!map->sets_condition);

  return tuple;
}

}  // namespace

// Convert two or more MAPs into a single MAP that reads its data from
// a union, where that union reads its data from the sources of the two
// MAPs.
bool Node<QueryMerge>::SinkThroughMaps(
    QueryImpl *impl, std::vector<VIEW *> &inout_views) {

  MAP *first_map = nullptr;
  MAP *merged_map = nullptr;
  MERGE *sunk_merge = nullptr;
  unsigned first_map_index = 0u;
  unsigned num_cols = 0u;
  unsigned num_input_cols = 0u;
  unsigned num_attached_cols = 0u;

  for (auto &inout_view : inout_views) {
    if (!inout_view) {
      first_map_index++;
      continue;

    // We've found the first non-null map; we'll try to match the remaining
    // non-`nullptr` entries in `inout_views` against it.
    } else if (!first_map) {
      first_map = inout_view->AsMap();
      inout_view = nullptr;
      num_cols = first_map->columns.Size();
      num_input_cols = first_map->input_columns.Size();
      num_attached_cols = first_map->attached_columns.Size();
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
        num_cols != next_map->columns.Size() ||
        num_input_cols != next_map->input_columns.Size() ||
        num_attached_cols != next_map->attached_columns.Size()) {
      continue;
    }

    // Make sure the types of input columns match.
    for (auto j = 0u; j < num_input_cols; ++j) {
      if (first_map->input_columns[j]->type !=
          next_map->input_columns[j]->type) {
        continue;
      }
    }

    // Make sure the types of attached columns match.
    for (auto j = 0u; j < num_attached_cols; ++j) {
      if (first_map->attached_columns[j]->type !=
          next_map->attached_columns[j]->type) {
        continue;
      }
    }

    assert(ParsedDeclaration(first_map->functor).BindingPattern() ==
           ParsedDeclaration(next_map->functor).BindingPattern());

    if (!merged_map) {
      first_map->is_canonical = false;
      const auto first_map_input_tuple = MakeSameShapedTuple(impl, first_map);

      // Create the sunken merge.
      sunk_merge = impl->merges.Create();
      sunk_merge->color = color;
      for (auto j = 0u; j < (num_input_cols + num_attached_cols); ++j) {
        const auto first_tuple_pred_col = first_map_input_tuple->columns[j];
        (void) sunk_merge->columns.Create(
            first_tuple_pred_col->var, first_tuple_pred_col->type,
            sunk_merge, first_tuple_pred_col->id, j);
      }

      // Now create the merged MAP that will operate on the sunken MERGE.
      merged_map = impl->maps.Create(first_map->functor, first_map->range,
                                     first_map->is_positive);
      merged_map->color = color;
      for (auto j = 0u; j < num_cols; ++j) {
        const auto first_map_col = first_map->columns[j];
        (void) merged_map->columns.Create(
            first_map_col->var, first_map_col->type,
            merged_map, first_map_col->id, j);
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

    next_map->is_canonical = false;
    inout_view = nullptr;
    sunk_merge->merged_views.AddUse(MakeSameShapedTuple(impl, next_map));
  }

  // We've bottomed out with a list of `nullptr` tuples, i.e. we've tried to
  // recursively process stuff in the list and there's nothing more to do.
  if (!first_map) {
    return false;
  }

  // It's worth it to try recursion.
  auto changed_rec = SinkThroughMaps(impl, inout_views);

  // We failed to match `first_tuple` against anything else. It could be that
  // `first_tuple` takes some constant inputs, or that its predecessor's
  // shape or its usage of its predecessor doesn't match anything else. We'll
  // assume we did a recursive call and return the result of that.
  if (!merged_map) {
    inout_views[first_map_index] = first_map;
    return changed_rec;

  // We've successfully merged at least two tuples into one tuple taking a
  // union of the predecessors of the merged tuples.
  } else {
    assert(merged_map != nullptr);
    inout_views[first_map_index] = merged_map;
    merged_map->sink_penalty = merged_map->Depth() + 1u;
    return true;
  }
}

namespace {

static MERGE *MakeSameShapedMerge(QueryImpl *impl, NEGATION *negation) {
  MERGE *merge = impl->merges.Create();
  auto col_index = 0u;
  for (auto col : negation->columns) {
    merge->columns.Create(col->var, col->type, merge, col->id, col_index++);
  }
  return merge;
}

// Make a tuple that will take the place of a negation that will be merged
// with other negations.
static TUPLE *MakeSameShapedTuple(QueryImpl *impl, NEGATION *negation) {
  TUPLE *tuple = impl->tuples.Create();
  auto col_index = 0u;
  for (auto col : negation->columns) {
    tuple->columns.Create(col->var, col->type, tuple, col->id, col_index++);
  }

  for (auto col : negation->input_columns) {
    tuple->input_columns.AddUse(col);
  }

  for (auto col : negation->attached_columns) {
    tuple->input_columns.AddUse(col);
  }

  negation->CopyTestedConditionsTo(tuple);
  negation->CopyDifferentialAndGroupIdsTo(tuple);
  assert(!negation->sets_condition);

  return tuple;
}

static TUPLE *TagProxy(QueryImpl *impl, VIEW *view, COL *tag_in_col) {
  TUPLE * const tuple = impl->tuples.Create();
  COL *tag_out_col = tuple->columns.Create(
      tag_in_col->var, tag_in_col->type, tuple, tag_in_col->id, 0u);
  tag_out_col->CopyConstantFrom(tag_in_col);

  tuple->input_columns.AddUse(tag_in_col);

  auto col_index = 1u;
  for (COL *in_col : view->columns) {
    (void) tuple->columns.Create(
        in_col->var, in_col->type, tuple, in_col->id, col_index++);
    tuple->input_columns.AddUse(in_col);
  }

  return tuple;
}

static TUPLE *TagPredecessor(QueryImpl *impl, NEGATION *view, COL *tag_in_col) {
  TUPLE * const tuple = impl->tuples.Create();
  COL *tag_out_col = tuple->columns.Create(
      tag_in_col->var, tag_in_col->type, tuple, tag_in_col->id, 0u);
  tag_out_col->CopyConstantFrom(tag_in_col);

  tuple->input_columns.AddUse(tag_in_col);

  auto col_index = 1u;
  for (COL *in_col : view->input_columns) {
    (void) tuple->columns.Create(
        in_col->var, in_col->type, tuple, in_col->id, col_index++);
    tuple->input_columns.AddUse(in_col);
  }

  for (COL *in_col : view->attached_columns) {
    (void) tuple->columns.Create(
        in_col->var, in_col->type, tuple, in_col->id, col_index++);
    tuple->input_columns.AddUse(in_col);
  }

  return tuple;
}

// Create a special constant tag value that we can use to make two or more
// different but like-shaped negated views get combined into a single negated
// view, so that we can sink MERGEs through the NEGATIONs.
static COL *CreateTag(QueryImpl *impl, unsigned &num_used_tags) {
  if (num_used_tags < impl->tag_columns.size()) {
    return impl->tag_columns[num_used_tags++];
  }

  const auto tag_val = impl->tags.Size();
  assert(tag_val == (tag_val & 0xffffu));  // 16-bit vals!

  TAG * const tag = impl->tags.Create(static_cast<uint16_t>(tag_val));

  SELECT *select = impl->selects.Create(tag, DisplayRange());
  COL *tag_in_col = select->columns.Create(
      TypeLoc(TypeKind::kUnsigned16), select, tag_val, 0u);
  assert(tag_in_col->IsConstant());

  impl->tag_columns.push_back(tag_in_col);
  ++num_used_tags;
  return tag_in_col;
}

}  // namespace

bool Node<QueryMerge>::SinkThroughNegations(
    QueryImpl *impl, std::vector<VIEW *> &inout_views) {

  auto first_negate_index = 0u;
  NEGATION *first_negate = nullptr;
  NEGATION *merged_negation = nullptr;
  VIEW *output = nullptr;
  MERGE *input_merge = nullptr;
  MERGE *negated_view_merge = nullptr;
  unsigned num_cols = 0u;
  unsigned num_input_cols = 0u;
  unsigned num_attached_cols = 0u;
  VIEW *negated_view = nullptr;
  unsigned num_used_tags = 0u;

  // TODO(pag): Default `false` tries to greedily go with the equivalent negated
  //            views. `true` unconditionally tags the negated view.
  bool using_tags = false;

  EqualitySet eq;
  for (auto &inout_view : inout_views) {
    if (!inout_view) {
      first_negate_index++;
      continue;

    } else if (!first_negate) {
      first_negate = inout_view->AsNegate();
      num_cols = first_negate->columns.Size();
      num_input_cols = first_negate->input_columns.Size();
      num_attached_cols = first_negate->attached_columns.Size();
      negated_view = first_negate->negated_view.get();
      inout_view = nullptr;
      continue;
    }

    NEGATION * const next_negate = inout_view->AsNegate();
    assert(next_negate != first_negate);

    if(next_negate->columns.Size() != num_cols ||
       next_negate->input_columns.Size() != num_input_cols ||
       next_negate->attached_columns.Size() != num_attached_cols) {
      continue;
    }

    // Make sure the types of input columns match.
    for (auto j = 0u; j < num_input_cols; ++j) {
      if (first_negate->input_columns[j]->type !=
          next_negate->input_columns[j]->type) {
        continue;
      }
    }

    // Make sure the types of attached columns match.
    for (auto j = 0u; j < num_attached_cols; ++j) {
      if (first_negate->attached_columns[j]->type !=
          next_negate->attached_columns[j]->type) {
        continue;
      }
    }

    if (!using_tags) {
      eq.Clear();
      if (!negated_view->Equals(eq, next_negate->negated_view.get())) {
        using_tags = true;
      }
    }

    // Mark this view as no longer available because we'll be merging it.
    inout_view = nullptr;
    next_negate->is_canonical = false;

    auto col_index = 0u;

    // We've found some negations that are same-shaped, but use different
    // negated views. We can convert these into a union of the different
    // negated views, and inject in an additional "tag" column that allows
    // new tag-aware versions of the negations to be comined via a merge.
    if (using_tags) {

      if (!output) {
        first_negate->color = 0xff00;

        COL * const first_tag_col = CreateTag(impl, num_used_tags);

        // Create a tag for the first negated view.
        VIEW * const first_negated_view = TagProxy(
            impl, first_negate->negated_view.get(), first_tag_col);

        negated_view_merge = impl->merges.Create();
        negated_view_merge->merged_views.AddUse(first_negated_view);

        col_index = 0u;
        for (COL *col : first_negated_view->columns) {
          (void) negated_view_merge->columns.Create(
              col->var, col->type, negated_view_merge, col->id, col_index++);
        }

        // Create a merge of the predecessors of the negation.
        input_merge = impl->merges.Create();
        col_index = 0u;
        (void) input_merge->columns.Create(
            first_tag_col->var, first_tag_col->type, input_merge,
            first_tag_col->id, col_index++);
        for (auto col : first_negate->columns) {
          input_merge->columns.Create(
              col->var, col->type, input_merge, col->id, col_index++);
        }

        // Add the first negation's predecessor to `input_merge`.
        input_merge->merged_views.AddUse(
            TagPredecessor(impl, first_negate, first_tag_col));

        // Make a new, tag column-aware negation that operates on `input_merge`.
        merged_negation = impl->negations.Create();
        merged_negation->negated_view.Emplace(
            merged_negation, negated_view_merge);

        // Tag column.
        col_index = 0u;
        (void) merged_negation->columns.Create(
            first_tag_col->var, first_tag_col->type, merged_negation,
            first_tag_col->id, 0u);
        merged_negation->input_columns.AddUse(input_merge->columns[0u]);

        // Columns to negate.
        for (auto i = 0u; i < first_negate->input_columns.Size(); ++i) {
          ++col_index;
          COL * const col = input_merge->columns[col_index];
          (void) merged_negation->columns.Create(
              col->var, col->type, merged_negation, col->id, col_index - 1u);
          merged_negation->input_columns.AddUse(col);
        }

        // Columns to bring along for the ride.
        for (auto i = 0u; i < first_negate->attached_columns.Size(); ++i) {
          ++col_index;
          COL * const col = input_merge->columns[col_index];
          (void) merged_negation->columns.Create(
              col->var, col->type, merged_negation, col->id, col_index - 1u);
          merged_negation->attached_columns.AddUse(col);
        }

        // Present an original-sized tuple as the result.
        output = impl->tuples.Create();
        for (auto i = 0u; i < first_negate->columns.Size(); ++i) {
          COL * const col = merged_negation->columns[i + 1u];
          (void) output->columns.Create(
              col->var, col->type, output, col->id, i);
          output->input_columns.AddUse(col);
        }

        // NOTE(pag): This code is not tested, so whatever triggers this can
        //            act as a test :-)
        output->color = 0xff;

      }

      COL * const next_tag_col = CreateTag(impl, num_used_tags);

      next_negate->color = 0xff00;
      // Create a tag for the Nth negated view.
      negated_view_merge->merged_views.AddUse(TagProxy(
          impl, next_negate->negated_view.get(), next_tag_col));

      input_merge->merged_views.AddUse(
          TagPredecessor(impl, next_negate, next_tag_col));

    // Not using tags, we've found some negations that use equivalent negated
    // views.
    } else {
      if (!merged_negation) {
        first_negate->is_canonical = false;

        input_merge = MakeSameShapedMerge(impl, first_negate);
        input_merge->color = 0xff0000;
        input_merge->merged_views.AddUse(MakeSameShapedTuple(impl, first_negate));

        // Make the new negation.
        merged_negation = impl->negations.Create();
        output = merged_negation;

        merged_negation->negated_view.Emplace(merged_negation, negated_view);

        col_index = 0u;
        for (auto col : first_negate->columns) {
          (void) merged_negation->columns.Create(
              col->var, col->type, merged_negation, col->id, col_index++);
        }
        col_index = 0u;
        for (auto col : first_negate->input_columns) {
          (void) col;
          merged_negation->input_columns.AddUse(
              input_merge->columns[col_index++]);
        }
        for (auto col : first_negate->attached_columns) {
          (void) col;
          merged_negation->attached_columns.AddUse(
              input_merge->columns[col_index++]);
        }
      }

      // Add the mergable negation into `input_merge`.
      input_merge->merged_views.AddUse(MakeSameShapedTuple(impl, next_negate));
    }
  }

  if (!first_negate) {
    return false;
  }

  // Remove this view from availability.
  auto changed_rec = SinkThroughNegations(impl, inout_views);

  if (output) {
    inout_views[first_negate_index] = output;
    output->sink_penalty = output->Depth() + 1u;
    return true;

  } else {
    inout_views[first_negate_index] = first_negate;
    return changed_rec;
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
