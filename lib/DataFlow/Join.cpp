// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <unordered_set>

#include <drlojekyll/Util/EqualitySet.h>

#include <sstream>

#include "Optimize.h"

namespace hyde {

Node<QueryJoin>::~Node(void) {}

Node<QueryJoin> *Node<QueryJoin>::AsJoin(void) noexcept {
  return this;
}

uint64_t Node<QueryJoin>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = HashInit();

  if (out_to_in.empty()) {
    return hash;
  }

  auto local_hash = hash;

  assert(input_columns.Size() == 0);
//
//  for (auto col : columns) {
//    auto in_set = out_to_in.find(col);
//    assert(in_set != out_to_in.end());
//    uint64_t pivot_hash = 0xC4CEB9FE1A85EC53ull;
//    for (auto in_col : in_set->second) {
//      pivot_hash ^= in_col->Hash();
//    }
//    local_hash ^= __builtin_rotateright64(local_hash, 13) * pivot_hash;
//  }

  for (auto joined_view : joined_views) {
    local_hash ^= joined_view->Hash();
  }

  if (num_pivots) {
    local_hash ^= __builtin_rotateright64(local_hash, (num_pivots + 53u) % 64u) *
                  local_hash;
  }

  local_hash ^= __builtin_rotateright64(local_hash, (columns.Size() + 43u) % 64u) *
                local_hash;

  local_hash ^= __builtin_rotateright64(local_hash, (joined_views.Size() + 33u) % 64u) *
                local_hash;

  hash = local_hash;
  return local_hash;
}

unsigned Node<QueryJoin>::Depth(void) noexcept {
  if (depth) {
    return depth;
  }

  auto estimate = EstimateDepth(positive_conditions, 1u);
  estimate = EstimateDepth(negative_conditions, estimate);
  for (const auto &[out_col, in_cols] : out_to_in) {
    for (COL *in_col : in_cols) {
      estimate = std::max(estimate, in_col->view->depth);
    }
  }

  depth = estimate + 1u;  // Base case in case of cycles.

  auto real = 1u;
  for (const auto &[out_col, in_cols] : out_to_in) {
    real = GetDepth(in_cols, real);
  }

  real = GetDepth(positive_conditions, real);
  real = GetDepth(negative_conditions, real);
  depth = real + 1u;

  return depth;
}

// Verify that all pivot sets cover the same views.
//
// TODO(pag): Re-implement to work in the case of constant propagations.
void Node<QueryJoin>::VerifyPivots(void) {
#if 0
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
#endif
}

// Replace `view`, which should be a member of `joined_views` with
// `replacement_view` in this JOIN.
void Node<QueryJoin>::ReplaceViewInJoin(VIEW *view, VIEW *replacement_view) {
  is_canonical = false;

  UseList<VIEW> new_joined_views(this, true  /* is_weak */);
  for (auto joined_view : joined_views) {
    if (joined_view) {
      if (joined_view == view) {
        new_joined_views.AddUse(replacement_view);
      } else {
        new_joined_views.AddUse(joined_view);
      }
    }
  }


  std::unordered_map<COL *, UseList<COL>> new_out_to_in;
  for (auto &[out_col, in_cols] : out_to_in) {
    new_out_to_in.emplace(out_col, this);
    auto &new_in_cols = new_out_to_in.find(out_col)->second;

    for (auto col : in_cols) {
      if (col->view == view) {
        new_in_cols.AddUse(replacement_view->columns[col->Index()]);
      } else {
        new_in_cols.AddUse(col);
      }
    }
  }

  joined_views.Swap(new_joined_views);
  out_to_in.swap(new_out_to_in);
}

// Replace the pivot column `pivot_col` with `const_col`.
void Node<QueryJoin>::ReplacePivotWithConstant(QueryImpl *query, COL *pivot_col,
                                               COL *const_col) {
  is_canonical = false;

  TUPLE *tuple = query->tuples.Create();
  for (auto col : columns) {
    auto out_col = tuple->columns.Create(
        col->var, tuple, col->id, col->Index());
    out_col->CopyConstant(col);
  }

#ifndef NDEBUG
  std::stringstream ss;
  ss << "DEL-PIVOT-" << pivot_col->Index() << "(" << KindName();
  if (!producer.empty()) {
    ss << ": " << producer;
  }
  ss << ')';
  tuple->producer = ss.str();
#endif

  ReplaceAllUsesWith(tuple);

  std::unordered_map<COL *, COL *> out_to_new_out;
  DefList<COL> new_columns(this);
  std::unordered_map<COL *, UseList<COL>> new_out_to_in;

  auto new_col_index = 0u;
  for (auto col : columns) {
    if (col == pivot_col) {
      col->CopyConstant(const_col);
      col->ReplaceAllUsesWith(const_col);
      tuple->input_columns.AddUse(const_col);

    } else {
      auto new_col = new_columns.Create(
          col->var, this, col->id, new_col_index++);
      tuple->input_columns.AddUse(new_col);
      new_col->CopyConstant(col);
      new_out_to_in.emplace(new_col, this);
      out_to_new_out.emplace(col, new_col);
    }
  }

  bool is_pivot_set = false;
  if (auto in_cols_it = out_to_in.find(pivot_col); in_cols_it != out_to_in.end()) {
    auto &pivot_cols = in_cols_it->second;
    if (1u < pivot_cols.Size()) {
      is_pivot_set = true;
      PropagateConstAcrossPivotSet(query, const_col, pivot_cols);
    }
  }

  for (auto col : columns) {
    if (col == pivot_col) {
      continue;
    }

    auto new_col = out_to_new_out[col];
    assert(new_col != nullptr);
    col->ReplaceAllUsesWith(new_col);
  }

  std::vector<VIEW *> views;
  unsigned num_joined_views = 0u;
  for (auto view : joined_views) {
    if (view) {  // It's a weak use list, so some might be `nullptr`s.
      ++num_joined_views;
    }
  }

  (void) num_joined_views;

  for (auto col : columns) {
    if (col == pivot_col) {
      continue;
    }

    auto new_col = out_to_new_out[col];
    assert(new_col != nullptr);

    auto &old_pivot_set = out_to_in.find(col)->second;
    auto &new_pivot_set = new_out_to_in.find(new_col)->second;

    const auto old_size = old_pivot_set.Size();

    // If this is a pivot set then use its size as our estimate of how many
    // views to join.
    if (1u < old_size) {
      assert(old_size == num_joined_views);
      if (views.empty()) {
        views.resize(old_size);
      } else {
        assert(old_size == views.size());
      }
    }

    // Get the views in the same order that we saw them before. Given that
    // we're eliminating a pivot, it's also possible that we're eliminating
    // an incoming VIEW, and so we may end up with `nullptr`s inside of `views`.
    auto i = 0u;
    for (auto in_col : old_pivot_set) {
      new_pivot_set.AddUse(in_col);

      if (1u < old_size && !in_col->IsConstant()) {
        auto &view = views[i];
        if (!view) {
          view = in_col->view;
        } else {
          assert(view == in_col->view);
        }
      }

      ++i;
    }
  }

//  UseList<VIEW> new_joined_views(this);
//  for (auto view : views) {
//    if (view) {
//      new_joined_views.AddUse(view);
//    }
//  }

  // The column that we're removing is indeed a true pivot column.
  if (is_pivot_set) {
    assert(0u < num_pivots);
    num_pivots -= 1u;
  }

//  joined_views.Swap(new_joined_views);
  out_to_in.swap(new_out_to_in);
  columns.Swap(new_columns);

  return;
#if 0

  if (!joined_views.Empty()) {
    return;
  }

  views.clear();

  // This JOIN looks like it should probably be a cross-product, except that
  // we still think there should be pivots. The `conflicting_constants.dr`
  // example emodies the issue:
  //
  //    foo(A, B, C) : bar(A, B, C), baz(A, B, C).
  //    bar(1, 2, 3).
  //    baz(4, 5, 6).
  //
  // We have a join of two views, but all pivot columns are constants, and so
  // we cannot recover the provenance of them from the pivot sets, and worse,
  // because of this, we also can't figure out what comparisons to enforce on
  // the source views to make the
  if (num_pivots) {
    assert(1u < new_joined_views.Size());

    for (auto view : new_joined_views) {
      views.push_back(view);
    }

    for (auto &[out_col, in_cols] : out_to_in) {
      if (1u == in_cols.Size()) {
        continue;
      }

      assert(in_cols.Size() == views.size());
      COL *pivot_const_col = nullptr;
      auto i = 0u;
      for (auto in_col : in_cols) {
        assert(in_col->IsConstantOrConstantRef());
        if (!pivot_const_col) {
          pivot_const_col = in_col;
        }

        // Add a filter on the incoming view that the first constant column
        // in the pivot set matches the incoming constant from this view, then
        // update `view`s to point at this now constrained joined view.
        if (pivot_const_col != in_col) {
          views[i] = views[i]->ProxyWithComparison(
              query, ComparisonOperator::kEqual, pivot_const_col, in_col);
        }

        ++i;
      }
    }
  }

  // Go find all the views that are being joined.
  for (auto &[out_col, in_cols] : out_to_in) {
    for (auto in_col : in_cols) {
      if (!in_col->IsConstantOrConstantRef()) {
        views.push_back(in_col->view);
      }
    }
  }

  // Keep only unique views.
  std::sort(views.begin(), views.end());
  auto it = std::unique(views.begin(), views.end());
  views.erase(it, views.end());

  // This JOIN isn't needed, get rid of it. This could be that all incoming
  // values are constants (`views` is empty), or that there is just one
  // incoming view.
  if (views.size() <= 1u) {
    TUPLE *tuple = query->tuples.Create();
    for (auto col : columns) {
      auto &in_cols = out_to_in.find(col)->second;
      assert(in_cols.Size() == 1u);
      auto in_col = in_cols[0];
      if (in_col->IsConstantOrConstantRef()) {
        col->ReplaceAllUsesWith(in_col);
      }
      tuple->columns.Create(col->var, tuple, col->id);
      tuple->input_columns.AddUse(in_col);
    }

    ReplaceAllUsesWith(tuple);
    is_used = false;
    is_dead = true;
    out_to_in.clear();
    return;

  // This JOIN is still needed and is a cross-product.
  } else {
    for (auto view : views) {
      joined_views.AddUse(view);
    }
  }
#endif
}

// If we have a constant feeding into one of the pivot sets, then we want to
// eliminate that pivot set and instead propagate CMP nodes to all pivot
// sources.
void Node<QueryJoin>::PropagateConstAcrossPivotSet(
    QueryImpl *query, COL *const_col, UseList<COL> &pivot_cols) {

  is_canonical = false;

  std::vector<COL *> cols;
  std::vector<VIEW *> views;

  // NOTE(pag): Cache `pivot_cols` because calls below to `ReplaceViewInJoin`
  //            are going to destroy it below.
  for (auto in_col : pivot_cols) {
    cols.push_back(in_col);
    views.push_back(in_col->IsConstant() ? nullptr : in_col->view);
  }

  // This is super annoying but technically possible. Suppose we had the
  // following clause body: `foo(A, B), foo(C, B), A=1, C=A, C=2`. We
  // distinguish constants by their spelling, so two different constants may
  // logically be the same (`1` and `0x1`), or may be different (`1` and `2`)
  // and we don't really try to figure that out, we just punt on the C++
  // compiler to do that for us.
  //
  // Anyway, the `out_to_in` mapping between JOIN output columns and their
  // respective input columns doesn't give us provenance for those input
  // columns. Normally that's fine, except when those input columns are
  // constants, in which case we have no idea what views proposed those
  // constants. Thus, if we ever encounter two or more unique constants in
  // a pivot set, then it's best for us to add comparisons between both of them
  // to all incoming views.
  for (auto in_col : cols) {
    if (in_col->IsConstant() && in_col != const_col) {

      // Apply the comparison to all views because we don't know who proposed
      // this constant.
      for (auto &view : views) {
        if (view) {
          auto proxy_view = view->ProxyWithComparison(
              query, ComparisonOperator::kEqual, const_col, in_col);
          ReplaceViewInJoin(view, proxy_view);
          view = proxy_view;
        }
      }
    }
  }

  auto i = 0u;
  for (auto in_col : cols) {
    const auto view = views[i++];
    if (in_col == const_col || in_col->IsConstant()) {
      continue;
    }
    const auto proxy_view = view->ProxyWithComparison(
        query, ComparisonOperator::kEqual, const_col, in_col);
    ReplaceViewInJoin(view, proxy_view);
  }
}

// Put this join into a canonical form, which will make comparisons and
// replacements easier. The approach taken is to sort the incoming columns, and
// to ensure that the iteration order of `out_to_in` matches `columns`.
//
// TODO(pag): If *all* incoming columns for a pivot column are the same, then
//            it no longer needs to be a pivot column.
//
// TODO(pag): If we make the above transform, then a JOIN could devolve into
//            a cross-product.
bool Node<QueryJoin>::Canonicalize(
    QueryImpl *query, const OptimizationContext &opt) {

  if (is_dead || out_to_in.empty()) {
    is_dead = true;
    is_canonical = true;
    return false;
  }

  assert(num_pivots <= columns.Size());
  assert(out_to_in.size() == columns.Size());

  auto non_local_changes = false;
  auto need_remove_non_pivots = false;
  auto all_pivots_are_const_or_constref = true;

  for (auto &[out_col, in_cols] : out_to_in) {
    assert(!in_cols.Empty());
    COL *same_col = in_cols[0];
    COL *same_const = same_col->IsConstant() ? same_col : nullptr;
    COL *same_const_ref = same_col->AsConstant();
    COL *first_const = nullptr;
    COL *first_const_ref = nullptr;

    const auto is_pivot = in_cols.Size() > 1u;
    const auto out_is_const_ref = out_col->IsConstantRef();

    for (auto in_col : in_cols) {
      if (in_col != same_col) {
        same_col = nullptr;
      }

      if (in_col->IsConstant()) {
        if (!first_const) {
          first_const = in_col;
          out_col->CopyConstant(first_const);
        }

        // If we've found a different constant, then set `same_const` to
        // `nullptr` to say that all columns have different constant values.
        if (in_col != same_const) {
          same_const = nullptr;
        }

        // If this is a constant, and we're tracking if all pivot entries
        // are the same constant ref, and if this constant differs from the
        // referred constant, then mark it so that not all pivots share the
        // same constant ref.
        if (in_col != same_const_ref) {
          same_const_ref = nullptr;
        }

        // If this isn't a pivot then do const propagation now, otherwise we'll
        // defer to below for that.
        if (!is_pivot) {
          if (!out_is_const_ref || out_col->IsUsedIgnoreMerges()) {
            non_local_changes = true;
          }
          out_col->CopyConstant(first_const);
          out_col->ReplaceAllUsesWith(first_const);
        }

      } else if (in_col->IsConstantRef()) {
        same_const = nullptr;  // Not all columns are constant.

        if (!first_const_ref) {
          first_const_ref = in_col;
          out_col->CopyConstant(first_const_ref);
        }

        // Two of the pivots refer to different constants.
        if (same_const_ref && in_col->AsConstant() != same_const_ref) {
          same_const_ref = nullptr;
        }

      } else {
        // Not all columns are constant.
        same_const = nullptr;

        // Not all columns are constants or constant refs.
        same_const_ref = nullptr;

        // Not all pivots are constants or constant refs.
        all_pivots_are_const_or_constref = false;
      }
    }

    // All incoming pivot values are the same constant. We can eliminate this
    // pivot.
    if (same_const) {
      ReplacePivotWithConstant(query, out_col, same_const);
      Canonicalize(query, opt);
      return true;

    // All incoming pivot values are either constant, or constant refs, and
    // they all refer to the same constant. We need to keep this pivot around
    // to enforce a data dependency, but we can do constant propagation on the
    // output, thus "breaking" the control dependency.
    //
    // TODO(pag): This is disabled for now, as it's too aggressive / myopic. The
    //            issue comes up in `disappearing_invalid.dr`, where over-
    //            aggressive constant propagation will turn the following
    //            program:
    //
    //                one(1).
    //                impossible(1, B) : one(B), B=2.
    //                output(A) : input(A), impossible(A, B).
    //
    //            into one that behaves as if it contained only `output(1).`.
    } else if (same_const_ref) {
//      if (!out_is_const_ref || out_col->IsUsedIgnoreMerges()) {
//        non_local_changes = true;
//      }
//
//      out_col->ReplaceAllUsesWith(same_const_ref);

    // At least one of the pivots is a reference to a constant; we will inject
    // comparisons across all incominging pivots to ensure that they match this
    // constant.
    } else if (first_const_ref) {
      if (!out_is_const_ref) {
        PropagateConstAcrossPivotSet(query, first_const_ref->AsConstant(), in_cols);
        Canonicalize(query, opt);
        return true;
      }

    // At least one of the pivots is a constant, and none of them are constant
    // references (otherwise we would have been in the prior case). It is
    // possible that after more optimization, we will discover that some of the
    // other pivots are constants or constant refs. Thus, we cannot eagerly
    // eliminate this pivot until we know that *all* incoming values are
    // the same constant.
    } else if (first_const) {
      if (!out_is_const_ref) {
        PropagateConstAcrossPivotSet(query, first_const, in_cols);
        Canonicalize(query, opt);
        return true;
      }
    }

    // Check to see if we should try to remove non-pivot output columns
    // (because they aren't used).
    if (opt.can_remove_unused_columns && !need_remove_non_pivots &&
        in_cols.Size() == 1 && !out_col->IsUsed()) {
      is_canonical = false;
      need_remove_non_pivots = true;
    }
  }

  // The `joined_views` list is a list of weak uses, so some might get nulled
  // out over time. If any are null, then make sure we recompute them.
  for (auto joined_view : joined_views) {
    if (!joined_view) {
      is_canonical = false;
      break;
    }
  }

  if (!non_local_changes && is_canonical) {
    return false;
  }

  // There is at least one output column that isn't needed; go remove it.
  if (opt.can_remove_unused_columns && need_remove_non_pivots) {
    DefList<COL> new_columns(this);
    std::unordered_map<COL *, UseList<COL>> new_out_to_in;

    auto col_index = 0u;
    for (auto &[out_col, in_cols] : out_to_in) {
      const auto is_pivot_col = in_cols.Size() != 1u;
      if (is_pivot_col || out_col->IsUsed()) {
        auto new_out_col = new_columns.Create(
            out_col->var, this, out_col->id, col_index++);
        new_out_to_in.emplace(new_out_col, this);
        new_out_col->CopyConstant(out_col);
        out_col->ReplaceAllUsesWith(new_out_col);

      }
    }

    col_index = 0u;
    for (auto &[out_col, in_cols] : out_to_in) {
      if (in_cols.Size() != 1u || out_col->IsUsed()) {
        auto in_cols_it = new_out_to_in.find(new_columns[col_index++]);
        in_cols_it->second.Swap(in_cols);

      } else {
        assert(in_cols.Size() == 1u);
        in_cols[0]->view->is_canonical = false;
      }

//      // We have to keep this constant ref alive because otherwise we might
//      // risk turning some bad programs into good programs.
//      } else if (in_cols[0]->IsConstantRef()) {
//        attached_columns.AddUse(in_cols[0]);
//      }
    }

    columns.Swap(new_columns);
    out_to_in.swap(new_out_to_in);
    non_local_changes = true;
  }

  std::vector<VIEW *> seen_views;
  for (auto &[out_col, in_cols] : out_to_in) {
    for (auto in_col : in_cols) {
      if (!in_col->IsConstant()) {
        seen_views.push_back(in_col->view);
      }
    }
  }

  std::sort(seen_views.begin(), seen_views.end());
  auto it = std::unique(seen_views.begin(), seen_views.end());
  seen_views.erase(it, seen_views.end());

  if (seen_views.size() < joined_views.Size()) {
    non_local_changes = true;

    UseList<VIEW> new_joined_views(this,  true /* is_weak */);
    for (auto view : seen_views) {
      new_joined_views.AddUse(view);
    }

    joined_views.Swap(new_joined_views);
  }

  // This JOIN isn't needed. If all incoming things are constant then by the
  // time we get down here, we should have sunk conditions down to all the
  // source views
  if (joined_views.Empty() ||
      (all_pivots_are_const_or_constref && joined_views.Size() == 1)) {
    TUPLE *tuple = query->tuples.Create();

    for (auto &[out_col, in_cols] : out_to_in) {
      COL *new_out_col = tuple->columns.Create(out_col->var, tuple, out_col->id);
      COL *new_in_col = in_cols[0];
      for (auto in_col : in_cols) {
        if (in_col->IsConstantRef()) {
          new_in_col = in_col;
          break;
        }
      }

      tuple->input_columns.AddUse(new_in_col);
      new_out_col->CopyConstant(new_in_col);
    }

#ifndef NDEBUG
    std::stringstream ss;
    ss << "JOIN-ELIM-CONSTREF(" << producer << ")";
    tuple->producer = ss.str();
#endif

    ReplaceAllUsesWith(tuple);

    is_canonical = true;
    is_dead = true;
    out_to_in.clear();

    return true;
  }

  is_canonical = true;
  return non_local_changes;

#if 0

  // Maps incoming VIEWs to the pairs of `(out_col, in_col)`, where `out_col`
  // is the output column associated with `in_col`, and `in_col` belongs to
  // the mapped VIEW.
  std::unordered_map<VIEW *, std::vector<std::pair<COL *, COL *>>>
      in_view_to_outin_cols;

  // Maps output columns to constant inputs.
  std::unordered_map<COL *, COL *> out_to_constant_in;

  // If any input column is a constant, then we're going to force this join to
  // be guarded by a tuple. That simplifies const prop and downward
  // restructuring.
  for (auto &[out_col, input_cols] : out_to_in) {

    for (COL *in_col : input_cols) {
      if (in_col->IsConstantOrConstantRef()) {
        out_to_constant_in.emplace(out_col, in_col);
      } else {
        in_view_to_outin_cols[in_col->view].emplace_back(out_col, in_col);
      }
    }
  }

  // If this view is used by a merge then we're not allowed to re-order the
  // columns. Instead, what we can do is create a tuple that will maintain
  // the ordering, and the canonicalize the join order below that tuple.
//  const auto guard_tuple = GuardWithTuple(query, !out_to_constant_in.empty());
  bool non_local_changes = false;

//  if (!out_to_constant_in.empty()) {
////    (void) GuardWithTuple(query, true);
//    non_local_changes = true;
//  }

  in_to_out.clear();

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
      if (in_col->IsConstantOrConstantRef()) {
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
        if (out_col->IsUsedIgnoreMerges()) {
          non_local_changes = true;  // Changing number of columns.
        }
        out_col->ReplaceAllUsesWith(prev_out);
      } else {
        prev_out = out_col;
      }
    }

    // There's a constant, either in the pivot set or just a normal incoming
    // column. Perform constant propagation.
    //
    // NOTE(pag): This JOIN is guaranteed to be guarded by a tuple.
    if (constant_col) {
      if (out_col->IsUsedIgnoreMerges()) {
        non_local_changes = true;
      }
      out_col->ReplaceAllUsesWith(constant_col);
    }
  }

  // This join isn't needed.
  //
  // TODO(pag): Double check how this code behaves in the presence of
  //            constant propagation. I think the check that leads to
  //            `goto skip_remove;` should be sufficient to prevent anything
  //            unsafe.
  if (!joins_at_least_two_views) {
    assert(1u >= in_view_to_outin_cols.size());

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
    tuple->producer = "JOIN-WITH-NO-PIVOTS";
    for (auto cond : positive_conditions) {
      tuple->positive_conditions.AddUse(cond);
    }
    for (auto cond : negative_conditions) {
      tuple->negative_conditions.AddUse(cond);
    }

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
        if (in_col->IsConstantOrConstantRef()) {
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
    joined_views.Clear();
    num_pivots = 0;
    is_used = false;
    is_dead = false;
    return true;  // We created a TUPLE.

  } else {
    assert(1u < in_view_to_outin_cols.size());
  }

  // At least one of the pivot columns is a constant. We want to generate
  // FILTERs for all of the incoming views.
  if (!out_to_constant_in.empty()) {
    for (auto join_out_col : columns) {
      const auto const_col_it = out_to_constant_in.find(join_out_col);
      if (const_col_it == out_to_constant_in.end()) {
        continue;
      }

      auto join_in_cols_it = out_to_in.find(join_out_col);
      if (join_in_cols_it == out_to_in.end()) {
        assert(false);
        continue;
      }

      // We can't propagate to other pivots.
      if (join_in_cols_it->second.Size() == 1) {
        continue;
      }

      for (auto join_in_col : out_to_in.find(join_out_col)->second) {

      }
    }

    for (auto &[view, outin_cols] : in_view_to_outin_cols) {
      std::sort(outin_cols.begin(), outin_cols.end(),
                [] (std::pair<COL *, COL *> a, std::pair<COL *, COL *> b) {
                  return a.first->index < b.first->index;
                });

      for (auto &[out_col, in_col] : outin_cols) {
        const auto const_col_it = out_to_constant_in.find(out_col);
        if (const_col_it == out_to_constant_in.end()) {
          continue;
        }

        const VIEW *in_view = in_col->view;
        assert(in_view == view);

        COL * const const_col = const_col_it->second;
        const auto filter = query->constraints.Create(ComparisonOperator::kEqual);
        filter->producer = "JOIN-CONST-PIVOT";
        filter->can_receive_deletions = in_view->can_produce_deletions;
        filter->can_produce_deletions = filter->can_receive_deletions;
        filter->input_columns.AddUse(in_col);
        filter->input_columns.AddUse(const_col);

        const auto new_in_col = filter->columns.Create(
            in_col->var, filter, in_col->id);

        for (auto &[out_col2, in_col2] : outin_cols) {
          if (out_col != out_col2) {
            filter->attached_columns.AddUse(in_col2);
            in_col2 = filter->columns.Create(in_col2->var, filter, in_col2->id);
          }
        }

        in_col = new_in_col;
      }
    }
  }

skip_remove:

  if (!sort) {
    is_canonical = true;
    return non_local_changes;
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
        for (auto &[old_in_view, inout_cols] : in_view_to_outin_cols) {
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

  if (in_view_to_outin_cols.size() < joined_views.Size()) {
    UseList<VIEW> new_joined_views(this);
    for (auto view : joined_views) {
      if (in_view_to_outin_cols.count(view)) {
        new_joined_views.AddUse(view);
      }
    }
    joined_views.Swap(new_joined_views);
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

    const auto a_view_sort = a->view->Sort();
    const auto b_view_sort = b->view->Sort();

    if (a_size > b_size) {
      return true;
    } else if (a_size < b_size) {
      return false;

    // This isn't a pivot column, it's a regular column. We want to order
    // them together, and put them in order of their views, then in order
    // of their appearance within their views.
    } else if (a_size == 1) {
      auto a_order = std::make_pair(a_view_sort, a->Index());
      auto b_order = std::make_pair(b_view_sort, b->Index());
      return a_order < b_order;
    }

    // Pivot sets are same size. Order the pivot output columns by the
    // lexicographic order of the columns in the pivot set.
    for (auto i = 0u; i < a_size; ++i) {
      auto a_col = a_cols->second[i];
      auto b_col = b_cols->second[i];
      auto a_order = std::make_pair(a_view_sort, a_col->Index());
      auto b_order = std::make_pair(b_view_sort, b_col->Index());

      if (a_order < b_order) {
        return true;
      } else if (a_order > b_order) {
        return false;
      } else {
        continue;
      }
    }

    // Pivot sets are identical... this is interesting, order no longer
    // matters.
    //
    // TODO(pag): Remove duplicate pivot set?
    return a->Sort() < b->Sort();
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
#endif
}

// Equality over joins is pointer-based.
bool Node<QueryJoin>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsJoin();
  if (!that ||
      columns.Size() != that->columns.Size() ||
      num_pivots != that->num_pivots ||
      out_to_in.size() != that->out_to_in.size() ||
      joined_views.Size() != that->joined_views.Size() ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
      InsertSetsOverlap(this, that)) {
    return false;
  }

  eq.Insert(this, that);

  // Check that we've joined together the right views.
  const auto num_joined_views = joined_views.Size();
  auto i = 0u;
  for (; i < num_joined_views; ++i) {
    if (!joined_views[i]->Equals(eq, that->joined_views[i])) {
      eq.Remove(this, that);
      return false;
    }
  }

  // Check that the columns are joined together in the same way.
  i = 0u;
  for (const auto j1_out_col : columns) {
    assert(j1_out_col->index == i);

    const auto j2_out_col = that->columns[i];
    assert(j2_out_col->index == i);
    ++i;

    const auto j1_in_cols = out_to_in.find(j1_out_col);
    const auto j2_in_cols = that->out_to_in.find(j2_out_col);

    if (j1_in_cols == out_to_in.end() ||  // Join not used.
        j2_in_cols == that->out_to_in.end() ||  // Join not used.
        !ColumnsEq(eq, j1_in_cols->second, j2_in_cols->second)) {
      eq.Remove(this, that);
      return false;
    }
  }

  return true;
}

}  // namespace hyde
