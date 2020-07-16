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

// Replace `view`, which should be a member of `joined_views` with
// `replacement_view` in this JOIN.
void Node<QueryJoin>::ReplaceViewInJoin(VIEW *view, VIEW *replacement_view) {
  is_canonical = false;

  for (auto &col : prev_input_columns) {
    if (col->view == view) {
      col = replacement_view->columns[col->Index()];
    }
  }

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
  for (const auto &[out_col, in_cols] : out_to_in) {
    auto &new_in_cols = new_out_to_in.emplace(out_col, this).first->second;

    for (auto col : in_cols) {
      if (col->view == view) {
        new_in_cols.AddUse(replacement_view->columns[col->Index()]);
      } else {
        new_in_cols.AddUse(col);
      }
    }

    assert(!new_in_cols.Empty());
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
  ss << "DEL-PIVOT-" << pivot_col->Index() << "("
     << KindName() << ": " << producer << ')';
  tuple->producer = ss.str();
#endif

  SubstituteAllUsesWith(tuple);

  std::unordered_map<COL *, COL *> out_to_new_out;
  DefList<COL> new_columns(this);

  auto new_col_index = 0u;
  for (auto out_col : columns) {
    assert(!out_col->IsUsed());
    if (out_col == pivot_col) {
      out_col->CopyConstant(const_col);
      tuple->input_columns.AddUse(const_col);

    } else {
      auto new_out_col = new_columns.Create(
          out_col->var, this, out_col->id, new_col_index++);
      tuple->input_columns.AddUse(new_out_col);
      new_out_col->CopyConstant(out_col);
      out_to_new_out.emplace(out_col, new_out_col);
    }
  }

  bool is_pivot_set = false;
  if (auto in_cols_it = out_to_in.find(pivot_col);
      in_cols_it != out_to_in.end()) {
    auto &pivot_cols = in_cols_it->second;
    if (1u < pivot_cols.Size()) {
      is_pivot_set = true;
      PropagateConstAcrossPivotSet(query, const_col, pivot_cols);
    }
  }

  std::vector<VIEW *> views;
  unsigned num_joined_views = 0u;
  for (auto view : joined_views) {
    if (view) {  // It's a weak use list, so some might be `nullptr`s.
      ++num_joined_views;
    }
  }

  (void) num_joined_views;
  std::unordered_map<COL *, UseList<COL>> new_out_to_in;

  for (auto out_col : columns) {
    if (out_col == pivot_col) {
      continue;
    }

    const auto new_out_col = out_to_new_out[out_col];
    assert(new_out_col != nullptr);

    auto &old_pivot_set = out_to_in.find(out_col)->second;
    auto &new_pivot_set = new_out_to_in.emplace(new_out_col, this).first->second;

    assert(!old_pivot_set.Empty());
    assert(new_pivot_set.Empty());

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

    assert(!new_pivot_set.Empty());
  }

  // The column that we're removing is indeed a true pivot column.
  if (is_pivot_set) {
    assert(0u < num_pivots);
    num_pivots -= 1u;
  }

  out_to_in.swap(new_out_to_in);
  columns.Swap(new_columns);
  assert(out_to_in.size() == columns.Size());

  UpdateJoinedViews(query);

  // It's possible that we just replaced the last pivot, which happened to be
  // the last column, with a constant, and thus after updating the joined views
  // and fixing up the columns, we find ourselves with `out_to_in` empty, and
  // thus when this JOIN is next canonicalized, it will be deleted. Because we
  // just eliminated the last incoming column, we also reduced the number of
  // joined views, but replaced that deficit with a CONDition, which was added
  // to the JOIN. Go and add any such conditions onto the just-created TUPLE.
  CopyTestedConditionsTo(tuple);
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

// Update the set of joined views.
void Node<QueryJoin>::UpdateJoinedViews(QueryImpl *query) {
  assert(!prev_input_columns.empty());
  auto num_found_pivots = 0u;

  std::vector<VIEW *> old_views;
  std::vector<VIEW *> new_views;

  for (auto in_col : prev_input_columns) {
    if (!in_col->IsConstant()) {
      old_views.push_back(in_col->view);
    }
  }

  for (const auto &[out_col, in_cols] : out_to_in) {
    for (auto in_col : in_cols) {
      if (!in_col->IsConstant()) {
        new_views.push_back(in_col->view);
      }
    }
    if (1u < in_cols.Size()) {
      ++num_found_pivots;
    }
  }

  std::sort(old_views.begin(), old_views.end());
  auto it = std::unique(old_views.begin(), old_views.end());
  old_views.erase(it, old_views.end());

  std::sort(new_views.begin(), new_views.end());
  it = std::unique(new_views.begin(), new_views.end());
  new_views.erase(it, new_views.end());

  assert(num_pivots == num_found_pivots);
  assert(old_views.size() == joined_views.Size());

  // Looks to see if we've lost any JOINed views. If so, then convert those
  // into new, anonymous CONDitions.
  if (new_views.size() < old_views.size()) {
    auto new_it = new_views.begin();
    auto new_end = new_views.end();

    for (auto old_view : old_views) {
      auto found_it = std::find(new_it, new_end, old_view);
      if (found_it != new_end) {
        new_it = ++found_it;
        continue;  // Old view is still being used.
      }

      UseList<COL> old_inputs(this);
      for (auto in_col : prev_input_columns) {
        if (in_col->view == old_view) {
          old_inputs.AddUse(in_col);
        }
      }

      const auto cond = CreateOrInheritConditionOnView(
          query, old_view, std::move(old_inputs));

      positive_conditions.AddUse(cond);
      cond->positive_users.AddUse(this);
    }
  }

  joined_views.Clear();
  for (auto view : new_views) {
    joined_views.AddUse(view);
  }

  prev_input_columns.clear();
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

  if (out_to_in.empty()) {
    PrepareToDelete();
    return false;
  }

  if (is_dead || valid != kValid) {
    is_canonical = true;
    return false;
  }

  prev_input_columns.clear();

  auto num_found_pivots = 0u;
  for (const auto &[out_col, in_cols] : out_to_in) {
    if (in_cols.Empty()) {
      invalid_var = out_col->var;
      valid = kInvalidBeforeCanonicalize;
      is_canonical = true;
      return false;

    } else if (1u < in_cols.Size()) {
      ++num_found_pivots;
    }

    for (auto in_col : in_cols) {
      prev_input_columns.push_back(in_col);
    }
  }

  assert(num_pivots == num_found_pivots);

  for (auto out_col : columns) {
    (void) out_col;
    assert(out_to_in.count(out_col));
    assert(!out_to_in.find(out_col)->second.Empty());
  }

  assert(num_pivots <= columns.Size());
  assert(out_to_in.size() == columns.Size());

  auto non_local_changes = false;
  auto need_remove_non_pivots = false;
  auto all_pivots_are_const_or_constref = true;

  VIEW *first_joined_view = nullptr;
  bool joins_at_least_two_views = false;

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
        if (!first_joined_view) {
          first_joined_view = in_col->view;
        } else if (in_col->view != first_joined_view) {
          joins_at_least_two_views = true;
        }

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
        if (!first_joined_view) {
          first_joined_view = in_col->view;
        } else if (in_col->view != first_joined_view) {
          joins_at_least_two_views = true;
        }

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
    } else if (same_const_ref) {
      if (opt.can_replace_inputs_with_constants) {

#ifndef NDEBUG
        producer = "PIVOT-TO-CONST(" + producer + ")";
#endif
        UseList<COL> new_in_cols(this);
        for (auto old_in_col : in_cols) {
          old_in_col->view->is_canonical = false;
          new_in_cols.AddUse(same_const_ref);
        }

        in_cols.Swap(new_in_cols);
        UpdateJoinedViews(query);
        Canonicalize(query, opt);
        return true;
      }

    // At least one of the pivots is a reference to a constant; we will inject
    // comparisons across all incominging pivots to ensure that they match this
    // constant.
    } else if (first_const_ref) {
      if (!out_is_const_ref) {
#ifndef NDEBUG
        producer = "PIVOT-CONSTREF-ENFORCE(" + producer + ")";
#endif
        PropagateConstAcrossPivotSet(
            query, first_const ? first_const : first_const_ref->AsConstant(),
            in_cols);
        UpdateJoinedViews(query);
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
#ifndef NDEBUG
        producer = "PIVOT-CONST-ENFORCE(" + producer + ")";
#endif

        PropagateConstAcrossPivotSet(query, first_const, in_cols);
        UpdateJoinedViews(query);
        Canonicalize(query, opt);
        return true;
      }
    }

    // Check to see if we should try to remove non-pivot output columns
    // (because they aren't used).
    if (opt.can_remove_unused_columns &&
        !need_remove_non_pivots &&
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

  if (!non_local_changes && is_canonical && joins_at_least_two_views) {
    return false;
  }

  // There is at least one output column that isn't needed; go remove it.
  if (opt.can_remove_unused_columns && need_remove_non_pivots) {
    assert(!this->Def<Node<QueryView>>::IsUsed());

    DefList<COL> new_columns(this);
    std::unordered_map<COL *, UseList<COL>> new_out_to_in;

    auto col_index = 0u;
    num_found_pivots = 0u;
    for (auto out_col : columns) {
      auto &in_cols = out_to_in.find(out_col)->second;
      const auto is_pivot_col = 1u < in_cols.Size();
      if (is_pivot_col) {
        ++num_found_pivots;
      }
      if (is_pivot_col || out_col->IsUsed()) {
        const auto new_out_col = new_columns.Create(
            out_col->var, this, out_col->id, col_index++);
        new_out_col->CopyConstant(out_col);
        out_col->ReplaceAllUsesWith(new_out_col);

        assert(!in_cols.Empty());
        in_cols.Swap(new_out_to_in.emplace(new_out_col, this).first->second);
        assert(in_cols.Empty());
      } else {
        assert(in_cols.Size() == 1u);
        in_cols[0]->view->is_canonical = false;
      }
    }

#ifndef NDEBUG
    producer = "REMOVE-UNUSED-NON-PIVOTS(" + producer + ")";
#endif

    columns.Swap(new_columns);
    out_to_in.swap(new_out_to_in);
    non_local_changes = true;

    assert(num_found_pivots == num_pivots);
    assert(out_to_in.size() == columns.Size());
  }

  UpdateJoinedViews(query);

#ifndef NDEBUG
  for (auto out_col : columns) {
    (void) out_col;
    assert(out_to_in.count(out_col));
    assert(!out_to_in.find(out_col)->second.Empty());
  }
#endif

  // This JOIN isn't needed. If all incoming things are constant then by the
  // time we get down here, we should have sunk conditions down to all the
  // source views
  if (1u >= joined_views.Size()) {

    TUPLE *tuple = query->tuples.Create();

    for (auto out_col : columns) {
      auto &in_cols = out_to_in.find(out_col)->second;
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
    return true;
  }

  is_canonical = true;
  return non_local_changes;
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
