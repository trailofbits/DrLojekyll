// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include <sstream>
#include <unordered_set>
#include <iostream>

#include "Optimize.h"
#include "Query.h"

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
  //    local_hash ^= RotateRight64(local_hash, 13) * pivot_hash;
  //  }

  for (auto joined_view : joined_views) {
    local_hash ^= joined_view->Hash();
  }

  if (num_pivots) {
    local_hash ^=
        RotateRight64(local_hash, (num_pivots + 53u) % 64u) * local_hash;
  }

  local_hash ^=
      RotateRight64(local_hash, (columns.Size() + 43u) % 64u) * local_hash;

  local_hash ^=
      RotateRight64(local_hash, (joined_views.Size() + 33u) % 64u) * local_hash;

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
    (void) out_col;
    for (COL *in_col : in_cols) {
      estimate = std::max(estimate, in_col->view->depth);
    }
  }

  depth = estimate + 1u;  // Base case in case of cycles.

  auto real = 1u;
  for (const auto &[out_col, in_cols] : out_to_in) {
    (void) out_col;
    real = GetDepth(in_cols, real);
  }

  real = GetDepth(positive_conditions, real);
  real = GetDepth(negative_conditions, real);
  depth = real + 1u;

  return depth;
}

// Convert a trivial join (only has a single input view) into a TUPLE.
void Node<QueryJoin>::ConvertTrivialJoinToTuple(QueryImpl *impl) {
  auto tuple = impl->tuples.Create();

  auto col_index = 0u;
  for (auto out_col : columns) {
    const auto new_out_col = tuple->columns.Create(
        out_col->var, tuple, out_col->id, col_index++);
    new_out_col->CopyConstantFrom(out_col);
  }

  UseList<COL> new_tuple_inputs(tuple);
  for (auto out_col : columns) {
    auto in_cols_it = out_to_in.find(out_col);
    if (in_cols_it != out_to_in.end()) {
      const auto &in_cols = in_cols_it->second;
      assert(in_cols.Size() == 1u);
      new_tuple_inputs.AddUse(in_cols[0]);

    } else if (auto const_col = out_col->AsConstant(); const_col){
      new_tuple_inputs.AddUse(const_col);

    } else {
      assert(false);
    }
  }

  ReplaceAllUsesWith(tuple);

  tuple->input_columns.Swap(new_tuple_inputs);
}

// Returns `true` if any joined views were identified where one or more of
// their columns are not used by the JOIN. If so, we proxy those views with
// TUPLEs.
bool Node<QueryJoin>::ProxyUnusedInputColumns(QueryImpl *impl) {
  const auto is_used_in_merge = this->Def<Node<QueryView>>::IsUsed();
  if (is_used_in_merge) {
    return false;
  }

  auto has_unused_cols = false;
  auto num_cols = 0u;

  // Look to see if any of the non-pivot output columns of the JOIN are unused.
  for (const auto &[out_col, in_cols] : out_to_in) {
    const auto is_pivot = out_col->Index() < num_pivots;
    if (is_pivot) {
      assert(1u < in_cols.Size());
      num_cols += in_cols.Size();
      continue;
    } else if (!out_col->IsUsedIgnoreMerges()) {
      has_unused_cols = true;
      break;
    } else {
      num_cols += 1u;
    }
  }

  // Look to see if any of the columns of any of the input joined views aren't
  // represented by the JOIN.
  if (!has_unused_cols) {
    auto num_expected_cols = 0u;
    for (auto joined_view : joined_views) {
      num_expected_cols += joined_view->columns.Size();
    }

    has_unused_cols = num_cols < num_expected_cols;
  }

  if (!has_unused_cols) {
    return false;
  }

  std::unordered_map<COL *, bool> needed_cols;

  for (const auto &[out_col, in_cols] : out_to_in) {
    const auto is_pivot = out_col->Index() < num_pivots;
    if (is_pivot) {
      for (auto in_col : in_cols) {
        needed_cols.emplace(in_col, true);
      }
      needed_cols.emplace(out_col, true);

    } else if (out_col->IsUsedIgnoreMerges()) {
      assert(in_cols.Size() == 1u);
      needed_cols.emplace(in_cols[0], true);
      needed_cols.emplace(out_col, true);
    }
  }

  WeakUseList<VIEW> new_joined_views(this);
  std::unordered_map<COL *, COL *> col_map;

  for (auto joined_view : joined_views) {
    has_unused_cols = false;
    for (auto in_col : joined_view->columns) {
      if (!needed_cols[in_col]) {
        has_unused_cols = true;
      } else {
        col_map.emplace(in_col, in_col);
      }
    }

    if (!has_unused_cols) {
      new_joined_views.AddUse(joined_view);
      continue;
    }

    auto tuple = impl->tuples.Create();
    auto col_index = 0u;
    for (auto in_col : joined_view->columns) {
      if (needed_cols[in_col]) {
        auto new_in_col = tuple->columns.Create(
            in_col->var, tuple, in_col->id, col_index++);
        new_in_col->CopyConstantFrom(in_col);
        tuple->input_columns.AddUse(in_col);
        col_map[in_col] = new_in_col;
      }
    }

    joined_view->CopyDifferentialAndGroupIdsTo(tuple);
    new_joined_views.AddUse(tuple);
  }

  std::unordered_map<COL *, UseList<COL>> new_out_to_in;
  DefList<COL> new_columns;

  auto col_index = 0u;
  for (auto out_col : columns) {
    const auto &in_cols = out_to_in.find(out_col)->second;
    const auto is_pivot = out_col->Index() < num_pivots;
    UseList<COL> new_in_cols(this);
    if (is_pivot) {
      for (auto in_col : in_cols) {
        new_in_cols.AddUse(col_map[in_col]);
      }
      auto new_out_col = new_columns.Create(
          out_col->var, this, out_col->id, col_index++);
      new_out_to_in.emplace(new_out_col, std::move(new_in_cols));
      out_col->ReplaceAllUsesWith(new_out_col);

    } else if (out_col->IsUsedIgnoreMerges()) {
      auto new_out_col = new_columns.Create(
          out_col->var, this, out_col->id, col_index++);
      new_in_cols.AddUse(col_map[in_cols[0]]);
      new_out_to_in.emplace(new_out_col, std::move(new_in_cols));
      out_col->ReplaceAllUsesWith(new_out_col);
    }
  }

  columns.Swap(new_columns);
  joined_views.Swap(new_joined_views);
  out_to_in.swap(new_out_to_in);
  return true;
}

// Remove all constant uses and outputs. This is a pretty aggressive function.
void Node<QueryJoin>::RemoveConstants(QueryImpl *impl) {

  std::unordered_map<VIEW *, VIEW *> view_map;
  std::unordered_map<COL *, COL *> new_to_old_col_map;
  std::unordered_map<COL *, COL *> old_to_new_col_map;
  std::unordered_map<COL *, COL *> in_col_const;

  // Figure out what the intended output constants are for this join, and map
  // the input columns to those outputs. This basically decides on how the JOIN
  // will interpret the constants.
  for (const auto &[out_col, in_cols] : out_to_in) {
    if (COL *out_const = out_col->AsConstant(); out_const) {
      for (COL * const in_col : in_cols) {
        in_col_const.emplace(in_col, out_const);
      }
    }
  }

  // Create guard views for each joined view. The purpose is to compare
  // everything that could be constant with constants. One challenge is that
  // we possibly need to make a tower of comparisons for any incoming view that
  // is sending more than one constant into the join.
  for (auto joined_view : joined_views) {

    auto view_to_process = joined_view;
    for (auto col : joined_view->columns) {
      new_to_old_col_map.emplace(col, col);
      old_to_new_col_map.emplace(col, col);
    }

    // Go create a tower of compares, so that all
    for (VIEW *prev_view_to_process = nullptr;
        view_to_process != prev_view_to_process; ) {
      prev_view_to_process = view_to_process;

      for (auto col : view_to_process->columns) {
        const auto const_col = in_col_const[col];
        if (!const_col || const_col == col->AsConstant()) {
          continue;
        }

        CMP * const cmp = impl->compares.Create(ComparisonOperator::kEqual);
        COL * const new_col = cmp->columns.Create(
            col->var, cmp, col->id, 0u);

        view_to_process->CopyDifferentialAndGroupIdsTo(cmp);

        new_to_old_col_map.emplace(new_col, new_to_old_col_map[col]);
        old_to_new_col_map[new_to_old_col_map[col]] = new_col;
        in_col_const.emplace(new_col, const_col);

        new_col->CopyConstantFrom(const_col);
        cmp->input_columns.AddUse(const_col);
        cmp->input_columns.AddUse(col);

        auto col_index = 1u;
        for (auto other_col : view_to_process->columns) {
          if (other_col == col) {
            continue;
          }

          const auto new_other_col = cmp->columns.Create(
              other_col->var, cmp, other_col->id, col_index++);

          new_other_col->CopyConstantFrom(other_col);
          cmp->attached_columns.AddUse(other_col);

          in_col_const.emplace(new_other_col, in_col_const[other_col]);
          new_to_old_col_map.emplace(new_other_col, new_to_old_col_map[other_col]);
          old_to_new_col_map[new_to_old_col_map[other_col]] = new_other_col;
        }

        view_to_process = cmp;
        break;
      }
    }

    view_map.emplace(joined_view, view_to_process);
  }

  // Alright, by this point we've conditioned every input view, making sure we
  // only keep non-constant input columns. We'll start by creating a new set of
  // pivot columns, and that will tell us what views need to be kept.

  DefList<COL> new_columns;
  WeakUseList<VIEW> new_joined_views(this);
  std::unordered_map<COL *, UseList<COL>> new_out_to_in;
  std::vector<VIEW *> pivot_views;
  std::vector<VIEW *> all_input_views;

  auto col_index = 0u;
  auto last_pivot_set_size = 0u;
  auto new_num_pivots = 0u;

  // Add in the non-constant pivot columns first.
  for (const auto out_col : columns) {
    if (out_col->AsConstant() || out_col->Index() >= num_pivots) {
      continue;
    }

    ++new_num_pivots;
    const auto new_out_col = new_columns.Create(
        out_col->var, this, out_col->id, col_index++);

    new_to_old_col_map.emplace(new_out_col, out_col);
    old_to_new_col_map[out_col] = new_out_col;

    const auto &old_in_cols = out_to_in.find(out_col)->second;
    assert(1u < old_in_cols.Size());
    UseList<COL> new_in_cols(this);

    for (auto old_in_col : old_in_cols) {
      const auto new_in_col = old_to_new_col_map[old_in_col];
      assert(new_in_col != nullptr);
      assert(!new_in_col->AsConstant());

      new_in_cols.AddUse(new_in_col);

      if (!last_pivot_set_size) {
        pivot_views.push_back(new_in_col->view);
      }

      all_input_views.push_back(new_in_col->view);
    }

    // Make sure our pivot sets aren't changing shape on us.
    if (!last_pivot_set_size) {
      last_pivot_set_size = new_in_cols.Size();
    } else {
      assert(last_pivot_set_size == new_in_cols.Size());
    }

    new_out_to_in.emplace(new_out_col, std::move(new_in_cols));
  }

  // Add in the non-constant, non-pivot columns.
  auto new_num_non_pivots = 0u;
  for (const auto out_col : columns) {
    if (out_col->AsConstant() || out_col->Index() < num_pivots) {
      continue;
    }

    ++new_num_non_pivots;
    const auto new_out_col = new_columns.Create(
        out_col->var, this, out_col->id, col_index++);

    new_to_old_col_map.emplace(new_out_col, out_col);
    old_to_new_col_map[out_col] = new_out_col;

    const auto &old_in_cols = out_to_in.find(out_col)->second;
    assert(1u == old_in_cols.Size());

    UseList<COL> new_in_cols(this);

    const auto old_in_col = old_in_cols[0];
    const auto new_in_col = old_to_new_col_map[old_in_col];
    assert(new_in_col != nullptr);
    assert(!new_in_col->AsConstant());

    all_input_views.push_back(new_in_col->view);

    // Make sure this non-pivot column is represented by a pivot column on
    // the same view. If it's not, then we must have made a cross-product.
    if (new_num_pivots) {
      assert(std::find(pivot_views.begin(), pivot_views.end(),
                       new_in_col->view) != pivot_views.end());
    }

    new_in_cols.AddUse(new_in_col);
    new_out_to_in.emplace(new_out_col, std::move(new_in_cols));
  }

  // This is our new output tuple. It matches the size/shape of the original
  // JOIN, but uses all the new columns, or uses constant columns where
  // necessary.
  TUPLE * const tuple = impl->tuples.Create();
  col_index = 0u;
  for (auto out_col : columns) {
    COL * const new_out_col = tuple->columns.Create(
        out_col->var, tuple, out_col->id, col_index++);

    if (auto const_col = out_col->AsConstant(); const_col) {
      tuple->input_columns.AddUse(const_col);
      new_out_col->CopyConstantFrom(const_col);

    } else {
      const auto new_join_col = old_to_new_col_map[out_col];
      assert(!new_join_col->AsConstant());
      assert(new_join_col->view == this);
      tuple->input_columns.AddUse(new_join_col);
    }
  }

  // Go figure out if we've dropped any views.
  std::sort(all_input_views.begin(), all_input_views.end());
  auto it = std::unique(all_input_views.begin(), all_input_views.end());
  all_input_views.erase(it, all_input_views.end());

  // Looks like we've dropped some views, so go create a bunch of conditions
  // and make the tuple which will go above the join conditional on the now
  // dropped views.
  assert(view_map.size() == joined_views.Size());
  if (all_input_views.size() < joined_views.Size()) {
    for (auto [old_in_view, new_in_view_] : view_map) {
      VIEW * const new_in_view = new_in_view_;
      if (std::find(all_input_views.begin(), all_input_views.end(),
                    new_in_view) != all_input_views.end()) {
        (void) old_in_view;
        continue;  // `old_in_view` / `new_in_view` is represented.
      }

      COND * const cond = impl->conditions.Create();
      cond->setters.AddUse(new_in_view);
      new_in_view->sets_condition.Emplace(new_in_view, cond);

      tuple->positive_conditions.AddUse(cond);
      cond->positive_users.AddUse(tuple);
    }
  }

  SubstituteAllUsesWith(tuple);
  CopyTestedConditionsTo(tuple);
  DropTestedConditions();
  DropSetConditions();

  // All of the pivots were constant!
  if (!new_num_pivots) {

    // Every column associated with this join is actually constant!
    if (!new_num_non_pivots) {
      assert(tuple->positive_conditions.Size() == joined_views.Size());
      PrepareToDelete();
      return;

    // We've created a cross-product!
    } else if (all_input_views.size() != 1) {
      for (auto view : all_input_views) {
        new_joined_views.AddUse(view);
      }

    // Every column we want to publish is available in just one of the views.
    } else {
      UseList<COL> new_tuple_inputs(tuple);
      for (auto tuple_in_col : tuple->input_columns) {
        auto in_cols_it = new_out_to_in.find(tuple_in_col);
        if (in_cols_it != new_out_to_in.end()) {
          const auto &in_cols = in_cols_it->second;
          assert(in_cols.Size() == 1u);
          new_tuple_inputs.AddUse(in_cols[0]);

        } else if (auto const_col = tuple_in_col->AsConstant(); const_col){
          new_tuple_inputs.AddUse(const_col);

        } else {
          assert(false);
        }
      }

      tuple->input_columns.Swap(new_tuple_inputs);
      PrepareToDelete();
      return;
    }

  // This is still a join, though possibly on fewer pivots.
  } else {
    assert(view_map.size() == pivot_views.size());
    assert(pivot_views.size() == all_input_views.size());

    for (auto view : pivot_views) {
      new_joined_views.AddUse(view);
    }
  }

  num_pivots = new_num_pivots;
  joined_views.Swap(new_joined_views);
  out_to_in.swap(new_out_to_in);
  columns.Swap(new_columns);
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
bool Node<QueryJoin>::Canonicalize(QueryImpl *query,
                                   const OptimizationContext &opt) {

  if (out_to_in.empty()) {
    PrepareToDelete();
    return false;
  }

  if (is_dead || valid != kValid) {
    is_canonical = true;
    return false;
  }

  is_canonical = false;

  // Go detect if we need to guard the input views with compares.
  auto need_constant_guard = false;
  for (const auto &[out_col, in_cols] : out_to_in) {
    COL *const_col = out_col->AsConstant();

    if (!const_col) {
      for (COL * in_col : in_cols) {
        if (auto in_const_col = in_col->AsConstant(); in_const_col) {
          out_col->CopyConstantFrom(in_const_col);
          const_col = in_const_col;
          break;
        }
      }
    }
    if (const_col) {
      need_constant_guard = true;
    }
  }

  if (need_constant_guard) {
    RemoveConstants(query);
    return true;
  }

  if (opt.can_remove_unused_columns &&
      ProxyUnusedInputColumns(query)) {
    return true;
  }

  // There's only one incoming view, convert this into a tuple.
  if (joined_views.Size() == 1u) {
    ConvertTrivialJoinToTuple(query);
    return true;
  }

  is_canonical = true;
  return false;
}

// Equality over joins is pointer-based.
bool Node<QueryJoin>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsJoin();
  if (!that || columns.Size() != that->columns.Size() ||
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
