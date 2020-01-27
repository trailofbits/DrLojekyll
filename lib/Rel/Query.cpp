// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

#include <cassert>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"

#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Util/EqualitySet.h>

#include <iostream>
#include <iomanip>
#include <unordered_set>

namespace hyde {
namespace query {

QueryContext::QueryContext(void) {}

QueryContext::~QueryContext(void) {}

}  // namespace query

namespace {

static bool ColumnSetCompare(COL *a, COL *b) {

  // They are in the same view (covers them being the same).
  if (a->view == b->view) {
    return a->index < b->index;
  }

  const auto a_depth = a->view->Depth();
  const auto b_depth = b->view->Depth();

  // Deeper (from inputs/streams) columns are ordered first.
  if (a_depth > b_depth) {
    return true;

  } else if (a_depth < b_depth) {
    return false;
  }

  if (a->id != b->id) {
    return a->id < b->id;

  } else if (a->var != b->var) {
    return a->var.Order() < b->var.Order();

  } else {
    return a < b;
  }
}

}  // namespace

ColumnSet *ColumnSet::Find(void) {
  if (!parent) {
    return this;
  } else {
    const auto ret = parent->Find();
    if (ret != parent.get()) {
      parent = ret->shared_from_this();  // Path compression.
    }
    return ret;
  }
}

Node<QueryColumn> *ColumnSet::Leader(void) {
  const auto col_set = Find();
  if (!col_set->is_sorted) {
    std::sort(col_set->columns.begin(), col_set->columns.end(),
              ColumnSetCompare);
    col_set->is_sorted = true;
  }
  return col_set->columns[0];
}

Node<QueryColumn> *Node<QueryColumn>::Find(void) {
  return equiv_columns->Leader();
}

void Node<QueryColumn>::Union(Node<QueryColumn> *a, Node<QueryColumn> *b) {
  if (a == b) {
    return;
  }

  auto a_set = a->equiv_columns->Find();
  auto b_set = b->equiv_columns->Find();

  if (a_set == b_set) {
    return;
  }

  if (a_set->columns.size() > b_set->columns.size()) {
    a_set->is_sorted = false;
    a_set->columns.insert(
        a_set->columns.end(),
        b_set->columns.begin(),
        b_set->columns.end());

    const auto set = a_set->shared_from_this();

    b_set->parent = set;
    b_set->columns.clear();

    a->equiv_columns = set;
    b->equiv_columns = set;

  } else {
    b_set->is_sorted = false;
    b_set->columns.insert(
        b_set->columns.end(),
        a_set->columns.begin(),
        a_set->columns.end());

    const auto set = b_set->shared_from_this();

    a_set->parent = set;
    a_set->columns.clear();

    a->equiv_columns = set;
    b->equiv_columns = set;
  }
}

QueryImpl::~QueryImpl(void) {
  ForEachView([] (VIEW *view) {
    view->input_columns.ClearWithoutErasure();
  });

  for (auto select : selects) {
    select->relation.ClearWithoutErasure();
    select->stream.ClearWithoutErasure();
  }

  for (auto join : joins) {
    for (auto &[out_col, in_cols] : join->out_to_in) {
      in_cols.ClearWithoutErasure();
    }
  }

  for (auto agg : aggregates) {
    agg->group_by_columns.ClearWithoutErasure();
    agg->bound_columns.ClearWithoutErasure();
    agg->summarized_columns.ClearWithoutErasure();
  }

  for (auto merge : merges) {
    merge->merged_views.ClearWithoutErasure();
  }
}

Node<QueryColumn>::~Node(void) {
  if (equiv_columns) {
    const auto col_set = equiv_columns->Find();
    auto it = std::remove_if(
        col_set->columns.begin(), col_set->columns.end(),
        [=](COL *col) {
          return col == this;
        });
    col_set->columns.erase(it);

    equiv_columns->parent.reset();
    equiv_columns.reset();
  }
}

Node<QueryStream>::~Node(void) {}
Node<QueryConstant>::~Node(void) {}
Node<QueryGenerator>::~Node(void) {}
Node<QueryInput>::~Node(void) {}
Node<QueryView>::~Node(void) {}
Node<QuerySelect>::~Node(void) {}
Node<QueryTuple>::~Node(void) {}
Node<QueryJoin>::~Node(void) {}
Node<QueryMap>::~Node(void) {}
Node<QueryAggregate>::~Node(void) {}
Node<QueryMerge>::~Node(void) {}
Node<QueryConstraint>::~Node(void) {}
Node<QueryInsert>::~Node(void) {}

Node<QueryConstant> *Node<QueryStream>::AsConstant(void) noexcept {
  return nullptr;
}

Node<QueryGenerator> *Node<QueryStream>::AsGenerator(void) noexcept {
  return nullptr;
}

Node<QueryInput> *Node<QueryStream>::AsInput(void) noexcept {
  return nullptr;
}

Node<QueryConstant> *Node<QueryConstant>::AsConstant(void) noexcept {
  return this;
}

Node<QueryGenerator> *Node<QueryGenerator>::AsGenerator(void) noexcept {
  return this;
}

Node<QueryInput> *Node<QueryInput>::AsInput(void) noexcept {
  return this;
}

// Returns `true` if this view is being used.
//
// NOTE(pag): Even if the column doesn't look used, it might be used indirectly
//            via a merge, and thus we want to capture this.
bool Node<QueryColumn>::IsUsed(void) const noexcept {
  if (this->Def<Node<QueryColumn>>::IsUsed()) {
    return true;
  }

  return view->Def<Node<QueryView>>::IsUsed();
}

// Returns `true` if this view is being used. This is defined in terms of
// whether or not the view is used in a merge, or whether or not any of its
// columns are used.
bool Node<QueryView>::IsUsed(void) const noexcept {
  if (this->Def<Node<QueryView>>::IsUsed()) {
    return true;
  }

  for (auto col : columns) {
    if (col->Def<Node<QueryColumn>>::IsUsed()) {
      return true;
    }
  }

  return false;
}

void Node<QueryView>::Update(uint64_t next_timestamp) {
  if (timestamp >= next_timestamp) {
    return;
  }

  timestamp = next_timestamp;
  hash = 0;
  depth = 0;
  is_canonical = false;

  for (auto col : columns) {
    col->ForEachUse([=] (User *user, COL *) {
      user->Update(next_timestamp);
    });
  }

  // Update merges.
  ForEachUse([=] (User *user, Node<QueryView> *) {
    user->Update(next_timestamp);
  });
}

// Put this view into a canonical form.
bool Node<QueryView>::Canonicalize(QueryImpl *) {
  return false;
}

Node<QuerySelect> *Node<QueryView>::AsSelect(void) noexcept {
  return nullptr;
}

Node<QueryTuple> *Node<QueryView>::AsTuple(void) noexcept {
  return nullptr;
}

Node<QueryJoin> *Node<QueryView>::AsJoin(void) noexcept {
  return nullptr;
}

Node<QueryMap> *Node<QueryView>::AsMap(void) noexcept {
  return nullptr;
}

Node<QueryAggregate> *Node<QueryView>::AsAggregate(void) noexcept {
  return nullptr;
}

Node<QueryMerge> *Node<QueryView>::AsMerge(void) noexcept {
  return nullptr;
}

Node<QueryConstraint> *Node<QueryView>::AsConstraint(void) noexcept {
  return nullptr;
}

Node<QueryInsert> *Node<QueryView>::AsInsert(void) noexcept {
  return nullptr;
}

Node<QuerySelect> *Node<QuerySelect>::AsSelect(void) noexcept {
  return this;
}

Node<QueryTuple> *Node<QueryTuple>::AsTuple(void) noexcept {
  return this;
}

Node<QueryJoin> *Node<QueryJoin>::AsJoin(void) noexcept {
  return this;
}

Node<QueryMap> *Node<QueryMap>::AsMap(void) noexcept {
  return this;
}

Node<QueryAggregate> *Node<QueryAggregate>::AsAggregate(void) noexcept {
  return this;
}

Node<QueryMerge> *Node<QueryMerge>::AsMerge(void) noexcept {
  return this;
}

Node<QueryConstraint> *Node<QueryConstraint>::AsConstraint(void) noexcept {
  return this;
}

Node<QueryInsert> *Node<QueryInsert>::AsInsert(void) noexcept {
  return this;
}

namespace {

static uint64_t HashColumn(Node<QueryColumn> *col) {
  return __builtin_rotateright64(reinterpret_cast<uintptr_t>(col), 4);
}

}  // namespace

uint64_t Node<QuerySelect>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  if (relation) {
    hash = relation->decl.Id();

  } else if (stream) {
    if (auto generator_stream = stream->AsGenerator()) {
      hash = generator_stream->functor.Id();

    } else if (auto const_stream  = stream->AsConstant()) {
      hash = std::hash<std::string_view>()(
          const_stream->literal.Spelling());

    } else if (auto input_stream = stream->AsInput()) {
      hash = input_stream->declaration.Id();

    } else {
      hash = 0;
    }
  }
  hash <<= 4;
  hash |= 1;
  return hash;
}

uint64_t Node<QueryConstraint>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = __builtin_rotateright64(HashColumn(input_columns[0]), 16) ^
         HashColumn(input_columns[1]);

  hash <<= 4;
  hash |= 2;

  return hash;
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

  // Mix in the hashes of the merged views. Don't double-mix an already seen
  // hash, otherwise it will remove its effect.
  for (auto view : merged_views) {
    hash = __builtin_rotateleft64(hash, 16) ^ view->Hash();
  }

  hash <<= 4;
  hash |= 3;

  return hash;
}

uint64_t Node<QueryMap>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Mix in the hashes of the merged views and columns;
  for (auto input_col : input_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ HashColumn(input_col);
  }

  hash <<= 4;
  hash |= 4;

  return hash;
}

uint64_t Node<QueryAggregate>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Mix in the hashes of the group by columns.
  uint64_t group_hash = 0;
  for (auto col : group_by_columns) {
    group_hash = __builtin_rotateright64(group_hash, 16) ^ HashColumn(col);
  }

  // Mix in the hashes of the configuration columns.
  uint64_t bound_hash = 0;
  for (auto col : bound_columns) {
    bound_hash = __builtin_rotateright64(bound_hash, 16) ^ HashColumn(col);
  }

  // Mix in the hashes of the summarized columns.
  uint64_t summary_hash = 0;
  for (auto col : summarized_columns) {
    summary_hash = __builtin_rotateright64(summary_hash, 16) ^ HashColumn(col);
  }

  hash = functor.Id() ^ group_hash ^ bound_hash ^ summary_hash;
  hash <<= 4;
  hash |= 5;

  return hash;
}

uint64_t Node<QueryJoin>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  assert(input_columns.Size() == 0);

  for (auto &[out_col, in_cols] : out_to_in) {
    uint64_t col_set_hash = 0;
    for (auto col : in_cols) {
      col_set_hash = __builtin_rotateright64(col_set_hash, 16) ^
                     HashColumn(col);
    }

    hash ^= col_set_hash;
  }

  hash <<= 4;
  hash |= 6;

  return hash;
}

uint64_t Node<QueryInsert>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = decl.Id();

  // Mix in the hashes of the input by columns; these are ordered.
  for (auto col : input_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ HashColumn(col);
  }

  hash <<= 4;
  hash |= 7;

  return hash;
}

uint64_t Node<QueryTuple>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = columns.Size();

  // Mix in the hashes of the tuple by columns; these are ordered.
  for (auto col : input_columns) {
    hash = __builtin_rotateright64(hash, 16) ^ HashColumn(col);
  }

  hash <<= 4;
  hash |= 8;
  return hash;
}

namespace {

static unsigned GetDepth(const UseList<COL> &cols, unsigned depth) {
  for (const auto input_col : cols) {
    const auto input_depth = input_col->view->Depth();
    if (input_depth >= depth) {
      depth = input_depth;
    }
  }
  return depth;
}

}  // namespace

unsigned Node<QuerySelect>::Depth(void) noexcept {
  depth = 1;
  return depth;
}

unsigned Node<QueryJoin>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.

    auto real = 1u;
    for (const auto &[out_col, in_cols] : out_to_in) {
      real = GetDepth(in_cols, real);
    }
    depth = real + 1u;
  }
  return depth;
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

unsigned Node<QueryAggregate>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.

    auto real = GetDepth(bound_columns, 1u);
    real = GetDepth(group_by_columns, real);
    real = GetDepth(summarized_columns, real);
    depth = real + 1u;
  }
  return depth;
}

unsigned Node<QueryConstraint>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.

    depth = GetDepth(input_columns, 1u) + 1u;
  }
  return depth;
}

unsigned Node<QueryMap>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.
    depth = GetDepth(input_columns, 1u) + 1u;
  }
  return depth;
}

unsigned Node<QueryInsert>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.
    depth = GetDepth(input_columns, 1u) + 1u;
  }
  return depth;
}

unsigned Node<QueryTuple>::Depth(void) noexcept {
  if (!depth) {
    depth = 2u;  // Base case in case of cycles.
    depth = GetDepth(input_columns, 1u) + 1u;
  }
  return depth;
}

// Used to tell if a tuple looks canonical. While canonicalizing a tuple, we
// may affect the query graph and accidentally convert it back into a non-
// canonical form.
bool Node<QueryTuple>::LooksCanonical(void) const {

  // Check if the input columns are in sorted order, and that they are all
  // unique. If they are, then this tuple is in a canonical form already.
  COL *prev_col = nullptr;
  for (auto col : input_columns) {
    if (prev_col >= col) {
      return false;
    }
    prev_col = col;
  }

  // Check that all output columns are used. If they aren't used, then get
  // rid of them.
  for (auto col : columns) {
    if (!col->IsUsed()) {
      return false;
    }
  }

  return true;
}

// Put this tuple into a canonical form, which will make comparisons and
// replacements easier. Because comparisons are mostly pointer-based, the
// canonical form of this tuple is one where all input columns are sorted,
// deduplicated, and where all output columns are guaranteed to be used.
bool Node<QueryTuple>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  std::unordered_map<COL *, COL *> in_to_out;
  auto non_local_changes = false;

  // It's feasible that there's a cycle in the graph which would have triggered
  // an update to an input, thus requiring us to try again.
  while (!LooksCanonical()) {
    non_local_changes = true;

    in_to_out.clear();

    DefList<COL> new_output_cols;
    UseList<COL> new_input_cols(this);

    for (auto i = 0u; i < columns.Size(); ++i) {
      const auto old_out_col = columns[i];

      // If the output column is never used, then get rid of it.
      //
      // NOTE(pag): `IsUsed` on a column checks to see if its view is used
      //            in a merge, which would not show up in a normal def-use
      //            list.
      if (!old_out_col->IsUsed()) {
        continue;
      }

      const auto in_col = input_columns[i];
      auto &out_col = in_to_out[in_col];
      if (out_col) {
        columns[i]->ReplaceAllUsesWith(out_col);

      } else {
        out_col = old_out_col;
        new_input_cols.AddUse(in_col);
      }
    }

    new_input_cols.Sort();

    for (auto in_col : new_input_cols) {
      const auto old_out_col = in_to_out[in_col];
      const auto new_out_col = new_output_cols.Create(
          old_out_col->var, this, old_out_col->id, new_output_cols.Size());
      old_out_col->ReplaceAllUsesWith(new_out_col);
    }

    input_columns.Swap(new_input_cols);
    columns.Swap(new_output_cols);
  }

  is_canonical = true;
  return non_local_changes;
}

// Put this join into a canonical form, which will make comparisons and
// replacements easier. The approach taken is to sort the incoming columns, and
// to ensure that the iteration order of `out_to_in` matches `columns`.
//
// TODO(pag): If *all* incoming columns for a pivot column are the same, then
//            it no longer needs to be a pivot column.
//
// TODO(pag): If we make the above transform, then a join could devolve into
//            a merge.
bool Node<QueryJoin>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  is_canonical = true;

  for (auto &[out_col, input_cols] : out_to_in) {
    assert(1 <= input_cols.Size());
    assert(input_cols.Size() <= num_pivots);
    input_cols.Sort();
  }

  // If this view is not used by a merge then we're allowed to re-order the
  // columns.
  //
  // We'll order them in terms of:
  //    - Largest pivot set first.
  //    - Lexicographic order of pivot sets.
  //    - Pointer ordering.
  if (this->Def<Node<QueryView>>::IsUsed()) {

    // Change the sorting of the
    columns.Sort([=] (COL *a, COL *b) {
      assert(a != b);
      const auto a_cols = out_to_in.find(a);
      const auto b_cols = out_to_in.find(b);

      assert(a_cols != out_to_in.end());
      assert(b_cols != out_to_in.end());

      const auto a_size = a_cols->second.Size();
      const auto b_size = b_cols->second.Size();

      if (a_size > b_size) {
        return true;
      } else if (a_size < b_size) {
        return false;
      }

      // Pivot sets are same size. Lexicographically order them.
      for (auto i = 0u; i < a_size; ++i) {
        if (a_cols->second[i] < b_cols->second[i]) {
          return true;
        } else if (a_cols->second[i] > b_cols->second[i]) {
          return false;
        } else {
          continue;
        }
      }

      // Pivot sets are identical... this is interesting, order no longer
      // matters.
      //
      // TODO(pag): Remove duplicate pivot set?
      return a < b;
    });

    auto i = 0u;
    for (auto col : columns) {
      col->index = i++;
    }
  }

  return false;
}

// Put this constraint into a canonical form, which will make comparisons and
// replacements easier. If this constraint's operator is unordered, then we
// sort the inputs to make comparisons trivial.
bool Node<QueryConstraint>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  if (ComparisonOperator::kEqual == op || ComparisonOperator::kNotEqual == op) {
    if (input_columns[0] > input_columns[1]) {
      input_columns.Sort();
    }

    is_canonical = true;
    return false;

  } else {
    is_canonical = true;
    return false;
  }
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

  is_canonical = true;
  bool non_local_changes = false;

  UseList<VIEW> next_merged_views(this);
  std::vector<VIEW *> seen_merges;
  seen_merges.insert(seen_merges.end(), this);

  VIEW *prev_view = nullptr;
  for (auto retry = true; retry; ) {
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
        for (auto sub_view : incoming_merge->merged_views) {
          if (sub_view != this && sub_view != incoming_merge) {
            next_merged_views.AddUse(sub_view);
            retry = true;
            non_local_changes = true;
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

  return non_local_changes;
}

// Put this aggregate into a canonical form, which will make comparisons and
// replacements easier.
bool Node<QueryAggregate>::Canonicalize(QueryImpl *query) {
  if (is_canonical) {
    return false;
  }

  is_canonical = true;

  group_by_columns.Sort();
  COL *prev_col = nullptr;
  for (auto col : group_by_columns) {
    if (prev_col == col) {
      goto remove_duplicates;
    }
    prev_col = col;
  }

  return false;

remove_duplicates:
  UseList<COL> new_group_by_columns(this);
  prev_col = nullptr;
  for (auto col : group_by_columns) {
    if (prev_col != col) {
      new_group_by_columns.AddUse(col);
    }
    prev_col = col;
  }

  group_by_columns.Swap(new_group_by_columns);
  return false;
}

// Equality over selects is a mix of structural and pointer-based.
bool Node<QuerySelect>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsSelect();
  if (!that || columns.Size() != that->columns.Size()) {
    return false;
  }

  if (stream) {
    if (stream.get() != that->stream.get()) {
      return false;
    }

    if (stream->AsInput() || stream->AsConstant()) {
      return true;

    // Never let generators be merged, e.g. imagine that we have a generating
    // functor that emulates SQL's "primary key auto increment". That should
    // never be merged, even across `group_ids`.
    } else if (stream->AsGenerator()) {
      return false;

    } else {
      assert(false);
      return false;
    }

  } else if (relation) {
    if (!that->relation || relation->decl != that->relation->decl) {
      return false;
    }

    if (eq.Contains(this, that)) {
      return true;
    }

    // Two selects in the same logical clause are not allowed to be merged,
    // except in rare cases like constant streams. For example, consider the
    // following:
    //
    //    node_pairs(A, B) : node(A), node(B).
    //
    // `node_pairs` is the cross-product of `node`. The two selects associated
    // with each invocation of `node` are structurally the same, but cannot
    // be merged because otherwise we would not get the cross product.
    //
    // NOTE(pag): The `group_ids` are sorted.
    for (auto i = 0u, j = 0u;
         i < group_ids.size() && j < that->group_ids.size(); ) {

      if (group_ids[i] == that->group_ids[j]) {
        return false;

      } else if (group_ids[i] < that->group_ids[j]) {
        ++i;

      } else {
        ++j;
      }
    }

    eq.Insert(this, that);
    return true;

  } else {
    assert(false);
    return false;
  }
}

namespace {

static bool ColumnsEq(const UseList<COL> &c1s, const UseList<COL> &c2s) {
  const auto num_cols = c1s.Size();
  if (num_cols != c2s.Size()) {
    return false;
  }
  for (auto i = 0u; i < num_cols; ++i) {
    if (c1s[i] != c2s[i]) {
      return false;
    }
  }
  return true;
}

}  // namespace

// Equality over joins is pointer-based.
bool Node<QueryJoin>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsJoin();
  if (!that || columns.Size() != that->columns.Size() ||
      num_pivots != that->num_pivots) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  auto i = 0u;
  for (const auto j1_out_col : columns) {
    assert(j1_out_col->index == i);

    const auto j2_out_col = that->columns[i];
    assert(j2_out_col->index == i);
    ++i;

    const auto j1_in_cols = out_to_in.find(j1_out_col);
    const auto j2_in_cols = that->out_to_in.find(j2_out_col);
    assert(j1_in_cols != out_to_in.end());
    assert(j2_in_cols != that->out_to_in.end());
    if (!ColumnsEq(j1_in_cols->second, j2_in_cols->second)) {
      return false;
    }
  }

  eq.Insert(this, that);
  return true;
}

// Equality over maps is pointer-based.
bool Node<QueryMap>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsMap();
  if (!that || columns.Size() != that->columns.Size()) {
    return false;
  }

  if (functor != that->functor) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  if (!ColumnsEq(input_columns, that->input_columns)) {
    return false;
  }

  eq.Insert(this, that);

  return true;
}

// Equality over aggregates is structural.
bool Node<QueryAggregate>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsAggregate();
  if (!that || columns.Size() != that->columns.Size()) {
    return false;
  }

  if (functor != that->functor) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  if (!ColumnsEq(group_by_columns, that->group_by_columns) ||
      !ColumnsEq(bound_columns, that->bound_columns) ||
      !ColumnsEq(summarized_columns, that->summarized_columns)) {
    return false;
  }

  // In case of cycles, assume that these two aggregates are equivalent.
  eq.Insert(this, that);

  return true;
}

// Equality over merge is structural.
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

  eq.Insert(this, that);  // Base case in case of cycles.

  for (auto i = 0u; i < num_views; ++i) {
    if (!merged_views[i]->Equals(eq, that->merged_views[i])) {
      eq.Remove(this, that);
      for (auto j = 0u; j < i; ++j) {
        eq.Remove(merged_views[j], that->merged_views[j]);
      }
      return false;
    }
  }

  return true;
}

// Equality over constraints is pointer-based.
bool Node<QueryConstraint>::Equals(
    EqualitySet &, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsConstraint();
  return that &&
         op == that->op &&
         columns.Size() == that_->columns.Size() &&
         ColumnsEq(input_columns, that->input_columns);
}

// Equality over inserts is structural.
bool Node<QueryInsert>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsInsert();
  if (!that || columns.Size() != that->columns.Size()) {
    return false;
  }

  if (decl != that->decl) {
    return false;
  }

  if (!eq.Contains(this, that)) {
    const auto max_i = columns.Size();
    for (auto i = 0u; i < max_i; ++i) {
      if (input_columns[i] != that->input_columns[i]) {
        return false;
      }
    }

    eq.Insert(this, that);
  }

  return true;
}

// Equality over inserts is structural.
bool Node<QueryTuple>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsTuple();
  if (!that || columns.Size() != that->columns.Size()) {
    return false;
  }

  if (!eq.Contains(this, that)) {
    const auto max_i = columns.Size();
    for (auto i = 0u; i < max_i; ++i) {
      if (input_columns[i] != that->input_columns[i]) {
        return false;
      }
    }

    eq.Insert(this, that);
  }

  return true;
}

bool QueryStream::IsConstant(void) const noexcept {
  return impl->AsConstant() != nullptr;
}

bool QueryStream::IsGenerator(void) const noexcept {
  return impl->AsGenerator() != nullptr;
}

bool QueryStream::IsInput(void) const noexcept {
  return impl->AsInput() != nullptr;
}

QueryView QueryView::Containing(QueryColumn col) {
  return QueryView(col.impl->view);
}

DefinedNodeRange<QueryColumn> QueryView::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

DefinedNodeRange<QueryColumn> QueryJoin::Columns(void) const {
  return QueryView(impl).Columns();
}

bool QueryView::IsSelect(void) const noexcept {
  return impl->AsSelect() != nullptr;
}

bool QueryView::IsTuple(void) const noexcept {
  return impl->AsTuple() != nullptr;
}

bool QueryView::IsJoin(void) const noexcept {
  return impl->AsJoin() != nullptr;
}

bool QueryView::IsMap(void) const noexcept {
  return impl->AsMap() != nullptr;
}

bool QueryView::IsAggregate(void) const noexcept {
  return impl->AsAggregate() != nullptr;
}

bool QueryView::IsMerge(void) const noexcept {
  return impl->AsMerge() != nullptr;
}

bool QueryView::IsConstraint(void) const noexcept {
  return impl->AsConstraint() != nullptr;
}

// Replace all uses of this view with `that` view.
bool QueryView::ReplaceAllUsesWith(
    EqualitySet &eq, QueryView that) const noexcept {
  if (impl == that.impl) {
    return true;

  } else if (impl->columns.Size() != that.impl->columns.Size()) {
    return false;

  } else if (!impl->Equals(eq, that.impl)) {
    return false;
  }

  const auto num_cols = impl->columns.Size();
  assert(num_cols == that.impl->columns.Size());

  // Maintain the set of group IDs, to prevent "unlucky" cases where we
  // over-merge.
  if (const auto this_select = impl->AsSelect()) {
    const auto that_select = that.impl->AsSelect();
    assert(that_select != nullptr);
    that_select->group_ids.insert(
        that_select->group_ids.end(),
        this_select->group_ids.begin(),
        this_select->group_ids.end());
    std::sort(that_select->group_ids.begin(), that_select->group_ids.end());
  }

  for (auto i = 0u; i < num_cols; ++i) {
    auto col = impl->columns[i];
    assert(col->view == impl);
    assert(col->index == i);

    auto that_col = that.impl->columns[i];
    assert(that_col->view == that.impl);
    assert(that_col->index == i);

    col->ReplaceAllUsesWith(that_col);
  }

  impl->ReplaceAllUsesWith(that.impl);
  impl->input_columns.Clear();

  if (const auto as_merge = impl->AsMerge()) {
    as_merge->merged_views.Clear();

  } else if (const auto as_join = impl->AsJoin()) {
    as_join->out_to_in.clear();

  } else if (const auto as_agg = impl->AsAggregate()) {
    as_agg->group_by_columns.Clear();
    as_agg->bound_columns.Clear();
    as_agg->summarized_columns.Clear();
  }

  return true;
}

DefinedNodeRange<QueryColumn> QuerySelect::Columns(void) const {
  return QueryView(impl).Columns();
}

bool QueryColumn::IsSelect(void) const noexcept {
  return impl->view->AsSelect() != nullptr;
}

bool QueryColumn::IsJoin(void) const noexcept {
  return impl->view->AsJoin() != nullptr;
}

bool QueryColumn::IsMap(void) const noexcept {
  return impl->view->AsMap() != nullptr;
}

bool QueryColumn::IsAggregate(void) const noexcept {
  return impl->view->AsAggregate() != nullptr;
}

bool QueryColumn::IsMerge(void) const noexcept {
  return impl->view->AsMerge() != nullptr;
}

bool QueryColumn::IsConstraint(void) const noexcept {
  return impl->view->AsConstraint() != nullptr;
}

// Returns a unique ID representing the equivalence class of this column.
// Two columns with the same equivalence class will have the same values.
uint64_t QueryColumn::EquivalenceClass(void) const noexcept {
  return reinterpret_cast<uintptr_t>(impl->Find());
}

// Number of uses of this column.
unsigned QueryColumn::NumUses(void) const noexcept {
  return impl->NumUses();
}

// Replace all uses of one column with another column.
bool QueryColumn::ReplaceAllUsesWith(QueryColumn that) const noexcept {
  if (impl == that.impl) {
    return true;

  } else if (impl->var.Type().Kind() != that.impl->var.Type().Kind()) {
    return false;
  }

  impl->ReplaceAllUsesWith(that.impl);
  return true;
}

const ParsedVariable &QueryColumn::Variable(void) const noexcept {
  return impl->var;
}

bool QueryColumn::operator==(QueryColumn that) const noexcept {
  return impl == that.impl;
}

bool QueryColumn::operator!=(QueryColumn that) const noexcept {
  return impl != that.impl;
}

const ParsedLiteral &QueryConstant::Literal(void) const noexcept {
  return impl->literal;
}

QueryConstant &QueryConstant::From(QueryStream &stream) {
  assert(stream.IsConstant());
  return reinterpret_cast<QueryConstant &>(stream);
}

QueryInput &QueryInput::From(QueryStream &stream) {
  assert(stream.IsInput());
  return reinterpret_cast<QueryInput &>(stream);
}

QueryGenerator &QueryGenerator::From(QueryStream &stream) {
  assert(stream.IsGenerator());
  return reinterpret_cast<QueryGenerator &>(stream);
}

const ParsedDeclaration &QueryInput::Declaration(void) const noexcept {
  return impl->declaration;
}

const ParsedFunctor &QueryGenerator::Declaration(void) const noexcept {
  return impl->functor;
}

const ParsedDeclaration &QueryRelation::Declaration(void) const noexcept {
  return impl->decl;
}

bool QueryRelation::IsPositive(void) const noexcept {
  return impl->is_positive;
}

bool QueryRelation::IsNegative(void) const noexcept {
  return !impl->is_positive;
}

QuerySelect &QuerySelect::From(QueryView &view) {
  assert(view.IsSelect());
  return reinterpret_cast<QuerySelect &>(view);
}

bool QuerySelect::IsRelation(void) const noexcept {
  return impl->relation;
}

bool QuerySelect::IsStream(void) const noexcept {
  return impl->stream;
}

QueryRelation QuerySelect::Relation(void) const noexcept {
  assert(impl->relation);
  return QueryRelation(impl->relation.get());
}

QueryStream QuerySelect::Stream(void) const noexcept {
  assert(impl->stream);
  return QueryStream(impl->stream.get());
}

QueryJoin &QueryJoin::From(QueryView &view) {
  assert(view.IsJoin());
  return reinterpret_cast<QueryJoin &>(view);
}

// The number of output columns. This is the number of all non-pivot incoming
// columns.
unsigned QueryJoin::NumOutputColumns(void) const noexcept {
  return impl->columns.Size() - impl->num_pivots;
}

unsigned QueryJoin::NumPivots(void) const noexcept {
  return impl->num_pivots;
}

// Returns the set of pivot columns proposed by the Nth incoming view.
UsedNodeRange<QueryColumn> QueryJoin::NthPivotSet(unsigned n) const noexcept {
  assert(n < impl->num_pivots);
  auto use_list = impl->out_to_in.find(impl->columns[n]);
  assert(use_list != impl->out_to_in.end());
  assert(1u < use_list->second.Size());
  return {use_list->second.begin(), use_list->second.end()};
}

// Returns the set of input columns proposed by the Nth incoming view.
QueryColumn QueryJoin::NthInputColumn(unsigned n) const noexcept {
  assert((n + impl->num_pivots) < impl->columns.Size());
  auto use_list = impl->out_to_in.find(impl->columns[n + impl->num_pivots]);
  assert(use_list != impl->out_to_in.end());
  assert(1u == use_list->second.Size());
  return QueryColumn(use_list->second[0]);
}

// Returns the `nth` output column.
QueryColumn QueryJoin::NthOutputColumn(unsigned n) const noexcept {
  assert((n + impl->num_pivots)  < impl->columns.Size());
  return QueryColumn(impl->columns[n + impl->num_pivots]);
}

// Returns the `nth` pivot output column.
QueryColumn QueryJoin::NthPivotColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  assert(n < impl->num_pivots);
  return QueryColumn(impl->columns[n]);
}

QueryMap &QueryMap::From(QueryView &view) {
  assert(view.IsMap());
  return reinterpret_cast<QueryMap &>(view);
}

unsigned QueryMap::NumInputColumns(void) const noexcept {
  return static_cast<unsigned>(impl->input_columns.Size());
}

QueryColumn QueryMap::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryMap::InputColumns(void) const noexcept {
  return {impl->input_columns.begin(), impl->input_columns.end()};
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryMap::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Returns the number of output columns.
unsigned QueryMap::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
}

// Returns the `nth` output column.
QueryColumn QueryMap::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

const ParsedFunctor &QueryMap::Functor(void) const noexcept {
  return impl->functor;
}

QueryAggregate &QueryAggregate::From(QueryView &view) {
  assert(view.IsAggregate());
  return reinterpret_cast<QueryAggregate &>(view);
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryAggregate::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Returns the number of output columns.
unsigned QueryAggregate::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
}

// Returns the `nth` output column.
QueryColumn QueryAggregate::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

// Returns the number of columns used for grouping.
unsigned QueryAggregate::NumGroupColumns(void) const noexcept {
  return impl->group_by_columns.Size();
}

// Returns the `nth` grouping column.
QueryColumn QueryAggregate::NthGroupColumn(unsigned n) const noexcept {
  assert(n < impl->group_by_columns.Size());
  return QueryColumn(impl->group_by_columns[n]);
}

// Returns the number of columns used for configuration.
unsigned QueryAggregate::NumConfigColumns(void) const noexcept {
  return impl->bound_columns.Size();
}

// Returns the `nth` config column.
QueryColumn QueryAggregate::NthConfigColumn(unsigned n) const noexcept {
  assert(n < impl->bound_columns.Size());
  return QueryColumn(impl->bound_columns[n]);
}

// Returns the number of columns being summarized.
unsigned QueryAggregate::NumSummarizedColumns(void) const noexcept {
  return impl->summarized_columns.Size();
}

// Returns the `nth` summarized column.
QueryColumn QueryAggregate::NthSummarizedColumn(unsigned n) const noexcept {
  assert(n < impl->summarized_columns.Size());
  return QueryColumn(impl->summarized_columns[n]);
}

// The functor doing the aggregating.
const ParsedFunctor &QueryAggregate::Functor(void) const noexcept {
  return impl->functor;
}

QueryMerge &QueryMerge::From(QueryView &view) {
  assert(view.IsMerge());
  return reinterpret_cast<QueryMerge &>(view);
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryMerge::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Returns the number of output columns.
unsigned QueryMerge::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
}

// Returns the `nth` output column.
QueryColumn QueryMerge::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

// Number of views that are merged together at this point.
unsigned QueryMerge::NumMergedViews(void) const noexcept {
  return static_cast<unsigned>(impl->merged_views.Size());
}

// Nth view that is merged together at this point.
QueryView QueryMerge::NthMergedView(unsigned n) const noexcept {
  assert(n < impl->merged_views.Size());
  return QueryView(impl->merged_views[n]);
}

ComparisonOperator QueryConstraint::Operator(void) const {
  return impl->op;
}

QueryConstraint &QueryConstraint::From(QueryView &view) {
  assert(view.IsConstraint());
  return reinterpret_cast<QueryConstraint &>(view);
}

QueryColumn QueryConstraint::LHS(void) const {
  return QueryColumn(impl->columns[0]);
}

QueryColumn QueryConstraint::RHS(void) const {
  if (ComparisonOperator::kEqual == impl->op) {
    return QueryColumn(impl->columns[0]);
  } else {
    return QueryColumn(impl->columns[1]);
  }
}

QueryColumn QueryConstraint::InputLHS(void) const {
  return QueryColumn(impl->input_columns[0]);
}

QueryColumn QueryConstraint::InputRHS(void) const {
  return QueryColumn(impl->input_columns[1]);
}

DefinedNodeRange<QueryColumn> QueryConstraint::AttachedOutputColumns(void) const {
  auto begin = impl->columns.begin();
  const auto end = impl->columns.end();
  ++begin;
  if (ComparisonOperator::kEqual == impl->op) {
    return {begin, end};
  } else {
    ++begin;
    return {begin, end};
  }
}

UsedNodeRange<QueryColumn> QueryConstraint::AttachedInputColumns(void) const {
  auto begin = impl->input_columns.begin();
  ++begin;
  ++begin;
  return {begin, impl->input_columns.end()};
}

QueryRelation QueryInsert::Relation(void) const noexcept {
  return QueryRelation(impl->relation.get());
}

unsigned QueryInsert::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->input_columns.Size());
}

QueryColumn QueryInsert::NthColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

QueryTuple &QueryTuple::From(QueryView &view) {
  assert(view.IsTuple());
  return reinterpret_cast<QueryTuple &>(view);
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryTuple::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

unsigned QueryTuple::Arity(void) const noexcept {
  return impl->columns.Size();
}

QueryColumn QueryTuple::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

unsigned QueryTuple::NumInputColumns(void) const noexcept {
  return impl->input_columns.Size();
}

QueryColumn QueryTuple::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryTuple::InputColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->input_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->input_columns.end())};
}

DefinedNodeRange<QueryJoin> Query::Joins(void) const {
  return {DefinedNodeIterator<QueryJoin>(impl->joins.begin()),
          DefinedNodeIterator<QueryJoin>(impl->joins.end())};
}

DefinedNodeRange<QuerySelect> Query::Selects(void) const {
  return {DefinedNodeIterator<QuerySelect>(impl->selects.begin()),
          DefinedNodeIterator<QuerySelect>(impl->selects.end())};
}

DefinedNodeRange<QueryTuple> Query::Tuples(void) const {
  return {DefinedNodeIterator<QueryTuple>(impl->tuples.begin()),
          DefinedNodeIterator<QueryTuple>(impl->tuples.end())};
}

DefinedNodeRange<QueryRelation> Query::Relations(void) const {
  return {DefinedNodeIterator<QueryRelation>(impl->context->relations.begin()),
          DefinedNodeIterator<QueryRelation>(impl->context->relations.end())};
}

DefinedNodeRange<QueryConstant> Query::Constants(void) const {
  return {DefinedNodeIterator<QueryConstant>(impl->context->constants.begin()),
          DefinedNodeIterator<QueryConstant>(impl->context->constants.end())};
}

DefinedNodeRange<QueryGenerator> Query::Generators(void) const {
  return {DefinedNodeIterator<QueryGenerator>(impl->context->generators.begin()),
          DefinedNodeIterator<QueryGenerator>(impl->context->generators.end())};
}

DefinedNodeRange<QueryInput> Query::Inputs(void) const {
  return {DefinedNodeIterator<QueryInput>(impl->context->inputs.begin()),
          DefinedNodeIterator<QueryInput>(impl->context->inputs.end())};
}

DefinedNodeRange<QueryInsert> Query::Inserts(void) const {
  return {DefinedNodeIterator<QueryInsert>(impl->inserts.begin()),
          DefinedNodeIterator<QueryInsert>(impl->inserts.end())};
}

DefinedNodeRange<QueryMap> Query::Maps(void) const {
  return {DefinedNodeIterator<QueryMap>(impl->maps.begin()),
          DefinedNodeIterator<QueryMap>(impl->maps.end())};
}

DefinedNodeRange<QueryAggregate> Query::Aggregates(void) const {
  return {DefinedNodeIterator<QueryAggregate>(impl->aggregates.begin()),
          DefinedNodeIterator<QueryAggregate>(impl->aggregates.end())};
}

DefinedNodeRange<QueryMerge> Query::Merges(void) const {
  return {DefinedNodeIterator<QueryMerge>(impl->merges.begin()),
          DefinedNodeIterator<QueryMerge>(impl->merges.end())};
}

DefinedNodeRange<QueryConstraint> Query::Constraints(void) const {
  return {DefinedNodeIterator<QueryConstraint>(impl->constraints.begin()),
          DefinedNodeIterator<QueryConstraint>(impl->constraints.end())};
}

Query::~Query(void) {}

}  // namespace hyde

#pragma clang diagnostic pop
