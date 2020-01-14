// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

#include <cassert>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"

#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Util/EqualitySet.h>

#include <iostream>
#include <iomanip>

namespace hyde {
namespace query {

QueryContext::QueryContext(void) {}

QueryContext::~QueryContext(void) {}

}  // namespace query

ColumnReference::ColumnReference(ParsedVariable var_, Node<QueryColumn> *column_)
    : var(var_),
      column(column_) {
  if (column) {
    column->num_uses += 1;
  }
}

ColumnReference::ColumnReference(const ColumnReference &that)
    : var(that.var),
      column(that.column) {
  if (that.column) {
    that.column->num_uses += 1;
  }
}

ColumnReference::ColumnReference(ColumnReference &&that) noexcept
    : var(that.var),
      column(that.column) {
  that.column = nullptr;
}

ColumnReference::~ColumnReference(void) {
  if (column) {
    assert(0 < column->num_uses);
    column->num_uses -= 1;
  }
}

ColumnReference &ColumnReference::operator=(Node<QueryColumn> *that) noexcept {
  if (that) {
    that->num_uses += 1;
  }
  if (column) {
    assert(0 < column->num_uses);
    column->num_uses -= 1;
  }
  column = that;
  return *this;
}

ColumnReference &ColumnReference::operator=(
    const ColumnReference &that) noexcept {
  if (that.column) {
    that.column->num_uses += 1;
  }
  if (column) {
    assert(0 < column->num_uses);
    column->num_uses -= 1;
  }
  column = that.column;
  return *this;
}

ColumnReference &ColumnReference::operator=(ColumnReference &&that) noexcept {
  if (column != that.column && column) {
    assert(0 < column->num_uses);
    column->num_uses -= 1;
  }
  column = that.column;
  that.column = nullptr;
  return *this;
}

Node<QueryStream>::~Node(void) {}
Node<QueryConstant>::~Node(void) {}
Node<QueryGenerator>::~Node(void) {}
Node<QueryInput>::~Node(void) {}
Node<QueryView>::~Node(void) {}
Node<QuerySelect>::~Node(void) {}
Node<QueryJoin>::~Node(void) {}
Node<QueryMap>::~Node(void) {}
Node<QueryAggregate>::~Node(void) {}
Node<QueryMerge>::~Node(void) {}
Node<QueryConstraint>::~Node(void) {}
Node<QueryInsert>::~Node(void) {}

bool Node<QueryStream>::IsConstant(void) const noexcept {
  return false;
}

bool Node<QueryStream>::IsGenerator(void) const noexcept {
  return false;
}

bool Node<QueryStream>::IsInput(void) const noexcept {
  return false;
}

bool Node<QueryConstant>::IsConstant(void) const noexcept {
  return true;
}

bool Node<QueryGenerator>::IsGenerator(void) const noexcept {
  return true;
}

bool Node<QueryInput>::IsInput(void) const noexcept {
  return true;
}

bool Node<QueryView>::IsSelect(void) const noexcept {
  return false;
}

bool Node<QueryView>::IsJoin(void) const noexcept {
  return false;
}

bool Node<QueryView>::IsMap(void) const noexcept {
  return false;
}

bool Node<QueryView>::IsAggregate(void) const noexcept {
  return false;
}

bool Node<QueryView>::IsMerge(void) const noexcept {
  return false;
}

bool Node<QueryView>::IsConstraint(void) const noexcept {
  return false;
}

bool Node<QueryView>::IsInsert(void) const noexcept {
  return false;
}

bool Node<QuerySelect>::IsSelect(void) const noexcept {
  return true;
}

bool Node<QueryJoin>::IsJoin(void) const noexcept {
  return true;
}

bool Node<QueryMap>::IsMap(void) const noexcept {
  return true;
}

bool Node<QueryAggregate>::IsAggregate(void) const noexcept {
  return true;
}

bool Node<QueryMerge>::IsMerge(void) const noexcept {
  return true;
}

bool Node<QueryConstraint>::IsConstraint(void) const noexcept {
  return true;
}

bool Node<QueryInsert>::IsInsert(void) const noexcept {
  return true;
}

void Node<QuerySelect>::Clear(void) noexcept {
  columns.clear();
  group_ids.clear();
}

void Node<QueryJoin>::Clear(void) noexcept {
  columns.clear();
  joined_columns.clear();
  pivot_columns.clear();
  output_columns.clear();
}

void Node<QueryMap>::Clear(void) noexcept {
  columns.clear();
}

void Node<QueryAggregate>::Clear(void) noexcept {
  columns.clear();
  group_by_columns.clear();
  bound_columns.clear();
  summarized_columns.clear();
  id_to_col.clear();
}

void Node<QueryMerge>::Clear(void) noexcept {
  columns.clear();
  merged_views.clear();
}

void Node<QueryConstraint>::Clear(void) noexcept {
  columns.clear();
  input_columns.clear();
}

void Node<QueryInsert>::Clear(void) noexcept {
  columns.clear();
  input_columns.clear();
}

namespace {

static uint64_t MixedIndex(Node<QueryColumn> *col) {
  uint64_t base = (3u + col->index) | col->view->index_mask;
  return base * 0x85ebca6bull;
}

}  // namespace

uint64_t Node<QuerySelect>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  if (relation) {
    hash = relation->decl.Id();
  } else if (stream) {
    if (stream->IsGenerator()) {
      hash = reinterpret_cast<Node<QueryGenerator> *>(stream)->functor.Id();

    } else if (stream->IsConstant()) {
      const auto const_stream = reinterpret_cast<Node<QueryConstant> *>(stream);
      hash = std::hash<std::string_view>()(
          const_stream->literal.Spelling());

    } else if (stream->IsInput()) {
      hash = reinterpret_cast<Node<QueryInput> *>(stream)->declaration.Id();

    } else {
      hash = 0;
    }
  }
  hash <<= 3;
  hash |= 1;
  return hash;
}

uint64_t Node<QueryConstraint>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  const auto is_unordered = ComparisonOperator::kEqual == op ||
                            ComparisonOperator::kNotEqual == op;

  const auto lhs_var_index = input_columns[0]->index;
  const auto rhs_var_index = input_columns[1]->index;

  // Start with an initial hash just in case there's a cycle somewhere.
  if (is_unordered) {
    hash = (std::min(lhs_var_index, rhs_var_index) << 12u) |
           std::max(lhs_var_index, rhs_var_index);
  } else {
    hash = (lhs_var_index << 12u) | rhs_var_index;
  }

  hash <<= 3;
  hash |= 2;

  // Hash descendents.
  const auto lhs_view_hash = input_columns[0]->view->Hash();
  const auto rhs_view_hash = input_columns[1]->view->Hash();

  // Mix the initial hash with column index info with the new one.
  if (is_unordered) {
    hash *= lhs_view_hash ^ rhs_view_hash;
    hash ^= __builtin_rotateleft64(lhs_view_hash, 13) *
            __builtin_rotateleft64(rhs_view_hash, 13);
  } else {
    hash ^= __builtin_rotateleft64(lhs_view_hash, 13) * rhs_view_hash;
  }

  hash <<= 3;
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
  if (!merged_views.empty()) {
    hash *= merged_views[0]->columns.size();
  }

  std::vector<uint64_t> seen;

  // Mix in the hashes of the merged views. Don't double-mix an already seen
  // hash, otherwise it will remove its effect.
  for (auto view : merged_views) {
    const auto view_hash = view->Hash();
    if (std::find(seen.begin(), seen.end(), view_hash) == seen.end()) {
      hash ^= __builtin_rotateleft64(view_hash, view_hash & 0xFu);
      seen.push_back(view_hash);
    }
  }

  hash <<= 3;
  hash |= 3;

  return hash;
}

uint64_t Node<QueryMap>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = functor.Id();
  hash <<= 3;
  hash |= 4;

  // Mix in the hashes of the merged views and columns;
  for (auto input_col : input_columns) {
    hash = __builtin_rotateleft64(hash, 13);
    hash ^= input_col->view->Hash() * MixedIndex(input_col);
  }

  hash <<= 3;
  hash |= 4;

  return hash;
}

uint64_t Node<QueryAggregate>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = functor.Id();
  hash <<= 3;
  hash |= 5;

  // Mix in the hashes of the group by columns; these are unordered.
  for (auto input_col : group_by_columns) {
    hash ^= input_col->view->Hash() * MixedIndex(input_col);
  }

  // Mix in the hashes of the configuration columns; these are ordered.
  for (auto input_col : bound_columns) {
    hash = __builtin_rotateleft64(hash, 13);
    hash ^= input_col->view->Hash() * MixedIndex(input_col);
  }

  // Mix in the hashes of the summarized columns; these are ordered.
  for (auto input_col : summarized_columns) {
    hash = __builtin_rotateleft64(hash, 13);
    hash ^= input_col->view->Hash() * MixedIndex(input_col);
  }

  hash <<= 3;
  hash |= 5;

  return hash;
}

uint64_t Node<QueryJoin>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = joined_columns.size() * pivot_columns.size();
  hash <<= 3;
  hash |= 6;

  std::vector<uint64_t> seen;

  // Mix in the hashes of the merged views. Don't double-mix an already seen
  // hash, otherwise it will remove its effect.
  for (auto &col : joined_columns) {
    const auto view_hash = col->view->Hash();
    if (std::find(seen.begin(), seen.end(), view_hash) == seen.end()) {
      hash ^= __builtin_rotateleft64(view_hash, view_hash & 0xFu);
      seen.push_back(view_hash);
    }
  }

  hash <<= 3;
  hash |= 6;

  return hash;
}

uint64_t Node<QueryInsert>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = decl.Id();
  hash <<= 3;
  hash |= 7;

  // Mix in the hashes of the group by columns; these are ordered.
  for (auto input_col : input_columns) {
    hash = __builtin_rotateleft64(hash, 13);
    hash ^= input_col->view->Hash() * MixedIndex(input_col);
  }

  hash <<= 3;
  hash |= 7;

  return hash;
}

namespace {

static unsigned GetDepthEstimate(const std::vector<ColumnReference> &cols,
                                 unsigned depth) {
  for (const auto &input_col : cols) {
    const auto input_depth = input_col->view->depth;
    if (input_depth >= depth) {
      depth = input_depth;
    }
  }
  return depth;
}

static unsigned GetDepth(const std::vector<ColumnReference> &cols,
                         unsigned depth) {
  for (const auto &input_col : cols) {
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
    depth = GetDepthEstimate(joined_columns, 1) + 1;
    depth = GetDepth(joined_columns, 1) + 1;
  }
  return depth;
}

unsigned Node<QueryMerge>::Depth(void) noexcept {
  if (!depth) {
    // Get a depth estimate that will be good enough in the presence of cycles.
    depth = 1;
    for (auto merged_view : merged_views) {
      if (merged_view->depth >= depth) {
        depth = merged_view->depth;
      }
    }
    depth += 1;

    // Go get a real depth.
    auto real_depth = 1u;
    for (auto merged_view : merged_views) {
      real_depth = std::max(real_depth, merged_view->Depth());
    }
    depth = real_depth + 1;
  }
  return depth;
}

unsigned Node<QueryAggregate>::Depth(void) noexcept {
  if (!depth) {
    depth = GetDepthEstimate(group_by_columns, 1);
    depth = GetDepthEstimate(summarized_columns, depth);
    depth += 1;

    depth = GetDepth(group_by_columns, 1);
    depth = GetDepth(summarized_columns, depth);
    depth += 1;
  }
  return depth;
}

unsigned Node<QueryConstraint>::Depth(void) noexcept {
  if (!depth) {
    depth = GetDepthEstimate(input_columns, 1) + 1;
    depth = GetDepth(input_columns, 1) + 1;
  }
  return depth;
}

unsigned Node<QueryMap>::Depth(void) noexcept {
  if (!depth) {
    depth = GetDepthEstimate(input_columns, 1) + 1;
    depth = GetDepth(input_columns, 1) + 1;
  }
  return depth;
}

unsigned Node<QueryInsert>::Depth(void) noexcept {
  if (!depth) {
    depth = GetDepthEstimate(input_columns, 1) + 1;
    depth = GetDepth(input_columns, 1) + 1;
  }
  return depth;
}

namespace {

static bool PreCheckColumnsEq(const std::vector<ColumnReference> &this_cols,
                              const std::vector<ColumnReference> &that_cols) {
  const auto max_i = this_cols.size();
  for (auto i = 0u; i < max_i; ++i) {
    const auto this_col = this_cols[i].get();
    const auto that_col = that_cols[i].get();
    if (this_col == that_col) {
      continue;
    } else if (((this_col->index | this_col->view->index_mask) !=
                (that_col->index | that_col->view->index_mask)) ||
               this_col->view->Hash() != that_col->view->Hash() ||
               this_col->var.Type().Kind() != that_col->var.Type().Kind()) {
      return false;
    }
  }
  return true;
}

static bool CheckColumnsEq(EqualitySet &eq,
                           const std::vector<ColumnReference> &this_cols,
                           const std::vector<ColumnReference> &that_cols) {
  const auto max_i = this_cols.size();
  for (auto i = 0u; i < max_i; ++i) {
    const auto this_col = this_cols[i].get();
    const auto that_col = that_cols[i].get();
    if (this_col == that_col) {
      continue;
    } else if (!this_col->view->Equals(eq, that_col->view)) {
      return false;
    }

    // NOTE(pag): We have already done index and type checks in pre-check.
  }
  return true;
}

}  // namespace

// Equality over selects is pointer-based.
bool Node<QuerySelect>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsSelect() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QuerySelect> *>(that_);
  if (eq.Contains(this, that)) {
    return true;
  }

  if (stream) {
    if (stream != that->stream) {
      return false;
    }

    if (stream->IsInput() || stream->IsConstant()) {
      return true;

    // Never let generators be merged, e.g. imagine that we have a generating
    // functor that emulates SQL's "primary key auto increment". That should
    // never be merged, even across `group_ids`.
    } else if (stream->IsGenerator()) {
      return false;

    } else {
      assert(false);
      return false;
    }

  } else if (relation) {
    if (!that->relation || relation->decl != that->relation->decl) {
      return false;
    }

    // Two selects in the same logical clause are not allowed to be merged,
    // except in rare cases like constant streams.
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

  } else {
    assert(false);
    return false;
  }

  eq.Insert(this, that);
  return true;
}

// Equality over joins is pointer-based, but unordered.
bool Node<QueryJoin>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {

  if (!that_->IsJoin() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryJoin> *>(that_);

//  if (pivot_columns.size() != that->pivot_columns.size() ||
//      joined_columns.size() != that->joined_columns.size()) {
//    return false;
//  }

  if (eq.Contains(this, that)) {
    return true;
  }

  using ColMap = std::unordered_map<Node<QueryColumn> *, Node<QueryColumn> *>;

  auto map_in_vars_to_out_cols = [] (Node<QueryJoin> *join, ColMap &mapping) {
    assert(join->columns.size() <= join->joined_columns.size());
    assert(join->joined_columns.size() == join->output_columns.size());

    auto i = 0u;
    for (const auto &in_col : join->joined_columns) {
      mapping.emplace(in_col.get(), join->output_columns[i++]);
    }
  };

  ColMap this_inout_col_map;
  ColMap that_inout_col_map;
  ColMap out_out_map;

  this_inout_col_map.reserve(joined_columns.size());
  that_inout_col_map.reserve(that->joined_columns.size());

  map_in_vars_to_out_cols(this, this_inout_col_map);
  map_in_vars_to_out_cols(that, that_inout_col_map);

  for (auto this_in_out : this_inout_col_map) {
    const auto this_out = this_in_out.second;
    const auto that_out = that_inout_col_map[this_in_out.first];
    if (!that_out) {
      return false;
    }
    assert(that_out != nullptr);
    out_out_map.emplace(this_out, that_out);
  }

  if (out_out_map.size() != columns.size()) {
    return false;
  }

  eq.Insert(this, that);
  return true;
}

// Equality over maps is structural.
bool Node<QueryMap>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsMap() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryMap> *>(that_);
  if (functor != that->functor ||
      input_columns.size() != that->input_columns.size()) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  if (!PreCheckColumnsEq(input_columns, that->input_columns)) {
    return false;
  }

  // In case of cycles, assume that these two maps are equivalent.
  eq.Insert(this, that);

  // Make sure all input views are equivalent.
  if (!CheckColumnsEq(eq, input_columns, that->input_columns)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

// Equality over aggregates is structural.
bool Node<QueryAggregate>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsAggregate() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryAggregate> *>(that_);
  if (functor != that->functor ||
      group_by_columns.size() != that->group_by_columns.size() ||
      bound_columns.size() != that->bound_columns.size() ||
      summarized_columns.size() != that->summarized_columns.size()) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  if (!PreCheckColumnsEq(group_by_columns, that->group_by_columns) ||
      !PreCheckColumnsEq(bound_columns, that->bound_columns) ||
      !PreCheckColumnsEq(summarized_columns, that->summarized_columns)) {
    return false;
  }

  // In case of cycles, assume that these two aggregates are equivalent.
  eq.Insert(this, that);

  // Now check that all incoming views are equivalent, given the assumption
  // that the main views are equivalent.
  if (!CheckColumnsEq(eq, group_by_columns, that->group_by_columns) ||
      !CheckColumnsEq(eq, bound_columns, that->bound_columns) ||
      !CheckColumnsEq(eq, summarized_columns, that->summarized_columns)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

// Equality over merge is structural.
bool Node<QueryMerge>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsMerge() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryMerge> *>(that_);

  if (eq.Contains(this, that)) {
    return true;
  }

  const auto max_i = columns.size();

  // Make sure all column types match.
  for (auto i = 0u; i < max_i; ++i) {
    auto this_col = columns[i];
    auto that_col = that->columns[i];
    if (this_col->var.Type().Kind() != that_col->var.Type().Kind()) {
      return false;
    }
  }

  // Check that the every merged view hash is covered.
  auto pre_check_views = [] (const std::vector<Node<QueryView> *> &v0s,
                             const std::vector<Node<QueryView> *> &v1s) {
    for (auto v0 : v0s) {
      const auto v0_hash = v0->Hash();
      for (auto v1 : v1s) {
        const auto v1_hash = v1->Hash();
        if (v0_hash == v1_hash) {
          goto found_candidate;
        }
      }
      return false;

    found_candidate:
      continue;
    }
    return true;
  };

  if (!pre_check_views(merged_views, that->merged_views) ||
      !pre_check_views(that->merged_views, merged_views)) {
    return false;
  }

  // In case of cycles, assume that these two merges are equivalent.
  eq.Insert(this, that);

  // Check that the every merged view hash is covered.
  auto check_views = [] (EqualitySet &eq,
                         const std::vector<Node<QueryView> *> &v0s,
                         const std::vector<Node<QueryView> *> &v1s) {
    for (auto v0 : v0s) {
      for (auto v1 : v1s) {
        if (v0->Equals(eq, v1)) {
          goto found_candidate;
        }
      }
      return false;

    found_candidate:
      continue;
    }
    return true;
  };

  if (!check_views(eq, merged_views, that->merged_views) ||
      !check_views(eq, that->merged_views, merged_views)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

// Equality over constraints is pointer-based.
bool Node<QueryConstraint>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsConstraint() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryConstraint> *>(that_);
  if (input_columns.size() != 2 ||
      that->input_columns.size() != 2 ||
      op != that->op) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  const auto v0_c0 = input_columns[0].get();
  const auto v0_c1 = input_columns[1].get();
  const auto v1_c0 = that->input_columns[0].get();
  const auto v1_c1 = that->input_columns[1].get();

  const auto eq_ordered = v0_c0 == v1_c0 && v0_c1 == v1_c1;
  const auto eq_unordered = v0_c0 == v1_c1 && v0_c1 == v1_c0;

  if (op == ComparisonOperator::kEqual || op == ComparisonOperator::kNotEqual) {
    if (!eq_ordered && !eq_unordered) {
      return false;
    }

  } else if (!eq_ordered) {
    return false;
  }

  if (ComparisonOperator::kNotEqual == op) {
    assert(v0_c0->Find() != v0_c1->Find());
    assert(v1_c0->Find() != v1_c1->Find());
  }

  eq.Insert(this, that);
  return true;
}

// Equality over inserts is structural.
bool Node<QueryInsert>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsInsert() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryInsert> *>(that_);
  if (decl != that->decl) {
    return false;
  }

  if (!eq.Contains(this, that)) {
    const auto max_i = columns.size();
    for (auto i = 0u; i < max_i; ++i) {
      if (input_columns[i].get() != that->input_columns[i].get()) {
        return false;
      }
    }

    eq.Insert(this, that);
  }

  return true;
}

Node<QueryConstraint>::Node(ComparisonOperator op_,
                            ParsedVariable lhs_var, Node<QueryColumn> *lhs_,
                            ParsedVariable rhs_var, Node<QueryColumn> *rhs_)
    : op(op_) {
  input_columns.emplace_back(lhs_var, lhs_);
  input_columns.emplace_back(rhs_var, rhs_);

  if (op == ComparisonOperator::kEqual || op == ComparisonOperator::kNotEqual) {
    index_mask = ~0u;
  }
}

namespace {

static bool ReplaceColumnUses(Node<QueryColumn> *old_col,
                              Node<QueryColumn> *new_col,
                              std::vector<ColumnReference> &cols) {
  auto ret = false;
  for (auto &col : cols) {
    if (col == old_col) {
      col = new_col;
      ret = true;
    }
  }
  return true;
}

}  // namespace

void Node<QueryColumn>::ReplaceAllUsesWith(
    QueryImpl *query,
    Node<QueryColumn> *old_col,
    Node<QueryColumn> *new_col) {
  if (old_col == new_col) {
    return;
  }

  assert(old_col->var.Type().Kind() == new_col->var.Type().Kind());

  auto replaced = false;

  for (auto &join : query->joins) {
    replaced |= ReplaceColumnUses(old_col, new_col, join->joined_columns);
    join->hash = 0;
    join->depth = 0;
  }

  for (auto &map : query->maps) {
    replaced |= ReplaceColumnUses(old_col, new_col, map->input_columns);
    map->hash = 0;
    map->depth = 0;
  }

  for (auto &agg : query->aggregates) {
    replaced |= ReplaceColumnUses(old_col, new_col, agg->group_by_columns);
    replaced |= ReplaceColumnUses(old_col, new_col, agg->bound_columns);
    replaced |= ReplaceColumnUses(old_col, new_col, agg->summarized_columns);
    agg->hash = 0;
    agg->depth = 0;
  }

  for (auto &insert : query->inserts) {
    if (ReplaceColumnUses(old_col, new_col, insert->input_columns)) {
      insert->columns.clear();
      for (auto &col : insert->input_columns) {
        insert->columns.push_back(col.get());
      }
      replaced = true;
    }
    insert->hash = 0;
    insert->depth = 0;
  }

  for (auto &cmp : query->constraints) {
    replaced |= ReplaceColumnUses(old_col, new_col, cmp->input_columns);
    cmp->hash = 0;
    cmp->depth = 0;
  }

  for (auto &sel : query->selects) {
    sel->hash = 0;
    sel->depth = 0;
  }

  for (auto &merge : query->merges) {
    merge->hash = 0;
    merge->depth = 0;
  }

  if (replaced) {
    DisjointSet::UnionInto(old_col, new_col);
  }
}

bool QueryStream::IsConstant(void) const noexcept {
  return impl->IsConstant();
}

bool QueryStream::IsGenerator(void) const noexcept {
  return impl->IsGenerator();
}

bool QueryStream::IsInput(void) const noexcept {
  return impl->IsInput();
}

QueryView QueryView::Containing(QueryColumn col) {
  // If the column belongs to a join, then it's possible that two separate
  // joins were merged together, so go find that merged view.
  if (col.impl->view->IsJoin()) {
    return QueryView(col.impl->view);
  } else {
    return QueryView(col.impl->view);
  }
}

NodeRange<QueryColumn> QueryView::Columns(void) const {
  if (impl->columns.empty()) {
    return NodeRange<QueryColumn>();
  } else {
    return NodeRange<QueryColumn>(
        impl->columns.front(),
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryColumn>, next_in_view)));
  }
}

NodeRange<QueryColumn> QueryJoin::Columns(void) const {
  return QueryView(impl).Columns();
}

bool QueryView::IsSelect(void) const noexcept {
  return impl->IsSelect();
}

bool QueryView::IsJoin(void) const noexcept {
  return impl->IsJoin();
}

bool QueryView::IsMap(void) const noexcept {
  return impl->IsMap();
}

bool QueryView::IsAggregate(void) const noexcept {
  return impl->IsAggregate();
}

bool QueryView::IsMerge(void) const noexcept {
  return impl->IsMerge();
}

bool QueryView::IsConstraint(void) const noexcept {
  return impl->IsConstraint();
}

namespace {

static void ReplaceViewInMerges(
    Node<QueryView> *old_view, Node<QueryView> *new_view,
    const std::vector<std::unique_ptr<Node<QueryMerge>>> &merges) {
  for (auto &merge : merges) {
    for (auto &merged_view : merge->merged_views) {
      if (merged_view == old_view) {
        merged_view = new_view;
      }
    }
  }
}

}  // namespace

// Replace all uses of this view with `that` view.
bool QueryView::ReplaceAllUsesWith(
    EqualitySet &eq, QueryView that) const noexcept {
  if (impl == that.impl) {
    return true;

  } else if (impl->query != that.impl->query) {
    return false;

  } else if (impl->columns.size() != that.impl->columns.size()) {
    return false;

  } else if (!impl->Equals(eq, that.impl)) {
    return false;
  }

  // Joins are a special case for replacements, because we need to try to
  // match up their columns, which might be out of order / have different
  // variable names (e.g. due to them being from different clauses). Thus,
  // we require that the two joins be equivalent first, and if they are,
  // then we permit replacement.
  if (impl->IsJoin() && that.impl->IsJoin()) {

    // NOTE(pag): Equality for joins is pointer-based, so we know that the
    //            joined columns match (there may be differences in repetitions,
    //            and thus that the joined views match.
    //
    //            Thus, the issue is about matching input columns to output
    //            columns, so that we can then match output to output columns.
    //            Unfortunately, we can't depend upon matching up output
    //            variable names to input variable names, because the two
    //            joins could be from different clauses, and moreso, their
    //            inputs variables may have changed. However, we do maintain
    //            (in the column references) the original variable names of the
    //            referred columns, and so we can depend on those.

    using ColMap = std::unordered_map<Node<QueryColumn> *, Node<QueryColumn> *>;

    auto map_in_vars_to_out_cols = [] (Node<QueryJoin> *join, ColMap &mapping) {
      assert(join->columns.size() <= join->joined_columns.size());
      assert(join->joined_columns.size() == join->output_columns.size());

      auto i = 0u;
      for (const auto &in_col : join->joined_columns) {
        mapping.emplace(in_col.get(), join->output_columns[i++]);
      }
    };

    ColMap this_inout_col_map;
    ColMap that_inout_col_map;
    ColMap out_out_map;

    const auto this_join = reinterpret_cast<Node<QueryJoin> *>(impl);
    const auto that_join = reinterpret_cast<Node<QueryJoin> *>(that.impl);
    map_in_vars_to_out_cols(this_join, this_inout_col_map);
    map_in_vars_to_out_cols(that_join, that_inout_col_map);

    for (auto this_in_out : this_inout_col_map) {
      const auto this_out = this_in_out.second;
      const auto that_out = that_inout_col_map[this_in_out.first];
      assert(that_out != nullptr);
      out_out_map.emplace(this_out, that_out);
    }

    assert(out_out_map.size() == impl->columns.size());

    for (auto out_out : out_out_map) {
      Node<QueryColumn>::ReplaceAllUsesWith(
          this_join->query, out_out.first, out_out.second);
      out_out.first->next_in_view = nullptr;
    }

  // Constraints are interesting when they are unordered. We could have two
  // uses of an unordered constraint (eq, neq) and while the order of the
  // input/output columns to these constraints are not super relevant, the
  // order in which users use them are very relevant.
  //
  // NOTE(pag): The constraint equality is defined in terms of pointers, not
  //            structurally.
  } else if (impl->IsConstraint()) {

    const auto this_cmp = reinterpret_cast<Node<QueryConstraint> *>(impl);
    const auto that_cmp = reinterpret_cast<Node<QueryConstraint> *>(that.impl);

    if (this_cmp->input_columns[0].get() ==
        that_cmp->input_columns[0].get()) {
      Node<QueryColumn>::ReplaceAllUsesWith(
          impl->query, impl->columns[0], that.impl->columns[0]);
      Node<QueryColumn>::ReplaceAllUsesWith(
          impl->query, impl->columns[1], that.impl->columns[1]);

    } else if (this_cmp->input_columns[0].get() ==
               that_cmp->input_columns[1].get()) {
      Node<QueryColumn>::ReplaceAllUsesWith(
          impl->query, impl->columns[0], that.impl->columns[1]);
      Node<QueryColumn>::ReplaceAllUsesWith(
          impl->query, impl->columns[1], that.impl->columns[0]);

    } else {
      assert(false);
      return false;  // How??
    }

    impl->columns[0]->next_in_view = nullptr;
    impl->columns[1]->next_in_view = nullptr;

  // One of them is an insert; if they aren't both inserts, then fail, but if
  // they are, then we don't need to do the column replacement, as nothing
  // uses the columns of inserts, and they are actually copies of their input
  // columns.
  } else if (impl->IsInsert()) {

    // Nothing to do column wise: replacing an insert is equivalent to removing
    // it, as it is redundant.

  } else {

    // Maintain the set of group IDs, to prevent "unlucky" cases where we
    // over-merge.
    if (impl->IsSelect()) {
      assert(that.impl->IsSelect());
      auto this_select = reinterpret_cast<Node<QuerySelect> *>(impl);
      auto that_select = reinterpret_cast<Node<QuerySelect> *>(that.impl);
      that_select->group_ids.insert(
          that_select->group_ids.end(),
          this_select->group_ids.begin(),
          this_select->group_ids.end());
      std::sort(that_select->group_ids.begin(), that_select->group_ids.end());
    }

    const auto max_i = impl->columns.size();
    for (auto i = 0u; i < max_i; ++i) {
      auto col = impl->columns[i];
      assert(col->view == impl);
      auto that_col = that.impl->columns[i];
      assert(that_col->view != impl);
      Node<QueryColumn>::ReplaceAllUsesWith(impl->query, col, that_col);
      col->next_in_view = nullptr;
    }
  }

  ReplaceViewInMerges(impl, that.impl, impl->query->merges);
  impl->Clear();
  return true;
}

NodeRange<QueryColumn> QuerySelect::Columns(void) const {
  return QueryView(impl).Columns();
}

bool QueryColumn::IsSelect(void) const noexcept {
  return impl->view->IsSelect();
}

bool QueryColumn::IsJoin(void) const noexcept {
  return impl->view->IsJoin();
}

bool QueryColumn::IsMap(void) const noexcept {
  return impl->view->IsMap();
}

bool QueryColumn::IsAggregateGroup(void) const noexcept {
  if (!impl->view->IsAggregate()) {
    return false;
  }

  auto agg = reinterpret_cast<Node<QueryAggregate> *>(impl->view);
  if (impl->index >= agg->group_by_columns.size()) {
    return false;
  }

  return impl->index < (agg->group_by_columns.size() -
                        agg->bound_columns.size());
}

bool QueryColumn::IsAggregateConfig(void) const noexcept {
  if (!impl->view->IsAggregate()) {
    return false;
  }

  auto agg = reinterpret_cast<Node<QueryAggregate> *>(impl->view);
  if (impl->index >= agg->group_by_columns.size()) {
    return false;
  }

  return impl->index >= (agg->group_by_columns.size() -
                         agg->bound_columns.size());
}

bool QueryColumn::IsAggregateSummary(void) const noexcept {
  if (!impl->view->IsAggregate()) {
    return false;
  }

  auto agg = reinterpret_cast<Node<QueryAggregate> *>(impl->view);
  return impl->index >= agg->group_by_columns.size();
}

bool QueryColumn::IsMerge(void) const noexcept {
  return impl->view->IsMerge();
}

bool QueryColumn::IsConstraint(void) const noexcept {
  return impl->view->IsConstraint();
}

// Returns a unique ID representing the equivalence class of this column.
// Two columns with the same equivalence class will have the same values.
uint64_t QueryColumn::EquivalenceClass(void) const noexcept {
  return reinterpret_cast<uintptr_t>(impl->Find());
}

// Number of uses of this column.
unsigned QueryColumn::NumUses(void) const noexcept {
  return impl->num_uses;
}

// Replace all uses of one column with another column.
bool QueryColumn::ReplaceAllUsesWith(QueryColumn that) const noexcept {
  if (impl == that.impl) {
    return true;

  } else if (impl->view->query != that.impl->view->query) {
    return false;

  } else if (impl->var.Type().Kind() != that.impl->var.Type().Kind()) {
    return false;
  }

  Node<QueryColumn>::ReplaceAllUsesWith(
      impl->view->query, impl, that.impl);
  return true;
}

QueryColumn::QueryColumn(Node<QueryColumn> *impl_)
    : query::QueryNode<QueryColumn>(impl_) {
  assert(impl->index < impl->view->columns.size());
  assert(impl == impl->view->columns[impl->index]);
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
  return nullptr != impl->relation;
}
bool QuerySelect::IsStream(void) const noexcept {
  return nullptr != impl->stream;
}

QueryRelation QuerySelect::Relation(void) const noexcept {
  assert(nullptr != impl->relation);
  return QueryRelation(impl->relation);
}

QueryStream QuerySelect::Stream(void) const noexcept {
  assert(nullptr != impl->stream);
  return QueryStream(impl->stream);
}

QueryJoin &QueryJoin::From(QueryView &view) {
  assert(view.IsJoin());
  return reinterpret_cast<QueryJoin &>(view);
}

// Returns the number of joined columns.
unsigned QueryJoin::NumInputColumns(void) const noexcept {
  return static_cast<unsigned>(impl->joined_columns.size());
}

// Returns the number of joined columns.
unsigned QueryJoin::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.size());
}

unsigned QueryJoin::NumPivotColumns(void) const noexcept {
  return static_cast<unsigned>(impl->pivot_columns.size());
}

// Returns the `nth` pivot column.
QueryColumn QueryJoin::NthPivotColumn(unsigned n) const noexcept {
  assert(n < impl->pivot_columns.size());
  return QueryColumn(impl->pivot_columns[n]);
}

// Returns the `nth` joined column.
QueryColumn QueryJoin::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->joined_columns.size());
  return QueryColumn(impl->joined_columns[n]);
}

// Returns the `nth` joined column's original input variable. This might
// not correspond with the variable of the nth input column, though.
ParsedVariable QueryJoin::NthInputVariable(unsigned n) const noexcept {
  assert(n < impl->joined_columns.size());
  return impl->joined_columns[n].Variable();
}

// Returns the `nth` output column.
QueryColumn QueryJoin::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.size());
  return QueryColumn(impl->columns[n]);
}

QueryMap &QueryMap::From(QueryView &view) {
  assert(view.IsMap());
  return reinterpret_cast<QueryMap &>(view);
}

unsigned QueryMap::NumInputColumns(void) const noexcept {
  return static_cast<unsigned>(impl->input_columns.size());
}

QueryColumn QueryMap::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.size());
  return QueryColumn(impl->input_columns[n]);
}

// The variable associated with the nth input. This may be different than
// the nth input column's variable, due to optimizations.
ParsedVariable QueryMap::NthInputVariable(unsigned n) const noexcept {
  assert(n < impl->input_columns.size());
  return impl->input_columns[n].Variable();
}

// The resulting mapped columns.
NodeRange<QueryColumn> QueryMap::Columns(void) const {
  if (impl->columns.empty()) {
    return NodeRange<QueryColumn>();
  } else {
    return NodeRange<QueryColumn>(
        impl->columns.front(),
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryColumn>, next_in_view)));
  }
}

// Returns the number of output columns.
unsigned QueryMap::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.size());
}

// Returns the `nth` output column.
QueryColumn QueryMap::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.size());
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
NodeRange<QueryColumn> QueryAggregate::Columns(void) const {
  if (impl->columns.empty()) {
    return NodeRange<QueryColumn>();
  } else {
    return NodeRange<QueryColumn>(
        impl->columns.front(),
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryColumn>, next_in_view)));
  }
}

// Returns the number of output columns.
unsigned QueryAggregate::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.size());
}

// Returns the `nth` output column.
QueryColumn QueryAggregate::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.size());
  return QueryColumn(impl->columns[n]);
}

// Returns the number of columns used for grouping.
unsigned QueryAggregate::NumGroupColumns(void) const noexcept {
  return static_cast<unsigned>(impl->group_by_columns.size() -
                               impl->bound_columns.size());
}

// Returns the `nth` grouping column.
QueryColumn QueryAggregate::NthGroupColumn(unsigned n) const noexcept {
  assert(n < (impl->group_by_columns.size() - impl->bound_columns.size()));
  assert(QueryColumn(impl->columns[n]).IsAggregateGroup());
  return QueryColumn(impl->group_by_columns[n]);
}

// Returns the number of columns used for configuration.
unsigned QueryAggregate::NumConfigColumns(void) const noexcept {
  return static_cast<unsigned>(impl->bound_columns.size());
}

// Returns the `nth` config column.
QueryColumn QueryAggregate::NthConfigColumn(unsigned n) const noexcept {
  n += NumGroupColumns();
  assert(n < impl->group_by_columns.size());
  assert(QueryColumn(impl->columns[n]).IsAggregateConfig());
  return QueryColumn(impl->group_by_columns[n]);
}

// Returns the number of columns being summarized.
unsigned QueryAggregate::NumSummarizedColumns(void) const noexcept {
  return static_cast<unsigned>(impl->summarized_columns.size());
}

// Returns the `nth` summarized column.
QueryColumn QueryAggregate::NthSummarizedColumn(unsigned n) const noexcept {
  assert(n < impl->summarized_columns.size());
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
NodeRange<QueryColumn> QueryMerge::Columns(void) const {
  if (impl->columns.empty()) {
    return NodeRange<QueryColumn>();
  } else {
    return NodeRange<QueryColumn>(
        impl->columns.front(),
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryColumn>, next_in_view)));
  }
}

// Returns the number of output columns.
unsigned QueryMerge::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.size());
}

// Returns the `nth` output column.
QueryColumn QueryMerge::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.size());
  return QueryColumn(impl->columns[n]);
}

// Number of views that are merged together at this point.
unsigned QueryMerge::NumMergedViews(void) const noexcept {
  return static_cast<unsigned>(impl->merged_views.size());
}

// Nth view that is merged together at this point.
QueryView QueryMerge::NthMergedView(unsigned n) const noexcept {
  assert(n < impl->merged_views.size());
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
  return QueryColumn(impl->columns[1]);
}

QueryColumn QueryConstraint::InputLHS(void) const {
  return QueryColumn(impl->input_columns[0]);
}

QueryColumn QueryConstraint::InputRHS(void) const {
  return QueryColumn(impl->input_columns[1]);
}

ParsedVariable QueryConstraint::InputLHSVariable(void) const {
  return impl->input_columns[0].Variable();
}

ParsedVariable QueryConstraint::InputRHSVariable(void) const {
  return impl->input_columns[1].Variable();
}

QueryRelation QueryInsert::Relation(void) const noexcept {
  return QueryRelation(impl->relation);
}

unsigned QueryInsert::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->input_columns.size());
}

QueryColumn QueryInsert::NthColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.size());
  return QueryColumn(impl->input_columns[n]);
}

NodeRange<QueryJoin> Query::Joins(void) const {
  if (!impl->next_join) {
    return NodeRange<QueryJoin>();
  } else {
    return NodeRange<QueryJoin>(impl->next_join);
  }
}

NodeRange<QuerySelect> Query::Selects(void) const {
  if (!impl->next_select) {
    return NodeRange<QuerySelect>();
  } else {
    return NodeRange<QuerySelect>(impl->next_select);
  }
}

NodeRange<QueryRelation> Query::Relations(void) const {
  if (!impl->context->next_relation) {
    return NodeRange<QueryRelation>();
  } else {
    return NodeRange<QueryRelation>(impl->context->next_relation);
  }
}

NodeRange<QueryStream> Query::Streams(void) const {
  if (!impl->context->next_stream) {
    return NodeRange<QueryStream>();
  } else {
    return NodeRange<QueryStream>(
        impl->context->next_stream,
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryStream>, next_stream)));
  }
}

NodeRange<QueryConstant> Query::Constants(void) const {
  if (!impl->context->next_constant) {
    return NodeRange<QueryConstant>();
  } else {
    return NodeRange<QueryConstant>(impl->context->next_constant);
  }
}

NodeRange<QueryGenerator> Query::Generators(void) const {
  if (!impl->context->next_generator) {
    return NodeRange<QueryGenerator>();
  } else {
    return NodeRange<QueryGenerator>(impl->context->next_generator);
  }
}

NodeRange<QueryInput> Query::Inputs(void) const {
  if (!impl->context->next_input) {
    return NodeRange<QueryInput>();
  } else {
    return NodeRange<QueryInput>(impl->context->next_input);
  }
}

NodeRange<QueryView> Query::Views(void) const {
  if (!impl->next_view) {
    return NodeRange<QueryView>();
  } else {
    return NodeRange<QueryView>(
        impl->next_view,
        static_cast<intptr_t>(__builtin_offsetof(Node<QueryView>, next_view)));
  }
}

NodeRange<QueryInsert> Query::Inserts(void) const {
  if (impl->inserts.empty()) {
    return NodeRange<QueryInsert>();
  } else {
    return NodeRange<QueryInsert>(impl->next_insert);
  }
}

NodeRange<QueryMap> Query::Maps(void) const {
  if (!impl->next_map) {
    return NodeRange<QueryMap>();
  } else {
    return NodeRange<QueryMap>(impl->next_map);
  }
}

NodeRange<QueryAggregate> Query::Aggregates(void) const {
  if (!impl->next_aggregate) {
    return NodeRange<QueryAggregate>();
  } else {
    return NodeRange<QueryAggregate>(impl->next_aggregate);
  }
}

NodeRange<QueryMerge> Query::Merges(void) const {
  if (!impl->next_merge) {
    return NodeRange<QueryMerge>();
  } else {
    return NodeRange<QueryMerge>(impl->next_merge);
  }
}

NodeRange<QueryConstraint> Query::Constraints(void) const {
  if (!impl->next_constraint) {
    return NodeRange<QueryConstraint>();
  } else {
    return NodeRange<QueryConstraint>(impl->next_constraint);
  }
}

Query::~Query(void) {}

}  // namespace hyde

#pragma clang diagnostic pop
