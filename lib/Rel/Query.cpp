// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

#include <cassert>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"

#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {
namespace query {

QueryContext::QueryContext(void) {}

QueryContext::~QueryContext(void) {}

}  // namespace query

ColumnReference::ColumnReference(Node<QueryColumn> *column_)
    : column(column_) {
  if (column) {
    column->num_uses += 1;
  }
}

ColumnReference::ColumnReference(const ColumnReference &that)
    : column(that.column) {
  if (that.column) {
    that.column->num_uses += 1;
  }
}

ColumnReference::ColumnReference(ColumnReference &&that) noexcept
    : column(that.column) {
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

void Node<QuerySelect>::Clear(void) noexcept {
  columns.clear();
}

void Node<QueryJoin>::Clear(void) noexcept {
  columns.clear();
  joined_columns.clear();
  pivot_columns.clear();
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
      hash ^= view_hash;  // Simple XOR mix as order isn't relevant.
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
    hash ^= input_col->view->Hash() * input_col->index;
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
    hash ^= input_col->view->Hash() * input_col->index;
  }

  // Mix in the hashes of the configuration columns; these are ordered.
  for (auto input_col : bound_columns) {
    hash = __builtin_rotateleft64(hash, 13);
    hash ^= input_col->view->Hash() * input_col->index;
  }

  // Mix in the hashes of the summarized columns; these are ordered.
  for (auto input_col : summarized_columns) {
    hash = __builtin_rotateleft64(hash, 13);
    hash ^= input_col->view->Hash() * input_col->index;
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

  // Mix in the hashes of the group by columns; these are unordered.
  for (auto input_col : joined_columns) {
    hash ^= input_col->view->Hash() * input_col->index;
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
    hash ^= input_col->view->Hash() * input_col->index;
  }

  hash <<= 3;
  hash |= 7;

  return hash;
}

namespace {

static bool PreCheckColumnsEq(const std::vector<ColumnReference> &this_cols,
                              const std::vector<ColumnReference> &that_cols) {
  const auto max_i = this_cols.size();
  for (auto i = 0u; i < max_i; ++i) {
    const auto this_col = this_cols[i].get();
    const auto that_col = that_cols[i].get();

    if (((this_col->index | this_col->view->index_mask) !=
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
    if (!this_col->view->Equals(eq, that_col->view)) {
      return false;
    }
  }
  return true;
}

static void EquateColumns(const std::vector<ColumnReference> &this_cols,
                          const std::vector<ColumnReference> &that_cols) {
  const auto max_i = this_cols.size();
  for (auto i = 0u; i < max_i; ++i) {
    const auto this_col = this_cols[i].get();
    const auto that_col = that_cols[i].get();
    DisjointSet::Union(this_col, that_col);
  }
}

static void EquateColumns(const std::vector<Node<QueryColumn> *> &this_cols,
                          const std::vector<Node<QueryColumn> *> &that_cols) {
  const auto max_i = this_cols.size();
  for (auto i = 0u; i < max_i; ++i) {
    const auto this_col = this_cols[i];
    const auto that_col = that_cols[i];
    DisjointSet::Union(this_col, that_col);
  }
}

}  // namespace

bool Node<QuerySelect>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsSelect() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QuerySelect> *>(that_);
  if (eq.Contains(this, that)) {
    return true;

  } else if (Find() == that->Find()) {
    EquateColumns(columns, that->columns);
    eq.Insert(this, that);
    return true;
  }

  // Two selects in the same logical clause are not allowed to be merged,
  // except in rare cases like constant streams.
  if (group_id == that->group_id) {
    return false;
  }

  if (stream == that->stream && relation == that->relation &&
      columns.size() == that->columns.size()) {
    eq.Insert(this, that);
    EquateColumns(columns, that->columns);
    DisjointSet::Union(this, that);
    return true;

  } else {
    return false;
  }
}

bool Node<QueryJoin>::Equals(EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsJoin() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryJoin> *>(that_);
  if (pivot_columns.size() != that->pivot_columns.size() ||
      joined_columns.size() != that->joined_columns.size()) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;

  } else if (Find() == that->Find()) {
    EquateColumns(columns, that->columns);
    eq.Insert(this, that);
    return true;
  }

  std::unordered_map<Node<QueryView> *, uint64_t> this_joined_views;
  std::unordered_map<Node<QueryView> *, uint64_t> that_joined_views;

  for (auto &joined_col : joined_columns) {
    this_joined_views.emplace(joined_col->view, joined_col->view->Hash());
  }

  for (auto &joined_col : that->joined_columns) {
    that_joined_views.emplace(joined_col->view, joined_col->view->Hash());
  }

  if (this_joined_views.size() != that_joined_views.size()) {
    return false;
  }

  // In case of cycles, assume that these two joins are equivalent.
  eq.Insert(this, that);

  // Maps up views in `this` with views in `that`.
  std::unordered_map<Node<QueryView> *, Node<QueryView> *> matches;
  for (auto &view_hash : this_joined_views) {
    const auto v1 = view_hash.first;
    for (auto &that_view_hash : that_joined_views) {
      const auto v2 = that_view_hash.first;
      if (view_hash.second != that_view_hash.second || matches.count(v2)) {
        continue;
      }
      if (v1->Equals(eq, v2)) {
        matches.emplace(v2, v1);
      }
    }
  }

  // Make sure that we matched up views in both.
  if (matches.size() != this_joined_views.size()) {
    eq.Remove(this, that);  // Remove the assumption.
    return false;
  }

  // Now go match up columns in both.
  for (auto v1_v2 : matches) {
    const auto v1 = v1_v2.first;
    const auto v2 = v1_v2.second;
    const auto max_i = v1->columns.size();
    assert(v2->columns.size() == max_i);

    for (auto i = 0u; i < max_i; ++i) {
      const auto v1_col = v1->columns[i];
      const auto v2_col = v2->columns[i];
      assert(v1_col->var.Type().Kind() == v2_col->var.Type().Kind());

      // Needed by `ReplaceAllUsesWith`.
      DisjointSet::Union(v1_col, v2_col);
    }
  }

  // Mark them as being part of teh same equivalence class. This is needed
  // by `ReplaceAllUsesWith` when dealing with trying to replace one join with
  // another join.
  DisjointSet::Union(this, that);
  return true;
}

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

  } else if (Find() == that->Find()) {
    EquateColumns(columns, that->columns);
    eq.Insert(this, that);
    return true;
  }

  if (!PreCheckColumnsEq(input_columns, that->input_columns)) {
    return false;
  }

  // In case of cycles, assume that these two joins are equivalent.
  eq.Insert(this, that);

  // Make sure all input views are equivalent.
  if (!CheckColumnsEq(eq, input_columns, that->input_columns)) {
    eq.Remove(this, that);
    return false;
  }

  const auto max_i = input_columns.size();
  for (auto i = 0u; i < max_i; ++i) {
    auto this_col = input_columns[i].get();
    auto that_col = that->input_columns[i].get();
    DisjointSet::Union(this_col, that_col);
  }

  DisjointSet::Union(this, that);

  return true;
}

bool Node<QueryAggregate>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsMap() || query != that_->query ||
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

  } else if (Find() == that->Find()) {
    EquateColumns(columns, that->columns);
    eq.Insert(this, that);
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

  EquateColumns(group_by_columns, that->group_by_columns);
  EquateColumns(bound_columns, that->bound_columns);
  EquateColumns(summarized_columns, that->summarized_columns);
  EquateColumns(columns, that->columns);
  DisjointSet::Union(this, that);

  return true;
}

bool Node<QueryMerge>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsSelect() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryMerge> *>(that_);

  if (eq.Contains(this, that)) {
    return true;

  } else if (Find() == that->Find()) {
    EquateColumns(columns, that->columns);
    eq.Insert(this, that);
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

  EquateColumns(columns, that->columns);
  DisjointSet::Union(this, that);
  return true;
}

bool Node<QueryConstraint>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsSelect() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryConstraint> *>(that_);
  if (input_columns.size() != 2 || that->input_columns.size() != 2 ||
      op != that->op) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  const auto is_unordered = op == ComparisonOperator::kEqual ||
                            op == ComparisonOperator::kNotEqual;

  auto v0_c0 = input_columns[0];
  auto v0_c1 = input_columns[1];
  auto v0_c0_out = columns[0];
  auto v0_c1_out = columns[1];
  auto v0_c0_view = v0_c0->view;
  auto v0_c1_view = v0_c1->view;
  auto v0_c0_hash = v0_c0_view->Hash();
  auto v0_c1_hash = v0_c1_view->Hash();
  if (is_unordered && v0_c1_hash < v0_c0_hash) {
    std::swap(v0_c0, v0_c1);
    std::swap(v0_c0_out, v0_c1_out);
    std::swap(v0_c0_view, v0_c1_view);
    std::swap(v0_c0_hash, v0_c1_hash);
  }

  auto v1_c0 = that->input_columns[0];
  auto v1_c1 = that->input_columns[1];
  auto v1_c0_out = that->columns[0];
  auto v1_c1_out = that->columns[1];
  auto v1_c0_view = v1_c0->view;
  auto v1_c1_view = v1_c1->view;
  auto v1_c0_hash = v1_c0_view->Hash();
  auto v1_c1_hash = v1_c1_view->Hash();
  if (is_unordered && v1_c1_hash < v1_c0_hash) {
    std::swap(v1_c0, v1_c1);
    std::swap(v1_c0_out, v1_c1_out);
    std::swap(v1_c0_view, v1_c1_view);
    std::swap(v1_c0_hash, v1_c1_hash);
  }

  DisjointSet::Union(v0_c0, v0_c0_out);
  DisjointSet::Union(v0_c1, v0_c1_out);

  DisjointSet::Union(v1_c0, v1_c0_out);
  DisjointSet::Union(v1_c1, v1_c1_out);

  if (v0_c0_hash != v1_c0_hash || v0_c1_hash != v1_c1_hash ||
      ((v0_c0->index | v0_c0_view->index_mask) !=
       (v1_c0->index | v1_c0_view->index_mask)) ||
      ((v0_c1->index | v0_c1_view->index_mask) !=
       (v1_c1->index | v1_c1_view->index_mask)) ||
      v0_c0->var.Type().Kind() != v1_c0->var.Type().Kind() ||
      v0_c1->var.Type().Kind() != v1_c1->var.Type().Kind()) {
    return false;
  }

  // In case of cycles, assume that these two constraints are equivalent.
  eq.Insert(this, that);

  if (!v0_c0_view->Equals(eq, v1_c0_view) ||
      !v0_c1_view->Equals(eq, v1_c1_view)) {
    eq.Remove(this, that);
    return false;
  }

  DisjointSet::Union(v0_c0, v1_c0);
  DisjointSet::Union(v0_c1, v1_c1);
  DisjointSet::Union(v0_c0_out, v1_c0_out);
  DisjointSet::Union(v0_c1_out, v1_c1_out);
  DisjointSet::Union(this, that);
  return true;
}

bool Node<QueryInsert>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  if (!that_->IsSelect() || query != that_->query ||
      columns.size() != that_->columns.size()) {
    return false;
  }

  const auto that = reinterpret_cast<Node<QueryInsert> *>(that_);
  if (decl != that->decl) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;

  } else if (Find() == that->Find()) {
    EquateColumns(columns, that->columns);
    eq.Insert(this, that);
    return true;
  }

  if (!PreCheckColumnsEq(input_columns, that->input_columns)) {
    return false;
  }

  eq.Insert(this, that);
  if (!CheckColumnsEq(eq, input_columns, that->input_columns)) {
    eq.Remove(this, that);
    return false;
  }

  EquateColumns(input_columns, that->input_columns);
  DisjointSet::Union(this, that);
  return true;
}

Node<QueryConstraint>::Node(ComparisonOperator op_, Node<QueryColumn> *lhs_,
                            Node<QueryColumn> *rhs_)
    : op(op_) {
  input_columns.emplace_back(lhs_);
  input_columns.emplace_back(rhs_);

  if (op == ComparisonOperator::kEqual || op == ComparisonOperator::kNotEqual) {
    index_mask = ~0u;
  }
}

namespace {

static void ReplaceColumnUses(Node<QueryColumn> *old_col,
                              Node<QueryColumn> *new_col,
                              std::vector<ColumnReference> &cols) {
  for (auto &col : cols) {
    if (col == old_col) {
      col = new_col;
    }
  }
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

  for (auto &join : query->joins) {
    ReplaceColumnUses(old_col, new_col, join->joined_columns);
    join->hash = 0;
  }

  for (auto &map : query->maps) {
    ReplaceColumnUses(old_col, new_col, map->input_columns);
    map->hash = 0;
  }

  for (auto &agg : query->aggregates) {
    ReplaceColumnUses(old_col, new_col, agg->group_by_columns);
    ReplaceColumnUses(old_col, new_col, agg->bound_columns);
    ReplaceColumnUses(old_col, new_col, agg->summarized_columns);
    agg->hash = 0;
  }

  for (auto &insert : query->inserts) {
    ReplaceColumnUses(old_col, new_col, insert->input_columns);
    insert->hash = 0;
  }

  for (auto &cmp : query->constraints) {
    ReplaceColumnUses(old_col, new_col, cmp->input_columns);
    cmp->hash = 0;
  }

  DisjointSet::UnionInto(old_col, new_col);
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
bool QueryView::ReplaceAllUsesWith(QueryView that) const noexcept {
  if (impl == that.impl) {
    return true;

  } else if (impl->query != that.impl->query) {
    return false;

  } else if (impl->columns.size() != that.impl->columns.size()) {
    return false;
  }

  // If one of them isn't a join, then do a straight up replacement.
  if (!impl->IsJoin() || !that.impl->IsJoin()) {

    const auto max_i = impl->columns.size();
    for (auto i = 0u; i < max_i; ++i) {
      if (impl->columns[i]->var.Type().Kind() !=
          that.impl->columns[i]->var.Type().Kind()) {
        return false;
      }
    }

    for (auto i = 0u; i < max_i; ++i) {
      auto col = impl->columns[i];
      assert(col->view == impl);
      auto that_col = that.impl->columns[i];
      assert(that_col->view != impl);
      Node<QueryColumn>::ReplaceAllUsesWith(impl->query, col, that_col);
      col->next_in_view = nullptr;
    }

  } else {

    // Joins are a special case for replacements, because we need to try to
    // match up their columns, which might be out of order / have different
    // variable names (e.g. due to them being from different clauses). Thus,
    // we require that the two joins be equivalent first, and if they are,
    // then we permit replacement.
    EqualitySet eq;
    if (!impl->Equals(eq, that.impl)) {
      return false;
    }

    auto this_join = reinterpret_cast<Node<QueryJoin> *>(impl);
    auto that_join = reinterpret_cast<Node<QueryJoin> *>(that.impl);

    // TODO(pag): Implement me.
    (void) this_join;
    (void) that_join;
    return false;
  }

  DisjointSet::Union(impl, that.impl);
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
