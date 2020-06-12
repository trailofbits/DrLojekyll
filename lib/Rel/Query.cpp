// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

#include <cassert>

#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Sema/SIPSScore.h>

#include "Builder.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"

namespace hyde {
namespace query {

QueryContext::~QueryContext(void) {
  for (auto cond : conditions) {
    cond->setters.ClearWithoutErasure();
    cond->positive_users.ClearWithoutErasure();
    cond->negative_users.ClearWithoutErasure();
  }
}

}  // namespace query

QueryImpl::~QueryImpl(void) {
  ForEachView([] (VIEW *view) {
    view->input_columns.ClearWithoutErasure();
    view->attached_columns.ClearWithoutErasure();
    view->positive_conditions.ClearWithoutErasure();
    view->negative_conditions.ClearWithoutErasure();
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
    agg->config_columns.ClearWithoutErasure();
    agg->aggregated_columns.ClearWithoutErasure();
  }

  for (auto merge : merges) {
    merge->merged_views.ClearWithoutErasure();
  }
}

QueryStream QueryStream::From(const QuerySelect &sel) noexcept {
  const auto stream = sel.impl->stream.get();
  assert(stream != nullptr);
  return QueryStream(stream);
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

bool QueryStream::IsBoundQueryInput(void) const noexcept {
  const auto input = impl->AsInput();
  if (!input) {
    return false;
  }
  return input->declaration.IsQuery();
}

QueryView QueryView::Containing(QueryColumn col) {
  return QueryView(col.impl->view);
}

QueryView::QueryView(const QuerySelect &view)
    : QueryView(view.impl) {}

QueryView::QueryView(const QueryTuple &view)
    : QueryView(view.impl) {}

QueryView::QueryView(const QueryKVIndex &view)
    : QueryView(view.impl) {}

QueryView::QueryView(const QueryJoin &view)
    : QueryView(view.impl) {}

QueryView::QueryView(const QueryMap &view)
    : QueryView(view.impl) {}

QueryView::QueryView(const QueryAggregate &view)
    : QueryView(view.impl) {}

QueryView::QueryView(const QueryMerge &view)
    : QueryView(view.impl) {}

QueryView::QueryView(const QueryConstraint &view)
    : QueryView(view.impl) {}

QueryView::QueryView(const QueryInsert &view)
    : QueryView(view.impl) {}

QueryView &QueryView::From(QuerySelect &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

QueryView &QueryView::From(QueryTuple &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

QueryView &QueryView::From(QueryKVIndex &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

QueryView &QueryView::From(QueryJoin &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

QueryView &QueryView::From(QueryMap &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

QueryView &QueryView::From(QueryAggregate &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

QueryView &QueryView::From(QueryMerge &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

QueryView &QueryView::From(QueryConstraint &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

QueryView &QueryView::From(QueryInsert &view) noexcept {
  return reinterpret_cast<QueryView &>(view);
}

DefinedNodeRange<QueryColumn> QueryView::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

DefinedNodeRange<QueryColumn> QueryJoin::Columns(void) const {
  return QueryView(impl).Columns();
}

// List of the output pivot columns.
DefinedNodeRange<QueryColumn> QueryJoin::PivotColumns(void) const noexcept {
  const auto it = impl->columns.begin();
  return {DefinedNodeIterator<QueryColumn>(it),
          DefinedNodeIterator<QueryColumn>(it + NumPivotColumns())};
}

// List of the output non-pivot columns.
DefinedNodeRange<QueryColumn> QueryJoin::MergedColumns(void) const noexcept {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin() +
                                           NumPivotColumns()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

const char *QueryView::KindName(void) const noexcept {
  return impl->KindName();
}

bool QueryView::IsSelect(void) const noexcept {
  return impl->AsSelect() != nullptr;
}

bool QueryView::IsTuple(void) const noexcept {
  return impl->AsTuple() != nullptr;
}

bool QueryView::IsKVIndex(void) const noexcept {
  return impl->AsKVIndex() != nullptr;
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

bool QueryView::IsInsert(void) const noexcept {
  return impl->AsInsert() != nullptr;
}

// Can this view receive inputs that should logically "delete" entries?
bool QueryView::CanReceiveDeletions(void) const noexcept {
  return impl->can_receive_deletions;
}

// Can this view produce outputs that should logically "delete" entries?
bool QueryView::CanProduceDeletions(void) const noexcept {
  return impl->can_produce_deletions;
}

// Returns the depth of this node in the graph. This is defined as depth
// from an input (associated with a message receive) node, where the deepest
// nodes are typically responses to queries, or message publications.
unsigned QueryView::Depth(void) const noexcept {
  return impl->depth;
}

// Returns a useful string of internal metadata about this view.
std::string QueryView::DebugString(void) const noexcept {
  return impl->DebugString();
}

// Get a hash of this view.
uint64_t QueryView::Hash(void) const noexcept {
  return impl->hash ? impl->hash : impl->Hash();
}

// Positive conditions, i.e. zero-argument predicates, that must be true
// for tuples to be accepted into this node.
UsedNodeRange<QueryCondition> QueryView::PositiveConditions(void) const noexcept {
  return {impl->positive_conditions.begin(), impl->positive_conditions.end()};
}

UsedNodeRange<QueryCondition> QueryView::NegativeConditions(void) const noexcept {
  return {impl->negative_conditions.begin(), impl->negative_conditions.end()};
}

// Replace all uses of this view with `that` view.
bool QueryView::ReplaceAllUsesWith(
    EqualitySet &eq, QueryView that) const noexcept {
  if (impl == that.impl) {
    return true;

  } else if (!impl->Equals(eq, that.impl)) {
    return false;
  }

  // TODO(pag): Think about relaxing these constraints? Relaxing means fewer
  //            unions but more differential tracking.
  assert(impl->can_receive_deletions == that.impl->can_receive_deletions);
  assert(impl->can_produce_deletions == that.impl->can_produce_deletions);

  const auto num_cols = impl->columns.Size();
  assert(num_cols == that.impl->columns.Size());

  // Maintain the set of group IDs, to prevent over-merging.
  that.impl->group_ids.insert(
      that.impl->group_ids.end(),
      impl->group_ids.begin(),
      impl->group_ids.end());
  std::sort(that.impl->group_ids.begin(), that.impl->group_ids.end());

  if (const auto this_join = impl->AsJoin()) {
    (void) this_join;
  }

  for (auto i = 0u; i < num_cols; ++i) {
    const auto col = impl->columns[i];
    assert(col->view == impl);

    const auto that_col = that.impl->columns[i];
    assert(that_col->view == that.impl);

    assert(col->var.Type() == that_col->var.Type());

    col->ReplaceAllUsesWith(that_col);
  }

  impl->ReplaceAllUsesWith(that.impl);
  impl->positive_conditions.Clear();
  impl->negative_conditions.Clear();
  impl->input_columns.Clear();
  impl->attached_columns.Clear();
  impl->is_used = false;

  if (const auto as_merge = impl->AsMerge()) {
    as_merge->merged_views.Clear();

  } else if (const auto as_join = impl->AsJoin()) {
    as_join->out_to_in.clear();
    as_join->num_pivots = 0;

  } else if (const auto as_agg = impl->AsAggregate()) {
    as_agg->group_by_columns.Clear();
    as_agg->config_columns.Clear();
    as_agg->aggregated_columns.Clear();
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

bool QueryColumn::IsBoundQueryInput(void) const noexcept {
  const auto sel = impl->view->AsSelect();
  if (!sel || !sel->stream) {
    return false;
  }
  return QueryStream(sel->stream.get()).IsBoundQueryInput();
}

bool QueryColumn::IsConstant(void) const noexcept {
  return impl->IsConstant();
}

bool QueryColumn::IsGenerator(void) const noexcept {
  return impl->IsGenerator();
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

// Apply a function to each user.
void QueryColumn::ForEachUser(std::function<void(QueryView)> user_cb) const {
  impl->view->ForEachUse<VIEW>([&user_cb] (VIEW *view, VIEW *) {
    user_cb(QueryView(view));
  });

  impl->ForEachUse<VIEW>([&user_cb] (VIEW *view, COL *) {
    user_cb(QueryView(view));
  });
}

const ParsedVariable &QueryColumn::Variable(void) const noexcept {
  return impl->var;
}

const TypeLoc &QueryColumn::Type(void) const noexcept {
  return impl->type;
}

bool QueryColumn::operator==(QueryColumn that) const noexcept {
  return impl == that.impl;
}

bool QueryColumn::operator!=(QueryColumn that) const noexcept {
  return impl != that.impl;
}

// The declaration of the
const ParsedDeclaration &QueryCondition::Predicate(void) const noexcept {
  return impl->declaration;
}

// The list of views that produce nodes iff this condition is true.
UsedNodeRange<QueryView> QueryCondition::PositiveUsers(void) const {
  impl->positive_users.RemoveNull();
  impl->positive_users.Unique();
  return {impl->positive_users.begin(), impl->positive_users.end()};
}

// The list of views that produce nodes iff this condition is false.
UsedNodeRange<QueryView> QueryCondition::NegativeUsers(void) const {
  impl->negative_users.RemoveNull();
  impl->negative_users.Unique();
  return {impl->negative_users.begin(), impl->negative_users.end()};
}

// The list of views that set or unset this condition.
UsedNodeRange<QueryView> QueryCondition::Setters(void) const {
  return {impl->setters.begin(), impl->setters.end()};
}

// Can this condition be deleted?
bool QueryCondition::CanBeDeleted(void) const noexcept {
  return 0 < impl->declaration.NumDeletionClauses();
}

// Depth of this node.
unsigned QueryCondition::Depth(void) const noexcept {
  auto depth = 1u;
  for (auto setter : impl->setters) {
    depth = std::max(depth, setter->Depth());
  }
  return depth + 1u;
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

QueryRelation QueryRelation::From(const QuerySelect &sel) noexcept {
  const auto rel = sel.impl->relation.get();
  assert(rel != nullptr);
  return QueryRelation(rel);
}

const ParsedDeclaration &QueryRelation::Declaration(void) const noexcept {
  return impl->declaration;
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

std::string QuerySelect::DebugString(void) const noexcept {
  return impl->DebugString();
}

QueryJoin &QueryJoin::From(QueryView &view) {
  assert(view.IsJoin());
  return reinterpret_cast<QueryJoin &>(view);
}

// The number of output columns. This is the number of all non-pivot incoming
// columns.
unsigned QueryJoin::NumMergedColumns(void) const noexcept {
  return impl->columns.Size() - impl->num_pivots;
}

unsigned QueryJoin::NumPivotColumns(void) const noexcept {
  return impl->num_pivots;
}

// The number of views joined together.
unsigned QueryJoin::NumJoinedViews(void) const noexcept {
  if (impl->pivot_views.empty()) {
    impl->VerifyPivots();
  }
  return static_cast<unsigned>(impl->pivot_views.size());
}

// Return a list of the joined views.
const std::vector<QueryView> &QueryJoin::JoinedViews(void) const noexcept {
  if (impl->pivot_views.empty()) {
    impl->VerifyPivots();
  }
  return impl->public_pivot_views;
}

// Returns the set of pivot columns proposed by the Nth incoming view.
UsedNodeRange<QueryColumn> QueryJoin::NthInputPivotSet(unsigned n) const noexcept {
  assert(n < impl->num_pivots);
  auto use_list = impl->out_to_in.find(impl->columns[n]);
  assert(use_list != impl->out_to_in.end());
  assert(1u < use_list->second.Size());
  return {use_list->second.begin(), use_list->second.end()};
}

// Returns the set of input columns proposed by the Nth incoming view.
QueryColumn QueryJoin::NthInputMergedColumn(unsigned n) const noexcept {
  assert((n + impl->num_pivots) < impl->columns.Size());
  auto use_list = impl->out_to_in.find(impl->columns[n + impl->num_pivots]);
  assert(use_list != impl->out_to_in.end());
  assert(1u == use_list->second.Size());
  return QueryColumn(use_list->second[0]);
}

// Returns the `nth` output column.
QueryColumn QueryJoin::NthOutputMergedColumn(unsigned n) const noexcept {
  assert((n + impl->num_pivots)  < impl->columns.Size());
  return QueryColumn(impl->columns[n + impl->num_pivots]);
}

// Returns the `nth` pivot output column.
QueryColumn QueryJoin::NthOutputPivotColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  assert(n < impl->num_pivots);
  return QueryColumn(impl->columns[n]);
}

std::string QueryJoin::DebugString(void) const noexcept {
  return impl->DebugString();
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

// The range of input group columns.
UsedNodeRange<QueryColumn> QueryMap::InputCopiedColumns(void) const {
  return {impl->attached_columns.begin(),
          impl->attached_columns.end()};
}

DefinedNodeRange<QueryColumn> QueryMap::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryMap::MappedColumns(void) const {
  const auto num_copied_cols = impl->attached_columns.Size();
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(
              impl->columns.end() - num_copied_cols)};
}

// The resulting grouped columns.
DefinedNodeRange<QueryColumn> QueryMap::CopiedColumns(void) const {
  const auto num_cols = impl->columns.Size();
  const auto num_copied_cols = impl->attached_columns.Size();
  const auto first_copied_col_index = num_cols - num_copied_cols;
  return {
    DefinedNodeIterator<QueryColumn>(
        impl->columns.begin() + first_copied_col_index),
    DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Returns the number of output columns.
unsigned QueryMap::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
}

// Returns whether or not this map behaves more like a filter, i.e. if the
// number of `free`-attributed parameters in `Functor()` is zero.
bool QueryMap::IsFilterLike(void) const noexcept {
  return 0 == impl->num_free_params;
}

// Returns the `nth` output column.
QueryColumn QueryMap::NthColumn(unsigned n) const noexcept {
  assert(n < (impl->columns.Size() - impl->attached_columns.Size()));
  return QueryColumn(impl->columns[n]);
}

const ParsedFunctor &QueryMap::Functor(void) const noexcept {
  return impl->functor;
}

// Returns the number of columns used for grouping.
unsigned QueryMap::NumCopiedColumns(void) const noexcept {
  return impl->attached_columns.Size();
}

// Returns the `nth` output grouping column.
QueryColumn QueryMap::NthCopiedColumn(unsigned n) const noexcept {
  const auto num_cols = impl->columns.Size();
  const auto num_group_cols = impl->attached_columns.Size();
  assert(n < num_group_cols);
  assert(num_group_cols < num_cols);
  return QueryColumn(impl->columns[n + (num_cols - num_group_cols)]);
}

// Returns the `nth` input grouping column.
QueryColumn QueryMap::NthInputCopiedColumn(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return QueryColumn(impl->attached_columns[n]);
}

std::string QueryMap::DebugString(void) const noexcept {
  return impl->DebugString();
}

QueryAggregate &QueryAggregate::From(QueryView &view) {
  assert(view.IsAggregate());
  return reinterpret_cast<QueryAggregate &>(view);
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryAggregate::Columns(void) const noexcept {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Subsequences of the above.
DefinedNodeRange<QueryColumn> QueryAggregate::GroupColumns(void) const noexcept {
  if (impl->group_by_columns.Empty()) {
    return {DefinedNodeIterator<QueryColumn>(impl->columns.end()),
            DefinedNodeIterator<QueryColumn>(impl->columns.end())};
  } else {
    auto begin = impl->columns.begin();
    return {DefinedNodeIterator<QueryColumn>(begin),
            DefinedNodeIterator<QueryColumn>(begin + impl->group_by_columns.Size())};
  }
}

DefinedNodeRange<QueryColumn> QueryAggregate::ConfigurationColumns(void) const noexcept {
  if (impl->config_columns.Empty()) {
    return {DefinedNodeIterator<QueryColumn>(impl->columns.end()),
            DefinedNodeIterator<QueryColumn>(impl->columns.end())};
  } else {
    auto begin = impl->columns.begin() + impl->group_by_columns.Size();
    return {DefinedNodeIterator<QueryColumn>(begin),
            DefinedNodeIterator<QueryColumn>(begin + impl->config_columns.Size())};
  }
}

// NOTE(pag): There should always be at least one summary column.
DefinedNodeRange<QueryColumn> QueryAggregate::SummaryColumns(void) const noexcept {
  auto begin = impl->columns.begin() +
       (impl->group_by_columns.Size() + impl->config_columns.Size());
  return {DefinedNodeIterator<QueryColumn>(begin),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

UsedNodeRange<QueryColumn> QueryAggregate::InputGroupColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->group_by_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->group_by_columns.end())};
}

UsedNodeRange<QueryColumn>
QueryAggregate::InputConfigurationColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->config_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->config_columns.end())};
}

UsedNodeRange<QueryColumn> QueryAggregate::InputAggregatedColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->aggregated_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->aggregated_columns.end())};
}

// Returns the number of output columns.
unsigned QueryAggregate::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
}

// Returns the number of columns used for grouping.
unsigned QueryAggregate::NumGroupColumns(void) const noexcept {
  return impl->group_by_columns.Size();
}

// Returns the number of columns used for configuration.
unsigned QueryAggregate::NumConfigurationColumns(void) const noexcept {
  return impl->config_columns.Size();
}

// Returns the number of columns being summarized.
unsigned QueryAggregate::NumAggregateColumns(void) const noexcept {
  return impl->aggregated_columns.Size();
}

// Returns the number of sumary columns being produced.
unsigned QueryAggregate::NumSummaryColumns(void) const noexcept {
  const auto num_group_cols = impl->group_by_columns.Size();
  const auto num_bound_cols = impl->config_columns.Size();
  return impl->columns.Size() - (num_group_cols + num_bound_cols);
}

// Returns the `nth` output grouping column.
QueryColumn QueryAggregate::NthGroupColumn(unsigned n) const noexcept {
  assert(n < impl->group_by_columns.Size());
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

// Returns the `nth` output config column.
QueryColumn QueryAggregate::NthConfigurationColumn(unsigned n) const noexcept {
  const auto num_group_cols = impl->group_by_columns.Size();
  assert((n + num_group_cols) < impl->config_columns.Size());
  return QueryColumn(impl->columns[n + num_group_cols]);
}

// Returns the `nth` output summarized column.
QueryColumn QueryAggregate::NthSummaryColumn(unsigned n) const noexcept {
  const auto num_group_cols = impl->group_by_columns.Size();
  const auto num_bound_cols = impl->config_columns.Size();
  const auto disp = num_group_cols + num_bound_cols;
  assert((n + disp) < impl->columns.Size());
  return QueryColumn(impl->columns[n + disp]);
}

// Returns the `nth` input grouping column.
QueryColumn QueryAggregate::NthInputGroupColumn(unsigned n) const noexcept {
  assert(n < impl->group_by_columns.Size());
  return QueryColumn(impl->group_by_columns[n]);
}

// Returns the `nth` input config column.
QueryColumn QueryAggregate::NthInputConfigurationColumn(unsigned n) const noexcept {
  assert(n < impl->config_columns.Size());
  return QueryColumn(impl->config_columns[n]);
}

// Returns the `nth` input summarized column.
QueryColumn QueryAggregate::NthInputAggregateColumn(unsigned n) const noexcept {
  assert(n < impl->aggregated_columns.Size());
  return QueryColumn(impl->aggregated_columns[n]);
}

// The functor doing the aggregating.
const ParsedFunctor &QueryAggregate::Functor(void) const noexcept {
  return impl->functor;
}

std::string QueryAggregate::DebugString(void) const noexcept {
  return impl->DebugString();
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

// Range of views unioned together by this MERGE.
UsedNodeRange<QueryView> QueryMerge::MergedViews(void) const {
  return {UsedNodeIterator<QueryView>(impl->merged_views.begin()),
          UsedNodeIterator<QueryView>(impl->merged_views.end())};
}

std::string QueryMerge::DebugString(void) const noexcept {
  return impl->DebugString();
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

unsigned QueryConstraint::NumCopiedColumns(void) const noexcept {
  return impl->attached_columns.Size();
}

QueryColumn QueryConstraint::NthCopiedColumn(unsigned n) const noexcept {
  auto offset = ComparisonOperator::kEqual == impl->op ? (n + 1) : (n + 2);
  assert(offset < impl->columns.Size());
  return QueryColumn(impl->columns[offset]);
}

DefinedNodeRange<QueryColumn> QueryConstraint::CopiedColumns(void) const {
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

UsedNodeRange<QueryColumn> QueryConstraint::InputCopiedColumns(void) const {
  return {impl->attached_columns.begin(), impl->attached_columns.end()};
}

std::string QueryConstraint::DebugString(void) const noexcept {
  return impl->DebugString();
}

QueryInsert &QueryInsert::From(QueryView &view) {
  assert(view.IsInsert());
  return reinterpret_cast<QueryInsert &>(view);
}

ParsedDeclaration QueryInsert::Declaration(void) const noexcept {
  return impl->declaration;
}

bool QueryInsert::IsDelete(void) const noexcept {
  return !impl->is_insert;
}

bool QueryInsert::IsRelation(void) const noexcept {
  return impl->relation.get() != nullptr;
}

bool QueryInsert::IsStream(void) const noexcept {
  return impl->stream.get() != nullptr;
}

QueryRelation QueryInsert::Relation(void) const noexcept {
  const auto rel = impl->relation.get();
  assert(rel != nullptr);
  return QueryRelation(rel);
}

QueryStream QueryInsert::Stream(void) const noexcept {
  const auto stream = impl->stream.get();
  assert(stream != nullptr);
  return QueryStream(stream);
}

unsigned QueryInsert::NumInputColumns(void) const noexcept {
  return static_cast<unsigned>(impl->input_columns.Size());
}

QueryColumn QueryInsert::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryInsert::InputColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->input_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->input_columns.end())};
}

std::string QueryInsert::DebugString(void) const noexcept {
  return impl->DebugString();
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

std::string QueryTuple::DebugString(void) const noexcept {
  return impl->DebugString();
}

QueryKVIndex &QueryKVIndex::From(QueryView &view) {
  assert(view.IsTuple());
  return reinterpret_cast<QueryKVIndex &>(view);
}

unsigned QueryKVIndex::Arity(void) const noexcept {
  return impl->columns.Size();
}

DefinedNodeRange<QueryColumn> QueryKVIndex::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

DefinedNodeRange<QueryColumn> QueryKVIndex::KeyColumns(void) const {
  const auto it = impl->columns.begin();
  return {DefinedNodeIterator<QueryColumn>(it),
          DefinedNodeIterator<QueryColumn>(it + impl->input_columns.Size())};
}

DefinedNodeRange<QueryColumn> QueryKVIndex::ValueColumns(void) const {
  const auto it = impl->columns.begin();
  return {DefinedNodeIterator<QueryColumn>(it + impl->input_columns.Size()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

unsigned QueryKVIndex::NumKeyColumns(void) const noexcept {
  return impl->input_columns.Size();
}

unsigned QueryKVIndex::NumValueColumns(void) const noexcept {
  return impl->attached_columns.Size();
}

QueryColumn QueryKVIndex::NthKeyColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->columns[n]);
}

QueryColumn QueryKVIndex::NthInputKeyColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryKVIndex::InputKeyColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->input_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->input_columns.end())};
}

QueryColumn QueryKVIndex::NthValueColumn(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return QueryColumn(impl->columns[impl->input_columns.Size() + n]);
}

QueryColumn QueryKVIndex::NthInputValueColumn(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return QueryColumn(impl->attached_columns[n]);
}

UsedNodeRange<QueryColumn> QueryKVIndex::InputValueColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->attached_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->attached_columns.end())};
}

const ParsedFunctor &QueryKVIndex::NthValueMergeFunctor(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return impl->merge_functors[n];
}

std::string QueryKVIndex::DebugString(void) const noexcept {
  return impl->DebugString();
}

DefinedNodeRange<QueryCondition> Query::Conditions(void) const {
  return {DefinedNodeIterator<QueryCondition>(impl->context->conditions.begin()),
          DefinedNodeIterator<QueryCondition>(impl->context->conditions.end())};
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

DefinedNodeRange<QueryKVIndex> Query::KVIndices(void) const {
  return {DefinedNodeIterator<QueryKVIndex>(impl->kv_indices.begin()),
          DefinedNodeIterator<QueryKVIndex>(impl->kv_indices.end())};
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

// Build and return a new query.
Query Query::Build(const ParsedModule &module) {
  hyde::QueryBuilder query_builder;
  for (auto clause : module.Clauses()) {
    hyde::FastBindingSIPSScorer scorer;
    hyde::SIPSGenerator generator(clause);
    query_builder.VisitClause(scorer, generator);
  }
  for (auto clause : module.DeletionClauses()) {
    hyde::FastBindingSIPSScorer scorer;
    hyde::SIPSGenerator generator(clause);
    query_builder.VisitClause(scorer, generator);
  }

  return query_builder.BuildQuery();
}

Query::~Query(void) {}

}  // namespace hyde

#pragma clang diagnostic pop
