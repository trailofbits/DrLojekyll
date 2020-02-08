// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

#include <cassert>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"

namespace hyde {
namespace query {

QueryContext::~QueryContext(void) {}

}  // namespace query

QueryImpl::~QueryImpl(void) {
  ForEachView([] (VIEW *view) {
    view->input_columns.ClearWithoutErasure();
    view->attached_columns.ClearWithoutErasure();
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

  } else if (!impl->Equals(eq, that.impl)) {
    return false;
  }

  const auto num_cols = impl->columns.Size();
  assert(num_cols == that.impl->columns.Size());

  // Maintain the set of group IDs, to prevent over-merging.
  if (impl->check_group_ids || that.impl->check_group_ids) {
    that.impl->group_ids.insert(
        that.impl->group_ids.end(),
        impl->group_ids.begin(),
        impl->group_ids.end());
    std::sort(that.impl->group_ids.begin(), that.impl->group_ids.end());

    that.impl->check_group_ids = true;
  }

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

QueryRelation QueryRelation::From(const QuerySelect &sel) noexcept {
  const auto rel = sel.impl->relation.get();
  assert(rel != nullptr);
  return QueryRelation(rel);
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

// The range of input group columns.
UsedNodeRange<QueryColumn> QueryMap::InputCopiedColumns(void) const {
  return {impl->attached_columns.begin(),
          impl->attached_columns.end()};
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryMap::Columns(void) const {
  const auto num_group_cols = impl->attached_columns.Size();
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(
              impl->columns.end() - num_group_cols)};
}

// The resulting grouped columns.
DefinedNodeRange<QueryColumn> QueryMap::CopiedColumns(void) const {
  const auto num_cols = impl->columns.Size();
  const auto num_group_cols = impl->attached_columns.Size();
  const auto first_group_col_index = num_cols - num_group_cols;
  return {DefinedNodeIterator<QueryColumn>(
      impl->columns.begin() + first_group_col_index),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Returns the number of output columns.
unsigned QueryMap::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
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

// Returns the number of columns used for grouping.
unsigned QueryAggregate::NumGroupColumns(void) const noexcept {
  return impl->group_by_columns.Size();
}

// Returns the number of columns used for configuration.
unsigned QueryAggregate::NumConfigColumns(void) const noexcept {
  return impl->bound_columns.Size();
}

// Returns the number of columns being summarized.
unsigned QueryAggregate::NumSummarizedColumns(void) const noexcept {
  return impl->summarized_columns.Size();
}

// Returns the `nth` output grouping column.
QueryColumn QueryAggregate::NthGroupColumn(unsigned n) const noexcept {
  assert(n < impl->group_by_columns.Size());
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

// Returns the `nth` output config column.
QueryColumn QueryAggregate::NthConfigColumn(unsigned n) const noexcept {
  const auto num_group_cols = impl->group_by_columns.Size();
  assert((n + num_group_cols) < impl->bound_columns.Size());
  return QueryColumn(impl->columns[n + num_group_cols]);
}

// Returns the `nth` output summarized column.
QueryColumn QueryAggregate::NthSummarizedColumn(unsigned n) const noexcept {
  const auto num_group_cols = impl->group_by_columns.Size();
  const auto num_bound_cols = impl->bound_columns.Size();
  const auto disp = num_group_cols + num_bound_cols;
  assert((n + disp) < impl->summarized_columns.Size());
  return QueryColumn(impl->columns[n + disp]);
}

// Returns the `nth` input grouping column.
QueryColumn QueryAggregate::NthInputGroupColumn(unsigned n) const noexcept {
  assert(n < impl->group_by_columns.Size());
  return QueryColumn(impl->group_by_columns[n]);
}

// Returns the `nth` input config column.
QueryColumn QueryAggregate::NthInputConfigColumn(unsigned n) const noexcept {
  assert(n < impl->bound_columns.Size());
  return QueryColumn(impl->bound_columns[n]);
}

// Returns the `nth` input summarized column.
QueryColumn QueryAggregate::NthInputSummarizedColumn(unsigned n) const noexcept {
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

// Range of views unioned together by this MERGE.
UsedNodeRange<QueryView> QueryMerge::MergedViews(void) const {
  return {UsedNodeIterator<QueryView>(impl->merged_views.begin()),
          UsedNodeIterator<QueryView>(impl->merged_views.end())};
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
