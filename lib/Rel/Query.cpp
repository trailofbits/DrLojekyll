// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

#include <cassert>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"

#include <drlojekyll/Sema/SIPSAnalysis.h>

namespace hyde {
namespace query {

QueryContext::QueryContext(void) {}

QueryContext::~QueryContext(void) {}

}  // namespace query

Node<QueryStream>::~Node(void) {}
Node<QueryConstant>::~Node(void) {}
Node<QueryMessage>::~Node(void) {}
Node<QueryGenerator>::~Node(void) {}
Node<QueryView>::~Node(void) {}
Node<QuerySelect>::~Node(void) {}
Node<QueryJoin>::~Node(void) {}
Node<QueryMap>::~Node(void) {}
Node<QueryAggregate>::~Node(void) {}

bool Node<QueryConstant>::IsConstant(void) const noexcept {
  return true;
}

bool Node<QueryConstant>::IsGenerator(void) const noexcept {
  return false;
}

bool Node<QueryConstant>::IsMessage(void) const noexcept {
  return false;
}

bool Node<QueryGenerator>::IsConstant(void) const noexcept {
  return false;
}

bool Node<QueryGenerator>::IsGenerator(void) const noexcept {
  return true;
}

bool Node<QueryGenerator>::IsMessage(void) const noexcept {
  return false;
}

bool Node<QueryMessage>::IsConstant(void) const noexcept {
  return false;
}

bool Node<QueryMessage>::IsGenerator(void) const noexcept {
  return false;
}

bool Node<QueryMessage>::IsMessage(void) const noexcept {
  return true;
}

bool Node<QuerySelect>::IsSelect(void) const noexcept {
  return true;
}

bool Node<QuerySelect>::IsJoin(void) const noexcept {
  return false;
}

bool Node<QuerySelect>::IsMap(void) const noexcept {
  return false;
}

bool Node<QuerySelect>::IsAggregate(void) const noexcept {
  return false;
}

bool Node<QueryJoin>::IsSelect(void) const noexcept {
  return false;
}

bool Node<QueryJoin>::IsJoin(void) const noexcept {
  return true;
}

bool Node<QueryJoin>::IsMap(void) const noexcept {
  return false;
}

bool Node<QueryJoin>::IsAggregate(void) const noexcept {
  return false;
}

bool Node<QueryMap>::IsSelect(void) const noexcept {
  return false;
}

bool Node<QueryMap>::IsJoin(void) const noexcept {
  return false;
}

bool Node<QueryMap>::IsMap(void) const noexcept {
  return true;
}

bool Node<QueryMap>::IsAggregate(void) const noexcept {
  return false;
}

bool Node<QueryAggregate>::IsSelect(void) const noexcept {
  return false;
}

bool Node<QueryAggregate>::IsJoin(void) const noexcept {
  return false;
}

bool Node<QueryAggregate>::IsMap(void) const noexcept {
  return false;
}

bool Node<QueryAggregate>::IsAggregate(void) const noexcept {
  return true;
}

bool QueryStream::IsBlocking(void) const noexcept {
  return impl->IsMessage();
}

bool QueryStream::IsNonBlocking(void) const noexcept {
  return !impl->IsMessage();
}

bool QueryStream::IsConstant(void) const noexcept {
  return impl->IsConstant();
}

bool QueryStream::IsGenerator(void) const noexcept {
  return impl->IsGenerator();
}

bool QueryStream::IsMessage(void) const noexcept {
  return impl->IsMessage();
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
  if (impl->columns.empty()) {
    return NodeRange<QueryColumn>();
  } else {
    return NodeRange<QueryColumn>(
        impl->columns.front(),
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryColumn>, next_in_view)));
  }
}

NodeRange<QueryColumn> QueryJoin::PivotColumns(void) const {
  if (impl->pivot_columns.empty()) {
    return NodeRange<QueryColumn>();
  } else {
    return NodeRange<QueryColumn>(
        impl->pivot_columns.front(),
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryColumn>, next_pivot_in_join)));
  }
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

QueryMessage &QueryMessage::From(QueryStream &stream) {
  assert(stream.IsMessage());
  return reinterpret_cast<QueryMessage &>(stream);
}

QueryGenerator &QueryGenerator::From(QueryStream &stream) {
  assert(stream.IsGenerator());
  return reinterpret_cast<QueryGenerator &>(stream);
}

const ParsedMessage &QueryMessage::Declaration(void) const noexcept {
  return impl->message;
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

// The list of pivot constraints.
NodeRange<QueryConstraint> QueryJoin::Constraints(void) const {
  if (impl->pivot_conditions.empty()) {
    return NodeRange<QueryConstraint>();
  } else {
    return NodeRange<QueryConstraint>(
        impl->pivot_conditions.front(),
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryConstraint>, next_pivot_condition)));
  }
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
  return static_cast<unsigned>(impl->group_by_columns.size());
}

// Returns the `nth` grouping column.
QueryColumn QueryAggregate::NthGroupColumn(unsigned n) const noexcept {
  assert(n < impl->group_by_columns.size());
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

ComparisonOperator QueryConstraint::Operator(void) const {
  return impl->op;
}

QueryColumn QueryConstraint::LHS(void) const {
  return QueryColumn(impl->lhs);
}

QueryColumn QueryConstraint::RHS(void) const {
  return QueryColumn(impl->rhs);
}

QueryRelation QueryInsert::Relation(void) const noexcept {
  return QueryRelation(impl->relation);
}

unsigned QueryInsert::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.size());
}

QueryColumn QueryInsert::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.size());
  return QueryColumn(impl->columns[n]);
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

NodeRange<QueryMessage> Query::Messages(void) const {
  if (!impl->context->next_message) {
    return NodeRange<QueryMessage>();
  } else {
    return NodeRange<QueryMessage>(impl->context->next_message);
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
    return NodeRange<QueryInsert>(impl->inserts.front().get());
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

Query::~Query(void) {}

}  // namespace hyde

#pragma clang diagnostic pop
