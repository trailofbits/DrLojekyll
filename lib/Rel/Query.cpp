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

Node<QueryTable>::~Node(void) {}

Node<QueryConstant>::~Node(void) {}

Node<QueryRelation>::~Node(void) {}

Node<QueryView>::~Node(void) {}

Node<QuerySelect>::~Node(void) {}

Node<QueryJoin>::~Node(void) {}

bool Node<QueryConstant>::IsConstant(void) const noexcept {
  return true;
}

bool Node<QueryConstant>::IsRelation(void) const noexcept {
  return false;
}

bool Node<QueryRelation>::IsConstant(void) const noexcept {
  return false;
}

bool Node<QueryRelation>::IsRelation(void) const noexcept {
  return true;
}

bool Node<QuerySelect>::IsSelect(void) const noexcept {
  return true;
}

bool Node<QuerySelect>::IsJoin(void) const noexcept {
  return false;
}

bool Node<QueryJoin>::IsSelect(void) const noexcept {
  return false;
}

bool Node<QueryJoin>::IsJoin(void) const noexcept {
  return true;
}

bool QueryTable::IsConstant(void) const noexcept {
  return impl->IsConstant();
}

bool QueryTable::IsRelation(void) const noexcept {
  return impl->IsRelation();
}

QueryView QueryView::Containing(QueryColumn col) {
  // If the column belongs to a join, then it's possible that two separate
  // joins were merged together, so go find that merged view.
  if (col.impl->view->IsJoin()) {
    return QueryView(col.impl->Find()->view);
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

bool QueryView::IsSelect(void) const noexcept {
  return impl->IsSelect();
}

bool QueryView::IsJoin(void) const noexcept {
  return impl->IsJoin();
}

NodeRange<QueryColumn> QuerySelect::Columns(void) const {
  return QueryView(impl).Columns();
}

QueryColumn QueryJoin::ResultColumn(void) const {
  assert(impl->columns.size() == 1);
  return QueryColumn(impl->columns[0]->Find());
}

bool QueryColumn::IsSelect(void) const noexcept {
  return impl->view->IsSelect();
}

bool QueryColumn::IsJoin(void) const noexcept {
  return impl->view->IsJoin();
}

const ParsedLiteral &QueryConstant::Literal(void) const noexcept {
  return impl->literal;
}

QueryConstant &QueryConstant::From(QueryTable &table) {
  assert(table.IsConstant());
  return reinterpret_cast<QueryConstant &>(table);
}

QueryRelation &QueryRelation::From(QueryTable &table) {
  assert(table.IsRelation());
  return reinterpret_cast<QueryRelation &>(table);
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

QueryTable QuerySelect::Table(void) const noexcept {
  return QueryTable(impl->table);
}

QueryJoin &QueryJoin::From(QueryView &view) {
  assert(view.IsJoin());
  return reinterpret_cast<QueryJoin &>(view);
}

// Returns the number of joined columns.
unsigned QueryJoin::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->joined_columns.size());
}

// Returns the `nth` joined column.
QueryColumn QueryJoin::NthColumn(unsigned n) const noexcept {
  assert(n < impl->joined_columns.size());
  auto col = impl->joined_columns[n];
  if (col->view->IsJoin()) {
    col = col->Find();
  }
  return QueryColumn(col);
}

// Returns the joined columns.
NodeRange<QueryColumn> QueryJoin::Columns(void) const {
  if (impl->joined_columns.empty()) {
    return NodeRange<QueryColumn>();
  } else {
    return NodeRange<QueryColumn>(
        impl->joined_columns.front(),
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryColumn>, next_joined)));
  }
}

ComparisonOperator QueryConstraint::Operator(void) const {
  return impl->op;
}

QueryColumn QueryConstraint::LHS(void) const {
  return QueryColumn(impl->lhs->Find());
}

QueryColumn QueryConstraint::RHS(void) const {
  return QueryColumn(impl->rhs->Find());
}

QueryRelation QueryInsert::Relation(void) const noexcept {
  return QueryRelation(impl->relation);
}

unsigned QueryInsert::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.size());
}

QueryColumn QueryInsert::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.size());
  return QueryColumn(impl->columns[n]->Find());
}

NodeRange<QueryJoin> Query::Joins(void) const {
  if (impl->joins.empty()) {
    return NodeRange<QueryJoin>();
  } else {
    return NodeRange<QueryJoin>(impl->joins.front().get());
  }
}

NodeRange<QuerySelect> Query::Selects(void) const {
  if (impl->selects.empty()) {
    return NodeRange<QuerySelect>();
  } else {
    return NodeRange<QuerySelect>(impl->selects.front().get());
  }
}

NodeRange<QueryTable> Query::Tables(void) const {
  if (!impl->context->next_table) {
    return NodeRange<QueryTable>();
  } else {
    return NodeRange<QueryTable>(
        impl->context->next_table,
        static_cast<intptr_t>(
            __builtin_offsetof(Node<QueryTable>, next_table)));
  }
}

NodeRange<QueryConstant> Query::Constants(void) const {
  if (!impl->context->next_constant) {
    return NodeRange<QueryConstant>();
  } else {
    return NodeRange<QueryConstant>(impl->context->next_constant);
  }
}

NodeRange<QueryRelation> Query::Relations(void) const {
  if (!impl->context->next_relation) {
    return NodeRange<QueryRelation>();
  } else {
    return NodeRange<QueryRelation>(impl->context->next_relation);
  }
}

NodeRange<QueryView> Query::Views(void) const {
  if (impl->views.empty()) {
    return NodeRange<QueryView>();
  } else {
    return NodeRange<QueryView>(
        impl->views.front(),
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

NodeRange<QueryColumn> Query::Columns(void) const {
  if (impl->columns.empty()) {
    return NodeRange<QueryColumn>();
  } else {
    return NodeRange<QueryColumn>(impl->columns.front().get());
  }
}

Query::~Query(void) {}

}  // namespace hyde

#pragma clang diagnostic pop
