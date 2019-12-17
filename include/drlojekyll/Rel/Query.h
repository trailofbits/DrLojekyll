// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Util/Node.h>

#include <memory>

namespace hyde {
namespace query {

template <typename T>
class QueryNode {
 public:
  inline QueryNode(Node<T> *impl_)
      : impl(impl_) {}

  inline bool operator==(QueryNode<T> that) const {
    return impl == that.impl;
  }

  inline bool operator!=(QueryNode<T> that) const {
    return impl == that.impl;
  }

  inline bool operator<(QueryNode<T> that) const {
    return impl < that.impl;
  }

  uintptr_t UniqueId(void) const noexcept {
    return reinterpret_cast<uintptr_t>(impl);
  }

 protected:
  Node<T> *impl;
};

}  // namespace query

enum class ComparisonOperator : int;
class ParsedDeclaration;
class ParsedLiteral;
class ParsedPredicate;
class QueryBuilder;
class QueryBuilderImpl;
class QueryColumn;
class QueryConstant;
class QueryConstraint;
class QueryImpl;
class QueryInsert;
class QueryJoin;
class QueryRelation;
class QueryTable;
class QuerySelect;
class QueryView;

// A table in a query. Corresponds with a declared predicate in a Datalog or
// with a literal in the datalog code.
class QueryTable : public query::QueryNode<QueryTable> {
 public:
  bool IsConstant(void) const noexcept;
  bool IsRelation(void) const noexcept;

 private:
  using query::QueryNode<QueryTable>::QueryNode;

  friend class QuerySelect;
};

// A literal in the Datalog code.
class QueryConstant : public query::QueryNode<QueryConstant> {
 public:
  const ParsedLiteral &Literal(void) const noexcept;

  static QueryConstant &From(QueryTable &table);

 private:
  using query::QueryNode<QueryConstant>::QueryNode;

  friend class QuerySelect;
};

// Corresponds with a declaration in the Datalog code.
class QueryRelation : public query::QueryNode<QueryRelation> {
 public:
  static QueryRelation &From(QueryTable &table);

  const ParsedDeclaration &Declaration(void) const noexcept;

  bool IsPositive(void) const noexcept;
  bool IsNegative(void) const noexcept;

 private:
  using query::QueryNode<QueryRelation>::QueryNode;

  friend class QuerySelect;
  friend class QueryInsert;
};

// A column. Columns may be derived from selections or from joins.
class QueryColumn : public query::QueryNode<QueryColumn> {
 public:

  bool IsSelect(void) const noexcept;
  bool IsJoin(void) const noexcept;

 private:
  friend class QueryConstraint;
  friend class QueryInsert;
  friend class QueryJoin;
  friend class QueryView;

  using query::QueryNode<QueryColumn>::QueryNode;
};

// A view into a collection of rows. The rows may be derived from a selection
// or a join.
class QueryView : public query::QueryNode<QueryView> {
 public:
  static QueryView Containing(QueryColumn col);

  NodeRange<QueryColumn> Columns(void) const;
  bool IsSelect(void) const noexcept;
  bool IsJoin(void) const noexcept;

 private:
  using query::QueryNode<QueryView>::QueryNode;
};

// A selection of all columns from a table.
class QuerySelect : public query::QueryNode<QuerySelect> {
 public:

  // The selected columns.
  NodeRange<QueryColumn> Columns(void) const;

  static QuerySelect &From(QueryView &view);

  QueryTable Table(void) const noexcept;

 private:
  using query::QueryNode<QuerySelect>::QueryNode;
};

// A join of two or more tables on one or more columns.
class QueryJoin : public query::QueryNode<QueryJoin> {
 public:
  static QueryJoin &From(QueryView &view);

  unsigned NumPivotColumns(void) const noexcept;

  QueryColumn NthPivotColumn(unsigned n) const noexcept;

  // Returns the number of joined columns.
  unsigned Arity(void) const noexcept;

  // Returns the `nth` joined column.
  QueryColumn NthInputColumn(unsigned n) const noexcept;

  // Returns the `nth` output column.
  QueryColumn NthOutputColumn(unsigned n) const noexcept;

 private:
  using query::QueryNode<QueryJoin>::QueryNode;
};

// A join of two or more tables on one or more columns.
class QueryConstraint : public query::QueryNode<QueryConstraint> {
 public:

  ComparisonOperator Operator(void) const;
  QueryColumn LHS(void) const;
  QueryColumn RHS(void) const;

 private:
  using query::QueryNode<QueryConstraint>::QueryNode;
};

// An insert of one or more columns into a relation.
class QueryInsert : public query::QueryNode<QueryInsert> {
 public:
  QueryRelation Relation(void) const noexcept;

  unsigned Arity(void) const noexcept;
  QueryColumn NthColumn(unsigned n) const noexcept;

 private:
  using query::QueryNode<QueryInsert>::QueryNode;
};

// A query.
class Query {
 public:
  ~Query(void);

  NodeRange<QueryJoin> Joins(void) const;
  NodeRange<QuerySelect> Selects(void) const;
  NodeRange<QueryTable> Tables(void) const;
  NodeRange<QueryRelation> Relations(void) const;
  NodeRange<QueryConstant> Constants(void) const;
  NodeRange<QueryView> Views(void) const;
  NodeRange<QueryInsert> Inserts(void) const;

  Query(const Query &) = default;
  Query(Query &&) noexcept = default;
  Query &operator=(const Query &) = default;
  Query &operator=(Query &&) noexcept = default;

 private:
  friend class QueryBuilder;
  friend class QueryBuilderImpl;

  inline explicit Query(std::shared_ptr<QueryImpl> impl_)
      : impl(impl_) {}

  std::shared_ptr<QueryImpl> impl;
};

}  // namespace hyde
