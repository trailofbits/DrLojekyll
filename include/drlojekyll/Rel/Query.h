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
class ParsedFunctor;
class ParsedLiteral;
class ParsedMessage;
class ParsedPredicate;
class ParsedVariable;
class QueryBuilder;
class QueryBuilderImpl;
class QueryColumn;
class QueryConstant;
class QueryConstraint;
class QueryGenerator;
class QueryImpl;
class QueryInsert;
class QueryJoin;
class QueryMessage;
class QueryRelation;
class QuerySelect;
class QueryStream;
class QueryView;

// A column. Columns may be derived from selections or from joins.
class QueryColumn : public query::QueryNode<QueryColumn> {
 public:

  bool IsSelect(void) const noexcept;
  bool IsJoin(void) const noexcept;
  bool IsMap(void) const noexcept;

  const ParsedVariable &Variable(void) const noexcept;

  bool operator==(QueryColumn that) const noexcept;
  bool operator!=(QueryColumn that) const noexcept;

 private:
  friend class QueryConstraint;
  friend class QueryInsert;
  friend class QueryJoin;
  friend class QueryMap;
  friend class QueryView;

  using query::QueryNode<QueryColumn>::QueryNode;
};

// A table in a query. Corresponds with a declared predicate in a Datalog.
class QueryRelation : public query::QueryNode<QueryRelation> {
 public:
  const ParsedDeclaration &Declaration(void) const noexcept;
  bool IsPositive(void) const noexcept;
  bool IsNegative(void) const noexcept;

 private:
  using query::QueryNode<QueryRelation>::QueryNode;

  friend class QuerySelect;
};

// A stream of inputs into the system. This represents messsages, functors
// that take no inputs and produce outputs, and constants. The latter two are
// considered non-blocking, and messages are considered to be blocking streams.
// The blocking vs. non-blocking is in relation to pull semantics.
class QueryStream : public query::QueryNode<QueryStream> {
 public:
  bool IsBlocking(void) const noexcept;
  bool IsNonBlocking(void) const noexcept;

  bool IsConstant(void) const noexcept;
  bool IsGenerator(void) const noexcept;
  bool IsMessage(void) const noexcept;

 private:
  using query::QueryNode<QueryStream>::QueryNode;
};

// A functor in the Datalog code, that has only free parameters. This is a form
// of non-blocking stream. Code wanting blocking functors should use messages
// instead.
class QueryGenerator : public query::QueryNode<QueryGenerator> {
 public:
  const ParsedFunctor &Declaration(void) const noexcept;

  static QueryGenerator &From(QueryStream &table);

 private:
  using query::QueryNode<QueryGenerator>::QueryNode;

  friend class QuerySelect;
};

// A literal in the Datalog code. A literal is a form of non-blocking stream.
class QueryConstant : public query::QueryNode<QueryConstant> {
 public:
  const ParsedLiteral &Literal(void) const noexcept;

  static QueryConstant &From(QueryStream &table);

 private:
  using query::QueryNode<QueryConstant>::QueryNode;

  friend class QuerySelect;
};

// A message in the Datalog code. A message is a form of blocking stream.
class QueryMessage : public query::QueryNode<QueryMessage> {
 public:
  const ParsedMessage &Declaration(void) const noexcept;

  static QueryMessage &From(QueryStream &table);

 private:
  using query::QueryNode<QueryMessage>::QueryNode;

  friend class QuerySelect;
};

// A view into a collection of rows. The rows may be derived from a selection
// or a join.
class QueryView : public query::QueryNode<QueryView> {
 public:
  static QueryView Containing(QueryColumn col);

  NodeRange<QueryColumn> Columns(void) const;

  bool IsSelect(void) const noexcept;
  bool IsJoin(void) const noexcept;
  bool IsMap(void) const noexcept;
  bool IsAggregate(void) const noexcept;
  bool IsMerge(void) const noexcept;

 private:
  using query::QueryNode<QueryView>::QueryNode;
};

// A selection of all columns from a table.
class QuerySelect : public query::QueryNode<QuerySelect> {
 public:

  // The selected columns.
  NodeRange<QueryColumn> Columns(void) const;

  static QuerySelect &From(QueryView &view);

  bool IsRelation(void) const noexcept;
  bool IsStream(void) const noexcept;

  QueryRelation Relation(void) const noexcept;
  QueryStream Stream(void) const noexcept;

 private:
  using query::QueryNode<QuerySelect>::QueryNode;
};

// A join of two or more tables on one or more columns.
class QueryJoin : public query::QueryNode<QueryJoin> {
 public:
  static QueryJoin &From(QueryView &view);

  // The resulting joined columns.
  NodeRange<QueryColumn> Columns(void) const;

  // Returns the number of joined output columns
  unsigned Arity(void) const noexcept;

  // Returns the `nth` joined output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  // The pivot columns.
  NodeRange<QueryColumn> PivotColumns(void) const;

  // Returns the number of pivot columns. Some of the output columns are
  // equal to the pivot columns.
  unsigned NumPivotColumns(void) const noexcept;

  QueryColumn NthPivotColumn(unsigned n) const noexcept;

  // Returns the number of input columns
  unsigned NumInputColumns(void) const noexcept;

  // Returns the `nth` joined column.
  QueryColumn NthInputColumn(unsigned n) const noexcept;

  // The list of pivot constraints.
  NodeRange<QueryConstraint> Constraints(void) const;

 private:
  using query::QueryNode<QueryJoin>::QueryNode;
};

// Map input to zero or more outputs. Maps correspond to non-aggregating
// functors with at least
class QueryMap : public query::QueryNode<QueryMap> {
 public:
  static QueryMap &From(QueryView &view);

  unsigned NumInputColumns(void) const noexcept;
  QueryColumn NthInputColumn(unsigned n) const noexcept;

  // The resulting mapped columns.
  NodeRange<QueryColumn> Columns(void) const;

  // Returns the number of output columns.
  unsigned Arity(void) const noexcept;

  // Returns the `nth` output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  const ParsedFunctor &Functor(void) const noexcept;

 private:
  using query::QueryNode<QueryMap>::QueryNode;
};

// An aggregate operation.
class QueryAggregate : public query::QueryNode<QueryAggregate> {
 public:
  static QueryAggregate &From(QueryView &view);

  // The resulting mapped columns.
  NodeRange<QueryColumn> Columns(void) const;

  // Returns the number of output columns.
  unsigned Arity(void) const noexcept;

  // Returns the `nth` output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  // Returns the number of columns used for grouping.
  unsigned NumGroupColumns(void) const noexcept;

  // Returns the `nth` grouping column.
  QueryColumn NthGroupColumn(unsigned n) const noexcept;

  // Returns the number of columns being summarized.
  unsigned NumSummarizedColumns(void) const noexcept;

  // Returns the `nth` summarized column.
  QueryColumn NthSummarizedColumn(unsigned n) const noexcept;

  // The functor doing the aggregating.
  const ParsedFunctor &Functor(void) const noexcept;

 private:
  using query::QueryNode<QueryAggregate>::QueryNode;
};

// A merge between two or more views of the same arity, where the columns have
// the same types.
class QueryMerge : public query::QueryNode<QueryMerge> {
 public:
  static QueryMerge &From(QueryView &view);

  // The resulting mapped columns.
  NodeRange<QueryColumn> Columns(void) const;

  // Returns the number of output columns.
  unsigned Arity(void) const noexcept;

  // Returns the `nth` output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  // Number of views that are merged together at this point.
  unsigned NumMergedViews(void) const noexcept ;

  // Nth view that is merged together at this point.
  QueryView NthMergedView(unsigned n) const noexcept;
};

// A constraint between two columns.
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
  NodeRange<QueryRelation> Relations(void) const;
  NodeRange<QueryView> Views(void) const;
  NodeRange<QueryInsert> Inserts(void) const;
  NodeRange<QueryMap> Maps(void) const;
  NodeRange<QueryAggregate> Aggregates(void) const;
  NodeRange<QueryStream> Streams(void) const;
  NodeRange<QueryMessage> Messages(void) const;
  NodeRange<QueryGenerator> Generators(void) const;
  NodeRange<QueryConstant> Constants(void) const;

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
