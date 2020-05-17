// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/Node.h>

#include <memory>
#include <string>
#include <utility>

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
class EqualitySet;
class ParsedDeclaration;
class ParsedFunctor;
class ParsedLiteral;
class ParsedMessage;
class ParsedPredicate;
class ParsedVariable;
class TypeLoc;
class QueryBuilder;
class QueryBuilderImpl;
class QueryColumn;
class QueryConstant;
class QueryConstraint;
class QueryGenerator;
class QueryImpl;
class QueryInsert;
class QueryJoin;
class QueryMerge;
class QueryRelation;
class QuerySelect;
class QueryStream;
class QueryView;

// A column. Columns may be derived from selections or from joins.
class QueryColumn : public query::QueryNode<QueryColumn> {
 public:
  QueryColumn(const QueryColumn &) = default;
  QueryColumn(QueryColumn &&) noexcept = default;
  QueryColumn &operator=(const QueryColumn &) = default;
  QueryColumn &operator=(QueryColumn &&) noexcept = default;

  bool IsSelect(void) const noexcept;
  bool IsJoin(void) const noexcept;
  bool IsMap(void) const noexcept;
  bool IsMerge(void) const noexcept;
  bool IsConstraint(void) const noexcept;
  bool IsAggregate(void) const noexcept;
  bool IsBoundQueryInput(void) const noexcept;

  bool IsConstant(void) const noexcept;
  bool IsGenerator(void) const noexcept;

  const ParsedVariable &Variable(void) const noexcept;
  const TypeLoc &Type(void) const noexcept;

  bool operator==(QueryColumn that) const noexcept;
  bool operator!=(QueryColumn that) const noexcept;

  // Returns a unique ID representing the equivalence class of this column.
  // Two columns with the same equivalence class will have the same values.
  uint64_t EquivalenceClass(void) const noexcept;

  // Number of uses of this column.
  unsigned NumUses(void) const noexcept;

  // Replace all uses of one column with another column. Returns `false` if
  // type column types don't match, or if the columns are from different
  // queries.
  bool ReplaceAllUsesWith(QueryColumn that) const noexcept;

  // Apply a function to each user.
  void ForEachUser(std::function<void(QueryView)> user_cb) const;

 private:
  using query::QueryNode<QueryColumn>::QueryNode;

  friend class QueryConstraint;
  friend class QueryInsert;
  friend class QueryJoin;
  friend class QueryMap;
  friend class QueryMerge;
  friend class QueryView;
  friend class QueryAggregate;

  template <typename>
  friend class NodeIterator;

  template <typename>
  friend class Node;
};

// A table in a query. Corresponds with a declared predicate in a Datalog.
class QueryRelation : public query::QueryNode<QueryRelation> {
 public:
  static QueryRelation From(const QuerySelect &sel) noexcept;

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
  static QueryStream From(const QuerySelect &sel) noexcept;

  bool IsConstant(void) const noexcept;
  bool IsGenerator(void) const noexcept;
  bool IsInput(void) const noexcept;
  bool IsBoundQueryInput(void) const noexcept;

 private:
  friend class QueryGenerator;
  friend class QueryConstant;
  friend class QueryInput;

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

// A set of concrete inputs to a query.
class QueryInput : public query::QueryNode<QueryInput> {
 public:
  const ParsedDeclaration &Declaration(void) const noexcept;

  // The input columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  static QueryInput &From(QueryStream &stream);

  QueryRelation Relation(void) const noexcept;

 private:
  using query::QueryNode<QueryInput>::QueryNode;

  friend class QuerySelect;
};

class QueryAggregate;
class QueryMap;
class QueryTuple;
class QueryKVIndex;

// A view into a collection of rows. The rows may be derived from a selection
// or a join.
class QueryView : public query::QueryNode<QueryView> {
 public:
  static QueryView Containing(QueryColumn col);

  DefinedNodeRange<QueryColumn> Columns(void) const;

  static QueryView &From(QuerySelect &view) noexcept;
  static QueryView &From(QueryTuple &view) noexcept;
  static QueryView &From(QueryKVIndex &view) noexcept;
  static QueryView &From(QueryJoin &view) noexcept;
  static QueryView &From(QueryMap &view) noexcept;
  static QueryView &From(QueryAggregate &view) noexcept;
  static QueryView &From(QueryMerge &view) noexcept;
  static QueryView &From(QueryConstraint &view) noexcept;

  bool IsSelect(void) const noexcept;
  bool IsTuple(void) const noexcept;
  bool IsKVIndex(void) const noexcept;
  bool IsJoin(void) const noexcept;
  bool IsMap(void) const noexcept;
  bool IsAggregate(void) const noexcept;
  bool IsMerge(void) const noexcept;
  bool IsConstraint(void) const noexcept;

  // Returns the depth of this node in the graph. This is defined as depth
  // from an input (associated with a message receive) node, where the deepest
  // nodes are typically responses to queries, or message publications.
  unsigned Depth(void) const noexcept;

  // Returns a useful string of internal metadata about this view.
  std::string DebugString(void) const noexcept;

  // Get a hash of this view.
  uint64_t Hash(void) const noexcept;

  // Replace all uses of this view with `that` view. Returns `false` if the
  // two views have different arities, column types, or are from different
  // queries.
  bool ReplaceAllUsesWith(EqualitySet &eq, QueryView that) const noexcept;

 private:
  using query::QueryNode<QueryView>::QueryNode;
};

// A selection of all columns from a table.
class QuerySelect : public query::QueryNode<QuerySelect> {
 public:

  // The selected columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  static QuerySelect &From(QueryView &view);

  bool IsRelation(void) const noexcept;
  bool IsStream(void) const noexcept;

  QueryRelation Relation(void) const noexcept;
  QueryStream Stream(void) const noexcept;

  std::string DebugString(void) const noexcept;

 private:
  friend class QueryRelation;
  friend class QueryStream;

  using query::QueryNode<QuerySelect>::QueryNode;
};

// A join of two or more tables on one or more columns.
class QueryJoin : public query::QueryNode<QueryJoin> {
 public:
  static QueryJoin &From(QueryView &view);

  // The resulting joined columns. This includes pivots and non-pivots. Pivots
  // are ordered first.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  // List of the output pivot columns.
  DefinedNodeRange<QueryColumn> PivotColumns(void) const noexcept;

  // List of the output non-pivot columns.
  DefinedNodeRange<QueryColumn> MergedColumns(void) const noexcept;

  // Returns the number of pivot columns. If the number of pivots is zero, then
  // this join is the cross-product.
  unsigned NumPivotColumns(void) const noexcept;

  // The number of output columns. This is the number of all non-pivot incoming
  // columns.
  unsigned NumMergedColumns(void) const noexcept;

  // Returns the `nth` pivot output column.
  QueryColumn NthOutputPivotColumn(unsigned n) const noexcept;

  // Returns the set of pivot columns proposed by the Nth incoming view.
  UsedNodeRange<QueryColumn> NthInputPivotSet(unsigned n) const noexcept;

  // Returns the `nth` joined output column. This column is not a pivot.
  QueryColumn NthOutputMergedColumn(unsigned n) const noexcept;

  // Returns the input column corresponding to the `n`th output column, where
  // this input column is not itself assocated with a pivot set.
  QueryColumn NthInputMergedColumn(unsigned n) const noexcept;

  std::string DebugString(void) const noexcept;

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
  UsedNodeRange<QueryColumn> InputColumns(void) const noexcept;

  // The resulting mapped columns. This does not include group by columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  // The resulting grouped columns.
  DefinedNodeRange<QueryColumn> CopiedColumns(void) const;

  // Returns the number of output columns.
  unsigned Arity(void) const noexcept;

  // Returns the `nth` output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  const ParsedFunctor &Functor(void) const noexcept;

  // Returns the number of columns copied along from source views.
  unsigned NumCopiedColumns(void) const noexcept;

  // Returns the `nth` output copied column.
  QueryColumn NthCopiedColumn(unsigned n) const noexcept;

  // Returns the `nth` input copied column.
  QueryColumn NthInputCopiedColumn(unsigned n) const noexcept;

  // The range of input group columns.
  UsedNodeRange<QueryColumn> InputCopiedColumns(void) const;

  std::string DebugString(void) const noexcept;

 private:
  using query::QueryNode<QueryMap>::QueryNode;
};

// An aggregate operation.
class QueryAggregate : public query::QueryNode<QueryAggregate> {
 public:
  static QueryAggregate &From(QueryView &view);

  // The resulting mapped columns.
  DefinedNodeRange<QueryColumn> Columns(void) const noexcept;

  // Subsequences of the above.
  DefinedNodeRange<QueryColumn> GroupColumns(void) const noexcept;
  DefinedNodeRange<QueryColumn> ConfigurationColumns(void) const noexcept;
  DefinedNodeRange<QueryColumn> SummaryColumns(void) const noexcept;

  // Returns the number of output columns.
  unsigned Arity(void) const noexcept;

  // Returns the number of columns used for grouping.
  unsigned NumGroupColumns(void) const noexcept;

  // Returns the number of columns used for configuration.
  unsigned NumConfigColumns(void) const noexcept;

  // Returns the number of columns being summarized.
  unsigned NumSummarizedColumns(void) const noexcept;

  // Returns the `nth` output grouping column.
  QueryColumn NthGroupColumn(unsigned n) const noexcept;

  // Returns the `nth` output config column.
  QueryColumn NthConfigColumn(unsigned n) const noexcept;

  // Returns the `nth` output summarized column.
  QueryColumn NthSummarizedColumn(unsigned n) const noexcept;

  // Returns the `nth` input grouping column.
  QueryColumn NthInputGroupColumn(unsigned n) const noexcept;

  // Returns the `nth` input config column.
  QueryColumn NthInputConfigColumn(unsigned n) const noexcept;

  // Returns the `nth` input summarized column.
  QueryColumn NthInputSummarizedColumn(unsigned n) const noexcept;

  UsedNodeRange<QueryColumn> InputGroupColumns(void) const noexcept;
  UsedNodeRange<QueryColumn> InputConfigurationColumns(void) const noexcept;
  UsedNodeRange<QueryColumn> InputAggregatedColumns(void) const noexcept;

  // The functor doing the aggregating.
  const ParsedFunctor &Functor(void) const noexcept;

  std::string DebugString(void) const noexcept;

 private:
  using query::QueryNode<QueryAggregate>::QueryNode;
};

// A merge between two or more views of the same arity, where the columns have
// the same types.
class QueryMerge : public query::QueryNode<QueryMerge> {
 public:
  static QueryMerge &From(QueryView &view);

  // The resulting mapped columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  // Returns the number of output columns.
  unsigned Arity(void) const noexcept;

  // Returns the `nth` output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  // Number of views that are merged together at this point.
  unsigned NumMergedViews(void) const noexcept ;

  // Nth view that is merged together at this point.
  QueryView NthMergedView(unsigned n) const noexcept;

  // Range of views unioned together by this MERGE.
  UsedNodeRange<QueryView> MergedViews(void) const;

  std::string DebugString(void) const noexcept;

 private:
  using query::QueryNode<QueryMerge>::QueryNode;
};

// A constraint between two columns. The constraint results in either one
// (in the case of equality) or two (inequality) output columns. The constraint
// also passes through the other columns from the view.
class QueryConstraint : public query::QueryNode<QueryConstraint> {
 public:
  static QueryConstraint &From(QueryView &view);

  ComparisonOperator Operator(void) const;
  QueryColumn LHS(void) const;
  QueryColumn RHS(void) const;

  QueryColumn InputLHS(void) const;
  QueryColumn InputRHS(void) const;

  unsigned NumCopiedColumns(void) const noexcept;
  QueryColumn NthCopiedColumn(unsigned n) const noexcept;

  DefinedNodeRange<QueryColumn> CopiedColumns(void) const;
  UsedNodeRange<QueryColumn> InputCopiedColumns(void) const;

  std::string DebugString(void) const noexcept;

 private:
  using query::QueryNode<QueryConstraint>::QueryNode;
};

// An insert of one or more columns into a relation.
class QueryInsert : public query::QueryNode<QueryInsert> {
 public:
  ParsedDeclaration Declaration(void) const noexcept;

  bool IsRelation(void) const noexcept;
  bool IsStream(void) const noexcept;

  QueryRelation Relation(void) const noexcept;
  QueryStream Stream(void) const noexcept;

  unsigned Arity(void) const noexcept;
  QueryColumn NthColumn(unsigned n) const noexcept;

  std::string DebugString(void) const noexcept;

 private:
  using query::QueryNode<QueryInsert>::QueryNode;
};

// An tuple packages one or more columns into a temporary relation for
// convenience.
class QueryTuple : public query::QueryNode<QueryTuple> {
 public:
  static QueryTuple &From(QueryView &view);

  // The resulting mapped columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  unsigned Arity(void) const noexcept;
  QueryColumn NthColumn(unsigned n) const noexcept;

  unsigned NumInputColumns(void) const noexcept;
  QueryColumn NthInputColumn(unsigned n) const noexcept;
  UsedNodeRange<QueryColumn> InputColumns(void) const noexcept;

  std::string DebugString(void) const noexcept;

 private:
  using query::QueryNode<QueryTuple>::QueryNode;
};

// A key-value index is similar to a tuple, except that some of the columns
// are mutable.
class QueryKVIndex : public query::QueryNode<QueryKVIndex> {
 public:
  static QueryKVIndex &From(QueryView &view);

  // The resulting mapped columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  QueryColumn NthKeyColumn(unsigned n) const noexcept;
  DefinedNodeRange<QueryColumn> KeyColumns(void) const;

  QueryColumn NthValueColumn(unsigned n) const noexcept;
  DefinedNodeRange<QueryColumn> ValueColumns(void) const;

  unsigned Arity(void) const noexcept;
  QueryColumn NthColumn(unsigned n) const noexcept;

  unsigned NumKeyColumns(void) const noexcept;
  QueryColumn NthInputKeyColumn(unsigned n) const noexcept;
  UsedNodeRange<QueryColumn> InputKeyColumns(void) const noexcept;

  unsigned NumValueColumns(void) const noexcept;
  QueryColumn NthInputValueColumn(unsigned n) const noexcept;
  UsedNodeRange<QueryColumn> InputValueColumns(void) const noexcept;

  const ParsedFunctor &NthValueMergeFunctor(unsigned n) const noexcept;

  std::string DebugString(void) const noexcept;

 private:
  using query::QueryNode<QueryKVIndex>::QueryNode;
};

// A query.
class Query {
 public:
  ~Query(void);

  DefinedNodeRange<QueryJoin> Joins(void) const;
  DefinedNodeRange<QuerySelect> Selects(void) const;
  DefinedNodeRange<QueryTuple> Tuples(void) const;
  DefinedNodeRange<QueryKVIndex> KVIndices(void) const;
  DefinedNodeRange<QueryRelation> Relations(void) const;
  DefinedNodeRange<QueryInsert> Inserts(void) const;
  DefinedNodeRange<QueryMap> Maps(void) const;
  DefinedNodeRange<QueryAggregate> Aggregates(void) const;
  DefinedNodeRange<QueryMerge> Merges(void) const;
  DefinedNodeRange<QueryConstraint> Constraints(void) const;
  DefinedNodeRange<QueryInput> Inputs(void) const;
  DefinedNodeRange<QueryGenerator> Generators(void) const;
  DefinedNodeRange<QueryConstant> Constants(void) const;


  template <typename T>
  void ForEachView(T cb) const {
    for (auto view : Joins()) {
      cb(QueryView::From(view));
    }

    for (auto view : Selects()) {
      cb(QueryView::From(view));
    }

    for (auto view : Tuples()) {
      cb(QueryView::From(view));
    }

    for (auto view : KVIndices()) {
      cb(QueryView::From(view));
    }

    for (auto view : Maps()) {
      cb(QueryView::From(view));
    }

    for (auto view : Aggregates()) {
      cb(QueryView::From(view));
    }

    for (auto view : Merges()) {
      cb(QueryView::From(view));
    }

    for (auto view : Constraints()) {
      cb(QueryView::From(view));
    }
  }

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
namespace std {

template<>
struct hash<::hyde::QueryColumn> {
  inline uint64_t operator()(::hyde::QueryColumn col) const noexcept {
    return col.UniqueId();
  }
};

template<>
struct hash<::hyde::QueryView> {
  inline uint64_t operator()(::hyde::QueryView view) const noexcept {
    return view.UniqueId();
  }
};

}  // namespace std
