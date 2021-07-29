// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/Node.h>

#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <unordered_set>

namespace hyde {

class ErrorLog;
class QueryImpl;
//class ColumnTainting;
class OutputStream;

namespace query {

template <typename T>
class QueryNode {
 public:
  inline QueryNode(Node<T> *impl_) : impl(impl_) {}

  inline bool operator==(const QueryNode<T> &that) const noexcept {
    return impl == that.impl;
  }

  inline bool operator!=(const QueryNode<T> &that) const noexcept {
    return impl != that.impl;
  }

  inline bool operator<(const QueryNode<T> &that) const noexcept {
    return impl < that.impl;
  }

  uintptr_t UniqueId(void) const noexcept {
    return reinterpret_cast<uintptr_t>(impl);
  }

 protected:
  friend class ::hyde::QueryImpl;
  //friend class ::hyde::ColumnTainting;

  Node<T> *impl;
};

}  // namespace query

enum class ComparisonOperator : int;
class ParsedDeclaration;
class ParsedFunctor;
class ParsedLiteral;
class ParsedMessage;
class ParsedModule;
class ParsedPredicate;
class ParsedVariable;
class TypeLoc;
class QueryColumn;
class QueryConstant;
class QueryCompare;
class QueryImpl;
class QueryInsert;
class QueryIO;
class QueryJoin;
class QueryMerge;
class QueryNegate;
class QueryRelation;
class QuerySelect;
class QueryStream;
class QueryTag;
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
  bool IsConstant(void) const noexcept;
  bool IsConstantRef(void) const noexcept;
  bool IsConstantOrConstantRef(void) const noexcept;
  bool IsNegate(void) const noexcept;

  std::optional<QueryColumn> AsConstantColumn(void) const noexcept;
  std::optional<ParsedVariable> Variable(void) const noexcept;
  const TypeLoc &Type(void) const noexcept;

  bool operator==(QueryColumn that) const noexcept;
  bool operator!=(QueryColumn that) const noexcept;

  // Number of uses of this column.
  unsigned NumUses(void) const noexcept;

  // Apply a function to each user.
  void ForEachUser(std::function<void(QueryView)> user_cb) const;

  // Unique identifier for columns.
  unsigned Id(void) const noexcept;

#ifndef NDEBUG
  // Comma separated list of all column ids in this columns taint set
  std::string ForwardsTaintIds(void) const;
  std::string BackwardsTaintIds(void) const;
#endif

  std::unordered_set<Node<QueryColumn> *> ForwardsColumnTaints(void) const;
  std::unordered_set<Node<QueryColumn> *> BackwardsColumnTaints(void) const;

  // Index of this column in its defining view. Returns nothing if this column
  // is a constant.
  std::optional<unsigned> Index(void) const noexcept;

 private:
  using query::QueryNode<QueryColumn>::QueryNode;

  friend class QueryConstant;
  friend class QueryCompare;
  friend class QueryInsert;
  friend class QueryJoin;
  friend class QueryMap;
  friend class QueryMerge;
  friend class QueryNegate;
  friend class QueryView;
  friend class QueryAggregate;

  template <typename>
  friend class NodeIterator;

  template <typename>
  friend class Node;
};

// A condition related to a zero-argument predicate that must be tested.
class QueryCondition : public query::QueryNode<QueryCondition> {
 public:
  // The declaration associated with this condition.
  const std::optional<ParsedDeclaration> &Predicate(void) const noexcept;

  // The list of views that produce nodes iff this condition is true.
  UsedNodeRange<QueryView> PositiveUsers(void) const;

  // The list of views that produce nodes iff this condition is false.
  UsedNodeRange<QueryView> NegativeUsers(void) const;

  // The list of views that set or unset this condition.
  UsedNodeRange<QueryView> Setters(void) const;

  // Depth of this node.
  unsigned Depth(void) const noexcept;

 private:
  friend class QueryView;

  using query::QueryNode<QueryCondition>::QueryNode;
};

// A table in a query. Corresponds with a declared predicate in a Datalog.
class QueryRelation : public query::QueryNode<QueryRelation> {
 public:
  static QueryRelation From(const QuerySelect &sel) noexcept;

  const ParsedDeclaration &Declaration(void) const noexcept;

  // The list of inserts into this relation.
  UsedNodeRange<QueryView> Inserts(void) const;

  // The list of SELECTs from this relation.
  UsedNodeRange<QueryView> Selects(void) const;

  // The list of negated uses of this relation.
  UsedNodeRange<QueryView> Negations(void) const;

 private:
  using query::QueryNode<QueryRelation>::QueryNode;

  friend class QuerySelect;
};

// A stream of inputs into the system, or outputs from the system.
class QueryStream : public query::QueryNode<QueryStream> {
 public:
  static QueryStream From(const QuerySelect &sel) noexcept;

  QueryStream(const QueryIO &io) noexcept;
  QueryStream(const QueryConstant &const_) noexcept;

  const char *KindName(void) const noexcept;

  bool IsConstant(void) const noexcept;

  // A special form of constant, auto-generated as a result of optimization.
  bool IsTag(void) const noexcept;

  bool IsIO(void) const noexcept;

 private:
  friend class QueryConstant;
  friend class QueryIO;

  using query::QueryNode<QueryStream>::QueryNode;
};

// A literal in the Datalog code. A literal is a form of non-blocking stream.
class QueryConstant : public query::QueryNode<QueryConstant> {
 public:
  QueryConstant(const QueryTag &tag);

  std::optional<ParsedLiteral> Literal(void) const noexcept;

  static QueryConstant From(const QueryStream &table);
  static QueryConstant From(QueryColumn col);

  // What is the type of this constant?
  TypeLoc Type(void) const noexcept;

  // Returns `true` if this is a tag value.
  bool IsTag(void) const;

 private:
  using query::QueryNode<QueryConstant>::QueryNode;

  friend class QuerySelect;
  friend class QueryStream;
  friend class QueryTag;
  friend class QueryView;
};

// An auto-generate "tag" constant value. These are created during optimization.
class QueryTag : public query::QueryNode<QueryTag> {
 public:
  static QueryTag From(const QueryConstant &const_val);

  // What is the type of this constant? Tags are always unsigned, 16-bit
  // integers.
  TypeLoc Type(void) const noexcept;

  // The value of this tag.
  uint16_t Value(void) const;

 private:
  using query::QueryNode<QueryTag>::QueryNode;

  friend class QueryConstant;
};

// A set of concrete inputs to a query.
class QueryIO : public query::QueryNode<QueryIO> {
 public:
  const ParsedDeclaration &Declaration(void) const noexcept;

  static QueryIO From(const QueryStream &stream);

  // The list of sends to this I/O.
  UsedNodeRange<QueryView> Transmits(void) const;

  // The list of receives of this I/O.
  UsedNodeRange<QueryView> Receives(void) const;

 private:
  using query::QueryNode<QueryIO>::QueryNode;

  friend class QuerySelect;
  friend class QueryStream;
};

class ParsedExport;
class QueryAggregate;
class QueryMap;
class QueryTuple;
class QueryKVIndex;
class QueryInsert;

// NOTE(pag): There is no `kSelected` because `SELECT` / `RECV` nodes have no
//            input columns.
enum class InputColumnRole {

  // The input column is copied to the output column, and it has no additional
  // semantic meaning.
  kCopied,

  // This is a column that is read indirectly from a negated by a negation.
  kNegated,

  // The input column is a pivot column in a join node.
  kJoinPivot,

  // The input column is a non-pivot column in a join node.
  kJoinNonPivot,

  // The input column is on the left-hand side of a binary comparison operator.
  kCompareLHS,

  // The input column is on the right-hand side of a binary comparison operator.
  kCompareRHS,

  // The input column corresponds with a non-`mutable`-attributed parameter of a
  // relation that has at least one `mutable`-attributed parameter. It behaves
  // like a key in a key-value mapping.
  kIndexKey,

  // The input column corresponds to the proposed new value to pass to
  // a merged functor, which corresponds with a `mutable`-attributed parameter
  // of a relation. It behaves like a value in a key-value mapping.
  kIndexValue,

  // The input column corresponds to a `bound`-attributed parameter of
  // a normal functor.
  kFunctorInput,

  // The input column corresponds to a `bound`-attributed parameter of
  // an aggregating functor. It behaves both as a grouping column and as
  // a value which can configure/change the behavior of the aggregating functor.
  kAggregateConfig,

  // The input column is part of the parameter list of the relation over which
  // an aggregating functor is applied. However, this parameter is not itself
  // passed as an argument to the aggregating functor.
  kAggregateGroup,

  // The input column corresponds to a `aggregate`-attributed parameter of
  // an aggregating functor.
  kAggregatedColumn,

  // The input column passes through a merge/union node.
  kMergedColumn,

  // The input column is inserted into a persistent relation.
  kMaterialized,

  // The input column is published into a message.
  kPublished,
};

// A view into a collection of rows. The rows may be derived from a selection
// or a join.
class QueryView : public query::QueryNode<QueryView> {
 public:
  static QueryView Containing(QueryColumn col);

  DefinedNodeRange<QueryColumn> Columns(void) const;

  QueryView(const QueryView &view) noexcept = default;
  QueryView(QueryView &&view) noexcept = default;
  QueryView &operator=(const QueryView &) noexcept = default;
  QueryView &operator=(QueryView &&) noexcept = default;

  QueryView(const QuerySelect &view);
  QueryView(const QueryTuple &view);
  QueryView(const QueryKVIndex &view);
  QueryView(const QueryJoin &view);
  QueryView(const QueryMap &view);
  QueryView(const QueryAggregate &view);
  QueryView(const QueryMerge &view);
  QueryView(const QueryNegate &view);
  QueryView(const QueryCompare &view);
  QueryView(const QueryInsert &view);

  inline static QueryView From(QueryView view) noexcept {
    return view;
  }

  // Returns the `nth` output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  static QueryView From(const QuerySelect &view) noexcept;
  static QueryView From(const QueryTuple &view) noexcept;
  static QueryView From(const QueryKVIndex &view) noexcept;
  static QueryView From(const QueryJoin &view) noexcept;
  static QueryView From(const QueryMap &view) noexcept;
  static QueryView From(const QueryAggregate &view) noexcept;
  static QueryView From(const QueryMerge &view) noexcept;
  static QueryView From(const QueryNegate &view) noexcept;
  static QueryView From(const QueryCompare &view) noexcept;
  static QueryView From(const QueryInsert &view) noexcept;

  const char *KindName(void) const noexcept;

  // These break abstraction layers, as table IDs come from the control-flow
  // IR, but it's nifty for debugging.
  void SetTableId(unsigned id) const noexcept;
  std::optional<unsigned> TableId(void) const noexcept;

  unsigned EquivalenceSetId(void) const noexcept;
  UsedNodeRange<QueryView> EquivalenceSetViews(void) const;

  // Is this view constant after the initialization of the program? This is
  // computed at the end of building the dataflow graph, and helps us optimize
  // JOINs and negations in the control-flow IR by letting us avoid persisting
  // data when that data is non-differential. That is, if non-differential
  // data is flowing through a JOIN, and the stuff against which we're joining
  // is constant after init, then we don't need to save our stuff to a table
  // prior to the join -- we can force it through and dedup it downstream.
  bool IsConstantAfterInitialization(void) const noexcept;

  bool IsSelect(void) const noexcept;
  bool IsTuple(void) const noexcept;
  bool IsKVIndex(void) const noexcept;
  bool IsJoin(void) const noexcept;
  bool IsMap(void) const noexcept;
  bool IsAggregate(void) const noexcept;
  bool IsMerge(void) const noexcept;
  bool IsNegate(void) const noexcept;
  bool IsCompare(void) const noexcept;
  bool IsInsert(void) const noexcept;

  // Returns `true` if this node is used by a `QueryNegate`.
  bool IsUsedByNegation(void) const noexcept;

  // Returns `true` if this node is used by a `QueryJoin`.
  bool IsUsedByJoin(void) const noexcept;

  // Returns `true` if this node is used by a `QueryMerge`.
  bool IsUsedByMerge(void) const noexcept;

  // Apply a callback `on_negate` to each negation using this view.
  void ForEachNegation(std::function<void(QueryNegate)> on_negate) const;

  // Can this view receive inputs that should logically "delete" entries?
  //
  // NOTE(pag): Not being able to receive deletions does not imply that a
  //            view can't produce deletions.
  bool CanReceiveDeletions(void) const noexcept;

  // Can this view produce outputs that should logically "delete" entries?
  //
  // NOTE(pag): Some views can produce deletions without receiving them. These
  //            include aggregates, key/value indices, and any view that tests
  //            condition variables.
  bool CanProduceDeletions(void) const noexcept;

  // Returns `true` if all users of this view use all the columns of this
  // view.
  bool AllUsersUseAllColumns(void) const noexcept;

  // Returns `true` if this view has a single predecessor, and if all of the
  // columns of that predecessor are used.
  bool AllColumnsOfSinglePredecessorAreUsed(void) const noexcept;

  // Returns the depth of this node in the graph. This is defined as depth
  // from an input (associated with a message receive) node, where the deepest
  // nodes are typically responses to queries, or message publications.
  unsigned Depth(void) const noexcept;

  // Color value for formatting. This is influenced by the `@highlight`
  // pragma, for example:
  //
  //      predicate(...) @highlight : body0(...), ..., bodyN(...).
  //
  // The color itself is mostly randomly generated, and best effort is applied
  // to maintain the coloring through optimzation. In some cases, new colors
  // are invented, e.g. when merging nodes when doing common sub-expression
  // elimination. In other cases, the color may be lost.
  unsigned Color(void) const noexcept;

  // Returns a useful string of internal metadata about this view.
  OutputStream &DebugString(OutputStream &) const noexcept;

  // Get a hash of this view.
  uint64_t Hash(void) const noexcept;

  // What condition this view sets, if any.
  std::optional<QueryCondition> SetCondition(void) const noexcept;

  // Conditions, i.e. zero-argument predicates, that must be true (or false)
  // for tuples to be accepted into this node.
  UsedNodeRange<QueryCondition> PositiveConditions(void) const noexcept;
  UsedNodeRange<QueryCondition> NegativeConditions(void) const noexcept;

  // Successor and predecessor views of this view.
  UsedNodeRange<QueryView> Successors(void) const noexcept;
  UsedNodeRange<QueryView> Predecessors(void) const noexcept;

  // Apply a callback `with_user` to each view that uses the columns of this
  // view.
  void ForEachUser(std::function<void(QueryView)> with_user) const;

  // Apply a callback `with_col` to each input column of this view.
  //
  // NOTE(pag): This does not provide any guarantees on column visiting order
  //            and one should assume the worst-case order.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

  // A unique integer that labels all UNIONs in the same induction.
  std::optional<unsigned> InductionGroupId(void) const;

  // A total ordering on the "depth" of inductions. Two inductions at the same
  // depth can be processed in parallel.
  std::optional<unsigned> InductionDepth(void) const;

  UsedNodeRange<QueryView> InductiveSuccessors(void) const;
  UsedNodeRange<QueryView> InductivePredecessors(void) const;

  UsedNodeRange<QueryView> NonInductiveSuccessors(void) const;
  UsedNodeRange<QueryView> NonInductivePredecessors(void) const;

  // All UNIONs, including this one, in the same inductive set.
  UsedNodeRange<QueryView> InductiveSet(void) const;

  // Can this view reach back to itself without first going through another
  // inductive union?
  bool IsOwnIndirectInductiveSuccessor(void) const;

 private:
  using query::QueryNode<QueryView>::QueryNode;
};

// A selection of all columns from a table.
class QuerySelect : public query::QueryNode<QuerySelect> {
 public:
  // The selected columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  static QuerySelect From(QueryView view);

  bool IsRelation(void) const noexcept;
  bool IsStream(void) const noexcept;

  QueryRelation Relation(void) const noexcept;
  QueryStream Stream(void) const noexcept;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  //
  // NOTE(pag): This will only call `with_col` if there is a corresponding
  //            `INSERT` on the underlying relation.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  friend class QueryRelation;
  friend class QueryStream;
  friend class QueryView;

  using query::QueryNode<QuerySelect>::QueryNode;
};

// A join of two or more tables on one or more columns.
class QueryJoin : public query::QueryNode<QueryJoin> {
 public:
  static QueryJoin From(QueryView view);

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

  // The number of views joined together.
  unsigned NumJoinedViews(void) const noexcept;

  // Return a list of the joined views.
  UsedNodeRange<QueryView> JoinedViews(void) const noexcept;

  // Returns the `nth` pivot output column.
  QueryColumn NthOutputPivotColumn(unsigned n) const noexcept;

  // Returns the input columns corresponding with the Nth output pivot column.
  // All of the input columns must have matching values in order for the
  // JOIN to succeed.
  UsedNodeRange<QueryColumn> NthInputPivotSet(unsigned n) const noexcept;

  // Returns the `nth` joined output column. This column is not a pivot.
  QueryColumn NthOutputMergedColumn(unsigned n) const noexcept;

  // Returns the input column corresponding to the `n`th output column, where
  // this input column is not itself assocated with a pivot set.
  QueryColumn NthInputMergedColumn(unsigned n) const noexcept;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  using query::QueryNode<QueryJoin>::QueryNode;

  friend class QueryView;
};

// Map input to zero or more outputs. Maps correspond to non-aggregating
// functors with at least
class QueryMap : public query::QueryNode<QueryMap> {
 public:
  static QueryMap From(QueryView view);

  unsigned NumInputColumns(void) const noexcept;
  QueryColumn NthInputColumn(unsigned n) const noexcept;
  UsedNodeRange<QueryColumn> InputColumns(void) const noexcept;

  // All output columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  // The resulting mapped columns. This does not include copied columns.
  DefinedNodeRange<QueryColumn> MappedColumns(void) const;

  // The resulting copied columns.
  DefinedNodeRange<QueryColumn> CopiedColumns(void) const;

  // Returns the number of output columns.
  unsigned Arity(void) const noexcept;

  // Returns whether or not this map behaves more like a filter, i.e. if the
  // number of `free`-attributed parameters in `Functor()` is zero.
  bool IsFilterLike(void) const noexcept;

  // Returns the `nth` output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  const ParsedFunctor &Functor(void) const noexcept;

  // Is this a positive application of the functor, or a negative application?
  // The meaning of a negative application is that it produces zero outputs. It
  // is invalid to negate a functor that is declared as
  bool IsPositive(void) const noexcept;

  // Returns the number of columns copied along from source views.
  unsigned NumCopiedColumns(void) const noexcept;

  // Returns the `nth` output copied column.
  QueryColumn NthCopiedColumn(unsigned n) const noexcept;

  // Returns the `nth` input copied column.
  QueryColumn NthInputCopiedColumn(unsigned n) const noexcept;

  // The range of input group columns.
  UsedNodeRange<QueryColumn> InputCopiedColumns(void) const;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  using query::QueryNode<QueryMap>::QueryNode;

  friend class QueryView;
};

// An aggregate operation.
class QueryAggregate : public query::QueryNode<QueryAggregate> {
 public:
  static QueryAggregate From(QueryView view);

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
  unsigned NumConfigurationColumns(void) const noexcept;

  // Returns the number of columns being aggregates.
  unsigned NumAggregateColumns(void) const noexcept;

  // Returns the number of sumary columns being produced.
  unsigned NumSummaryColumns(void) const noexcept;

  // Returns the `nth` output grouping column.
  QueryColumn NthGroupColumn(unsigned n) const noexcept;

  // Returns the `nth` output config column.
  QueryColumn NthConfigurationColumn(unsigned n) const noexcept;

  // Returns the `nth` output summarized column.
  QueryColumn NthSummaryColumn(unsigned n) const noexcept;

  // Returns the `nth` input grouping column.
  QueryColumn NthInputGroupColumn(unsigned n) const noexcept;

  // Returns the `nth` input config column.
  QueryColumn NthInputConfigurationColumn(unsigned n) const noexcept;

  // Returns the `nth` input summarized column.
  QueryColumn NthInputAggregateColumn(unsigned n) const noexcept;

  UsedNodeRange<QueryColumn> InputGroupColumns(void) const noexcept;
  UsedNodeRange<QueryColumn> InputConfigurationColumns(void) const noexcept;
  UsedNodeRange<QueryColumn> InputAggregatedColumns(void) const noexcept;

  // The functor doing the aggregating.
  const ParsedFunctor &Functor(void) const noexcept;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  using query::QueryNode<QueryAggregate>::QueryNode;

  friend class QueryView;
};

// A merge between two or more views of the same arity, where the columns have
// the same types.
class QueryMerge : public query::QueryNode<QueryMerge> {
 public:
  static QueryMerge From(QueryView view);

  // The resulting mapped columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  // Returns the number of output columns.
  unsigned Arity(void) const noexcept;

  // Returns the `nth` output column.
  QueryColumn NthColumn(unsigned n) const noexcept;

  // Number of views that are merged together at this point.
  unsigned NumMergedViews(void) const noexcept;

  // Nth view that is merged together at this point.
  QueryView NthMergedView(unsigned n) const noexcept;

  // Range of views unioned together by this MERGE.
  UsedNodeRange<QueryView> MergedViews(void) const;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

  bool CanReceiveDeletions(void) const;
  bool CanProduceDeletions(void) const;

 private:
  using query::QueryNode<QueryMerge>::QueryNode;

  friend class QueryView;
};

// A constraint between two columns. The constraint results in either one
// (in the case of equality) or two (inequality) output columns. The constraint
// also passes through the other columns from the view.
class QueryCompare : public query::QueryNode<QueryCompare> {
 public:
  static QueryCompare From(QueryView view);

  ComparisonOperator Operator(void) const;
  QueryColumn LHS(void) const;
  QueryColumn RHS(void) const;

  QueryColumn InputLHS(void) const;
  QueryColumn InputRHS(void) const;

  unsigned NumCopiedColumns(void) const noexcept;
  QueryColumn NthCopiedColumn(unsigned n) const noexcept;

  DefinedNodeRange<QueryColumn> CopiedColumns(void) const;
  UsedNodeRange<QueryColumn> InputCopiedColumns(void) const;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  using query::QueryNode<QueryCompare>::QueryNode;

  friend class QueryView;
};

// A test for the absence of a specific tuple in a relation.
class QueryNegate : public query::QueryNode<QueryNegate> {
 public:
  static QueryNegate From(QueryView view);

  DefinedNodeRange<QueryColumn> Columns(void) const;
  QueryColumn NthColumn(unsigned n) const noexcept;

  // If a negation has a never hint, then we know that if some data goes through
  // the output, then it will always go through, and nothing will get set in
  // the negated view that will result in the prior data being retracted.
  bool HasNeverHint(void) const noexcept;

  // The resulting copied columns.
  DefinedNodeRange<QueryColumn> CopiedColumns(void) const;

  DefinedNodeRange<QueryColumn> NegatedColumns(void) const;

  unsigned NumCopiedColumns(void) const noexcept;

  // Returns the `nth` input copied column.
  QueryColumn NthInputCopiedColumn(unsigned n) const noexcept;

  unsigned NumInputColumns(void) const noexcept;
  QueryColumn NthInputColumn(unsigned n) const noexcept;

  UsedNodeRange<QueryColumn> InputColumns(void) const noexcept;
  UsedNodeRange<QueryColumn> InputCopiedColumns(void) const noexcept;

  // Incoming view that represents a flow of data between the relation and
  // the negation.
  QueryView NegatedView(void) const noexcept;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  using query::QueryNode<QueryNegate>::QueryNode;

  friend class QueryView;
  friend class QueryRelation;
};

// An insert of one or more columns into a relation.
class QueryInsert : public query::QueryNode<QueryInsert> {
 public:
  static QueryInsert From(QueryView view);

  ParsedDeclaration Declaration(void) const noexcept;

  bool IsRelation(void) const noexcept;
  bool IsStream(void) const noexcept;

  QueryRelation Relation(void) const noexcept;
  QueryStream Stream(void) const noexcept;

  unsigned NumInputColumns(void) const noexcept;
  QueryColumn NthInputColumn(unsigned n) const noexcept;
  UsedNodeRange<QueryColumn> InputColumns(void) const noexcept;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  using query::QueryNode<QueryInsert>::QueryNode;

  friend class QueryView;
};

// An tuple packages one or more columns into a temporary relation for
// convenience.
class QueryTuple : public query::QueryNode<QueryTuple> {
 public:
  static QueryTuple From(QueryView view);

  // The resulting mapped columns.
  DefinedNodeRange<QueryColumn> Columns(void) const;

  unsigned Arity(void) const noexcept;
  QueryColumn NthColumn(unsigned n) const noexcept;

  unsigned NumInputColumns(void) const noexcept;
  QueryColumn NthInputColumn(unsigned n) const noexcept;
  UsedNodeRange<QueryColumn> InputColumns(void) const noexcept;

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  using query::QueryNode<QueryTuple>::QueryNode;

  friend class QueryView;
};

// A key-value index is similar to a tuple, except that some of the columns
// are mutable.
class QueryKVIndex : public query::QueryNode<QueryKVIndex> {
 public:
  static QueryKVIndex From(QueryView view);

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

  OutputStream &DebugString(OutputStream &) const noexcept;

  // Apply a callback `with_col` to each input column of this view.
  void ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                     std::optional<QueryColumn> /* out_col */)>
                      with_col) const;

 private:
  using query::QueryNode<QueryKVIndex>::QueryNode;

  friend class QueryView;
};

// A query.
class Query {
 public:
  // Build and return a new query.
  static std::optional<Query> Build(const ParsedModule &module,
                                    const ErrorLog &log);

  ~Query(void);

  ::hyde::ParsedModule ParsedModule(void) const noexcept;

  DefinedNodeRange<QueryCondition> Conditions(void) const;
  DefinedNodeRange<QueryJoin> Joins(void) const;
  DefinedNodeRange<QuerySelect> Selects(void) const;
  DefinedNodeRange<QueryTuple> Tuples(void) const;
  DefinedNodeRange<QueryKVIndex> KVIndices(void) const;
  DefinedNodeRange<QueryRelation> Relations(void) const;
  DefinedNodeRange<QueryInsert> Inserts(void) const;
  DefinedNodeRange<QueryNegate> Negations(void) const;
  DefinedNodeRange<QueryMap> Maps(void) const;
  DefinedNodeRange<QueryAggregate> Aggregates(void) const;
  DefinedNodeRange<QueryMerge> Merges(void) const;
  DefinedNodeRange<QueryCompare> Compares(void) const;
  DefinedNodeRange<QueryIO> IOs(void) const;
  DefinedNodeRange<QueryConstant> Constants(void) const;
  DefinedNodeRange<QueryTag> Tags(void) const;

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

    for (auto view : Negations()) {
      cb(QueryView::From(view));
    }

    for (auto view : Compares()) {
      cb(QueryView::From(view));
    }

    for (auto view : Inserts()) {
      cb(QueryView::From(view));
    }
  }

  Query(const Query &) = default;
  Query(Query &&) noexcept = default;
  Query &operator=(const Query &) = default;
  Query &operator=(Query &&) noexcept = default;

 private:
  inline explicit Query(std::shared_ptr<QueryImpl> impl_) : impl(impl_) {}

  std::shared_ptr<QueryImpl> impl;
};

}  // namespace hyde
namespace std {

template <>
struct hash<::hyde::QueryConstant> {
  using argument_type = ::hyde::QueryConstant;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::QueryConstant col) const noexcept {
    return col.UniqueId();
  }
};

template <>
struct hash<::hyde::QueryColumn> {
  using argument_type = ::hyde::QueryColumn;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::QueryColumn col) const noexcept {
    return col.UniqueId();
  }
};

template <>
struct hash<::hyde::QueryView> {
  using argument_type = ::hyde::QueryView;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::QueryView view) const noexcept {
    return view.UniqueId();
  }
};

template <>
struct hash<::hyde::QueryCondition> {
  using argument_type = ::hyde::QueryCondition;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::QueryCondition cond) const noexcept {
    return cond.UniqueId();
  }
};

}  // namespace std
