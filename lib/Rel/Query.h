// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Rel/Query.h>

#include <string>
#include <unordered_map>
#include <vector>

#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/DisjointSet.h>

namespace hyde {
namespace query {

// Contextual information shared by all queries generated by the same builder.
class QueryContext {
 public:
  QueryContext(void) = default;
  ~QueryContext(void);

  // A cache of the empty query. We may generate empty queries for some
  // clauses.
  std::weak_ptr<QueryImpl> empty_query;

  // The streams associated with generators, i.e. functors that only output
  // values.
  std::unordered_map<ParsedFunctor, Node<QueryGenerator> *>
      decl_to_generator;

  // The streams associated with input relations to queries.
  std::unordered_map<ParsedDeclaration, Node<QueryInput> *>
      decl_to_input;

  // The tables available within any query sharing this context.
  std::unordered_map<ParsedDeclaration, Node<QueryRelation> *>
      decl_to_pos_relation;

  // Negative tables, these are basically "opposites" of normal tables.
  // Selections from negative tables must be involved in a "full" join.
  std::unordered_map<ParsedDeclaration, Node<QueryRelation> *>
      decl_to_neg_relation;

  // String version of the constant's spelling and type, mapped to the constant
  // stream.
  std::unordered_map<std::string, Node<QueryConstant> *> spelling_to_constant;

  // Selects within the same group cannot be merged. A group comes from
  // importing a clause, given an assumption.
  unsigned select_group_id{0};

  // The streams associated with messages and other concrete inputs.
  DefList<Node<QueryInput>> inputs;

  DefList<Node<QueryRelation>> relations;
  DefList<Node<QueryConstant>> constants;
  DefList<Node<QueryGenerator>> generators;
};

}  // namespace query

class EqualitySet;

struct ColumnSet : std::enable_shared_from_this<ColumnSet> {
  ColumnSet(Node<QueryColumn> *self) {
    columns.push_back(self);
  }

  ColumnSet *Find(void);
  Node<QueryColumn> *Leader(void);

  std::shared_ptr<ColumnSet> parent;
  bool is_sorted{true};
  std::vector<Node<QueryColumn> *> columns;

  auto begin(void) -> decltype(columns.begin()) {
    return columns.begin();
  }

  auto end(void) -> decltype(columns.end()) {
    return columns.end();
  }
};

// Represents all values that could inhabit some relation's tuple.
//
// NOTE(pag): Columns derive from `DisjointSet`, which is used during query
//            build time to organize them into equivalence classes. Importantly,
//            the scope of validity of these equivalence classes is per build.
//            Across separate builds, the equivalence classes are all reset.
//            After query builds, they must not be depended upon. Consider the
//            following:
//
//                foo(A) : bar(A), A=1.
//                foo(A) : bar(A), A=2.
//
//            On a per-clause basis, `A` and `1` will end up in the same
//            equivalence class, as will `A` and `2`, but we cannot let those
//            equivalence classes interfere.
template <>
class Node<QueryColumn> : public Def<Node<QueryColumn>> {
 public:
  ~Node(void);

  static constexpr unsigned kInvalidIndex = ~0u;

  inline explicit Node(ParsedVariable var_, Node<QueryView> *view_,
                       unsigned id_, unsigned index_=kInvalidIndex)
      : Def<Node<QueryColumn>>(this),
        var(var_),
        type(var.Type()),
        view(view_),
        equiv_columns(std::make_shared<ColumnSet>(this)),
        id(id_),
        index(index_) {}

  void ReplaceAllUsesWith(Node<QueryColumn> *that);

  Node<QueryColumn> *Find(void);
  static void Union(Node<QueryColumn> *a, Node<QueryColumn> *b);

  // Hash this column.
  uint64_t Hash(void) const noexcept;

  // Returns `true` if this column is a constant.
  bool IsConstant(void) const noexcept;

  // Returns `true` if this column is the output from a generator.
  bool IsGenerator(void) const noexcept;

  // Returns `true` if this column is being used.
  bool IsUsed(void) const noexcept;

  // Basic form of `IsUsed`.
  inline bool IsUsedIgnoreMerges(void) const noexcept {
    return this->Def<Node<QueryColumn>>::IsUsed();
  }

  // Parsed variable associated with this column.
  const ParsedVariable var;

  // Type of the variable; convenient for returning by reference.
  const TypeLoc type;

  // View to which this column belongs.
  Node<QueryView> * const view;

  // Set of columns that are equivalent to this column.
  std::shared_ptr<ColumnSet> equiv_columns;

  // The ID of the column from the SIPS visitor.
  const unsigned id;

  // The index of this column as it relates to `var`s use within a predicate
  // or a comparison.
  //
  // This will have a value of `kInvalidIndex` if we don't have the information.
  //
  // NOTE(pag): All columns published by a join must have indices reflective of
  //            their position in the `columns` definition list. This is used
  //            when establishing join equality.
  unsigned index;
};

using COL = Node<QueryColumn>;

template <>
class Node<QueryRelation> : public Def<Node<QueryRelation>> {
 public:
  inline Node(ParsedDeclaration decl_, bool is_positive_)
      : Def<Node<QueryRelation>>(this),
        decl(decl_),
        is_positive(is_positive_) {}

  const ParsedDeclaration decl;

  // Is this a positive or negative table?
  const bool is_positive;
};

using REL = Node<QueryRelation>;

template <>
class Node<QueryStream> : public Def<Node<QueryStream>> {
 public:
  virtual ~Node(void);

  Node(void)
      : Def<Node<QueryStream>>(this) {}

  virtual Node<QueryConstant> *AsConstant(void) noexcept;
  virtual Node<QueryGenerator> *AsGenerator(void) noexcept;
  virtual Node<QueryInput> *AsInput(void) noexcept;
};

using STREAM = Node<QueryStream>;

template <>
class Node<QueryConstant> final : public Node<QueryStream> {
 public:
  virtual ~Node(void);

  inline Node(ParsedLiteral literal_)
      : literal(literal_) {}

  Node<QueryConstant> *AsConstant(void) noexcept override;

  const ParsedLiteral literal;
};

using CONST = Node<QueryConstant>;

template <>
class Node<QueryGenerator> final : public Node<QueryStream> {
 public:
  virtual ~Node(void);

  inline Node(ParsedFunctor functor_)
      : functor(functor_) {}

  Node<QueryGenerator> *AsGenerator(void) noexcept override;

  const ParsedFunctor functor;
};

using GEN = Node<QueryGenerator>;

template <>
class Node<QueryInput> final : public Node<QueryStream> {
 public:
  virtual ~Node(void);

  inline Node(ParsedDeclaration declaration_)
      : declaration(declaration_) {}

  Node<QueryInput> *AsInput(void) noexcept override;

  const ParsedDeclaration declaration;
};

using INPUT = Node<QueryInput>;

// A view "owns" its the columns pointed to by `columns`.
template <>
class Node<QueryView> : public User, public Def<Node<QueryView>> {
 public:
  virtual ~Node(void);

  Node(void)
      : Def<Node<QueryView>>(this),
        columns(this),
        input_columns(this),
        attached_columns(this) {
    assert(reinterpret_cast<uintptr_t>(static_cast<User *>(this)) ==
           reinterpret_cast<uintptr_t>(this));
  }

  // Returns `true` if we had to "guard" this view with a tuple so that we
  // can put it into canonical form.
  Node<QueryTuple> *GuardWithTuple(QueryImpl *query, bool force=false);

  // Returns `true` if this view is being used.
  bool IsUsed(void) const noexcept;

  // Invoked any time time that any of the columns used by this view are
  // modified.
  void Update(uint64_t) override;

  bool AttachedColumnsAreCanonical(void) const noexcept;

  // Put this view into a canonical form. Returns `true` if changes were made
  // beyond the scope of this view.
  virtual bool Canonicalize(QueryImpl *query);

  virtual Node<QuerySelect> *AsSelect(void) noexcept;
  virtual Node<QueryTuple> *AsTuple(void) noexcept;
  virtual Node<QueryJoin> *AsJoin(void) noexcept;
  virtual Node<QueryMap> *AsMap(void) noexcept;
  virtual Node<QueryAggregate> *AsAggregate(void) noexcept;
  virtual Node<QueryMerge> *AsMerge(void) noexcept;
  virtual Node<QueryConstraint> *AsConstraint(void) noexcept;
  virtual Node<QueryInsert> *AsInsert(void) noexcept;

  // Useful for communicating low-level debug info back to the formatter.
  virtual std::string DebugString(void) const noexcept;

  // Return a number that can be used to help sort this node. The idea here
  // is that we often want to try to merge together two different instances
  // of the same underlying node when we can.
  virtual uint64_t Sort(void) noexcept;

  // Hash this view, or return a cached hash. Useful for things like CSE. This
  // is a structural hash.
  virtual uint64_t Hash(void) noexcept = 0;

  // This is the depth of this node from an input node. This is useful when
  // running optimizations, where we ideally want to apply them bottom-up, i.e.
  // closer to the input nodes, then further away.
  virtual unsigned Depth(void) noexcept;

  // Returns `true` if `this` and `that` are structurally or pointer-input
  // equivalent. Works even if there are cycles in the graph.
  //
  // NOTE(pag): Some nodes use structural equivalence, and others pointer-
  //            equivalence, just to keep things sane.
  virtual bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept = 0;

  // The selected columns.
  DefList<COL> columns;

  // Input dependencies.
  //
  // For `QuerySelect`, these are empty.
  //
  // For `QueryMap`, these are the inputs being mapped. They correspond with
  // the bound columns of the mapping functor. Thus,
  // `input_columns.size() <= columns.size()`.
  //
  // For `QueryJoin`, these are the joined columns. The same joined column
  // map appear one or more times in `input_columns` after optimizations. Thus,
  // `input_columns.size() >= columns.size()`. These is a `output_columns`
  // vector to maintain a relationship between input-to-output columns.
  UseList<COL> input_columns;

  // Attached columns to bring along "lexical context" from their inputs.
  // These are used by MAPs and FILTERs, which need to pull along state from
  // their sources.
  UseList<COL> attached_columns;

  // Selects on within the same group generally cannot be merged. For example,
  // if you had this code:
  //
  //    node_pairs(A, B) : node(A), node(B).
  //
  // Then you don't want to merge the two selects from the `node` relation,
  // because then you won't get the cross-product of nodes, you'll just get
  // the pairs of all nodes.
  //
  // However, if within the same query you have:
  //
  //    node_pairs(A, B) : node(A), node(B).
  //    node_pairs(A, A) : node(A).
  //
  // Then across these two clauses, some selects *can* be merged. We still need
  // to be careful with how we go about merging selects across groups. There is
  // a situation where we can get unlucky and cross merge everything down to
  // some null case that we don't really want. What we need, then, is to
  // maintain which groups a given select is derived from.
  std::vector<unsigned> group_ids;

  // Hash of this node, and its dependencies. A zero value implies that the
  // hash is invalid. Final hashes always have their low 3 bits as a non-zero
  // identifier of the type of the node.
  uint64_t hash{0};

  // Depth from the input node. A zero value is invalid.
  unsigned depth{0U};

  // Is this view in a canonical form? Canonical forms help with doing equality
  // checks and replacements.
  bool is_canonical{false};

  // A different way of tracking usage. Initially, when creating queries,
  // nothing atually uses inserts, and so they will look trivially dead.
  // This exists to make them seem alive, while still allowing CSE to merge
  // INSERTs and mark the old old one as dead.
  bool is_used{false};

  // Should `group_ids` be used to constrain merging? This applies to SELECTs
  // as well as TUPLEs which have been used to replace SELECTs.
  bool check_group_ids{false};

 protected:
  // Utility for depth calculation.
  static unsigned GetDepth(const UseList<COL> &cols, unsigned depth);

  // Utility for comparing use lists.
  static bool ColumnsEq(const UseList<COL> &c1s, const UseList<COL> &c2s);

  // Check that all non-constant views in `cols1` and `cols2` match.
  //
  // NOTE(pag): This isn't a pairwise matching; instead it checks that all
  //            columns in both of the lists independently reference the same
  //            view.
  static bool CheckAllViewsMatch(const UseList<COL> &cols1,
                                 const UseList<COL> &cols2);

  // Check if teh `group_ids` of two views have any overlaps.
  static bool InsertSetsOverlap(Node<QueryView> *a, Node<QueryView> *b);
};

using VIEW = Node<QueryView>;

template <>
class Node<QuerySelect> final : public Node<QueryView> {
 public:
  inline Node(Node<QueryRelation> *relation_, DisplayRange range)
      : position(range.From()),
        relation(relation_->CreateUse(this)) {
    check_group_ids = true;
  }

  inline Node(Node<QueryStream> *stream_, DisplayRange range)
      : position(range.From()),
        stream(stream_->CreateUse(this)) {
    check_group_ids = true;
  }

  virtual ~Node(void);

  Node<QuerySelect> *AsSelect(void) noexcept override;

  uint64_t Sort(void) noexcept override;
  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // The instance of the predicate from which we are selecting.
  DisplayPosition position;

  // The table from which this select takes its columns.
  UseRef<REL> relation;
  UseRef<STREAM> stream;
};

using SELECT = Node<QuerySelect>;

template <>
class Node<QueryTuple> final : public Node<QueryView> {
 public:
  virtual ~Node(void);

  Node<QueryTuple> *AsTuple(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this tuple into a canonical form, which will make comparisons and
  // replacements easier. Because comparisons are mostly pointer-based, the
  // canonical form of this tuple is one where all columns are sorted by
  // their pointer values.
  bool Canonicalize(QueryImpl *query) override;
};

using TUPLE = Node<QueryTuple>;

template <>
class Node<QueryJoin> final : public Node<QueryView> {
 public:
  virtual ~Node(void);

  Node<QueryJoin> *AsJoin(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this join into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query) override;

  void VerifyPivots(void);

  // Maps output columns to input columns.
  std::unordered_map<COL *, UseList<COL>> out_to_in;

  // Used for verification.
  std::vector<VIEW *> pivot_views;
  std::vector<VIEW *> next_pivot_views;

  // Number of pivot columns.
  unsigned num_pivots{0};
};

using JOIN = Node<QueryJoin>;

template <>
class Node<QueryMap> final : public Node<QueryView> {
 public:

  virtual ~Node(void);

  Node<QueryMap> *AsMap(void) noexcept override;

  uint64_t Sort(void) noexcept override;
  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this map into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query) override;

  inline explicit Node(ParsedFunctor functor_, DisplayRange range)
      : position(range.From()),
        functor(functor_) {}

  const DisplayPosition position;
  const ParsedFunctor functor;
};

using MAP = Node<QueryMap>;

template <>
class Node<QueryAggregate> : public Node<QueryView> {
 public:
  inline explicit Node(ParsedFunctor functor_)
      : functor(functor_),
        group_by_columns(this),
        config_columns(this),
        aggregated_columns(this) {}

  virtual ~Node(void);

  Node<QueryAggregate> *AsAggregate(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this aggregate into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query) override;

  // Functor that does the aggregation.
  const ParsedFunctor functor;

  // Columns that are `bound` before the aggregate, used by the relation being
  // summarized, but not being passed to the aggregating functor. These are
  // unordered. These are not visible to the aggregating functor.
  UseList<COL> group_by_columns;

  // Columns that are `bound` for the aggregating functor. These are ordered.
  // We think of this as being a form of grouping, where really they act like
  // "specializations" for the aggregating functor. They kind of "configure" it.
  UseList<COL> config_columns;

  // Columns that are aggregated by this aggregating functor, and will be
  // summarized. These are "in scope" of the aggregation. These are ordered.
  UseList<COL> aggregated_columns;

  // `QueryBuilder::id_to_col`, at the time of building the aggregate, and then
  // after building, representing the state of `id_to_col` for the "scope" of
  // the summarization.
  std::unordered_map<unsigned, std::shared_ptr<ColumnSet>> id_to_col;
};

using AGG = Node<QueryAggregate>;

template <>
class Node<QueryMerge> : public Node<QueryView> {
 public:
  Node(void)
      : merged_views(this) {}

  virtual ~Node(void);

  Node<QueryMerge> *AsMerge(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this merge into a canonical form, which will make comparisons and
  // replacements easier. For example, after optimizations, some of the merged
  // views might be the same.
  bool Canonicalize(QueryImpl *query) override;

  UseList<VIEW> merged_views;
};

using MERGE = Node<QueryMerge>;

template <>
class Node<QueryConstraint> : public Node<QueryView> {
 public:
  Node(ComparisonOperator op_)
      : op(op_) {}

  virtual ~Node(void);

  Node<QueryConstraint> *AsConstraint(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this constraint into a canonical form, which will make comparisons and
  // replacements easier. If this constraint's operator is unordered, then we
  // sort the inputs to make comparisons trivial.
  bool Canonicalize(QueryImpl *query) override;

  const ComparisonOperator op;
};

using CMP = Node<QueryConstraint>;

// Inserts are technically views as that makes some things easier, but they
// are not exposed as such.
template <>
class Node<QueryInsert> : public Node<QueryView> {
 public:
  virtual ~Node(void);

  inline Node(Node<QueryRelation> *relation_, ParsedDeclaration decl_)
      : relation(relation_->CreateUse(this)),
        decl(decl_) {

    // Make all INSERTs initially look used. Replacing one with another will
    // make it go unused.
    is_used = true;
  }

  inline Node(Node<QueryStream> *stream_, ParsedDeclaration decl_)
      : stream(stream_->CreateUse(this)),
        decl(decl_) {

    // Make all INSERTs initially look used. Replacing one with another will
    // make it go unused.
    is_used = true;
  }

  Node<QueryInsert> *AsInsert(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;
  bool Canonicalize(QueryImpl *query) override;

  const UseRef<REL> relation;
  const UseRef<STREAM> stream;
  const ParsedDeclaration decl;
};

using INSERT = Node<QueryInsert>;

class QueryImpl {
 public:
  inline QueryImpl(std::shared_ptr<query::QueryContext> context_)
      : context(context_) {}

  ~QueryImpl(void);

  template <typename CB>
  void ForEachView(CB do_view) {
    for (auto view : joins) {
      do_view(view);
    }
    for (auto view : selects) {
      do_view(view);
    }
    for (auto view : tuples) {
      do_view(view);
    }
    for (auto view : joins) {
      do_view(view);
    }
    for (auto view : maps) {
      do_view(view);
    }
    for (auto view : aggregates) {
      do_view(view);
    }
    for (auto view : merges) {
      do_view(view);
    }
    for (auto view : constraints) {
      do_view(view);
    }
    for (auto view : inserts) {
      do_view(view);
    }
  }

  void Optimize(void);

  void ConnectInsertsToSelects(void);

  const std::shared_ptr<query::QueryContext> context;

  DefList<SELECT> selects;
  DefList<TUPLE> tuples;
  DefList<JOIN> joins;
  DefList<MAP> maps;
  DefList<AGG> aggregates;
  DefList<MERGE> merges;
  DefList<CMP> constraints;
  DefList<INSERT> inserts;

  std::vector<AGG *> pending_aggregates;
};

}  // namespace hyde
