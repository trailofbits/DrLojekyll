// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Rel/Query.h>

#include <string>
#include <unordered_map>
#include <vector>

#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>

namespace hyde {

class EqualitySet;

// Represents all values that could inhabit some relation's tuple.
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
        id(id_),
        index(index_) {}

  void ReplaceAllUsesWith(Node<QueryColumn> *that);

  // Return the index of this column inside of its view.
  unsigned Index(void) noexcept;

  // Hash this column.
  uint64_t Hash(void) noexcept;

  // Return a number that can be used to help sort this node. The idea here
  // is that we often want to try to merge together two different instances
  // of the same underlying node when we can.
  uint64_t Sort(void) noexcept;

  // Returns `true` if this column is a constant.
  bool IsConstant(void) const noexcept;

  // Returns `true` if this column is the output from a generator.
  bool IsGenerator(void) const noexcept;

  // Returns `true` if this column is being used.
  bool IsUsed(void) const noexcept;

  template <typename T>
  void ForEachUser(T user_cb) const;

  // Basic form of `IsUsed`.
  inline bool IsUsedIgnoreMerges(void) const noexcept {
    return this->Def<Node<QueryColumn>>::IsUsed();
  }

  // Parsed variable associated with this column.
  ParsedVariable var;

  // Type of the variable; convenient for returning by reference.
  const TypeLoc type;

  // View to which this column belongs.
  Node<QueryView> * const view;

  // The ID of the column. This roughly corresponds to the smallest
  // `ParsedVariable::Order` value within the clause that was first used to
  // produce this this column. This isn't meaningful except when constructing
  // the query-relational form.
  unsigned id;

  // The index of this column within its view. This will have a value of
  // `kInvalidIndex` if we don't have the information.
  unsigned index;

  // The hash of this column.
  uint64_t hash{0};
};

using COL = Node<QueryColumn>;

// A condition to be tested in order to admit tuples into a relation or
// produce tuples.
template <>
class Node<QueryCondition> : public User, public Def<Node<QueryCondition>> {
 public:
  inline explicit Node(ParsedExport decl_)
      : User(this),
        Def<Node<QueryCondition>>(this),
        declaration(decl_),
        positive_users(this, true  /* is_weak */),
        negative_users(this, true  /* is_weak */),
        setters(this) {}

  inline uint64_t Sort(void) const noexcept {
    return reinterpret_cast<uintptr_t>(this);
  }

  // The declaration of the `ParsedExport` that is associated with this
  // zero-argument predicate.
  const ParsedDeclaration declaration;

  // *WEAK* use list of views using this condition.
  UseList<Node<QueryView>> positive_users;
  UseList<Node<QueryView>> negative_users;

  // Views that produce values for this condition.
  UseList<Node<QueryView>> setters;
};

using COND = Node<QueryCondition>;

// A "table" of data.
template <>
class Node<QueryRelation> : public Def<Node<QueryRelation>> {
 public:
  inline explicit Node(ParsedDeclaration decl_)
      : Def<Node<QueryRelation>>(this),
        declaration(decl_) {}

  const ParsedDeclaration declaration;
};

using REL = Node<QueryRelation>;

// A stream of values.
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

// Use of a constant.
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

// Call to a functor that has no bound parameters.
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

// Input, i.e. a messsage.
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
      : User(this),
        Def<Node<QueryView>>(this),
        columns(this),
        input_columns(this),
        attached_columns(this),
        positive_conditions(this),
        negative_conditions(this) {
    assert(reinterpret_cast<uintptr_t>(static_cast<User *>(this)) ==
           reinterpret_cast<uintptr_t>(this));
  }

  // Returns the kind name, e.g. UNION, JOIN, etc.
  virtual const char *KindName(void) const noexcept = 0;

  // Returns `true` if we had to "guard" this view with a tuple so that we
  // can put it into canonical form.
  Node<QueryTuple> *GuardWithTuple(QueryImpl *query, bool force=false);

  // Returns `true` if this view is being used.
  bool IsUsed(void) const noexcept;

  // Invoked any time time that any of the columns used by this view are
  // modified.
  void Update(uint64_t) override;

  // Sort the `positive_conditions` and `negative_conditions`.
  void OrderConditions(void);

  // Check to see if the attached columns are ordered and unique. If they're
  // not unique then we can deduplicate them.
  bool AttachedColumnsAreCanonical(bool sort) const noexcept;

  // Put this view into a canonical form. Returns `true` if changes were made
  // beyond the scope of this view.
  virtual bool Canonicalize(QueryImpl *query, bool sort);

  virtual Node<QuerySelect> *AsSelect(void) noexcept;
  virtual Node<QueryTuple> *AsTuple(void) noexcept;
  virtual Node<QueryKVIndex> *AsKVIndex(void) noexcept;
  virtual Node<QueryJoin> *AsJoin(void) noexcept;
  virtual Node<QueryMap> *AsMap(void) noexcept;
  virtual Node<QueryAggregate> *AsAggregate(void) noexcept;
  virtual Node<QueryMerge> *AsMerge(void) noexcept;
  virtual Node<QueryConstraint> *AsConstraint(void) noexcept;
  virtual Node<QueryInsert> *AsInsert(void) noexcept;

  // Useful for communicating low-level debug info back to the formatter.
  virtual std::string DebugString(void) noexcept;

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

  // Return the number of uses of this view.
  unsigned NumUses(void) const noexcept;

  // Initializer for an updated hash value.
  uint64_t HashInit(void) const noexcept;

  // Upward facing hash. The idea here is that we sometimes have multiple nodes
  // that have the same hash, and thus are candidates for CSE, and we want to
  // decide: among those candidates, which nodes /should/ be merged. We decide
  // this by looking up the dataflow graph (to some limited depth) and creating
  // a rough hash of how this node gets used.
  uint64_t UpHash(unsigned depth) const noexcept;

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

  // Zero argument predicates that constrain this node.
  UseList<COND> positive_conditions;
  UseList<COND> negative_conditions;

  // Used during canonicalization. Mostly just convenient to have around for
  // re-use of memory.
  std::unordered_map<COL *, COL *> in_to_out;

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

  // The group ID of this node that it will push forward to its dependencies.
  unsigned group_id{0};

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

  // Is this node dead?
  bool is_dead{false};

  // `true` if this view can receive/produce deletions. For example, when an
  // aggregate is updated, the old summary values are produced as a deletion.
  // Similarly, when a kvindex is updated, if the new values differ from the
  // old ones, a deletion record is produced.
  bool can_receive_deletions{false};
  bool can_produce_deletions{false};

  std::string producer;

  // Does this ode break an invariant?
  enum {
    kValid,
    kInvalidBeforeCanonicalize,
    kInvalidAfterCanonicalize
  } valid{kValid};

  // Check that all non-constant views in `cols1` and `cols2` match.
  //
  // NOTE(pag): This isn't a pairwise matching; instead it checks that all
  //            columns in both of the lists independently reference the same
  //            view.
  static bool CheckAllViewsMatch(const UseList<COL> &cols1);
  static bool CheckAllViewsMatch(const UseList<COL> &cols1,
                                 const UseList<COL> &cols2);

 protected:
  // Utilities for depth calculation.
  static unsigned EstimateDepth(const UseList<COL> &cols, unsigned depth);
  static unsigned EstimateDepth(const UseList<COND> &conds, unsigned depth);
  static unsigned GetDepth(const UseList<COL> &cols, unsigned depth);
  static unsigned GetDepth(const UseList<COND> &conds, unsigned depth);

  // Utility for comparing use lists.
  static bool ColumnsEq(EqualitySet &eq, const UseList<COL> &c1s,
                        const UseList<COL> &c2s);

  // Check if teh `group_ids` of two views have any overlaps.
  static bool InsertSetsOverlap(Node<QueryView> *a, Node<QueryView> *b);
};

using VIEW = Node<QueryView>;

template <>
class Node<QuerySelect> final : public Node<QueryView> {
 public:
  inline Node(Node<QueryRelation> *relation_, DisplayRange range)
      : position(range.From()),
        relation(relation_->CreateUse(this)),
        inserts(this) {
    this->can_receive_deletions = 0u < relation->declaration.NumDeletionClauses();
    this->can_produce_deletions = this->can_receive_deletions;
  }

  inline Node(Node<QueryStream> *stream_, DisplayRange range)
      : position(range.From()),
        stream(stream_->CreateUse(this)),
        inserts(this) {
    if (auto input_stream = stream->AsInput(); input_stream) {
      this->can_receive_deletions = 0u < input_stream->declaration.NumDeletionClauses();
      this->can_produce_deletions = this->can_receive_deletions;
    }
  }

  virtual ~Node(void);

  const char *KindName(void) const noexcept override;
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

  // Inserts that might feed this SELECT.
  UseList<Node<QueryView>> inserts;
};

using SELECT = Node<QuerySelect>;

template <>
class Node<QueryTuple> final : public Node<QueryView> {
 public:
  virtual ~Node(void);

  const char *KindName(void) const noexcept override;
  Node<QueryTuple> *AsTuple(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this tuple into a canonical form, which will make comparisons and
  // replacements easier. Because comparisons are mostly pointer-based, the
  // canonical form of this tuple is one where all columns are sorted by
  // their pointer values.
  bool Canonicalize(QueryImpl *query, bool sort) override;
};

using TUPLE = Node<QueryTuple>;

// The KV index will have the `input_columns` as the keys, and the
// `attached_columns` as the values.
template <>
class Node<QueryKVIndex> final : public Node<QueryView> {
 public:
  Node(void)
      : Node<QueryView>() {
    can_produce_deletions = true;
  }

  virtual ~Node(void);

  const char *KindName(void) const noexcept override;
  Node<QueryKVIndex> *AsKVIndex(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put the KV index into a canonical form. The only real internal optimization
  // that will happen is constant propagation of keys, but NOT values (as we can't
  // predict how the merge functors will affect them).
  bool Canonicalize(QueryImpl *query, bool sort) override;

  // Functors that get called to merge old and new values.
  std::vector<ParsedFunctor> merge_functors;
};

using KVINDEX = Node<QueryKVIndex>;

template <>
class Node<QueryJoin> final : public Node<QueryView> {
 public:
  virtual ~Node(void);

  Node(void)
      : joined_views(this) {}

  const char *KindName(void) const noexcept override;
  Node<QueryJoin> *AsJoin(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this join into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query, bool sort) override;

  void VerifyPivots(void);

  // Maps output columns to input columns.
  std::unordered_map<COL *, UseList<COL>> out_to_in;

  // List of views merged by this JOIN. Columns in pivot sets in `out_to_in` are
  // in the same order as they appear in `pivot_views`.
  UseList<VIEW> joined_views;

  // Number of pivot columns. If this value is zero then this is actuall a
  // cross-product.
  unsigned num_pivots{0};
};

using JOIN = Node<QueryJoin>;

template <>
class Node<QueryMap> final : public Node<QueryView> {
 public:

  virtual ~Node(void);

  const char *KindName(void) const noexcept override;
  Node<QueryMap> *AsMap(void) noexcept override;

  uint64_t Sort(void) noexcept override;
  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this map into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query, bool sort) override;

  inline explicit Node(ParsedFunctor functor_, DisplayRange range)
      : position(range.From()),
        functor(functor_) {
    this->can_produce_deletions = !functor.IsPure();
    for (auto param : functor.Parameters()) {
      if (ParameterBinding::kFree == param.Binding()) {
        ++num_free_params;
      }
    }
  }

  const DisplayPosition position;
  const ParsedFunctor functor;

  // Number of `free` parameters in this functor. This distinguishes this map
  // from being a filter/predicate.
  unsigned num_free_params{0};
};

using MAP = Node<QueryMap>;

template <>
class Node<QueryAggregate> : public Node<QueryView> {
 public:
  inline explicit Node(ParsedFunctor functor_)
      : functor(functor_),
        group_by_columns(this),
        config_columns(this),
        aggregated_columns(this) {
    can_produce_deletions = true;
  }

  virtual ~Node(void);

  const char *KindName(void) const noexcept override;
  Node<QueryAggregate> *AsAggregate(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this aggregate into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query, bool sort) override;

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
};

using AGG = Node<QueryAggregate>;

template <>
class Node<QueryMerge> : public Node<QueryView> {
 public:
  Node(void)
      : merged_views(this) {}

  virtual ~Node(void);

  const char *KindName(void) const noexcept override;
  Node<QueryMerge> *AsMerge(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this merge into a canonical form, which will make comparisons and
  // replacements easier. For example, after optimizations, some of the merged
  // views might be the same.
  bool Canonicalize(QueryImpl *query, bool sort) override;

  UseList<VIEW> merged_views;

  // Should all the incoming merged views be treated as an equivalence class?
  // That is, only one of them should actually be used.
  bool is_equivalence_class{false};
};

using MERGE = Node<QueryMerge>;

template <>
class Node<QueryConstraint> : public Node<QueryView> {
 public:
  Node(ComparisonOperator op_)
      : op(op_) {}

  virtual ~Node(void);

  const char *KindName(void) const noexcept override;
  Node<QueryConstraint> *AsConstraint(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this constraint into a canonical form, which will make comparisons and
  // replacements easier. If this constraint's operator is unordered, then we
  // sort the inputs to make comparisons trivial.
  bool Canonicalize(QueryImpl *query, bool sort) override;

  const ComparisonOperator op;
};

using CMP = Node<QueryConstraint>;

// Inserts are technically views as that makes some things easier, but they
// are not exposed as such.
template <>
class Node<QueryInsert> : public Node<QueryView> {
 public:
  virtual ~Node(void);

  inline Node(Node<QueryRelation> *relation_, ParsedDeclaration decl_,
              bool is_insert_=true)
      : relation(relation_->CreateUse(this)),
        declaration(decl_),
        is_insert(is_insert_) {

    // Make all INSERTs initially look used. Replacing one with another will
    // make it go unused.
    is_used = true;
  }

  inline Node(Node<QueryStream> *stream_, ParsedDeclaration decl_,
              bool is_insert_=true)
      : stream(stream_->CreateUse(this)),
        declaration(decl_),
        is_insert(is_insert_) {

    // Make all INSERTs initially look used. Replacing one with another will
    // make it go unused.
    is_used = true;

    if (!is_insert) {
      this->can_produce_deletions = true;
    }
  }

  const char *KindName(void) const noexcept override;
  Node<QueryInsert> *AsInsert(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;
  bool Canonicalize(QueryImpl *query, bool sort) override;

  const UseRef<REL> relation;
  const UseRef<STREAM> stream;
  const ParsedDeclaration declaration;
  const bool is_insert;
};

using INSERT = Node<QueryInsert>;

template <typename T>
void Node<QueryColumn>::ForEachUser(T user_cb) const {
  view->ForEachUse<VIEW>([&user_cb] (VIEW *user, VIEW *) {
    user_cb(user);
  });

  ForEachUse<VIEW>([&user_cb] (VIEW *view, COL *) {
    user_cb(view);
  });
}

class QueryImpl {
 public:
  inline QueryImpl(void) {}

  ~QueryImpl(void);

  template <typename CB>
  void ForEachView(CB do_view) {
    for (auto view : selects) {
      do_view(view);
    }
    for (auto view : tuples) {
      do_view(view);
    }
    for (auto view : kv_indices) {
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

  // Relabel group IDs. This enables us to better optimize SELECTs. Our initial
  // assignment of `group_id`s works well enough to start with, but isn't good
  // enough to help us merge some SELECTs. The key idea is that if a given
  // INSERT reaches two SELECTs, then those SELECTs cannot be merged.
  void RelabelGroupIDs(void);

  // Remove unused views.
  bool RemoveUnusedViews(void);

  void Simplify(void);
  void Canonicalize(void);
  void Optimize(void);

  void ConnectInsertsToSelects(void);

  void TrackDifferentialUpdates(void);

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

  // Mapping between export conditions and actual condition nodes.
  std::unordered_map<ParsedExport, Node<QueryCondition> *> decl_to_condition;

  // Selects within the same group cannot be merged. A group comes from
  // importing a clause, given an assumption.
  unsigned select_group_id{0};

  // The streams associated with messages and other concrete inputs.
  DefList<Node<QueryInput>> inputs;

  DefList<Node<QueryRelation>> relations;
  DefList<Node<QueryConstant>> constants;
  DefList<Node<QueryGenerator>> generators;
  DefList<Node<QueryCondition>> conditions;

  DefList<SELECT> selects;
  DefList<TUPLE> tuples;
  DefList<KVINDEX> kv_indices;
  DefList<JOIN> joins;
  DefList<MAP> maps;
  DefList<AGG> aggregates;
  DefList<MERGE> merges;
  DefList<CMP> constraints;
  DefList<INSERT> inserts;

  std::vector<AGG *> pending_aggregates;
};

}  // namespace hyde
