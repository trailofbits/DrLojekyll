// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/DataFlow/Query.h>

#include <cassert>
#include <string>
#include <unordered_map>
#include <vector>

#include <drlojekyll/Util/BitManipulation.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>

namespace hyde {

class EqualitySet;
class ErrorLog;
class OptimizationContext;

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

  void CopyConstant(Node<QueryColumn> *maybe_const_col);

  void ReplaceAllUsesWith(Node<QueryColumn> *that);

  // Return the index of this column inside of its view.
  unsigned Index(void) noexcept;

  // Hash this column.
  uint64_t Hash(void) noexcept;

  // Return a number that can be used to help sort this node. The idea here
  // is that we often want to try to merge together two different instances
  // of the same underlying node when we can.
  uint64_t Sort(void) noexcept;

  // Returns the real constant associated with this column if this column is
  // a constant or constant reference. Otherwise it returns `nullptr`.
  Node<QueryColumn> *AsConstant(void) noexcept;

  // Returns `true` if will have a constant value at runtime.
  bool IsConstantRef(void) const noexcept;

  // Returns `true` if this column is a constant or a reference to a constant.
  bool IsConstantOrConstantRef(void) const noexcept;

  // Returns `true` if this column is definitely a constant and not just a
  // reference to one.
  bool IsConstant(void) const noexcept;

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

  // Reference to a use of a real constant. We need this indirection because
  // we depend on dataflow to sometimes encode control dependencies, but if we
  // just blindly propagated constants around, then there are situations where
  // we could actually lose the control aspects needed by the data dependencies.
  // The `conflicting_constants.dr` example is an example that requires data
  // and control dependencies to forced together.
  UseRef<Node<QueryColumn>> referenced_constant;

  // The ID of the column. During building of a dataflow, this roughly
  // corresponds to the smallest `ParsedVariable::Order` value within the
  // clause that was first used to produce this this column.
  //
  // After optimizing a dataflow, we replace all ID values
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
class Node<QueryCondition> : public Def<Node<QueryCondition>>, public User {
 public:
  ~Node(void);

  // An anonymous, not-user-defined condition that is instead inferred based
  // off of optmizations.
  inline Node(void)
      : Def<Node<QueryCondition>>(this),
        User(this),
        positive_users(this, true  /* is_weak */),
        negative_users(this, true  /* is_weak */),
        setters(this, true  /* is_weak */) {}

  // An explicit, user-defined condition. Usually associated with there-exists
  // checks or configuration options.
  inline explicit Node(ParsedExport decl_)
      : Def<Node<QueryCondition>>(this),
        User(this),
        declaration(decl_),
        positive_users(this, true  /* is_weak */),
        negative_users(this, true  /* is_weak */),
        setters(this, true  /* is_weak */) {}

  inline uint64_t Sort(void) const noexcept {
    return declaration ? declaration->Id() : reinterpret_cast<uintptr_t>(this);
  }

  // The declaration of the `ParsedExport` that is associated with this
  // zero-argument predicate.
  const std::optional<ParsedDeclaration> declaration;

  // *WEAK* use list of views using this condition.
  UseList<Node<QueryView>> positive_users;
  UseList<Node<QueryView>> negative_users;

  // *WEAK* use list of views that produce values for this condition.
  //
  // TODO(pag): Consider making this not be a weak use list.
  UseList<Node<QueryView>> setters;
};

using COND = Node<QueryCondition>;

// A "table" of data.
template <>
class Node<QueryRelation> : public Def<Node<QueryRelation>>, public User {
 public:
  inline explicit Node(ParsedDeclaration decl_)
      : Def<Node<QueryRelation>>(this),
        User(this),
        declaration(decl_),
        inserts(this),
        selects(this) {}

  const ParsedDeclaration declaration;

  // List of nodes that insert data into this relation.
  UseList<Node<QueryView>> inserts;

  // List of nodes that select data from this relation.
  UseList<Node<QueryView>> selects;
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
  virtual Node<QueryIO> *AsIO(void) noexcept;
  virtual const char *KindName(void) const noexcept = 0;
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
  const char *KindName(void) const noexcept override;

  const ParsedLiteral literal;
};

using CONST = Node<QueryConstant>;

// Input, i.e. a messsage.
template <>
class Node<QueryIO> final : public Node<QueryStream>, public User{
 public:
  virtual ~Node(void);

  inline Node(ParsedDeclaration declaration_)
      : User(this),
        declaration(declaration_),
        sends(this),
        receives(this) {}

  Node<QueryIO> *AsIO(void) noexcept override;
  const char *KindName(void) const noexcept override;

  const ParsedDeclaration declaration;

  // List of nodes that send data to this I/O operation.
  UseList<Node<QueryView>> sends;

  // List of nodes that receive data from this I/O operation.
  UseList<Node<QueryView>> receives;
};

using IO = Node<QueryIO>;

// A view "owns" its the columns pointed to by `columns`.
template <>
class Node<QueryView> : public Def<Node<QueryView>>, public User {
 public:
  virtual ~Node(void);

  Node(void)
      : Def<Node<QueryView>>(this),
        User(this),
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

  // Prepare to delete this node. This tries to drop all dependencies and
  // unlink this node from the dataflow graph. It returns `true` if successful
  // and `false` if it has already been performed.
  bool PrepareToDelete(void);

  // Copy all positive and negative conditions from `this` into `that`.
  void CopyTestedConditionsTo(Node<QueryView> *that);

  // If `sets_condition` is non-null, then transfer the setter to `that`.
  void TransferSetConditionTo(Node<QueryView> *that);

  // Replace all uses of `this` with `that`. The semantic here is that `this`
  // remains valid and used.
  void SubstituteAllUsesWith(Node<QueryView> *that);

  // Replace all uses of `this` with `that`. The semantic here is that `this`
  // is completely subsumed/replaced by `that`.
  void ReplaceAllUsesWith(Node<QueryView> *that);

  // Does this view introduce a control dependency? If a node introduces a
  // control dependency then it generally needs to be kept around.
  bool IntroducesControlDependency(void) const noexcept;

  // Returns `true` if all output columns are used.
  bool AllColumnsAreUsed(void) const noexcept;

  // Returns `true` if we had to "guard" this view with a tuple so that we
  // can put it into canonical form.
  Node<QueryTuple> *GuardWithTuple(QueryImpl *query, bool force=false);

  // Proxy this node with a comparison of `lhs_col` and `rhs_col`, where
  // `lhs_col` and `rhs_col` either belong to `this->columns` or are constants.
  Node<QueryTuple> *ProxyWithComparison(QueryImpl *query, ComparisonOperator op,
                                        COL *lhs_col, COL *rhs_col);

  // Returns `true` if this view is being used.
  bool IsUsed(void) const noexcept;

  // Invoked any time time that any of the columns used by this view are
  // modified.
  void Update(uint64_t) override;

  // Sort the `positive_conditions` and `negative_conditions`.
  void OrderConditions(void);

  // Check to see if the attached columns are ordered and unique. If they're
  // not unique then we can deduplicate them.
  std::pair<bool, bool> CanonicalizeAttachedColumns(
      unsigned i, const OptimizationContext &opt) noexcept;

  // Canonicalizes an input/output column pair. Returns `true` in the first
  // element if non-local changes are made, and `true` in the second element
  // if the column pair can be removed.
  std::pair<bool, bool> CanonicalizeColumnPair(
      COL *in_col, COL *out_col, const OptimizationContext &opt) noexcept;

  // Put this view into a canonical form. Returns `true` if changes were made
  // beyond the scope of this view.
  virtual bool Canonicalize(QueryImpl *query, const OptimizationContext &opt);

  virtual Node<QuerySelect> *AsSelect(void) noexcept;
  virtual Node<QueryTuple> *AsTuple(void) noexcept;
  virtual Node<QueryKVIndex> *AsKVIndex(void) noexcept;
  virtual Node<QueryJoin> *AsJoin(void) noexcept;
  virtual Node<QueryMap> *AsMap(void) noexcept;
  virtual Node<QueryAggregate> *AsAggregate(void) noexcept;
  virtual Node<QueryMerge> *AsMerge(void) noexcept;
  virtual Node<QueryCompare> *AsCompare(void) noexcept;
  virtual Node<QueryInsert> *AsInsert(void) noexcept;

  // Useful for communicating low-level debug info back to the formatter.
  virtual OutputStream &DebugString(OutputStream &os) noexcept;

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

  // If this VIEW sets a CONDition, then keep track of that here.
  WeakUseRef<COND> sets_condition;

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
  unsigned group_id{0u};

  // Hash of this node, and its dependencies. A zero value implies that the
  // hash is invalid. We use this for JOIN merging during early dataflow
  // building. This is a good hint for CSE when the data flow is acyclic.
  uint64_t hash{0u};

  // Depth from the input node. A zero value is invalid.
  unsigned depth{0U};

  // Is this view in a canonical form? Canonical forms help with doing equality
  // checks and replacements. In practice, "canonical form" lost its meaning
  // over time as it used to be based on pointer ordering of columns, which
  // was fine when the CSE optimization was pointer comparison-based, but
  // eventually lost its original meaning when CSE switched to structural
  // equivalence, and early/aggressive constant propagation and dead column
  // elimination was scaled back. Now it mostly acts as a marker for whether or
  // not this node may have changed and needs to be re-inspected.
  bool is_canonical{false};

  // Is this node dead?
  bool is_dead{false};

  // `true` if this view can receive/produce deletions. For example, when an
  // aggregate is updated, the old summary values are produced as a deletion.
  // Similarly, when a kvindex is updated, if the new values differ from the
  // old ones, a deletion record is produced.
  bool can_receive_deletions{false};
  bool can_produce_deletions{false};

  // Debug string roughly tracking how or why this node was created.
  std::string producer;

  // Does this code break an invariant?
  enum {
    kValid,
    kInvalidBeforeCanonicalize,
    kInvalidAfterCanonicalize
  } valid{kValid};

  // If we broke an invariant, then highlight the variable name of the column
  // for which we broke the invariant.
  mutable std::optional<ParsedVariable> invalid_var;

  // Check that all non-constant views in `cols1` and `cols2` match.
  //
  // NOTE(pag): This isn't a pairwise matching; instead it checks that all
  //            columns in both of the lists independently reference the same
  //            view.
  bool CheckIncomingViewsMatch(const UseList<COL> &cols1) const;
  bool CheckIncomingViewsMatch(const UseList<COL> &cols1,
                                const UseList<COL> &cols2) const;

  // Figure out what the incoming view to `cols1` is.
  static Node<QueryView> *GetIncomingView(const UseList<COL> &cols1);
  static Node<QueryView> *GetIncomingView(const UseList<COL> &cols1,
                                          const UseList<COL> &cols2);

  // Returns a pointer to the only user of this node, or nullptr if there are
  // zero users, or more than one users.
  Node<QueryView> *OnlyUser(void) const noexcept;

  // Create or inherit a condition created on `view`.
  static COND *CreateOrInheritConditionOnView(
      QueryImpl *query, Node<QueryView> *view, UseList<COL> cols);

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
        relation(this, relation_),
        inserts(this, true  /* is_weak */) {
    this->can_receive_deletions = 0u < relation->declaration.NumDeletionClauses();
    this->can_produce_deletions = this->can_receive_deletions;
  }

  inline Node(Node<QueryStream> *stream_, DisplayRange range)
      : position(range.From()),
        stream(this, stream_),
        inserts(this, true  /* is_weak */) {
    if (auto input_stream = stream->AsIO(); input_stream) {
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

  // Put this view into a canonical form. Returns `true` if changes were made
  // beyond the scope of this view.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;

  // The instance of the predicate from which we are selecting.
  DisplayPosition position;

  // The table from which this select takes its columns.
  WeakUseRef<REL> relation;
  WeakUseRef<STREAM> stream;

  // *WEAK* list that might feed this SELECT.
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
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;
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
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;

  // Functors that get called to merge old and new values.
  std::vector<ParsedFunctor> merge_functors;
};

using KVINDEX = Node<QueryKVIndex>;

template <>
class Node<QueryJoin> final : public Node<QueryView> {
 public:
  virtual ~Node(void);

  Node(void)
      : joined_views(this, true  /* is_weak */) {}

  const char *KindName(void) const noexcept override;
  Node<QueryJoin> *AsJoin(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Update the set of joined views.
  void UpdateJoinedViews(QueryImpl *query);

  // Put this join into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;

  // If we have a constant feeding into one of the pivot sets, then we want to
  // eliminate that pivot set and instead propagate CMP nodes to all pivot
  // sources.
  void PropagateConstAcrossPivotSet(QueryImpl *query, COL *col,
                                    UseList<COL> &pivot_cols);

  // Replace the pivot column `pivot_col` with `const_col`.
  void ReplacePivotWithConstant(QueryImpl *query, COL *pivot_col,
                                COL *const_col);

  // Replace `view`, which should be a member of `joined_views` with
  // `replacement_view` in this JOIN.
  void ReplaceViewInJoin(VIEW *view, VIEW *replacement_view);

  // Maps output columns to input columns.
  std::unordered_map<COL *, UseList<COL>> out_to_in;

  // List of all inputs before the main canonicalization code runs. Used to keep
  // track of which columns are optimized away, so that we can find them again
  // if we end up being able to discard a JOINed view.
  std::vector<COL *> prev_input_columns;

  // List of views merged by this JOIN. Columns in pivot sets in `out_to_in` are
  // in the same order as they appear in `pivot_views`.
  //
  // TODO(pag): I don't think the ordering invariant is maintained through
  //            canonicalization.
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
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;

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
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;

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
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;

  // The views that are being merged together.
  UseList<VIEW> merged_views;

  // Should all the incoming merged views be treated as an equivalence class?
  // That is, only one of them should actually be used.
  bool is_equivalence_class{false};
};

using MERGE = Node<QueryMerge>;

template <>
class Node<QueryCompare> : public Node<QueryView> {
 public:
  Node(ComparisonOperator op_)
      : op(op_) {}

  virtual ~Node(void);

  const char *KindName(void) const noexcept override;
  Node<QueryCompare> *AsCompare(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;

  // Put this constraint into a canonical form, which will make comparisons and
  // replacements easier. If this constraint's operator is unordered, then we
  // sort the inputs to make comparisons trivial.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;

  const ComparisonOperator op;

  // Spelling range of the comparison that produced this. May not be valid.
  DisplayRange spelling_range;
};

using CMP = Node<QueryCompare>;

// Inserts are technically views as that makes some things easier, but they
// are not exposed as such.
template <>
class Node<QueryInsert> : public Node<QueryView> {
 public:
  virtual ~Node(void);

  inline Node(Node<QueryRelation> *relation_, ParsedDeclaration decl_,
              bool is_insert_=true)
      : relation(this, relation_),
        declaration(decl_),
        is_insert(is_insert_) {

    if (!is_insert) {
      this->can_produce_deletions = true;
    }
  }

  inline Node(Node<QueryStream> *stream_, ParsedDeclaration decl_,
              bool is_insert_=true)
      : stream(this, stream_),
        declaration(decl_),
        is_insert(is_insert_) {

    if (!is_insert) {
      this->can_produce_deletions = true;
    }
  }

  const char *KindName(void) const noexcept override;
  Node<QueryInsert> *AsInsert(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, Node<QueryView> *that) noexcept override;
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt) override;

  WeakUseRef<REL> relation;
  WeakUseRef<STREAM> stream;
  const ParsedDeclaration declaration;
  bool is_insert;
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
    std::vector<VIEW *> views;
    for (auto view : selects) {
      views.push_back(view);
    }
    for (auto view : tuples) {
      views.push_back(view);
    }
    for (auto view : kv_indices) {
      views.push_back(view);
    }
    for (auto view : joins) {
      views.push_back(view);
    }
    for (auto view : maps) {
      views.push_back(view);
    }
    for (auto view : aggregates) {
      views.push_back(view);
    }
    for (auto view : merges) {
      views.push_back(view);
    }
    for (auto view : constraints) {
      views.push_back(view);
    }
    for (auto view : inserts) {
      views.push_back(view);
    }

    for (auto view : views) {
      do_view(view);
    }
  }

  template <typename CB>
  void ForEachView(CB do_view) const {
    std::vector<VIEW *> views;
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

  template <typename CB>
  void ForEachViewInDepthOrder(CB do_view) const {
    std::vector<VIEW *> views;
    for (auto view : selects) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : tuples) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : kv_indices) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : joins) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : maps) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : aggregates) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : merges) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : constraints) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : inserts) {
      view->depth = 0;
      views.push_back(view);
    }

    std::sort(views.begin(), views.end(), [] (VIEW *a, VIEW *b) {
      return a->Depth() < b->Depth();
    });

    for (auto view : views) {
      do_view(view);
    }
  }

  template <typename CB>
  void ForEachViewInReverseDepthOrder(CB do_view) const {
    std::vector<VIEW *> views;
    for (auto view : selects) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : tuples) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : kv_indices) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : joins) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : maps) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : aggregates) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : merges) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : constraints) {
      view->depth = 0;
      views.push_back(view);
    }
    for (auto view : inserts) {
      view->depth = 0;
      views.push_back(view);
    }

    std::sort(views.begin(), views.end(), [] (VIEW *a, VIEW *b) {
      return a->Depth() > b->Depth();
    });

    for (auto view : views) {
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

  // Performs a limited amount of optimization before linking together
  // INSERTs and SELECTs.
  void Simplify(const ErrorLog &);

  // Connect INSERT nodes to SELECT nodes when the "full state" of the relation
  // does not need to be visible for point queries.
  bool ConnectInsertsToSelects(const ErrorLog &log);

  // Canonicalize the dataflow. This tries to put each node into its current
  // "most optimal" form. Previously it was more about re-arranging columns
  // to encourange better CSE results.
  void Canonicalize(const OptimizationContext &opt);

  // Sometimes we have a bunch of dumb condition patterns, roughly looking like
  // a chain of constant input tuples, conditioned on the next one in the chain,
  // and so we want to eliminate all the unnecessary intermediary tuples and
  // conditions and shrink down to a more minimal form.
  bool ShrinkConditions(void);

  // Eliminate dead flows. This uses a taint-based approach and identifies a
  // VIEW as dead if it is not derived directly or indirectly from input
  // messages.
  bool EliminateDeadFlows(void);

  // Apply common subexpression elimination (CSE) to the dataflow, canonicalize
  // the dataflow, minimize/shrink conditions, and eliminate dead flows.
  void Optimize(const ErrorLog &);

  // Identify which data flows can receive and produce deletions.
  void TrackDifferentialUpdates(void) const;

  // After the query is built, we want to push down any condition annotations
  // on nodes.
  void SinkConditions(void) const;

  // Finalize column ID values. Column ID values relate to lexical scope, to
  // some extent. Two columns with the same ID can be said to have the same
  // value at runtime.
  void FinalizeColumnIDs(void) const;

  // The streams associated with input relations to queries.
  std::unordered_map<ParsedDeclaration, Node<QueryIO> *>
      decl_to_input;

  // The tables available within any query sharing this context.
  std::unordered_map<ParsedDeclaration, Node<QueryRelation> *>
      decl_to_relation;

  // String version of the constant's spelling and type, mapped to the constant
  // stream.
  std::unordered_map<std::string, Node<QueryConstant> *> spelling_to_constant;

  // Mapping between export conditions and actual condition nodes.
  std::unordered_map<ParsedExport, Node<QueryCondition> *> decl_to_condition;

  // The streams associated with messages and other concrete inputs.
  DefList<Node<QueryIO>> ios;

  DefList<Node<QueryRelation>> relations;
  DefList<Node<QueryConstant>> constants;
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
};

}  // namespace hyde
