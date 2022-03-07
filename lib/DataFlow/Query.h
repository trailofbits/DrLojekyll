// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/BitManipulation.h>
#include <drlojekyll/Util/DefUse.h>

#include <cassert>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace hyde {

class EqualitySet;
class EquivalenceSet;
class ErrorLog;
class OptimizationContext;
class QueryViewImpl;

// Represents all values that could inhabit some relation's tuple.
class QueryColumnImpl : public Def<QueryColumnImpl> {
 public:
  ~QueryColumnImpl(void);

  static constexpr unsigned kInvalidIndex = ~0u;

  explicit QueryColumnImpl(
      std::optional<ParsedVariable> var_, TypeLoc type_,
      QueryViewImpl *view_, unsigned id_, unsigned index_ = kInvalidIndex);

  explicit QueryColumnImpl(
      ParsedVariable var_, QueryViewImpl *view_,
      unsigned id_, unsigned index_ = kInvalidIndex);

  explicit QueryColumnImpl(
      TypeLoc type_, QueryViewImpl *view_, unsigned id_,
      unsigned index_ = kInvalidIndex);

  void CopyConstantFrom(QueryColumnImpl *maybe_const_col);

  void ReplaceAllUsesWith(QueryColumnImpl *that);

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
  QueryColumnImpl *AsConstant(void) noexcept;

  // Try to resolve this column to a constant, and return it, otherwise returns
  // `this`.
  QueryColumnImpl *TryResolveToConstant(void) noexcept;

  // Returns `true` if will have a constant value at runtime.
  bool IsConstantRef(void) const noexcept;

  // Returns `true` if this column is a constant or a reference to a constant.
  bool IsConstantOrConstantRef(void) const noexcept;

  // Returns `true` if this column is definitely a constant and not just a
  // reference to one.
  bool IsConstant(void) const noexcept;

  // Returns `true` if this column is a constant that is marked as being
  // unique.
  bool IsUniqueConstant(void) const noexcept;

  // Returns `true` if this column is being used.
  bool IsUsed(void) const noexcept;

  template <typename T>
  void ForEachUser(T user_cb) const;

  // Basic form of `IsUsed`.
  inline bool IsUsedIgnoreMerges(void) const noexcept {
    return this->Def<QueryColumnImpl>::IsUsed();
  }

  // Parsed variable associated with this column.
  std::optional<ParsedVariable> var;

  // Type of the variable; convenient for returning by reference.
  const TypeLoc type;

  // View to which this column belongs.
  QueryViewImpl *const view;

  // Reference to a use of a real constant. We need this indirection because
  // we depend on dataflow to sometimes encode control dependencies, but if we
  // just blindly propagated constants around, then there are situations where
  // we could actually lose the control aspects needed by the data dependencies.
  // The `conflicting_constants.dr` example is an example that requires data
  // and control dependencies to forced together.
  UseRef<QueryColumnImpl> referenced_constant;

  // The ID of the column. During building of a dataflow, this roughly
  // corresponds to the smallest `ParsedVariable::Order` value within the
  // clause that was first used to produce this this column.
  //
  // After optimizing a dataflow, we replace all ID values
  unsigned id;

  // Display the range of column nodes in this columns forwards (or backwards)
  // taint set.
  std::shared_ptr<UseList<QueryColumnImpl>> forwards_col_taints;
  std::shared_ptr<UseList<QueryColumnImpl>> backwards_col_taints;

  // The index of this column within its view. This will have a value of
  // `kInvalidIndex` if we don't have the information.
  unsigned index;

  // The hash of this column.
  uint64_t hash{0};
};

// A condition to be tested in order to admit tuples into a relation or
// produce tuples.
class QueryConditionImpl : public Def<QueryConditionImpl>, public User {
 public:
  ~QueryConditionImpl(void);

  // An anonymous, not-user-defined condition that is instead inferred based
  // off of optmizations.
  QueryConditionImpl(void);

  // An explicit, user-defined condition. Usually associated with there-exists
  // checks or configuration options.
  explicit QueryConditionImpl(ParsedExport decl_);

  inline uint64_t Sort(void) const noexcept {
    return declaration ? declaration->Id() : reinterpret_cast<uintptr_t>(this);
  }

  // Is this a trivial condition?
  bool IsTrivial(std::unordered_map<QueryViewImpl *, bool> &conditional_views);

  // Is this a trivial condition?
  bool IsTrivial(void);

  // Are the `positive_users` and `negative_users` lists consistent?
  bool UsersAreConsistent(void) const;

  // Are the setters of this condition consistent?
  bool SettersAreConsistent(void) const;

  // The declaration of the `ParsedExport` that is associated with this
  // zero-argument predicate.
  const std::optional<ParsedDeclaration> declaration;

  // List of views using this condition.
  WeakUseList<QueryViewImpl> positive_users;
  WeakUseList<QueryViewImpl> negative_users;

  // List of views that produce values for this condition.
  //
  // TODO(pag): Consider making this not be a weak use list.
  WeakUseList<QueryViewImpl> setters;

  bool in_trivial_check{false};

  bool in_depth_calc{false};

  bool is_dead{false};
};

// A "table" of data.
class QueryRelationImpl : public Def<QueryRelationImpl>, public User {
 public:
  virtual ~QueryRelationImpl(void);
  explicit QueryRelationImpl(ParsedDeclaration decl_);

  const ParsedDeclaration declaration;

  // List of nodes that insert data into this relation.
  UseList<QueryViewImpl> inserts;

  // List of nodes that select data from this relation.
  UseList<QueryViewImpl> selects;
};

class QueryConstantImpl;
class QueryTagImpl;
class QueryIOImpl;

// A stream of values.
class QueryStreamImpl : public Def<QueryStreamImpl> {
 public:
  virtual ~QueryStreamImpl(void);

  QueryStreamImpl(void);

  virtual QueryConstantImpl *AsConstant(void) noexcept;
  virtual QueryTagImpl *AsTag(void) noexcept;
  virtual QueryIOImpl *AsIO(void) noexcept;
  virtual const char *KindName(void) const noexcept = 0;
};

// Use of a constant.
class QueryConstantImpl : public QueryStreamImpl {
 public:
  virtual ~QueryConstantImpl(void);

  QueryConstantImpl(ParsedLiteral literal_);

  QueryConstantImpl *AsConstant(void) noexcept override;
  const char *KindName(void) const noexcept override;

  const std::optional<ParsedLiteral> literal;

 protected:
  inline QueryConstantImpl(void) {}
};

// Use of a constant.
class QueryTagImpl final : public QueryConstantImpl {
 public:
  virtual ~QueryTagImpl(void);

  QueryTagImpl(uint16_t val_);

  QueryTagImpl *AsTag(void) noexcept override;
  const char *KindName(void) const noexcept override;

  const uint16_t val;
};

// Input, i.e. a messsage.
class QueryIOImpl final : public QueryStreamImpl, public User {
 public:
  virtual ~QueryIOImpl(void);

  QueryIOImpl(ParsedDeclaration declaration_);

  QueryIOImpl *AsIO(void) noexcept override;
  const char *KindName(void) const noexcept override;

  const ParsedDeclaration declaration;

  // List of nodes that send data to this I/O operation.
  UseList<QueryViewImpl> transmits;

  // List of nodes that receive data from this I/O operation.
  UseList<QueryViewImpl> receives;
};

// Information about this node as it relates to being inside of an induction.
// This affects MERGE, JOIN, and NEGATE nodes, as these are the only nodes which
// can reasonably take more than one predecessor. All cycles in data flow must
// go through UNIONs, so at first glance they seem to be enough to reign in
// control over inductions. However, data can flow out of one inductive union
// and into one side of a JOIN. The JOIN can be on the back-edge of another
// induction. Ideally, we want to capture the JOIN's pivot vector as being
// an induction vector, and not repeat JOIN codegen in the init region and the
// fixpoint cycle region.
struct InductionInfo {
 public:
  explicit InductionInfo(QueryViewImpl *owner);

  std::vector<QueryViewImpl *> predecessors;
  std::vector<QueryViewImpl *> successors;

  // Initially, when learning about inductions, we use a mask to figure out
  // which of the predecessors/successors are inductive. This is so that with
  // JOINs, we can learn that some of the predecessors are inductive for some
  // UNIONs and possibly not others.
  std::vector<bool> inductive_predecessors_mask;
  std::vector<bool> inductive_successors_mask;

  // Can we reach back to ourselves by not flowing through another induction?
  bool can_reach_self_not_through_another_induction{false};

  WeakUseList<QueryViewImpl> inductive_predecessors;
  WeakUseList<QueryViewImpl> inductive_successors;

  WeakUseList<QueryViewImpl> noninductive_predecessors;
  WeakUseList<QueryViewImpl> noninductive_successors;

  // List of UNION, JOIN, and NEGATE nodes.
  std::shared_ptr<WeakUseList<QueryViewImpl>> cyclic_views;

  unsigned merge_set_id{0};
  unsigned merge_depth{0};
};

class QueryImpl;
class QueryTupleImpl;
class QuerySelectImpl;
class QueryKVIndexImpl;
class QueryJoinImpl;
class QueryMapImpl;
class QueryAggregateImpl;
class QueryMergeImpl;
class QueryNegateImpl;
class QueryCompareImpl;
class QueryInsertImpl;

// A view "owns" its the columns pointed to by `columns`.
class QueryViewImpl : public Def<QueryViewImpl>, public User {
 public:
  virtual ~QueryViewImpl(void);

  QueryViewImpl(void);

  // Returns the kind name, e.g. UNION, JOIN, etc.
  virtual const char *KindName(void) const noexcept = 0;

  // Returns `true` if this node is inductive. Only MERGEs can be inductive.
  inline bool IsInductive(void) const {
    return !!induction_info;
  }

  // Prepare to delete this node. This tries to drop all dependencies and
  // unlink this node from the dataflow graph. It returns `true` if successful
  // and `false` if it has already been performed.
  bool PrepareToDelete(void);

  // Copy all positive and negative conditions from `this` into `that`.
  void CopyTestedConditionsTo(QueryViewImpl *that);

  // Transfer all positive and negative conditions from `this` into `that`.
  void TransferTestedConditionsTo(QueryViewImpl *that);

  // Converts this node to be unconditional, it doesn't affect set conditions.
  void DropTestedConditions(void);

  // Converts this node to not set any conditions.
  void DropSetConditions(void);

  // If `sets_condition` is non-null, then transfer the setter to `that`.
  void TransferSetConditionTo(QueryViewImpl *that);

  // Copy the group IDs and the receive/produce deletions from `this` to `that`.
  void CopyDifferentialAndGroupIdsTo(QueryViewImpl *that);

  // Replace all uses of `this` with `that`. The semantic here is that `this`
  // remains valid and used.
  void SubstituteAllUsesWith(QueryViewImpl *that);

  // Replace all uses of `this` with `that`. The semantic here is that `this`
  // is completely subsumed/replaced by `that`.
  void ReplaceAllUsesWith(QueryViewImpl *that);

  // Does this view introduce a control dependency? If a node introduces a
  // control dependency then it generally needs to be kept around.
  bool IntroducesControlDependency(void) const noexcept;

  // Returns `true` if all output columns are used.
  bool AllColumnsAreUsed(void) const noexcept;

  // Returns `true` if we had to "guard" this view with a tuple so that we
  // can put it into canonical form.
  QueryTupleImpl *GuardWithTuple(QueryImpl *query, bool force = false);

  // This is like an "optimized" form of `GuardWithTuple`, that also knows
  // about attached columns. It tries to propagate constants, and maintains
  // a backward reference to `this` if it drops all references.
  //
  // NOTE(pag): `incoming_view` is there to tell is if `this` ever even had any
  //            dependencies. This is really only relevant to TUPLEs, and so
  //            it's permissible for things like MAPs, NEGATEs, etc. to pass
  //            in `this` for `incoming_view`, to force a non-NULL.
  //
  // NOTE(pag): This assumes `in_to_out` is filled up!!
  QueryTupleImpl *GuardWithOptimizedTuple(QueryImpl *query,
                                            unsigned first_attached_col,
                                            QueryViewImpl *incoming_view);

  // Proxy this node with a comparison of `lhs_col` and `rhs_col`, where
  // `lhs_col` and `rhs_col` either belong to `this->columns` or are constants.
  QueryTupleImpl *ProxyWithComparison(
      QueryImpl *query, ComparisonOperator op,
      QueryColumnImpl *lhs_col, QueryColumnImpl *rhs_col);

  // Returns `true` if this view is being used.
  bool IsUsed(void) const noexcept;

  // Is this view directly being used? This does not check columns, but does
  // check conditions.
  bool IsUsedDirectly(void) const noexcept;

  // Invoked any time time that any of the columns used by this view are
  // modified.
  void Update(uint64_t) override;

  // Sort the `positive_conditions` and `negative_conditions`.
  void OrderConditions(void);

  // Canonicalizes an input/output column pair. Returns `true` in the first
  // element if non-local changes are made, and `true` in the second element
  // if the column pair can be removed.
  std::pair<bool, bool>
  CanonicalizeColumnPair(QueryColumnImpl *in_col, QueryColumnImpl *out_col,
                         const OptimizationContext &opt) noexcept;

  // Put this view into a canonical form. Returns `true` if changes were made
  // beyond the scope of this view.
  virtual bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                            const ErrorLog &);

  struct Discoveries {
    bool constant_inputs : 1;
    bool non_local_changes : 1;
    bool guardable_constant_output : 1;
    bool duplicated_input_column : 1;
    bool directly_used_column : 1;
    bool unused_column : 1;
  };

  // Record the mapping between `in_col` and `out_col` into `this->in_to_out`,
  // do constant propagation, and possibly to replacements. Sets
  // `is_canonical = false;` if anything is changed or should be changed.
  Discoveries CanonicalizeColumn(
      const OptimizationContext &opt, QueryColumnImpl *in_col,
      QueryColumnImpl *out_col, bool is_attached, Discoveries has);

  virtual QuerySelectImpl *AsSelect(void) noexcept;
  virtual QueryTupleImpl *AsTuple(void) noexcept;
  virtual QueryKVIndexImpl *AsKVIndex(void) noexcept;
  virtual QueryJoinImpl *AsJoin(void) noexcept;
  virtual QueryMapImpl *AsMap(void) noexcept;
  virtual QueryAggregateImpl *AsAggregate(void) noexcept;
  virtual QueryMergeImpl *AsMerge(void) noexcept;
  virtual QueryNegateImpl *AsNegate(void) noexcept;
  virtual QueryCompareImpl *AsCompare(void) noexcept;
  virtual QueryInsertImpl *AsInsert(void) noexcept;

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
  virtual bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept = 0;

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
  DefList<QueryColumnImpl> columns;

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
  UseList<QueryColumnImpl> input_columns;

  // Attached columns to bring along "lexical context" from their inputs.
  // These are used by MAPs and FILTERs, which need to pull along state from
  // their sources.
  UseList<QueryColumnImpl> attached_columns;

  // Zero argument predicates that constrain this node.
  UseList<QueryConditionImpl> positive_conditions;
  UseList<QueryConditionImpl> negative_conditions;

  // If this VIEW sets a CONDition, then keep track of that here.
  WeakUseRef<QueryConditionImpl> sets_condition;

  // Predecessors and successors of this VIEW.
  //
  // NOTE(pag): These are only filled in *after* all optimizations. They exist
  //            for external/public users.
  WeakUseList<QueryViewImpl> predecessors;
  WeakUseList<QueryViewImpl> successors;

  // Used during canonicalization. Mostly just convenient to have around for
  // re-use of memory.
  std::unordered_map<QueryColumnImpl *, QueryColumnImpl *> in_to_out;

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
  // hash is invalid. We use this for JOIN merging during early dataflow
  // building. This is a good hint for CSE when the data flow is acyclic.
  uint64_t hash{0u};

  // The group ID of this node that it will push forward to its dependencies.
  unsigned group_id{0u};

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

  // Is this node "locked", i.e. are we no longer allowed to canonicalize it?
  bool is_locked{false};

  // Is this node dead?
  bool is_dead{false};

  // Is this view unsatisfiable? This happens as a result of predicate pushdown,
  // where sometimes we know only later that something is unsatisfiable, and
  // so we need to propagate this property upward through the graph.
  bool is_unsat{false};

  // `true` if this view can receive/produce deletions. For example, when an
  // aggregate is updated, the old summary values are produced as a deletion.
  // Similarly, when a kvindex is updated, if the new values differ from the
  // old ones, a deletion record is produced.
  bool can_receive_deletions{false};
  bool can_produce_deletions{false};

  // Is this view used by a negation?
  bool is_used_by_negation{false};

  // Is this view used by a join?
  bool is_used_by_join{false};

  // Is this view used by a merge?
  bool is_used_by_merge{false};

  // Is this view constant after the initialization of the program? This is
  // computed at the end of building the dataflow graph, and helps us optimize
  // JOINs and negations in the control-flow IR by letting us avoid persisting
  // data when that data is non-differential. That is, if non-differential
  // data is flowing through a JOIN, and the stuff against which we're joining
  // is constant after init, then we don't need to save our stuff to a table
  // prior to the join -- we can force it through and dedup it downstream.
  bool is_const_after_init{false};

  // Color to use in the eventual data flow output. Default is black. This
  // is influenced by `ParsedClause::IsHighlighted`, which in turn is enabled
  // by using the `@highlight` pragma after a clause head.
  unsigned color{0};

#ifndef NDEBUG

  // Debug string roughly tracking how or why this node was created.
  std::string producer;
#endif

  // Does this code break an invariant?
  enum {
    kValid,
    kInvalidBeforeCanonicalize,
    kInvalidAfterCanonicalize
  } valid{kValid};

  // If we broke an invariant, then highlight the variable name of the column
  // for which we broke the invariant.
  mutable std::optional<ParsedVariable> invalid_var;

  // This breaks abstraction layers, as table IDs come from the control-flow
  // IR, but it's nifty for debugging.
  std::optional<unsigned> table_id;

  // Information about if this is inductive.
  std::unique_ptr<InductionInfo> induction_info;

  // Equivalence set of views sharing the same data model
  std::unique_ptr<EquivalenceSet> equivalence_set;

  // Check that all non-constant views in `cols1` and `cols2` match.
  //
  // NOTE(pag): This isn't a pairwise matching; instead it checks that all
  //            columns in both of the lists independently reference the same
  //            view.
  bool CheckIncomingViewsMatch(const UseList<QueryColumnImpl> &cols1) const;
  bool CheckIncomingViewsMatch(const UseList<QueryColumnImpl> &cols1,
                               const UseList<QueryColumnImpl> &cols2) const;

  // If `cols1:cols2` pull their data from a tuple, and if that tuple is
  // unconditional, or if its conditions are trivial, then update `cols1:cols2`
  // to point at the source of the data of those tuples.
  //
  // Takes in the `incoming_view` pulled from by `cols1:cols2` and returns the
  // updated `incoming_view`.
  //
  // NOTE(pag): This updates `is_canonical = false` if it changes anything.
  QueryViewImpl *
  PullDataFromBeyondTrivialTuples(
      QueryViewImpl *incoming_view,
      UseList<QueryColumnImpl> &cols1,
      UseList<QueryColumnImpl> &cols2);

 private:
  // Similar to, and called by, `PullDataFromBeyondTrivialTuples`.
  QueryViewImpl *
  PullDataFromBeyondTrivialUnions(
      QueryViewImpl *incoming_view,
      UseList<QueryColumnImpl> &cols1,
      UseList<QueryColumnImpl> &cols2);

 public:
  // Figure out what the incoming view to `cols1` is.
  static QueryViewImpl *GetIncomingView(const UseList<QueryColumnImpl> &cols1);
  static QueryViewImpl *GetIncomingView(const UseList<QueryColumnImpl> &cols1,
                                        const UseList<QueryColumnImpl> &cols2);

  // Try to figure out if `view` is conditional. That could mean that it
  // depends directly on a condition, or that it depends on something that
  // may be present or may be absent (e.g. the output of a `JOIN`).
  static bool IsConditional(
      QueryViewImpl *view,
      std::unordered_map<QueryViewImpl *, bool> &conditional_views);

  // Returns a pointer to the only user of this node, or nullptr if there are
  // zero users, or more than one users.
  QueryViewImpl *OnlyUser(void) const noexcept;

  // Create or inherit a condition created on `view`.
  void CreateDependencyOnView(QueryImpl *query, QueryViewImpl *view);

 protected:
  // Utilities for depth calculation.
  static unsigned EstimateDepth(const UseList<QueryColumnImpl> &cols,
                                unsigned depth);
  static unsigned EstimateDepth(const UseList<QueryConditionImpl> &conds,
                                unsigned depth);
  static unsigned GetDepth(const UseList<QueryColumnImpl> &cols,
                           unsigned depth);
  static unsigned GetDepth(const UseList<QueryConditionImpl> &conds,
                           unsigned depth);

  // Utility for comparing use lists.
  static bool ColumnsEq(EqualitySet &eq, const UseList<QueryColumnImpl> &c1s,
                        const UseList<QueryColumnImpl> &c2s);

  // Check if the `group_ids` of two views have any overlaps.
  static bool InsertSetsOverlap(QueryViewImpl *a, QueryViewImpl *b);

  // Mark this node as being unsatisfiable.
  void MarkAsUnsatisfiable(void);
};

class QuerySelectImpl final : public QueryViewImpl {
 public:
  QuerySelectImpl(QueryRelationImpl *relation_, ParsedPredicate pred_);
  QuerySelectImpl(QueryStreamImpl *stream_, ParsedPredicate pred_);
  QuerySelectImpl(QueryStreamImpl *stream_, DisplayRange spelling_range);

  virtual ~QuerySelectImpl(void);

  const char *KindName(void) const noexcept override;
  QuerySelectImpl *AsSelect(void) noexcept override;

  uint64_t Sort(void) noexcept override;
  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;

  // Put this view into a canonical form. Returns `true` if changes were made
  // beyond the scope of this view.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;

  // The instance of the predicate from which we are selecting.
  const std::optional<ParsedPredicate> pred;

  // The beginning location of the predicate. Used for sorting.
  DisplayPosition position;

  // The table from which this select takes its columns.
  WeakUseRef<QueryRelationImpl> relation;
  WeakUseRef<QueryStreamImpl> stream;

  // List of views that might feed this SELECT.
  WeakUseList<QueryViewImpl> inserts;
};

class QueryTupleImpl final : public QueryViewImpl {
 public:
  virtual ~QueryTupleImpl(void);

  const char *KindName(void) const noexcept override;
  QueryTupleImpl *AsTuple(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;

  // Does this tuple forward all of its inputs to the same columns as the
  // outputs, and if so, does it forward all columns of its input?
  bool ForwardsAllInputsAsIs(void) const noexcept;
  bool ForwardsAllInputsAsIs(QueryViewImpl *incoming_view) const noexcept;

  // Put this tuple into a canonical form, which will make comparisons and
  // replacements easier. Because comparisons are mostly pointer-based, the
  // canonical form of this tuple is one where all columns are sorted by
  // their pointer values.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;
};

// The KV index will have the `input_columns` as the keys, and the
// `attached_columns` as the values.
class QueryKVIndexImpl final : public QueryViewImpl {
 public:
  QueryKVIndexImpl(void) : QueryViewImpl() {
    can_produce_deletions = true;
  }

  virtual ~QueryKVIndexImpl(void);

  const char *KindName(void) const noexcept override;
  QueryKVIndexImpl *AsKVIndex(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;

  // Put the KV index into a canonical form. The only real internal optimization
  // that will happen is constant propagation of keys, but NOT values (as we can't
  // predict how the merge functors will affect them).
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;

  // Functors that get called to merge old and new values.
  std::vector<ParsedFunctor> merge_functors;
};

class QueryJoinImpl final : public QueryViewImpl {
 public:
  virtual ~QueryJoinImpl(void);

  QueryJoinImpl(void) : joined_views(this) {}

  const char *KindName(void) const noexcept override;
  QueryJoinImpl *AsJoin(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;

  // Put this join into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;

  // Remove all constant uses.
  void RemoveConstants(QueryImpl *query);

  // Convert a trivial join (only has a single input view) into a TUPLE.
  void ConvertTrivialJoinToTuple(QueryImpl *impl);

  // Returns `true` if any joined views were identified where one or more of
  // their columns are not used by the JOIN. If so, we proxy those views with
  // TUPLEs.
  bool ProxyUnusedInputColumns(QueryImpl *impl);

  // Maps output columns to input columns.
  std::unordered_map<QueryColumnImpl *, UseList<QueryColumnImpl>> out_to_in;

  // List of views merged by this JOIN. Columns in pivot sets in `out_to_in` are
  // in the same order as they appear in `pivot_views`.
  //
  // TODO(pag): I don't think the ordering invariant is maintained through
  //            canonicalization.
  WeakUseList<QueryViewImpl> joined_views;

  // Number of pivot columns. If this value is zero then this is actually a
  // cross-product.
  unsigned num_pivots{0};
};

class QueryMapImpl final : public QueryViewImpl {
 public:
  virtual ~QueryMapImpl(void);

  explicit QueryMapImpl(ParsedFunctor functor_, DisplayRange range_,
                        bool is_positive_);

  const char *KindName(void) const noexcept override;
  QueryMapImpl *AsMap(void) noexcept override;

  uint64_t Sort(void) noexcept override;
  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;

  // Put this map into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;

  const DisplayRange range;
  const ParsedFunctor functor;

  // Number of `free` parameters in this functor. This distinguishes this map
  // from being a filter/predicate.
  unsigned num_free_params{0};

  // Is this a positive functor application?
  const bool is_positive;
};

class QueryAggregateImpl : public QueryViewImpl {
 public:
  explicit QueryAggregateImpl(ParsedFunctor functor_);

  virtual ~QueryAggregateImpl(void);

  const char *KindName(void) const noexcept override;
  QueryAggregateImpl *AsAggregate(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;

  // Put this aggregate into a canonical form, which will make comparisons and
  // replacements easier.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;

  // Functor that does the aggregation.
  const ParsedFunctor functor;

  // Columns that are `bound` before the aggregate, used by the relation being
  // summarized, but not being passed to the aggregating functor. These are
  // unordered. These are not visible to the aggregating functor.
  UseList<QueryColumnImpl> group_by_columns;

  // Columns that are `bound` for the aggregating functor. These are ordered.
  // We think of this as being a form of grouping, where really they act like
  // "specializations" for the aggregating functor. They kind of "configure" it.
  UseList<QueryColumnImpl> config_columns;

  // Columns that are aggregated by this aggregating functor, and will be
  // summarized. These are "in scope" of the aggregation. These are ordered.
  UseList<QueryColumnImpl> aggregated_columns;
};

class QueryMergeImpl : public QueryViewImpl {
 public:
  QueryMergeImpl(void);

  virtual ~QueryMergeImpl(void);

  const char *KindName(void) const noexcept override;
  QueryMergeImpl *AsMerge(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  unsigned Depth(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;

  // Convert two or more tuples into a single tuple that reads its data from
  // a union, where that union reads its data from the sources of the two
  // tuples. The returned tuple is likely superficial but serves to prevent
  // the union of the tuple's sources from being merged upward. What we are
  // looking for are cases where the tuples leading into the union have
  // similarly shaped inputs. We want a union over those inputs (which may be
  // wider than the tuple itself, hence the returned tuple).
  //
  // Returns `true` if successful, and updates `tuples` in place with the
  // new merged entries.
  bool SinkThroughTuples(QueryImpl *impl, std::vector<QueryViewImpl *> &tuples);

  // Similar to above, but for maps.
  bool SinkThroughMaps(QueryImpl *impl, std::vector<QueryViewImpl *> &maps);

  // Similar to above, but for negations.
  bool SinkThroughNegations(
      QueryImpl *impl, std::vector<QueryViewImpl *> &negations);

  // Similar to above, but for joins.
  bool SinkThroughJoins(QueryImpl *impl, std::vector<QueryViewImpl *> &joins,
                        bool recursive=false);

  // Put this merge into a canonical form, which will make comparisons and
  // replacements easier. For example, after optimizations, some of the merged
  // views might be the same.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;

  // The views that are being merged together.
  UseList<QueryViewImpl> merged_views;
};

class QueryCompareImpl : public QueryViewImpl {
 public:
  QueryCompareImpl(ComparisonOperator op_);

  virtual ~QueryCompareImpl(void);

  const char *KindName(void) const noexcept override;
  QueryCompareImpl *AsCompare(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;

  // Put this constraint into a canonical form, which will make comparisons and
  // replacements easier. If this constraint's operator is unordered, then we
  // sort the inputs to make comparisons trivial.
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;

  // Try to sink this comparison through its predecessor.
  bool TrySink(QueryImpl *query);

  // Try to sink this comparison through a MERGE node.
  bool TrySinkThroughMerge(QueryImpl *query, QueryMergeImpl *merge);

  // Try to sink this comparison through a NEGATION node.
  bool TrySinkThroughNegate(QueryImpl *query, QueryNegateImpl *negate);

  const ComparisonOperator op;

  // Spelling range of the comparison that produced this. May not be valid.
  DisplayRange spelling_range;

  // If we created this merge from sinking / predicate pushdown, then we
  // make discover some unsatisfiable conditions, which we don't want to
  // report as errors.
  bool created_from_sinking{false};

  // Can this node be sunk?
  bool can_sink{true};
};

// Represents the check of the absence of a tuple from a relation.
class QueryNegateImpl : public QueryViewImpl {
 public:
  virtual ~QueryNegateImpl(void);

  QueryNegateImpl(void);

  const char *KindName(void) const noexcept override;
  QueryNegateImpl *AsNegate(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;
  unsigned Depth(void) noexcept override;

  UseRef<QueryViewImpl> negated_view;

  // Is this a normal negation, or one with `@never`?
  bool is_never{false};
  std::vector<ParsedPredicate> negations;
};

// Inserts are technically views as that makes some things easier, but they
// are not exposed as such.
class QueryInsertImpl : public QueryViewImpl {
 public:
  virtual ~QueryInsertImpl(void);

  QueryInsertImpl(QueryRelationImpl *relation_, ParsedDeclaration decl_);
  QueryInsertImpl(QueryStreamImpl *stream_, ParsedDeclaration decl_);

  const char *KindName(void) const noexcept override;
  QueryInsertImpl *AsInsert(void) noexcept override;

  uint64_t Hash(void) noexcept override;
  bool Equals(EqualitySet &eq, QueryViewImpl *that) noexcept override;
  bool Canonicalize(QueryImpl *query, const OptimizationContext &opt,
                    const ErrorLog &) override;

  WeakUseRef<QueryRelationImpl> relation;
  WeakUseRef<QueryStreamImpl> stream;
  const ParsedDeclaration declaration;
};

class QueryImpl {
 public:
  explicit QueryImpl(const ParsedModule &module_);

  ~QueryImpl(void);

  template <typename CB>
  void ForEachView(CB do_view);

  template <typename CB>
  void ForEachView(CB do_view) const;

  template <typename CB>
  void ForEachViewInDepthOrder(CB do_view) const;

  template <typename CB>
  void ForEachViewInReverseDepthOrder(CB do_view) const;

  // Clear all group IDs. Sometimes we want to do optimizations that excplicitly
  // don't need to deal with the issues of accidentally over-merging nodes.
  void ClearGroupIDs(void);

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
  void Canonicalize(const OptimizationContext &opt, const ErrorLog &);

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

  // Convert all views having constant inputs to depend upon tuple nodes, so
  // that we have the invariant that the only type of view that can take all
  // constants is a tuple. This simplifies lots of stuff later.
  void ConvertConstantInputsToTuples(void);

  // Identify the inductive unions in the data flow.
  void IdentifyInductions(const ErrorLog &log, bool recursive = false);

  // Identify which data flows can receive and produce deletions.
  void TrackDifferentialUpdates(const ErrorLog &log,
                                bool check_conds = false) const;

  // Track which views are constant after initialization.
  // See `VIEW::is_const_after_init`.
  void TrackConstAfterInit(void) const;

  // Extract conditions from regular nodes and force them to belong to only
  // tuple nodes. This simplifies things substantially for downstream users.
  void ExtractConditionsToTuples(void);

  // Finalize column ID values. Column ID values relate to lexical scope, to
  // some extent. Two columns with the same ID can be said to have the same
  // value at runtime.
  void FinalizeColumnIDs(void) const;

  // Ensure that every INSERT view is preceded by a TUPLE. This makes a bunch
  // of things easier downstream in the control-flow IR generation, because
  // then the input column indices of an insert line up perfectly with the
  // SELECTs and such.
  void ProxyInsertsWithTuples(void);

  // Link together views in terms of predecessors and successors.
  void LinkViews(bool recursive = false);

  // Finalize all depth calculations.
  void FinalizeDepths(void) const;

  // Root module associated with this query.
  const ParsedModule module;

  // The streams associated with input relations to queries.
  std::unordered_map<ParsedDeclaration, QueryIOImpl *> decl_to_input;

  // The tables available within any query sharing this context.
  std::unordered_map<ParsedDeclaration, QueryRelationImpl *> decl_to_relation;

  // String version of the constant's spelling and type, mapped to the constant
  // stream.
  std::unordered_map<std::string, QueryConstantImpl *> spelling_to_constant;

  // Mapping between export conditions and actual condition nodes.
  std::unordered_map<ParsedExport, QueryConditionImpl *> decl_to_condition;

  std::vector<QueryColumnImpl *> tag_columns;

  // The streams associated with messages and other concrete inputs.
  DefList<QueryIOImpl> ios;

  DefList<QueryRelationImpl> relations;
  DefList<QueryConstantImpl> constants;
  DefList<QueryTagImpl> tags;
  DefList<QueryConditionImpl> conditions;
  DefList<QuerySelectImpl> selects;
  DefList<QueryTupleImpl> tuples;
  DefList<QueryKVIndexImpl> kv_indices;
  DefList<QueryJoinImpl> joins;
  DefList<QueryMapImpl> maps;
  DefList<QueryAggregateImpl> aggregates;
  DefList<QueryMergeImpl> merges;
  DefList<QueryNegateImpl> negations;
  DefList<QueryCompareImpl> compares;
  DefList<QueryInsertImpl> inserts;

  // Forwards and Backwards Column Tainting
  void RunForwardsTaintAnalysis(void);
  void RunBackwardsTaintAnalysis(void);

  UsedNodeRange<QueryColumn> GetForwardsTaintsFromColId(unsigned col_id);
  UsedNodeRange<QueryColumn> GetBackwardsTaintsFromColId(unsigned col_id);

  std::vector<std::shared_ptr<UseList<QueryColumnImpl>>> forwards_col_taints;
  std::vector<std::shared_ptr<UseList<QueryColumnImpl>>> backwards_col_taints;
};

using COL = QueryColumnImpl;
using COND = QueryConditionImpl;
using REL = QueryRelationImpl;
using STREAM = QueryStreamImpl;
using CONST = QueryConstantImpl;
using TAG = QueryTagImpl;
using IO = QueryIOImpl;
using VIEW = QueryViewImpl;
using SELECT = QuerySelectImpl;
using TUPLE = QueryTupleImpl;
using KVINDEX = QueryKVIndexImpl;
using JOIN = QueryJoinImpl;
using MAP = QueryMapImpl;
using AGG = QueryAggregateImpl;
using MERGE = QueryMergeImpl;
using CMP = QueryCompareImpl;
using NEGATION = QueryNegateImpl;
using INSERT = QueryInsertImpl;

template <typename T>
void QueryColumnImpl::ForEachUser(T user_cb) const {
  view->ForEachUse<QueryViewImpl>([&user_cb](QueryViewImpl *user, QueryViewImpl *) {
    user_cb(user);
  });

  ForEachUse<QueryViewImpl>([&user_cb](QueryViewImpl *view, QueryColumnImpl *) {
    user_cb(view);
  });
}

template <typename CB>
void QueryImpl::ForEachView(CB do_view) {
  std::vector<QueryViewImpl *> views;
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
  for (auto view : negations) {
    views.push_back(view);
  }
  for (auto view : compares) {
    views.push_back(view);
  }
  for (auto view : inserts) {
    views.push_back(view);
  }

  for (auto view : views) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
}

template <typename CB>
void QueryImpl::ForEachView(CB do_view) const {
  for (auto view : selects) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : tuples) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : kv_indices) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : joins) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : maps) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : aggregates) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : merges) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : negations) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : compares) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
  for (auto view : inserts) {
    if (!view->is_dead) {
      do_view(view);
    }
  }
}

template <typename CB>
void QueryImpl::ForEachViewInDepthOrder(CB do_view) const {
  std::vector<QueryViewImpl *> views;
  for (auto view : selects) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : tuples) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : kv_indices) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : joins) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : maps) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : aggregates) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : merges) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : negations) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : compares) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : inserts) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }

  std::sort(views.begin(), views.end(),
            [](QueryViewImpl *a, QueryViewImpl *b) {
              return a->Depth() < b->Depth();
            });

  for (auto view : views) {
    do_view(view);
  }
}

template <typename CB>
void QueryImpl::ForEachViewInReverseDepthOrder(CB do_view) const {
  std::vector<QueryViewImpl *> views;
  for (auto view : selects) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : tuples) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : kv_indices) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : joins) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : maps) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : aggregates) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : merges) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : negations) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : compares) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }
  for (auto view : inserts) {
    view->depth = 0;
    if (!view->is_dead) {
      views.push_back(view);
    }
  }

  std::sort(views.begin(), views.end(),
            [](QueryViewImpl *a, QueryViewImpl *b) {
              return a->Depth() > b->Depth();
            });

  for (auto view : views) {
    do_view(view);
  }
}

}  // namespace hyde
