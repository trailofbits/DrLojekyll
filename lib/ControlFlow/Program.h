// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/DisjointSet.h>
#include <drlojekyll/Util/EqualitySet.h>

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define NOTE(msg) std::cerr << msg << "\n"

#define COMMENT(...) __VA_ARGS__

namespace std {

template <>
struct hash<std::pair<::hyde::QueryView, ::hyde::QueryView>> {
  using argument_type = std::pair<::hyde::QueryView, ::hyde::QueryView>;
  using result_type = uint64_t;
  inline uint64_t operator()(argument_type views) const noexcept {
    return (views.first.UniqueId() * views.second.Hash()) ^
           views.second.UniqueId();
  }
};

}  // namespace std
namespace hyde {

class ProgramInductionRegion;
class ProgramOperationRegion;

// A column within a table.
template <>
class Node<DataColumn> final : public Def<Node<DataColumn>>, public User {
 public:
  virtual ~Node(void);

  Node(unsigned id_, TypeKind type_, Node<DataTable> *table_);

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const unsigned index;
  const TypeKind type;

  std::vector<Token> names;

  WeakUseRef<Node<DataTable>> table;
};

using TABLECOLUMN = Node<DataColumn>;

// Represents an index on some subset of columns in a table.
template <>
class Node<DataIndex> final : public Def<Node<DataIndex>>, public User {
 public:
  virtual ~Node(void);

  Node(unsigned id_, Node<DataTable> *table_, std::string column_spec_);

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const std::string column_spec;

  UseList<TABLECOLUMN> columns;
  UseList<TABLECOLUMN> mapped_columns;

  WeakUseRef<Node<DataTable>> table;
};

using TABLEINDEX = Node<DataIndex>;

class Context;

// Represents a table of data.
//
// NOTE(pag): By default all tables already have a UNIQUE index on them.
template <>
class Node<DataTable> final : public Def<Node<DataTable>>, public User {
 public:
  virtual ~Node(void);

  inline Node(unsigned id_)
      : Def<Node<DataTable>>(this),
        User(this),
        id(id_),
        columns(this),
        indices(this) {}

  void Accept(ProgramVisitor &visitor);

  // Get or create a table in the program.
  static Node<DataTable> *GetOrCreate(ProgramImpl *impl, Context &context,
                                      QueryView view);

  // Get or create an index on the table.
  TABLEINDEX *GetOrCreateIndex(ProgramImpl *impl, std::vector<unsigned> cols);

  const unsigned id;

  // List of defined columns.
  DefList<TABLECOLUMN> columns;

  // Indexes that should be created on this table. By default, all tables have
  // a UNIQUE index.
  DefList<TABLEINDEX> indices;

  // All views sharing this table.
  std::vector<QueryView> views;
};

using TABLE = Node<DataTable>;

struct DataModel : public DisjointSet {
 public:
  TABLE *table{nullptr};
};

// A vector of tuples in the program.
template <>
class Node<DataVector> final : public Def<Node<DataVector>> {
 public:
  template <typename ColList>
  Node(unsigned id_, VectorKind kind_, ColList &&cols)
      : Def<Node<DataVector>>(this),
        id(id_),
        kind(kind_) {

    for (QueryColumn col : cols) {
      col_types.push_back(col.Type().Kind());
    }
  }

  Node(Node<DataVector> *that_)
      : Def<Node<DataVector>>(this),
        id(that_->id),
        kind(that_->kind),
        col_types(that_->col_types) {}

  bool IsRead(void) const;

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const VectorKind kind;
  std::vector<TypeKind> col_types;

  std::optional<ParsedMessage> added_message;
  std::optional<ParsedMessage> removed_message;

  // `true` if this vector must have variants of itself sharded across workers.
  bool is_sharded{false};
};

using VECTOR = Node<DataVector>;

// A variable in the program. This could be a procedure parameter or a local
// variable.
template <>
class Node<DataVariable> final : public Def<Node<DataVariable>> {
 public:
  explicit Node(unsigned id_, VariableRole role_);

  void Accept(ProgramVisitor &visitor);

  const VariableRole role;

  // Unique ID for this variable. Unrelated to column IDs.
  const unsigned id;

  // NOTE(pag): Only valid after optimization when building the control flow
  //            IR is complete.
  Node<ProgramRegion> *defining_region{nullptr};

  inline unsigned Sort(void) const noexcept {
    return id;
  }

  inline bool IsGlobal(void) const noexcept {
    switch (role) {
      case VariableRole::kConditionRefCount:
      case VariableRole::kInitGuard:
      case VariableRole::kConstant:
      case VariableRole::kConstantTag:
      case VariableRole::kConstantZero:
      case VariableRole::kConstantOne:
      case VariableRole::kConstantFalse:
      case VariableRole::kConstantTrue: return true;
      default: return false;
    }
  }

  TypeLoc Type(void) const noexcept;

  std::optional<QueryConstant> query_const;
  std::optional<QueryColumn> query_column;
  std::optional<QueryCondition> query_cond;
};

using VAR = Node<DataVariable>;

// A lexically scoped region in the program.
template <>
class Node<ProgramRegion> : public Def<Node<ProgramRegion>>, public User {
 public:
  virtual ~Node(void);
  explicit Node(Node<ProgramProcedure> *containing_procedure_, bool);
  explicit Node(Node<ProgramRegion> *parent_);

  virtual void Accept(ProgramVisitor &visitor) = 0;
  virtual uint64_t Hash(uint32_t depth) const = 0;

  virtual Node<ProgramProcedure> *AsProcedure(void) noexcept;
  virtual Node<ProgramOperationRegion> *AsOperation(void) noexcept;
  virtual Node<ProgramSeriesRegion> *AsSeries(void) noexcept;
  virtual Node<ProgramParallelRegion> *AsParallel(void) noexcept;
  virtual Node<ProgramInductionRegion> *AsInduction(void) noexcept;

  // Returns `true` if all paths through `this` ends with a `return` region.
  virtual bool EndsWithReturn(void) const noexcept = 0;

  inline void ReplaceAllUsesWith(Node<ProgramRegion> *that) {
    this->Def<Node<ProgramRegion>>::ReplaceAllUsesWith(that);
    if (!this->AsProcedure()) {
      assert(!that->AsProcedure());
      that->parent = this->parent;
      this->parent = nullptr;
    }
  }

  // Returns 'true' if 'this' was able to merge all of the regions in 'merges'.
  // Merging takes the form of a new PARALLEL region to execute the bodies of
  // 'Equals' regions.
  // This method assumes that all elements in 'merges' are 'Equals' at depth 0.
  virtual const bool MergeEqual(ProgramImpl *prog,
                                std::vector<Node<ProgramRegion> *> &merges);

  // Gets or creates a local variable in the procedure.
  VAR *VariableFor(ProgramImpl *impl, QueryColumn col);
  VAR *VariableForRec(QueryColumn col);

  // Returns the lexical level of this node.
  unsigned Depth(void) const noexcept;

  // Returns the lexical level of this node.
  unsigned CachedDepth(void) noexcept;

  // Returns `true` if this region is a no-op.
  virtual bool IsNoOp(void) const noexcept;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming) after searching down `depth` levels or until leaf,
  // whichever is first, and where `depth` is 0, compare `this` to `that.
  virtual bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
                      uint32_t depth) const noexcept;

  // Return the farthest ancestor of this region, in terms of linkage. Often this
  // just returns a `PROC *` if this region is linked in to its procedure.
  Node<ProgramRegion> *Ancestor(void) noexcept;

  // Return the nearest enclosing region that is itself enclosed by an
  // induction.
  Node<ProgramRegion> *NearestRegionEnclosedByInduction(void) noexcept;

  // Find an ancestor node that's both shared by `this` and `that`.
  Node<ProgramRegion> *FindCommonAncestor(Node<ProgramRegion> *that) noexcept;

  // Make sure that `this` will execute before `that`.
  void ExecuteBefore(ProgramImpl *program, Node<ProgramRegion> *that) noexcept;

  // Make sure that `this` will execute after `that`.
  void ExecuteAfter(ProgramImpl *program, Node<ProgramRegion> *that) noexcept;

  // Make sure that `this` will execute alongside `that`.
  void ExecuteAlongside(ProgramImpl *program,
                        Node<ProgramRegion> *that) noexcept;

  // Every child REGION of a procedure will have easy access to create new
  // variables.
  Node<ProgramProcedure> *containing_procedure;
  Node<ProgramRegion> *parent{nullptr};

  // Maps `QueryColumn::Id()` values to variables. Used to provide lexical
  // scoping of variables.
  //
  // NOTE(pag): Only valid before optimization, during the building of the
  //            control flow IR.
  std::unordered_map<unsigned, VAR *> col_id_to_var;

  // A comment about the creation of this node.
  std::string comment;

  unsigned cached_depth{0};
};

using REGION = Node<ProgramRegion>;

struct RegionRef : public UseRef<REGION> {
#ifndef NDEBUG
 private:
  REGION *const self;

 public:
  RegionRef(REGION *parent_) : self(parent_) {}
#else
  RegionRef(REGION *) {}
#endif

  void Emplace(REGION *parent, REGION *child) {
    assert(child->parent == self);
    this->UseRef<REGION>::Emplace(parent, child);
  }
};


enum class ProgramOperation {
  kInvalid,

  kClearVectorBeforePrimaryFlowFunction,

  // Insert into a table. Can be interpreted as conditional (a runtime may
  // choose to check if the insert is new or not). If the insert succeeds, then
  // execution descends into `body`.
  kInsertIntoTable,

  // Check the state of a tuple from a table. This executes one of three
  // bodies: `body` if the tuple is present, `absent_body` if the tuple is
  // absent, and `unknown_body` if the tuple may have been deleted.
  kCheckStateInTable,

  // When dealing with MERGE/UNION nodes with an inductive cycle.
  kAppendToInductionVector,
  kLoopOverInductionVector,
  kClearInductionVector,
  kSwapInductionVector,
  kSortAndUniqueInductionVector,

  // When dealing with a MERGE/UNION node that isn't part of an inductive
  // cycle.
  kAppendUnionInputToVector,
  kLoopOverUnionInputVector,
  kClearUnionInputVector,

  // Check if a row exists in a view. The tuple values are found in
  // `operands` and the table being used is `views[0]`.
  kCheckTupleIsPresentInTable,
  kCheckTupleIsNotPresentInTable,

  // Pairwise comparison between variables in two tuples.
  kCompareTuples,

  // Used by operations related to equi-joins over one or more tables.
  kJoinTables,
  kAppendJoinPivotsToVector,
  kSortAndUniquePivotVector,
  kClearJoinPivotVector,

  // Used to implement the cross-product of some tables.
  kCrossProduct,
  kAppendToProductInputVector,
  kSortAndUniqueProductInputVector,
  kClearProductInputVector,

  // Used to implement table/index scanning.
  kScanTable,
  kLoopOverScanVector,
  kClearScanVector,

  // Used to implement publication of messages that can be published with
  // additions or removals.
  kAppendToMessageOutputVector,
  kSortAndUniqueMessageOutputVector,
  kClearMessageOutputVector,
  kLoopOverMessageOutputVector,

  // Loop over a vector of inputs. The format of the vector is based off of
  // the variables in `variables`. The region `body` is executed for each
  // loop iteration.
  kLoopOverInputVector,

  // Call a filter functor, stored in `functor`, that is a functor where all
  // parameters are `bound`-attributed. These functors are interpreted as
  // predicates returning `true` or `false`. If `true` is returned, then
  // descend into `body`.
  kCallFilterFunctor,

  // Call a normal functor, stored in `functor`, where there is at least one
  // free parameter to the functor that it must generate. If anything is
  // generated, then descend into `body`.
  kCallFunctor,

  // Publish a message.
  kPublishMessage,
  kPublishMessageRemoval,

  // Creates a let binding, which assigns uses of variables to definitions of
  // variables. In practice, let bindings are eliminated during the process
  // of optimization.
  kLetBinding,

  // Computes a worker ID by hashing a bunch of variables.
  kWorkerId,

  // Used to test reference count variables associated with `QueryCondition`
  // nodes in the data flow.
  kTestAndAdd,
  kTestAndSub,

  // Call another procedure.
  kCallProcedure,

  // Return from a procedure.
  kReturnTrueFromProcedure,
  kReturnFalseFromProcedure,

  // TODO: use in future.

  // Test/set a global boolean variable to `true`. The variable is
  // `variables[0]`. The usage is for cross products. If we've ever executed a
  // lazy cross-product, then we must always visit it.
  kTestGlobalVariableIsTrue,
  kSetGlobalVariableToTrue,

  // Merge two values into an updated value when using `mutable`-attributed
  // parameters.
  kCallMergeFunctor,
};

// A generic operation.
template <>
class Node<ProgramOperationRegion> : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);
  explicit Node(REGION *parent_, ProgramOperation op_);

  virtual Node<ProgramCallRegion> *AsCall(void) noexcept;
  virtual Node<ProgramReturnRegion> *AsReturn(void) noexcept;
  virtual Node<ProgramTestAndSetRegion> *AsTestAndSet(void) noexcept;
  virtual Node<ProgramGenerateRegion> *AsGenerate(void) noexcept;
  virtual Node<ProgramLetBindingRegion> *AsLetBinding(void) noexcept;
  virtual Node<ProgramPublishRegion> *AsPublish(void) noexcept;
  virtual Node<ProgramTransitionStateRegion> *AsTransitionState(void) noexcept;
  virtual Node<ProgramCheckStateRegion> *AsCheckState(void) noexcept;
  virtual Node<ProgramTableJoinRegion> *AsTableJoin(void) noexcept;
  virtual Node<ProgramTableProductRegion> *AsTableProduct(void) noexcept;
  virtual Node<ProgramTableScanRegion> *AsTableScan(void) noexcept;
  virtual Node<ProgramTupleCompareRegion> *AsTupleCompare(void) noexcept;
  virtual Node<ProgramVectorLoopRegion> *AsVectorLoop(void) noexcept;
  virtual Node<ProgramVectorAppendRegion> *AsVectorAppend(void) noexcept;
  virtual Node<ProgramVectorClearRegion> *AsVectorClear(void) noexcept;
  virtual Node<ProgramVectorSwapRegion> *AsVectorSwap(void) noexcept;
  virtual Node<ProgramVectorUniqueRegion> *AsVectorUnique(void) noexcept;
  virtual Node<ProgramWorkerIdRegion> *AsWorkerId(void) noexcept;

  Node<ProgramOperationRegion> *AsOperation(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  ProgramOperation op;

  // If this operation does something conditional then this is the body it
  // executes.
  RegionRef body;
};

using OP = Node<ProgramOperationRegion>;

// A let binding, i.e. an assignment of zero or more variables. Variables
// are assigned pairwise from `used_vars` into `defined_vars`.
template <>
class Node<ProgramLetBindingRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  inline Node(REGION *parent_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kLetBinding),
        defined_vars(this),
        used_vars(this) {}

  Node<ProgramLetBindingRegion> *AsLetBinding(void) noexcept override;

  // Local variables that are defined/used in the body of this procedure.
  DefList<VAR> defined_vars;
  UseList<VAR> used_vars;

  std::optional<QueryView> view;
};

using LET = Node<ProgramLetBindingRegion>;

// Computes a worker ID by hashing one or more variables.
template <>
class Node<ProgramWorkerIdRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  inline Node(REGION *parent_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kWorkerId),
        hashed_vars(this) {}

  Node<ProgramWorkerIdRegion> *AsWorkerId(void) noexcept override;

  // Local variables that are hashed together to compute `worker_id`.
  UseList<VAR> hashed_vars;

  // Variable storing the hashed result.
  std::unique_ptr<VAR> worker_id;
};

using WORKERID = Node<ProgramWorkerIdRegion>;

// Loop over the vector `vector` and bind the extracted tuple elements into
// the variables specified in `defined_vars`.
template <>
class Node<ProgramVectorLoopRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  inline Node(unsigned id_, REGION *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        id(id_),
        defined_vars(this) {}

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramVectorLoopRegion> *AsVectorLoop(void) noexcept override;

  // ID of this region.
  const unsigned id;

  // Local variables bound to the vector being looped.
  DefList<VAR> defined_vars;

  // Vector being looped.
  UseRef<VECTOR> vector;

  // Optional ID of the target worker thread.
  UseRef<VAR> worker_id;
};

using VECTORLOOP = Node<ProgramVectorLoopRegion>;

// Append a tuple into a vector. The elements in the tuple must match the
// element/column types of the vector.
template <>
class Node<ProgramVectorAppendRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  inline Node(REGION *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        tuple_vars(this) {}

  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramVectorAppendRegion> *AsVectorAppend(void) noexcept override;

  UseList<VAR> tuple_vars;
  UseRef<VECTOR> vector;

  // Optional ID of the target worker thread.
  UseRef<VAR> worker_id;
};

using VECTORAPPEND = Node<ProgramVectorAppendRegion>;

// Clear a vector.
template <>
class Node<ProgramVectorClearRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  using Node<ProgramOperationRegion>::Node;

  void Accept(ProgramVisitor &visitor) override;

  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramVectorClearRegion> *AsVectorClear(void) noexcept override;

  UseRef<VECTOR> vector;

  // Optional ID of the target worker thread.
  UseRef<VAR> worker_id;
};

using VECTORCLEAR = Node<ProgramVectorClearRegion>;

// Swap the contents of two vectors.
template <>
class Node<ProgramVectorSwapRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  using Node<ProgramOperationRegion>::Node;

  void Accept(ProgramVisitor &visitor) override;

  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;
  Node<ProgramVectorSwapRegion> *AsVectorSwap(void) noexcept override;

  UseRef<VECTOR> lhs;
  UseRef<VECTOR> rhs;
};

using VECTORSWAP = Node<ProgramVectorSwapRegion>;

// Sort and unique a vector.
template <>
class Node<ProgramVectorUniqueRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  using Node<ProgramOperationRegion>::Node;

  void Accept(ProgramVisitor &visitor) override;

  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramVectorUniqueRegion> *AsVectorUnique(void) noexcept override;

  UseRef<VECTOR> vector;

  // Optional ID of the target worker thread.
  UseRef<VAR> worker_id;
};

using VECTORUNIQUE = Node<ProgramVectorUniqueRegion>;

// Set the state of a tuple in a view. In the simplest case, this behaves like
// a SQL `INSERT` statement: it says that some data exists in a relation. There
// are two other states that can be set: absent, which is like a `DELETE`, and
// unknown, which has no SQL equivalent, but it like a tentative `DELETE`. An
// unknown tuple is one which has been speculatively marked as deleted, and
// needs to be re-proven in order via alternate means in order for it to be
// used.
template <>
class Node<ProgramTransitionStateRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_, TupleState from_state_,
              TupleState to_state_)
      : Node<ProgramOperationRegion>(parent_,
                                     ProgramOperation::kInsertIntoTable),
        col_values(this),
        failed_body(this),
        from_state(from_state_),
        to_state(to_state_) {}

  void Accept(ProgramVisitor &visitor) override;

  Node<ProgramTransitionStateRegion> *AsTransitionState(void) noexcept override;

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Variables that make up the tuple.
  UseList<VAR> col_values;

  // View into which the tuple is being inserted.
  UseRef<TABLE> table;

  // If we failed to change the state, then execute this body.
  RegionRef failed_body;

  const TupleState from_state;
  const TupleState to_state;
};

using CHANGESTATE = Node<ProgramTransitionStateRegion>;

// Check the state of a tuple. This is sort of like asking if something exists,
// but has three conditionally executed children, based off of the state.
// One state is that the tuple os missing from a view. The second state is
// that the tuple is present in the view. The final state is that we are
// not sure if the tuple is present or absent, because it has been marked
// as a candidate for deletion, and thus we need to re-prove it.
template <>
class Node<ProgramCheckStateRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_)
      : Node<ProgramOperationRegion>(parent_,
                                     ProgramOperation::kCheckStateInTable),
        col_values(this),
        absent_body(this),
        unknown_body(this) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  Node<ProgramCheckStateRegion> *AsCheckState(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  // Variables that make up the tuple.
  UseList<VAR> col_values;

  // View into which the tuple is being inserted.
  UseRef<TABLE> table;

  // Region that is conditionally executed if the tuple is not present.
  RegionRef absent_body;

  // Region that is conditionally executed if the tuple was deleted and hasn't
  // been re-checked.
  RegionRef unknown_body;
};

using CHECKSTATE = Node<ProgramCheckStateRegion>;

// Calls another IR procedure. All IR procedures return `true` or `false`. This
// return value can be tested, and if it is, a body can be conditionally
// executed based off of the result of that test.
template <>
class Node<ProgramCallRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  Node(unsigned id_, Node<ProgramRegion> *parent_,
       Node<ProgramProcedure> *called_proc_,
       ProgramOperation op_ = ProgramOperation::kCallProcedure);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramCallRegion> *AsCall(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Procedure being called.
  UseRef<Node<ProgramProcedure>, REGION> called_proc;

  // Variables passed as arguments.
  UseList<VAR> arg_vars;

  // Vectors passed as arguments.
  UseList<VECTOR> arg_vecs;

  // If the `call` returns `true`, then `body` is executed, otherwise if it
  // returns `false` then `false_body` is executed.
  RegionRef false_body;

  const unsigned id;
};

using CALL = Node<ProgramCallRegion>;

// Returns true/false from a procedure.
template <>
class Node<ProgramReturnRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  Node(Node<ProgramRegion> *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramReturnRegion> *AsReturn(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;
};

using RETURN = Node<ProgramReturnRegion>;

// Publishes a message to the pub/sub.
template <>
class Node<ProgramPublishRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  Node(Node<ProgramRegion> *parent_, ParsedMessage message_, unsigned id_,
       ProgramOperation op_ = ProgramOperation::kPublishMessage)
      : Node<ProgramOperationRegion>(parent_, op_),
        message(message_),
        id(id_),
        arg_vars(this) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramPublishRegion> *AsPublish(void) noexcept override;

  // Message being published.
  const ParsedMessage message;

  // ID of this node.
  const unsigned id;

  // Variables passed as arguments.
  UseList<VAR> arg_vars;
};

using PUBLISH = Node<ProgramPublishRegion>;

// Represents a positive or negative existence check.
template <>
class Node<ProgramTestAndSetRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_) {}

  void Accept(ProgramVisitor &visitor) override;

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramTestAndSetRegion> *AsTestAndSet(void) noexcept override;

  // The variables are used as `(src_dest OP= update_val) == comapre_val`.
  UseRef<VAR> accumulator;
  UseRef<VAR> displacement;
  UseRef<VAR> comparator;
};

using TESTANDSET = Node<ProgramTestAndSetRegion>;

// An equi-join between two or more tables.
template <>
class Node<ProgramTableJoinRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, QueryJoin query_join_, unsigned id_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kJoinTables),
        query_join(query_join_),
        id(id_),
        tables(this),
        indices(this),
        pivot_vars(this),
        pivot_cols() {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramTableJoinRegion> *AsTableJoin(void) noexcept override;

  const QueryJoin query_join;
  const unsigned id;

  UseList<TABLE> tables;

  // NOTE(pag): There might be fewer indices than tables. If the Nth table's
  //            index is not present then `index_of_index[N]` will have a value
  //            of `0`, otherwise its index can be found as:
  //
  //                    indices[index_of_index[N] - 1u]
  //
  //            The only case where an index is absent is when it covers all
  //            columns of a table.
  std::vector<unsigned> index_of_index;
  UseList<TABLEINDEX> indices;

  UseRef<VECTOR> pivot_vec;

  // There is a `1:N` correspondence between `pivot_vars` and `pivot_cols`.
  DefList<VAR> pivot_vars;
  std::vector<UseList<TABLECOLUMN>> pivot_cols;

  // There is a 1:1 correspondence between `output_vars` and columns in the
  // selected tables. Not all of those columns will necessarily be used.
  std::vector<DefList<VAR>> output_vars;
  std::vector<UseList<TABLECOLUMN>> output_cols;
};

using TABLEJOIN = Node<ProgramTableJoinRegion>;

// A cross product between two or more tables.
template <>
class Node<ProgramTableProductRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, QueryJoin query_join_, unsigned id_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kCrossProduct),
        query_join(query_join_),
        tables(this),
        input_vecs(this),
        id(id_) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramTableProductRegion> *AsTableProduct(void) noexcept override;

  const QueryJoin query_join;

  UseList<TABLE> tables;
  UseList<VECTOR> input_vecs;
  std::vector<DefList<VAR>> output_vars;
  const unsigned id;
};

using TABLEPRODUCT = Node<ProgramTableProductRegion>;

// Perform a scan over a table, possibly using an index. If an index is being
// used the input variables are provided to perform equality matching against
// column values. The results of the scan fill a vector.
template <>
class Node<ProgramTableScanRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(unsigned id_, Node<ProgramRegion> *parent_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kScanTable),
        id(id_),
        out_cols(this),
        in_cols(this),
        in_vars(this),
        out_vars(this) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramTableScanRegion> *AsTableScan(void) noexcept override;

  const unsigned id;

  UseRef<TABLE> table;
  UseList<TABLECOLUMN> out_cols;

  UseRef<TABLEINDEX> index;
  UseList<TABLECOLUMN> in_cols;

  // One variable for each column in `in_cols`.
  UseList<VAR> in_vars;

  // Output variables, one per column in the table!
  DefList<VAR> out_vars;
};

using TABLESCAN = Node<ProgramTableScanRegion>;

// Comparison between two tuples.
template <>
class Node<ProgramTupleCompareRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, ComparisonOperator op_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kCompareTuples),
        cmp_op(op_),
        lhs_vars(this),
        rhs_vars(this),
        false_body(this) {
    assert(cmp_op != ComparisonOperator::kNotEqual);
  }

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramTupleCompareRegion> *AsTupleCompare(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  const ComparisonOperator cmp_op;
  UseList<VAR> lhs_vars;
  UseList<VAR> rhs_vars;

  // Optional body executed if the comparison fails.
  RegionRef false_body;
};

using TUPLECMP = Node<ProgramTupleCompareRegion>;

// Calling a functor.
template <>
class Node<ProgramGenerateRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, ParsedFunctor functor_,
              unsigned id_)
      : Node<ProgramOperationRegion>(
            parent_, functor_.IsFilter() ? ProgramOperation::kCallFilterFunctor
                                         : ProgramOperation::kCallFunctor),
        functor(functor_),
        id(id_),
        defined_vars(this),
        used_vars(this),
        empty_body(this) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramGenerateRegion> *AsGenerate(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  const ParsedFunctor functor;

  // Unique ID of this node. Useful during codegen to ensure we can count the
  // results of one generate without it interfering with the count of a nested
  // generate.
  const unsigned id;

  // Free variables that are generated from the application of the functor.
  DefList<VAR> defined_vars;

  // Bound variables passed in as arguments to the functor.
  UseList<VAR> used_vars;

  // If the `functor` produces results, then `body` is executed, otherwise if it
  // doesn't produce results then `empty_body` is executed.
  RegionRef empty_body;
};

using GENERATOR = Node<ProgramGenerateRegion>;

// A procedure region. This represents some entrypoint of data into the program.
template <>
class Node<ProgramProcedure> : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);

  Node(unsigned id_, ProcedureKind kind_);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramProcedure> *AsProcedure(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Create a new vector in this procedure for a list of columns.
  VECTOR *VectorFor(ProgramImpl *impl, VectorKind kind,
                    DefinedNodeRange<QueryColumn> cols);

  const unsigned id;
  const ProcedureKind kind;

  std::optional<QueryIO> io;

  // Temporary tables within this procedure.
  DefList<TABLE> tables;

  // Body of this procedure. Initially starts with a loop over an implicit
  // input vector.
  RegionRef body;

  // Input vectors and variables.
  DefList<VECTOR> input_vecs;
  DefList<VAR> input_vars;

  // Vectors defined in this procedure. If this is a vector procedure then
  // the first vector is the input vector.
  DefList<VECTOR> vectors;

  // Are we currently checking if this procedure is being used? This is to
  // avoid infinite recursion when doing a procedure call NoOp checks.
  mutable bool checking_if_nop{false};

  // Do we need to keep this procedure around? This happens if we're holding
  // a raw, non-`UseRef`/`UseList` pointer to this procedure, such as inside
  // of `ProgramQuery` specifications.
  bool has_raw_use{false};
};

using PROC = Node<ProgramProcedure>;

// A series region is where the `regions[N]` must finish before `regions[N+1]`
// begins.
template <>
class Node<ProgramSeriesRegion> final : public Node<ProgramRegion> {
 public:
  Node(REGION *parent_);

  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  inline void AddRegion(REGION *child) {
    assert(child->parent == this);
    regions.AddUse(child);
  }

  Node<ProgramSeriesRegion> *AsSeries(void) noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using SERIES = Node<ProgramSeriesRegion>;

// A region where multiple things can happen in parallel.
template <>
class Node<ProgramParallelRegion> final : public Node<ProgramRegion> {
 public:
  Node(REGION *parent_);

  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  inline void AddRegion(REGION *child) {
    assert(child->parent == this);
    regions.AddUse(child);
  }

  Node<ProgramParallelRegion> *AsParallel(void) noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using PARALLEL = Node<ProgramParallelRegion>;

// An induction is a loop centred on a `QueryMerge` node. Some of the views
// incoming to that `QueryMerge` are treated as "inputs", as they bring initial
// data into the `QueryMerge`. Other nodes are treated as "inductions" as they
// cycle back to the `QueryMerge`.
template <>
class Node<ProgramInductionRegion> final : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  explicit Node(ProgramImpl *impl, REGION *parent_);
  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, Node<ProgramRegion> *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<Node<ProgramRegion> *> &merges) override;

  Node<ProgramInductionRegion> *AsInduction(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Initial regions that fill up one or more of the inductive vectors.
  RegionRef init_region;

  // The cyclic regions of this induction. This is a PARALLEL region.
  RegionRef cyclic_region;

  // The output regions of this induction. This is a PARALLEL region.
  RegionRef output_region;

  // Vectors built up by this induction.
  UseList<VECTOR> vectors;

  // It could be the case that a when going through the induction we end up
  // going into a co-mingled induction, as is the case in
  // `transitive_closure2.dr` and `transitive_closure3.dr`. Thus, we have
  // multiple vectors which must be maintained during an induction.

  // This is the cycle vector, i.e. each iteration of the induction's fixpoint
  // loop operates on this vector. The init region of an induction fills this
  // vector.
  std::unordered_map<QueryView, VECTOR *> view_to_add_vec;
  std::unordered_map<QueryView, VECTOR *> view_to_remove_vec;

  // This is the swap vector. Inside of a fixpoint loop, we swap this with the
  // normal vector, so that the current iteration of the loop can append to
  // the normal vector, while still allowing us to iterate over what was added
  // from the prior iteration of the fixpoint loop.
  std::unordered_map<QueryView, VECTOR *> view_to_swap_vec;

  // We try to share swap vectors as much as possible.
  std::unordered_map<std::string, VECTOR *> col_types_to_swap_vec;

  // This is the output vector; it accumulates everything from all iterations
  // of the fixpoint loop.
  std::unordered_map<QueryView, VECTOR *> view_to_output_vec;

  // List of append to vector regions inside this induction.
  std::vector<REGION *> init_appends_add;
  std::vector<REGION *> init_appends_remove;
  std::vector<OP *> cycle_appends;

  std::unordered_map<QueryView, PARALLEL *> output_add_cycles;
  std::unordered_map<QueryView, PARALLEL *> output_remove_cycles;

  std::unordered_map<QueryView, PARALLEL *> fixpoint_add_cycles;
  std::unordered_map<QueryView, PARALLEL *> fixpoint_remove_cycles;

  const unsigned id;

  enum State {
    kAccumulatingInputRegions,
    kAccumulatingCycleRegions,
    kBuildingOutputRegions
  } state = kAccumulatingInputRegions;

  // Can this induction produce deletions?
  bool is_differential{false};

  // All of the UNIONs, JOINs, and NEGATEs of the induction.
  std::vector<QueryView> views;
};

using INDUCTION = Node<ProgramInductionRegion>;

class ProgramImpl : public User {
 public:
  ~ProgramImpl(void);

  inline explicit ProgramImpl(Query query_, IRFormat format_)
      : User(this),
        query(query_),
        format(format_),
        query_checkers(this),
        procedure_regions(this),
        series_regions(this),
        parallel_regions(this),
        induction_regions(this),
        operation_regions(this),
        join_regions(this),
        tables(this),
        global_vars(this),
        const_vars(this),
        zero(const_vars.Create(next_id++, VariableRole::kConstantZero)),
        one(const_vars.Create(next_id++, VariableRole::kConstantOne)),
        false_(const_vars.Create(next_id++, VariableRole::kConstantFalse)),
        true_(const_vars.Create(next_id++, VariableRole::kConstantTrue)) {}

  void Optimize(void);

  // The data flow representation from which this was created.
  const Query query;

  // The format of the IR.
  const IRFormat format;

  // Globally numbers things like procedures, variables, vectors, etc.
  unsigned next_id{0u};

  // List of query entry points.
  std::vector<ProgramQuery> queries;
  UseList<REGION> query_checkers;

  DefList<PROC> procedure_regions;
  DefList<SERIES> series_regions;
  DefList<PARALLEL> parallel_regions;
  DefList<INDUCTION> induction_regions;
  DefList<OP> operation_regions;
  DefList<TABLEJOIN> join_regions;
  DefList<TABLE> tables;

  // List of variables associated with globals (e.g. reference counts).
  DefList<VAR> global_vars;

  // List of variables associated with constants.
  DefList<VAR> const_vars;

  VAR *const zero;
  VAR *const one;
  VAR *const false_;
  VAR *const true_;

  // Maps constants to their global variables.
  std::unordered_map<QueryConstant, VAR *> const_to_var;

  // Maps query conditions to the reference counts associated with them.
  std::unordered_map<QueryCondition, VAR *> cond_ref_counts;

  // Maps views whose outputs are all constants or constant references to
  // condition variables that tracker whether or not we need to actually re-
  // execute the successors.
  std::unordered_map<QueryView, VAR *> const_view_to_var;

  // We build up "data models" of views that can share the same backing storage.
  std::vector<std::unique_ptr<DataModel>> models;
  std::unordered_map<QueryView, DataModel *> view_to_model;
};

}  // namespace hyde
