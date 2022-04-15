// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Parse/Type.h>
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
class ProgramVisitor;

class DataColumnImpl;
class DataIndexImpl;
class DataTableImpl;
class DataRecordImpl;
class DataRecordCaseImpl;

class ProgramImpl;

// A column within a table.
class DataColumnImpl final : public Def<DataColumnImpl>, public User {
 public:
  virtual ~DataColumnImpl(void);

  DataColumnImpl(unsigned id_, const TypeLoc &type_, DataTableImpl *table_);

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const unsigned index;
  const TypeLoc type;

  std::vector<Token> names;

  WeakUseRef<DataTableImpl> table;
};

using TABLECOLUMN = DataColumnImpl;

// Represents an index on some subset of columns in a table.
class DataIndexImpl final : public Def<DataIndexImpl>, public User {
 public:
  virtual ~DataIndexImpl(void);

  DataIndexImpl(unsigned id_, DataTableImpl *table_, std::string column_spec_);

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const std::string column_spec;

  UseList<TABLECOLUMN> columns;
  UseList<TABLECOLUMN> mapped_columns;

  WeakUseRef<DataTableImpl> table;
};

using TABLEINDEX = DataIndexImpl;

class Context;

// Represents a table of data.
//
// NOTE(pag): By default all tables already have a UNIQUE index on them.
class DataTableImpl final : public Def<DataTableImpl>, public User {
 public:
  virtual ~DataTableImpl(void);

  DataTableImpl(unsigned id_);

  void Accept(ProgramVisitor &visitor);

  // Get or create a table in the program.
  static DataTableImpl *GetOrCreate(ProgramImpl *impl, Context &context,
                                    QueryView view);

  // Get or create an index on the table.
  TABLEINDEX *GetOrCreateIndex(ProgramImpl *impl, std::vector<unsigned> cols);

  const unsigned id;

  // List of defined columns.
  DefList<TABLECOLUMN> columns;

  // Indexes that should be created on this table. By default, all tables have
  // a UNIQUE index.
  DefList<TABLEINDEX> indices;

  // Records associated with this table.
  DefList<DataRecordImpl> records;

  // All views sharing this table.
  std::vector<QueryView> views;
};

using TABLE = DataTableImpl;

struct DataModel : public DisjointSet {
 public:
  TABLE *table{nullptr};
};

// A vector of tuples in the program.
class DataVectorImpl final : public Def<DataVectorImpl> {
 public:
  DataVectorImpl(unsigned id_, VectorKind kind_,
                 const std::vector<TypeLoc> &col_types_, int)
      : Def<DataVectorImpl>(this),
        id(id_),
        kind(kind_),
        col_types(col_types_) {}

  template <typename ColList>
  DataVectorImpl(unsigned id_, VectorKind kind_, ColList &&cols)
      : Def<DataVectorImpl>(this),
        id(id_),
        kind(kind_) {

    for (QueryColumn col : cols) {
      col_types.push_back(col.Type());
    }
  }

  DataVectorImpl(DataVectorImpl *that_)
      : Def<DataVectorImpl>(this),
        id(that_->id),
        kind(that_->kind),
        col_types(that_->col_types) {}

  bool IsRead(void) const;

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const VectorKind kind;
  std::vector<TypeLoc> col_types;

  std::optional<ParsedMessage> added_message;
  std::optional<ParsedMessage> removed_message;

  // `true` if this vector must have variants of itself sharded across workers.
  bool is_sharded{false};
};

using VECTOR = DataVectorImpl;

// A variable in the program. This could be a procedure parameter or a local
// variable.
class DataVariableImpl final : public Def<DataVariableImpl> {
 public:
  explicit DataVariableImpl(unsigned id_, VariableRole role_);

  void Accept(ProgramVisitor &visitor);

  const VariableRole role;

  // Unique ID for this variable. Unrelated to column IDs.
  const unsigned id;

  // NOTE(pag): Only valid after optimization when building the control flow
  //            IR is complete.
  ProgramRegionImpl *defining_region{nullptr};

  inline unsigned Sort(void) const noexcept {
    return id;
  }

  bool IsGlobal(void) const noexcept;

  bool IsConstant(void) const noexcept;

  TypeLoc Type(void) const noexcept;

  std::optional<QueryConstant> query_const;
  std::optional<QueryColumn> query_column;
  std::optional<QueryCondition> query_cond;
  std::optional<ParsedParameter> parsed_param;
};

using VAR = DataVariableImpl;

struct RecordColumn {
  RecordColumn(void) = default;

  RecordColumn &operator=(RecordColumn &&that) noexcept {
    derived_index = that.derived_index;
    derived_offset = that.derived_offset;
    column.Swap(that.column);
    var.Swap(that.var);
    return *this;
  }

  RecordColumn(RecordColumn &&that) noexcept
      : derived_index(that.derived_index),
        derived_offset(that.derived_offset) {
    column.Swap(that.column);
    var.Swap(that.var);
  }

  // If this column is derived from another record, then what index in
  // `derived_from` is it from, and then what offset (column) within that
  // record.
  unsigned derived_index{~0u};
  unsigned derived_offset{~0u};

  UseRef<TABLECOLUMN> column;
  UseRef<VAR> var;
};

// A record case is a particular instantiation or variant of a record.
// A record might have multiple cases.
class DataRecordCaseImpl : public Def<DataRecordCaseImpl>, public User {
 public:
  DataRecordCaseImpl(unsigned id_);

  const unsigned id;

  UseList<DataRecordImpl> derived_from;

  std::vector<RecordColumn> columns;

  WeakUseRef<DataRecordImpl> record;

 private:
  DataRecordCaseImpl(void) = delete;
};

using DATARECORDCASE = DataRecordCaseImpl;

// A record is an abstraction over a persisted tuple. The storage for the
// record is implemented in terms of one or more cases.
class DataRecordImpl : public Def<DataRecordImpl>, public User {
 public:
  explicit DataRecordImpl(unsigned id_, TABLE *table_);

  const unsigned id;
  UseList<DATARECORDCASE> cases;
  WeakUseRef<TABLE> table;

 private:
  DataRecordImpl(void) = delete;
};

using DATARECORD = DataRecordImpl;

class ProgramOperationRegionImpl;

// A lexically scoped region in the program.
class ProgramRegionImpl : public Def<ProgramRegionImpl>, public User {
 public:
  virtual ~ProgramRegionImpl(void);
  explicit ProgramRegionImpl(ProgramProcedureImpl *containing_procedure_, bool);
  explicit ProgramRegionImpl(ProgramRegionImpl *parent_);

  virtual void Accept(ProgramVisitor &visitor) = 0;
  virtual uint64_t Hash(uint32_t depth) const = 0;

  virtual ProgramProcedureImpl *AsProcedure(void) noexcept;
  virtual ProgramOperationRegionImpl *AsOperation(void) noexcept;
  virtual ProgramSeriesRegionImpl *AsSeries(void) noexcept;
  virtual ProgramParallelRegionImpl *AsParallel(void) noexcept;
  virtual ProgramInductionRegionImpl *AsInduction(void) noexcept;

  // Find the nearest containing mode switch.
  ProgramModeSwitchRegionImpl *ContainingModeSwitch(void) noexcept;

  // Returns `true` if all paths through `this` ends with a `return` region.
  virtual bool EndsWithReturn(void) const noexcept = 0;

  inline void ReplaceAllUsesWith(ProgramRegionImpl *that) {
    this->Def<ProgramRegionImpl>::ReplaceAllUsesWith(that);
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
                                std::vector<ProgramRegionImpl *> &merges);

  // Gets or creates a local variable in the procedure.
  DataVariableImpl *VariableFor(ProgramImpl *impl, QueryColumn col);
  DataVariableImpl *VariableForRec(QueryColumn col);

  // Returns the lexical level of this node.
  unsigned Depth(void) const noexcept;

  // Returns the lexical level of this node.
  unsigned CachedDepth(void) noexcept;

  // Returns `true` if this region is a no-op.
  virtual bool IsNoOp(void) const noexcept;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming) after searching down `depth` levels or until leaf,
  // whichever is first, and where `depth` is 0, compare `this` to `that.
  virtual bool Equals(EqualitySet &eq, ProgramRegionImpl *that,
                      uint32_t depth) const noexcept;

  // Return the farthest ancestor of this region, in terms of linkage. Often this
  // just returns a `PROC *` if this region is linked in to its procedure.
  ProgramRegionImpl *Ancestor(void) noexcept;

  // Return the nearest enclosing region that is itself enclosed by an
  // induction.
  ProgramRegionImpl *NearestRegionEnclosedByInduction(void) noexcept;

  // Find an ancestor node that's both shared by `this` and `that`.
  ProgramRegionImpl *FindCommonAncestor(ProgramRegionImpl *that) noexcept;

  // Make sure that `this` will execute before `that`.
  void ExecuteBefore(ProgramImpl *program, ProgramRegionImpl *that) noexcept;

  // Make sure that `this` will execute after `that`.
  void ExecuteAfter(ProgramImpl *program, ProgramRegionImpl *that) noexcept;

  // Make sure that `this` will execute alongside `that`.
  void ExecuteAlongside(ProgramImpl *program,
                        ProgramRegionImpl *that) noexcept;

  // Every child REGION of a procedure will have easy access to create new
  // variables.
  ProgramProcedureImpl *containing_procedure;
  ProgramRegionImpl *parent{nullptr};

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

using REGION = ProgramRegionImpl;

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

  // Used for queries that force the injection of an internal message to
  // "unlock" computation that the query can produce results for the query
  // to observe / report on.
  kAppendQueryParamsToMessageInjectVector,

  // Insert into a table. Can be interpreted as conditional (a runtime may
  // choose to check if the insert is new or not). If the insert succeeds, then
  // execution descends into `body`.
  kInsertIntoTable,
  kEmplaceIntoTable,

  // Check the state of a tuple from a table. This executes one of three
  // bodies: `body` if the tuple is present, `absent_body` if the tuple is
  // absent, and `unknown_body` if the tuple may have been deleted.
  kCheckTupleInTable,
  kCheckRecordFromTable,

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

  // Switch modes, telling us the general intention of the containing code.
  kModeSwitch,

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
class ProgramOperationRegionImpl : public REGION {
 public:
  virtual ~ProgramOperationRegionImpl(void);
  explicit ProgramOperationRegionImpl(REGION *parent_, ProgramOperation op_);

  virtual ProgramCallRegionImpl *AsCall(void) noexcept;
  virtual ProgramReturnRegionImpl *AsReturn(void) noexcept;
  virtual ProgramTestAndSetRegionImpl *AsTestAndSet(void) noexcept;
  virtual ProgramGenerateRegionImpl *AsGenerate(void) noexcept;
  virtual ProgramModeSwitchRegionImpl *AsModeSwitch(void) noexcept;
  virtual ProgramLetBindingRegionImpl *AsLetBinding(void) noexcept;
  virtual ProgramPublishRegionImpl *AsPublish(void) noexcept;
  virtual ProgramChangeTupleRegionImpl *AsChangeTuple(void) noexcept;
  virtual ProgramChangeRecordRegionImpl *AsChangeRecord(void) noexcept;
  virtual ProgramCheckTupleRegionImpl *AsCheckTuple(void) noexcept;
  virtual ProgramCheckRecordRegionImpl *AsCheckRecord(void) noexcept;
  virtual ProgramTableJoinRegionImpl *AsTableJoin(void) noexcept;
  virtual ProgramTableProductRegionImpl *AsTableProduct(void) noexcept;
  virtual ProgramTableScanRegionImpl *AsTableScan(void) noexcept;
  virtual ProgramTupleCompareRegionImpl *AsTupleCompare(void) noexcept;
  virtual ProgramVectorLoopRegionImpl *AsVectorLoop(void) noexcept;
  virtual ProgramVectorAppendRegionImpl *AsVectorAppend(void) noexcept;
  virtual ProgramVectorClearRegionImpl *AsVectorClear(void) noexcept;
  virtual ProgramVectorSwapRegionImpl *AsVectorSwap(void) noexcept;
  virtual ProgramVectorUniqueRegionImpl *AsVectorUnique(void) noexcept;
  virtual ProgramWorkerIdRegionImpl *AsWorkerId(void) noexcept;

  ProgramOperationRegionImpl *AsOperation(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  ProgramOperation op;

  // If this operation does something conditional then this is the body it
  // executes.
  RegionRef body;
};

using OP = ProgramOperationRegionImpl;

// A region which semantically tells us we're swithing modes, e.g. to removing
// data, or to adding data.
class ProgramModeSwitchRegionImpl final : public OP {
 public:
  virtual ~ProgramModeSwitchRegionImpl(void);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  inline ProgramModeSwitchRegionImpl(REGION *parent_, Mode new_mode_)
      : OP(parent_, ProgramOperation::kModeSwitch),
        new_mode(new_mode_) {}

  const Mode new_mode;

  ProgramModeSwitchRegionImpl *AsModeSwitch(void) noexcept override;
};

using MODESWITCH = ProgramModeSwitchRegionImpl;

// A let binding, i.e. an assignment of zero or more variables. Variables
// are assigned pairwise from `used_vars` into `defined_vars`.
class ProgramLetBindingRegionImpl final : public OP {
 public:
  virtual ~ProgramLetBindingRegionImpl(void);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  inline ProgramLetBindingRegionImpl(REGION *parent_)
      : OP(parent_, ProgramOperation::kLetBinding),
        defined_vars(this),
        used_vars(this) {}

  ProgramLetBindingRegionImpl *AsLetBinding(void) noexcept override;

  // Local variables that are defined/used in the body of this procedure.
  DefList<VAR> defined_vars;
  UseList<VAR> used_vars;

  std::optional<QueryView> view;
};

using LET = ProgramLetBindingRegionImpl;

// Computes a worker ID by hashing one or more variables.
class ProgramWorkerIdRegionImpl final : public OP {
 public:
  virtual ~ProgramWorkerIdRegionImpl(void);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  inline ProgramWorkerIdRegionImpl(REGION *parent_)
      : OP(parent_, ProgramOperation::kWorkerId),
        hashed_vars(this) {}

  ProgramWorkerIdRegionImpl *AsWorkerId(void) noexcept override;

  // Local variables that are hashed together to compute `worker_id`.
  UseList<VAR> hashed_vars;

  // Variable storing the hashed result.
  std::unique_ptr<VAR> worker_id;
};

using WORKERID = ProgramWorkerIdRegionImpl;

// Loop over the vector `vector` and bind the extracted tuple elements into
// the variables specified in `defined_vars`.
class ProgramVectorLoopRegionImpl final : public OP {
 public:
  virtual ~ProgramVectorLoopRegionImpl(void);

  void Accept(ProgramVisitor &visitor) override;

  inline ProgramVectorLoopRegionImpl(
      unsigned id_, REGION *parent_, ProgramOperation op_)
      : OP(parent_, op_),
        id(id_),
        defined_vars(this) {}

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramVectorLoopRegionImpl *AsVectorLoop(void) noexcept override;

  // ID of this region.
  const unsigned id;

  // Local variables bound to the vector being looped.
  DefList<VAR> defined_vars;

  // Vector being looped.
  UseRef<VECTOR> vector;

  // Optional ID of the target worker thread.
  UseRef<VAR> worker_id;

  // If this is a loop over a table associated with an induction vector.
  UseRef<TABLE> induction_table;
};

using VECTORLOOP = ProgramVectorLoopRegionImpl;

// Append a tuple into a vector. The elements in the tuple must match the
// element/column types of the vector.
class ProgramVectorAppendRegionImpl final : public OP {
 public:
  virtual ~ProgramVectorAppendRegionImpl(void);

  void Accept(ProgramVisitor &visitor) override;

  inline ProgramVectorAppendRegionImpl(REGION *parent_, ProgramOperation op_)
      : OP(parent_, op_),
        tuple_vars(this) {}

  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramVectorAppendRegionImpl *AsVectorAppend(void) noexcept override;

  UseList<VAR> tuple_vars;
  UseRef<VECTOR> vector;

  // Optional ID of the target worker thread.
  UseRef<VAR> worker_id;
};

using VECTORAPPEND = ProgramVectorAppendRegionImpl;

// Clear a vector.
class ProgramVectorClearRegionImpl final : public OP {
 public:
  virtual ~ProgramVectorClearRegionImpl(void);

  using OP::ProgramOperationRegionImpl;

  void Accept(ProgramVisitor &visitor) override;

  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramVectorClearRegionImpl *AsVectorClear(void) noexcept override;

  UseRef<VECTOR> vector;

  // Optional ID of the target worker thread.
  UseRef<VAR> worker_id;
};

using VECTORCLEAR = ProgramVectorClearRegionImpl;

// Swap the contents of two vectors.
class ProgramVectorSwapRegionImpl final : public OP {
 public:
  virtual ~ProgramVectorSwapRegionImpl(void);
  using OP::ProgramOperationRegionImpl;

  void Accept(ProgramVisitor &visitor) override;

  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;
  ProgramVectorSwapRegionImpl *AsVectorSwap(void) noexcept override;

  UseRef<VECTOR> lhs;
  UseRef<VECTOR> rhs;
};

using VECTORSWAP = ProgramVectorSwapRegionImpl;

// Sort and unique a vector.
class ProgramVectorUniqueRegionImpl final : public OP {
 public:
  virtual ~ProgramVectorUniqueRegionImpl(void);
  using OP::ProgramOperationRegionImpl;

  void Accept(ProgramVisitor &visitor) override;

  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramVectorUniqueRegionImpl *AsVectorUnique(void) noexcept override;

  UseRef<VECTOR> vector;

  // Optional ID of the target worker thread.
  UseRef<VAR> worker_id;
};

using VECTORUNIQUE = ProgramVectorUniqueRegionImpl;

// Set the state of a tuple in a view. In the simplest case, this behaves like
// a SQL `INSERT` statement: it says that some data exists in a relation. There
// are two other states that can be set: absent, which is like a `DELETE`, and
// unknown, which has no SQL equivalent, but it like a tentative `DELETE`. An
// unknown tuple is one which has been speculatively marked as deleted, and
// needs to be re-proven in order via alternate means in order for it to be
// used.
class ProgramChangeTupleRegionImpl final : public OP {
 public:
  virtual ~ProgramChangeTupleRegionImpl(void);

  inline ProgramChangeTupleRegionImpl(REGION *parent_, TupleState from_state_,
                                      TupleState to_state_)
      : OP(parent_, ProgramOperation::kInsertIntoTable),
        col_values(this),
        failed_body(this),
        from_state(from_state_),
        to_state(to_state_) {}

  void Accept(ProgramVisitor &visitor) override;

  ProgramChangeTupleRegionImpl *AsChangeTuple(void) noexcept override;

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

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

using CHANGETUPLE = ProgramChangeTupleRegionImpl;

// This is similar to a `ProgramChangeTupleRegion`; however, it also
// creates new definitions for the variables which it is updating. The key
// idea is that this gets us the "record" associated with some tuple data,
// rather than us keeping with the tuple data itself.
class ProgramChangeRecordRegionImpl final : public OP {
 public:
  virtual ~ProgramChangeRecordRegionImpl(void);

  inline ProgramChangeRecordRegionImpl(unsigned id_, REGION *parent_,
                                       TupleState from_state_,
                                       TupleState to_state_)
      : OP(parent_, ProgramOperation::kEmplaceIntoTable),
        id(id_),
        col_values(this),
        record_vars(this),
        failed_body(this),
        from_state(from_state_),
        to_state(to_state_) {}

  void Accept(ProgramVisitor &visitor) override;

  ProgramChangeRecordRegionImpl *AsChangeRecord(void) noexcept override;

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  const unsigned id;

  // Variables that make up the tuple.
  UseList<VAR> col_values;

  // Output record variables from this state emplace.
  DefList<VAR> record_vars;

  // View into which the tuple is being inserted.
  UseRef<TABLE> table;

  // If we failed to change the state, then execute this body.
  RegionRef failed_body;

  const TupleState from_state;
  const TupleState to_state;
};

using CHANGERECORD = ProgramChangeRecordRegionImpl;

// Check the state of a tuple. This is sort of like asking if something exists,
// but has three conditionally executed children, based off of the state.
// One state is that the tuple os missing from a view. The second state is
// that the tuple is present in the view. The final state is that we are
// not sure if the tuple is present or absent, because it has been marked
// as a candidate for deletion, and thus we need to re-prove it.
class ProgramCheckTupleRegionImpl final : public OP {
 public:
  virtual ~ProgramCheckTupleRegionImpl(void);

  inline ProgramCheckTupleRegionImpl(REGION *parent_)
      : OP(parent_, ProgramOperation::kCheckTupleInTable),
        col_values(this),
        absent_body(this),
        unknown_body(this) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  ProgramCheckTupleRegionImpl *AsCheckTuple(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

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

using CHECKTUPLE = ProgramCheckTupleRegionImpl;

// This is like `ProgramCheckTupleRegion`, except that it operates on records,
// i.e. it defines new variables for what is being returned.
class ProgramCheckRecordRegionImpl final : public OP {
 public:
  virtual ~ProgramCheckRecordRegionImpl(void);

  inline ProgramCheckRecordRegionImpl(unsigned id_, REGION *parent_)
      : OP(parent_, ProgramOperation::kCheckRecordFromTable),
        id(id_),
        col_values(this),
        record_vars(this),
        absent_body(this),
        unknown_body(this) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  ProgramCheckRecordRegionImpl *AsCheckRecord(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  const unsigned id;

  // Variables that make up the tuple.
  UseList<VAR> col_values;

  // Defined variables from the record.
  DefList<VAR> record_vars;

  // View into which the tuple is being inserted.
  UseRef<TABLE> table;

  // Region that is conditionally executed if the tuple is not present.
  RegionRef absent_body;

  // Region that is conditionally executed if the tuple was deleted and hasn't
  // been re-checked.
  RegionRef unknown_body;
};

using CHECKRECORD = ProgramCheckRecordRegionImpl;

// Calls another IR procedure. All IR procedures return `true` or `false`. This
// return value can be tested, and if it is, a body can be conditionally
// executed based off of the result of that test.
class ProgramCallRegionImpl final : public OP {
 public:
  virtual ~ProgramCallRegionImpl(void);

  ProgramCallRegionImpl(
      unsigned id_, REGION *parent_, ProgramProcedureImpl *called_proc_,
      ProgramOperation op_ = ProgramOperation::kCallProcedure);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramCallRegionImpl *AsCall(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Procedure being called.
  UseRef<ProgramProcedureImpl, REGION> called_proc;

  // Variables passed as arguments.
  UseList<VAR> arg_vars;

  // Vectors passed as arguments.
  UseList<VECTOR> arg_vecs;

  // If the `call` returns `true`, then `body` is executed, otherwise if it
  // returns `false` then `false_body` is executed.
  RegionRef false_body;

  const unsigned id;
};

using CALL = ProgramCallRegionImpl;

// Returns true/false from a procedure.
class ProgramReturnRegionImpl final : public OP {
 public:
  virtual ~ProgramReturnRegionImpl(void);

  ProgramReturnRegionImpl(REGION *parent_, ProgramOperation op_)
      : OP(parent_, op_) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramReturnRegionImpl *AsReturn(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;
};

using RETURN = ProgramReturnRegionImpl;

// Publishes a message to the pub/sub.
class ProgramPublishRegionImpl final : public OP {
 public:
  virtual ~ProgramPublishRegionImpl(void);

  ProgramPublishRegionImpl(
      REGION *parent_, ParsedMessage message_, unsigned id_,
      ProgramOperation op_ = ProgramOperation::kPublishMessage)
      : OP(parent_, op_),
        message(message_),
        id(id_),
        arg_vars(this) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramPublishRegionImpl *AsPublish(void) noexcept override;

  // Message being published.
  const ParsedMessage message;

  // ID of this node.
  const unsigned id;

  // Variables passed as arguments.
  UseList<VAR> arg_vars;
};

using PUBLISH = ProgramPublishRegionImpl;

// Represents a positive or negative existence check.
class ProgramTestAndSetRegionImpl final : public OP {
 public:
  virtual ~ProgramTestAndSetRegionImpl(void);

  inline ProgramTestAndSetRegionImpl(REGION *parent_, ProgramOperation op_)
      : OP(parent_, op_) {}

  void Accept(ProgramVisitor &visitor) override;

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramTestAndSetRegionImpl *AsTestAndSet(void) noexcept override;

  // The variables are used as `(src_dest OP= update_val) == comapre_val`.
  UseRef<VAR> accumulator;
  UseRef<VAR> displacement;
  UseRef<VAR> comparator;
};

using TESTANDSET = ProgramTestAndSetRegionImpl;

// An equi-join between two or more tables.
class ProgramTableJoinRegionImpl final : public OP {
 public:
  virtual ~ProgramTableJoinRegionImpl(void);
  inline ProgramTableJoinRegionImpl(
      REGION *parent_, QueryJoin query_join_, unsigned id_)
      : OP(parent_, ProgramOperation::kJoinTables),
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
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramTableJoinRegionImpl *AsTableJoin(void) noexcept override;

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

using TABLEJOIN = ProgramTableJoinRegionImpl;

// A cross product between two or more tables.
class ProgramTableProductRegionImpl final : public OP {
 public:
  virtual ~ProgramTableProductRegionImpl(void);
  inline ProgramTableProductRegionImpl(
      REGION *parent_, QueryJoin query_join_, unsigned id_)
      : OP(parent_, ProgramOperation::kCrossProduct),
        query_join(query_join_),
        tables(this),
        input_vecs(this),
        id(id_) {}

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramTableProductRegionImpl *AsTableProduct(void) noexcept override;

  const QueryJoin query_join;

  UseList<TABLE> tables;
  UseList<VECTOR> input_vecs;
  std::vector<DefList<VAR>> output_vars;
  const unsigned id;
};

using TABLEPRODUCT = ProgramTableProductRegionImpl;

// Perform a scan over a table, possibly using an index. If an index is being
// used the input variables are provided to perform equality matching against
// column values. The results of the scan fill a vector.
class ProgramTableScanRegionImpl final : public OP {
 public:
  virtual ~ProgramTableScanRegionImpl(void);

  inline ProgramTableScanRegionImpl(unsigned id_, REGION *parent_)
      : OP(parent_, ProgramOperation::kScanTable),
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
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramTableScanRegionImpl *AsTableScan(void) noexcept override;

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

using TABLESCAN = ProgramTableScanRegionImpl;

// Comparison between two tuples.
class ProgramTupleCompareRegionImpl final : public OP {
 public:
  virtual ~ProgramTupleCompareRegionImpl(void);
  inline ProgramTupleCompareRegionImpl(REGION *parent_, ComparisonOperator op_)
      : OP(parent_, ProgramOperation::kCompareTuples),
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
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramTupleCompareRegionImpl *AsTupleCompare(void) noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  const ComparisonOperator cmp_op;
  UseList<VAR> lhs_vars;
  UseList<VAR> rhs_vars;

  // Optional body executed if the comparison fails.
  RegionRef false_body;
};

using TUPLECMP = ProgramTupleCompareRegionImpl;

// Calling a functor.
class ProgramGenerateRegionImpl final : public OP {
 public:
  virtual ~ProgramGenerateRegionImpl(void);
  inline ProgramGenerateRegionImpl(REGION *parent_, ParsedFunctor functor_,
                                   unsigned id_)
      : OP(
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
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramGenerateRegionImpl *AsGenerate(void) noexcept override;

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

using GENERATOR = ProgramGenerateRegionImpl;

// A procedure region. This represents some entrypoint of data into the program.
class ProgramProcedureImpl : public REGION {
 public:
  virtual ~ProgramProcedureImpl(void);

  ProgramProcedureImpl(unsigned id_, ProcedureKind kind_);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramProcedureImpl *AsProcedure(void) noexcept override;

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

using PROC = ProgramProcedureImpl;

// A series region is where the `regions[N]` must finish before `regions[N+1]`
// begins.
class ProgramSeriesRegionImpl final : public REGION {
 public:
  ProgramSeriesRegionImpl(REGION *parent_);

  virtual ~ProgramSeriesRegionImpl(void);

  void Accept(ProgramVisitor &visitor) override;
  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  inline void AddRegion(REGION *child) {
    assert(child->parent == this);
    regions.AddUse(child);
  }

  ProgramSeriesRegionImpl *AsSeries(void) noexcept override;

  UseList<REGION> regions;
};

using SERIES = ProgramSeriesRegionImpl;

// A region where multiple things can happen in parallel.
class ProgramParallelRegionImpl final : public REGION {
 public:
  ProgramParallelRegionImpl(REGION *parent_);

  virtual ~ProgramParallelRegionImpl(void);

  void Accept(ProgramVisitor &visitor) override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  uint64_t Hash(uint32_t depth) const override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if all paths through `this` ends with a `return` region.
  bool EndsWithReturn(void) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  inline void AddRegion(REGION *child) {
    assert(child->parent == this);
    regions.AddUse(child);
  }

  ProgramParallelRegionImpl *AsParallel(void) noexcept override;

  UseList<REGION> regions;
};

using PARALLEL = ProgramParallelRegionImpl;

// An induction is a loop centred on a `QueryMerge` node. Some of the views
// incoming to that `QueryMerge` are treated as "inputs", as they bring initial
// data into the `QueryMerge`. Other nodes are treated as "inductions" as they
// cycle back to the `QueryMerge`.
class ProgramInductionRegionImpl final : public REGION {
 public:
  virtual ~ProgramInductionRegionImpl(void);

  void Accept(ProgramVisitor &visitor) override;

  explicit ProgramInductionRegionImpl(ProgramImpl *impl, REGION *parent_);
  uint64_t Hash(uint32_t depth) const override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq, REGION *that,
              uint32_t depth) const noexcept override;

  const bool MergeEqual(ProgramImpl *prog,
                        std::vector<REGION *> &merges) override;

  ProgramInductionRegionImpl *AsInduction(void) noexcept override;

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

using INDUCTION = ProgramInductionRegionImpl;

class ProgramImpl : public User {
 public:
  ~ProgramImpl(void);

  explicit ProgramImpl(Query query_, unsigned next_id_);

  void Optimize(void);

  // Analyze the control-flow IR and table usage, looking for strategies that
  // can be used to eliminate redundancies in the data storage model. We do this
  // after optimizing the control-flow IR so that we can observe the effects
  // of copy propagation, which gives us the ability to "hop backward" to the
  // provenance of some data, as opposed to having to jump one `QueryView` at
  // a time.
  void Analyze(void);

  // The data flow representation from which this was created.
  const Query query;

  // Globally numbers things like procedures, variables, vectors, etc.
  unsigned next_id;

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
  DefList<DATARECORDCASE> record_cases;

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
