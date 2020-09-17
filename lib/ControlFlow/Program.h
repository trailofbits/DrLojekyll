// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/DisjointSet.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

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

class Program;
class ProgramImpl;

class ProgramRegion;
class ProgramInductionRegion;
class ProgramOperationRegion;
class ProgramParallelRegion;
class ProgramProcedure;
class ProgramSeriesRegion;

class DataIndex;
class DataVariable;
class DataTable;
class DataVector;

// A column within a table.
template <>
class Node<DataColumn> : public Def<Node<DataColumn>>, public User {
 public:
  virtual ~Node(void);

  Node(unsigned id_, TypeKind type_, Node<DataTable> *table_);

  const unsigned id;
  const unsigned index;
  const TypeKind type;

  std::vector<Token> names;

  WeakUseRef<Node<DataTable>> table;
};

using TABLECOLUMN = Node<DataColumn>;

// Represents an index on some subset of columns in a table.
template <>
class Node<DataIndex> : public Def<Node<DataIndex>>, public User {
 public:
  virtual ~Node(void);

  Node(unsigned id_, Node<DataTable> *table_, std::string column_spec_);

  const unsigned id;
  const std::string column_spec;

  UseList<TABLECOLUMN> columns;

  WeakUseRef<Node<DataTable>> table;
};

using TABLEINDEX = Node<DataIndex>;

// Represents a table of data.
//
// NOTE(pag): By default all tables already have a UNIQUE index on them.
template <>
class Node<DataTable> : public Def<Node<DataTable>>, public User {
 public:
  virtual ~Node(void);

  inline Node(unsigned id_)
      : Def<Node<DataTable>>(this),
        User(this),
        id(id_),
        columns(this),
        indices(this) {}

  // Get or create a table in the program.
  static Node<DataTable> *GetOrCreate(ProgramImpl *impl, QueryView view);

  // Get or create an index on the table.
  TABLEINDEX *GetOrCreateIndex(
      ProgramImpl *impl, std::vector<unsigned> cols);

  const unsigned id;

  // List of defined columns.
  DefList<TABLECOLUMN> columns;

  // Indexes that should be created on this table. By default, all tables have
  // a UNIQUE index.
  DefList<TABLEINDEX> indices;
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
  static constexpr unsigned kInputVectorId = 0u;

  template <typename ColList>
  Node(unsigned id_, VectorKind kind_, ColList &&cols)
      : Def<Node<DataVector>>(this),
        id(id_),
        kind(kind_) {

    for (QueryColumn col : cols) {
      col_types.push_back(col.Type().Kind());
    }
  }

  const unsigned id;
  const VectorKind kind;
  std::vector<TypeKind> col_types;
};

using VECTOR = Node<DataVector>;

// A variable in the program. This could be a procedure parameter or a local
// variable.
template <>
class Node<DataVariable> final : public Def<Node<DataVariable>> {
 public:
  inline explicit Node(unsigned id_, VariableRole role_)
      : Def<Node<DataVariable>>(this),
        role(role_),
        id(id_) {}

  const VariableRole role;

  // Unique ID for this variable. Unrelated to column IDs.
  const unsigned id;

  // NOTE(pag): Only valid after optimization when building the control flow
  //            IR is complete.
  Node<ProgramRegion> *defining_region{nullptr};

  inline unsigned Sort(void) const noexcept {
    return id;
  }

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
  explicit Node(Node<ProgramProcedure> *containing_procedure_);
  explicit Node(Node<ProgramRegion> *parent_);

  virtual Node<ProgramProcedure> *AsProcedure(void) noexcept;
  virtual Node<ProgramOperationRegion> *AsOperation(void) noexcept;
  virtual Node<ProgramSeriesRegion> *AsSeries(void) noexcept;
  virtual Node<ProgramParallelRegion> *AsParallel(void) noexcept;
  virtual Node<ProgramInductionRegion> *AsInduction(void) noexcept;

  inline void ReplaceAllUsesWith(Node<ProgramRegion> *that) {
    this->Def<Node<ProgramRegion>>::ReplaceAllUsesWith(that);
    that->parent = this->parent;
    this->parent = nullptr;
  }

  // Gets or creates a local variable in the procedure.
  VAR *VariableFor(ProgramImpl *impl, QueryColumn col);
  VAR *VariableForRec(QueryColumn col);

  // Returns the lexical level of this node.
  unsigned Depth(void) const noexcept;

  // Returns true if this region is a no-op.
  virtual bool IsNoOp(void) const noexcept;

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
  Node<ProgramProcedure> *const containing_procedure;
  Node<ProgramRegion> *parent{nullptr};

  // Maps `QueryColumn::Id()` values to variables. Used to provide lexical
  // scoping of variables.
  //
  // NOTE(pag): Only valid before optimization, during the building of the
  //            control flow IR.
  std::unordered_map<unsigned, VAR *> col_id_to_var;
};

using REGION = Node<ProgramRegion>;

enum class ProgramOperation {
  kInvalid,

  // Insert into a table. Can be interpreted as conditional (a runtime may
  // choose to check if the insert is new or not). If the insert succeeds, then
  // execution descends into `body`. The table into which we are inserting is
  // `views[0]`.
  kInsertIntoTable,

  // When dealing with MERGE/UNION nodes with an inductive cycle.
  kAppendInductionInputToVector,
  kLoopOverInductionInputVector,
  kClearInductionInputVector,

  // When dealing with a MERGE/UNION node that isn't part of an inductive
  // cycle.
  kAppendUnionInputToVector,
  kLoopOverUnionInputVector,
  kClearUnionInputVector,

  // Check if a row exists in a view. The tuple values are found in
  // `operands` and the table being used is `views[0]`.
  kCheckTupleIsPresentInTable,
  kCheckTupleIsNotPresentInTable,

  // JOIN-specific

  // A JOIN over some tables.
  kJoinTables,
  kAppendJoinPivotsToVector,
  kLoopOverJoinPivots,
  kClearJoinPivotVector,
  kSortAndUniquePivotVector,

  // Comparison between two tuples.
  kCompareTuples,

  // Used to implement the cross-product of some tables.
  kLoopOverTable,
  kAppendProductInputToVector,
  kLoopOverProductInputVector,
  kAppendProductOutputToVector,
  kLoopOverProductOutputVector,

  // Loop over a vector of inputs. The format of the vector is based off of
  // the variables in `variables`. The region `body` is executed for each
  // loop iteration.
  kLoopOverInputVector,

  // Merge two values into an updated value when using `mutable`-attributed
  // parameters.
  kCallMergeFunctor,

  // Call a filter function, stored in `functor`, that is a functor where all
  // parameters are `bound`-attributed. These functors are interpreted as
  // predicates returning `true` or `false`. If `true` is returned, then
  // descend into `body`.
  kCallFilterFunctor,

  // Compare two variables using `compare_operator`. `variables[0]` is the
  // left-hand side operand, and `variables[1]` is the right-hand side operand
  // of the comparison operator. If the comparison is `true` then descend into
  // `body`.
  kCompareVariables,

  // Creates a let binding, such that `variables[2*n]` is the newly bound
  // variable, and `variables[2*n + 1]` is the value being bound.
  kLetBinding,

  // Used to test reference count variables associated with `QueryCondition`
  // nodes in the data flow.
  kTestAllNonZero,
  kTestAllZero,

  // Test/set a global boolean variable to `true`. The variable is
  // `variables[0]`. The usage is for cross products. If we've ever executed a
  // lazy cross-product, then we must always visit it.
  kTestGlobalVariableIsTrue,
  kSetGlobalVariableToTrue,
};

// A generic operation.
template <>
class Node<ProgramOperationRegion> : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);
  explicit Node(REGION *parent_, ProgramOperation op_);

  virtual Node<ProgramVectorLoopRegion> *AsVectorLoop(void) noexcept;
  virtual Node<ProgramVectorAppendRegion> *AsVectorAppend(void) noexcept;
  virtual Node<ProgramVectorClearRegion> *AsVectorClear(void) noexcept;
  virtual Node<ProgramVectorUniqueRegion> *AsVectorUnique(void) noexcept;
  virtual Node<ProgramLetBindingRegion> *AsLetBinding(void) noexcept;
  virtual Node<ProgramTableInsertRegion> *AsTableInsert(void) noexcept;
  virtual Node<ProgramTableJoinRegion> *AsTableJoin(void) noexcept;
  virtual Node<ProgramExistenceCheckRegion> *AsExistenceCheck(void) noexcept;
  virtual Node<ProgramTupleCompareRegion> *AsTupleCompare(void) noexcept;

  Node<ProgramOperationRegion> *AsOperation(void) noexcept override;

  const ProgramOperation op;

  // If this operation does something conditional then this is the body it
  // executes.
  UseRef<REGION> body;
};

using OP = Node<ProgramOperationRegion>;

// A let binding, i.e. an assignment of zero or more variables.
template <>
class Node<ProgramLetBindingRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  bool IsNoOp(void) const noexcept override;

  inline Node(REGION *parent_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kLetBinding),
        defined_vars(this),
        used_vars(this) {}

  Node<ProgramLetBindingRegion> *AsLetBinding(void) noexcept override;

  // Local variables that are defined/used in the body of this procedure.
  DefList<VAR> defined_vars;
  UseList<VAR> used_vars;
};

using LET = Node<ProgramLetBindingRegion>;

// A loop over a vector, specified in `tables[0]`. This also performs variable
// binding into the variables specified in `variables`.
template <>
class Node<ProgramVectorLoopRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(REGION *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        defined_vars(this) {}

  bool IsNoOp(void) const noexcept override;

  Node<ProgramVectorLoopRegion> *AsVectorLoop(void) noexcept override;

  // Local variables bound to the vector being looped.
  DefList<VAR> defined_vars;

  // Vector being looped.
  UseRef<VECTOR> vector;
};

using VECTORLOOP = Node<ProgramVectorLoopRegion>;

// An append of a tuple (specified in terms of `variables`) into a vector,
// specified in terms of `tables[0]`.
template <>
class Node<ProgramVectorAppendRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(REGION *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        tuple_vars(this) {}

  Node<ProgramVectorAppendRegion> *AsVectorAppend(void) noexcept override;

  UseList<VAR> tuple_vars;
  UseRef<VECTOR> vector;
};

using VECTORAPPEND = Node<ProgramVectorAppendRegion>;

// Clear a vector.
template <>
class Node<ProgramVectorClearRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  using Node<ProgramOperationRegion>::Node;

  Node<ProgramVectorClearRegion> *AsVectorClear(void) noexcept override;

  UseRef<VECTOR> vector;
};

using VECTORCLEAR = Node<ProgramVectorClearRegion>;

// Sort and unique a vector.
template <>
class Node<ProgramVectorUniqueRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  using Node<ProgramOperationRegion>::Node;

  Node<ProgramVectorUniqueRegion> *AsVectorUnique(void) noexcept override;

  UseRef<VECTOR> vector;
};

using VECTORUNIQUE = Node<ProgramVectorUniqueRegion>;

// An append of a tuple (specified in terms of `variables`) into a vector,
// specified in terms of `tables[0]`.
template <>
class Node<ProgramTableInsertRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_)
      : Node<ProgramOperationRegion>(
            parent_, ProgramOperation::kInsertIntoTable),
        col_values(this) {}

  Node<ProgramTableInsertRegion> *AsTableInsert(void) noexcept override;

  // Variables that make up the tuple.
  UseList<VAR> col_values;

  // View into which the tuple is being inserted.
  UseRef<TABLE> table;
};

using TABLEINSERT = Node<ProgramTableInsertRegion>;

// Represents a positive or negative existence check.
template <>
class Node<ProgramExistenceCheckRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        cond_vars(this) {}

  bool IsNoOp(void) const noexcept override;

  Node<ProgramExistenceCheckRegion> *AsExistenceCheck(void) noexcept override;

  // Variables associated with these existence checks.
  UseList<VAR> cond_vars;
};

using EXISTS = Node<ProgramExistenceCheckRegion>;

// An equi-join between two or more tables.
template <>
class Node<ProgramTableJoinRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, QueryJoin query_join_)
      : Node<ProgramOperationRegion>(
            parent_, ProgramOperation::kJoinTables),
        query_join(query_join_),
        tables(this),
        indices(this) {}

  bool IsNoOp(void) const noexcept override;

  Node<ProgramTableJoinRegion> *AsTableJoin(void) noexcept override;

  const QueryJoin query_join;

  UseList<TABLE> tables;
  UseList<TABLEINDEX> indices;

  DefList<VAR> output_pivot_vars;
  std::vector<DefList<VAR>> output_vars;
};

using TABLEJOIN = Node<ProgramTableJoinRegion>;

// Comparison between two tuples.
template <>
class Node<ProgramTupleCompareRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, ComparisonOperator op_)
      : Node<ProgramOperationRegion>(
            parent_, ProgramOperation::kCompareTuples),
        op(op_),
        lhs_vars(this),
        rhs_vars(this) {}

  bool IsNoOp(void) const noexcept override;

  Node<ProgramTupleCompareRegion> *AsTupleCompare(void) noexcept override;

  const ComparisonOperator op;
  UseList<VAR> lhs_vars;
  UseList<VAR> rhs_vars;
};

using TUPLECMP = Node<ProgramTupleCompareRegion>;

// A procedure region. This represents some entrypoint of data into the program.
template <>
class Node<ProgramProcedure> : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);

  inline Node(unsigned id_)
      : Node<ProgramRegion>(this),
        id(id_),
        tables(this) {}

  Node<ProgramProcedure> *AsProcedure(void) noexcept override;

  // Create a new vector in this procedure for a list of columns.
  VECTOR *VectorFor(
      ProgramImpl *impl, VectorKind kind, DefinedNodeRange<QueryColumn> cols);

  const unsigned id;

  std::optional<QueryIO> io;

  // Temporary tables within this procedure.
  DefList<TABLE> tables;

  // Body of this procedure. Initially starts with a loop over an implicit
  // input vector.
  UseRef<REGION> body;

  // Input vectors and variables.
  DefList<VECTOR> input_vectors;
  DefList<VAR> input_vars;

  // Vectors defined in this procedure. If this is a vector procedure then
  // the first vector is the input vector.
  DefList<VECTOR> vectors;
};

using PROC = Node<ProgramProcedure>;

// A series region is where the `regions[N]` must finish before `regions[N+1]`
// begins.
template <>
class Node<ProgramSeriesRegion> final : public Node<ProgramRegion> {
 public:
  inline Node(REGION *parent_) : Node<ProgramRegion>(parent_), regions(this) {}

  virtual ~Node(void);
  Node<ProgramSeriesRegion> *AsSeries(void) noexcept override;
  bool IsNoOp(void) const noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using SERIES = Node<ProgramSeriesRegion>;

// A region where multiple things can happen in parallel.
template <>
class Node<ProgramParallelRegion> final : public Node<ProgramRegion> {
 public:
  inline Node(REGION *parent_) : Node<ProgramRegion>(parent_), regions(this) {}

  virtual ~Node(void);
  Node<ProgramParallelRegion> *AsParallel(void) noexcept override;
  bool IsNoOp(void) const noexcept override;

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
  Node<ProgramInductionRegion> *AsInduction(void) noexcept override;

  explicit Node(ProgramImpl *impl, REGION *parent_);

  // Initial regions that fill up one or more of the inductive vectors.
  UseRef<REGION> init_region;

  // The cyclic regions of this induction. This is a PARALLEL region.
  UseRef<REGION> cyclic_region;

  // The output regions of this induction. This is a PARALLEL region.
  UseRef<REGION> output_region;

  // Vectors built up by this induction.
  UseList<VECTOR> vectors;

  // It could be the case that a when going through the induction we end up
  // going into a co-mingled induction, as is the case in
  // `transitive_closure2.dr` and `transitive_closure3.dr`.
  std::unordered_map<QueryView, UseRef<VECTOR>> view_to_vec;

  // List of append to vector regions inside this induction.
  std::unordered_map<QueryView, UseList<REGION>> view_to_init_appends;

  // List of append to vector regions inside this induction.
  std::unordered_map<QueryView, UseList<REGION>> view_to_cycle_appends;

  // Maps views to their cycles inside of the `cyclic_region`.
  std::unordered_map<QueryView, UseRef<REGION>> view_to_cycle_loop;

  // Maps views to their cycles inside of the `output_region`.
  std::unordered_map<QueryView, UseRef<REGION>> view_to_output_loop;

  enum State {
    kAccumulatingInputRegions,
    kAccumulatingCycleRegions,
    kBuildingOutputRegions
  } state = kAccumulatingInputRegions;
};

using INDUCTION = Node<ProgramInductionRegion>;

class ProgramImpl : public User {
 public:
  ~ProgramImpl(void);

  inline explicit ProgramImpl(Query query_)
      : User(this),
        query(query_),
        procedure_regions(this),
        series_regions(this),
        parallel_regions(this),
        induction_regions(this),
        operation_regions(this),
        tables(this) {}

  void Optimize(void);

  // The dataflow from which this was created.
  const Query query;

  DefList<PROC> procedure_regions;
  DefList<SERIES> series_regions;
  DefList<PARALLEL> parallel_regions;
  DefList<INDUCTION> induction_regions;
  DefList<OP> operation_regions;
  DefList<TABLE> tables;
  DefList<VAR> global_vars;
  unsigned next_id{0u};

  DefList<VAR> const_vars;
  std::unordered_map<QueryConstant, VAR *> const_to_var;
  std::unordered_map<QueryCondition, VAR *> cond_ref_counts;

  // We build up "data models" of views that can share the same backing storage.
  std::vector<std::unique_ptr<DataModel>> models;
  std::unordered_map<QueryView, DataModel *> view_to_model;
};

}  // namespace hyde
