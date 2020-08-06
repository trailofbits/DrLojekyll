// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Parse/Parse.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace hyde {

class Program;
class ProgramImpl;

class ProgramRegion;
class ProgramInductionRegion;
class ProgramOperationRegion;
class ProgramParallelRegion;
class ProgramProcedureRegion;
class ProgramSeriesRegion;

class DataIndex;
class DataVariable;
class DataTable;
class DataView;

// Represents a view into a table of data. Each view is basically the subset
// of some data in a table that is specific to a `QueryView`. Thus, there is
// a one-to-many relationship between individual rows of data in a table and
// the views which identify those rows as containing
template <>
class Node<DataView> : public Def<Node<DataView>>, public User {
 public:
  virtual ~Node(void);

  Node(Node<DataTable> *table_, QueryView view_tag_);

  // Get or create an index on the table.
  Node<DataIndex> *GetOrCreateIndex(std::vector<QueryColumn> cols);

  const QueryView view_tag;
  const WeakUseRef<Node<DataTable>> viewed_table;
};

using VIEW = Node<DataView>;

// Represents an index on some subset of columns in a table.
template <>
class Node<DataIndex> : public Def<Node<DataIndex>>, public User {
 public:
  virtual ~Node(void);

  Node(Node<DataTable> *table_, std::string column_spec_);

  const std::string column_spec;
  const WeakUseRef<Node<DataTable>> indexed_table;
};

using INDEX = Node<DataIndex>;

// Represents a table of data.
//
// NOTE(pag): By default all tables already have a UNIQUE index on them.
template <>
class Node<DataTable> : public Def<Node<DataTable>>, public User {
 public:
  virtual ~Node(void);

  Node(std::vector<unsigned> column_ids_);

  // Get or create a table in the program.
  static Node<DataView> *GetOrCreate(ProgramImpl *program,
                                     DefinedNodeRange<QueryColumn> cols,
                                     QueryView view_tag);

  static Node<DataView> *GetOrCreate(ProgramImpl *program,
                                     UsedNodeRange<QueryColumn> cols,
                                     QueryView view_tag);

  // Get or create a table in a procedure. This corresponds to a temporary
  // table / vector of tuples.
  static Node<DataTable> *Create(Node<ProgramRegion> *region,
                                 DefinedNodeRange<QueryColumn> cols);

  // Get or create a table in a procedure. This corresponds to a temporary
  // table / vector of tuples.
  static Node<DataTable> *Create(Node<ProgramRegion> *region,
                                 UsedNodeRange<QueryColumn> cols);

  // Get or create an index on the table.
  Node<DataIndex> *GetOrCreateIndex(std::vector<QueryColumn> cols);

  // The sorted, uniqued column IDs (derived from `QueryColumn::Id`).
  const std::vector<unsigned> column_ids;

  // The views into this table. A given row might belong to one or more views.
  //
  // The general idea here is that we can say that two sets of columns can
  // belong to the same view if the arity of the rows will be the same, or if
  // one will be a subset of the other.
  DefList<VIEW> views;

  // Indexes that should be created on this table. By default, all tables have
  // a UNIQUE index.
  DefList<INDEX> indices;
};

using TABLE = Node<DataTable>;

template <>
class Node<ProgramRegion> : public Def<Node<ProgramRegion>>, public User {
 public:
  virtual ~Node(void);
  explicit Node(Node<ProgramProcedureRegion> *containing_procedure_);
  explicit Node(Node<ProgramRegion> *parent_);

  virtual Node<ProgramProcedureRegion> *AsProcedure(void) noexcept;
  virtual Node<ProgramOperationRegion> *AsOperation(void) noexcept;
  virtual Node<ProgramSeriesRegion> *AsSeries(void) noexcept;
  virtual Node<ProgramParallelRegion> *AsParallel(void) noexcept;
  virtual Node<ProgramInductionRegion> *AsInduction(void) noexcept;

  // Returns the lexical level of this node.
  unsigned Depth(void) const noexcept;

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
  Node<ProgramProcedureRegion> *const containing_procedure;
  Node<ProgramRegion> *parent{nullptr};
};

using REGION = Node<ProgramRegion>;

enum class VariableRole {
  kParameter, kLocal, kFree, kGlobalBoolean, kConditionRefCount
};

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
  const unsigned id;

  inline unsigned Sort(void) const noexcept {
    return id;
  }

  std::vector<QueryColumn> query_columns;
  std::vector<ParsedVariable> parsed_vars;
};

using VAR = Node<DataVariable>;


enum class ProgramOperation {

  // Insert into a table. Can be interpreted as conditional (a runtime may
  // choose to check if the insert is new or not). If the insert succeeds, then
  // execution descends into `body`. The table into which we are inserting is
  // `views[0]`.
  kInsertIntoView,

  // Check if a row exists in a view. The tuple values are found in
  // `operands` and the table being used is `views[0]`.
  kCheckTupleIsPresentInView,

  // JOIN-specific

  // A JOIN over some tables.
  kJoinTables,
  kAppendJoinPivotsToVector,
  kLoopOverJoinPivots,

  // Used to implement the cross-product of some tables.
  kLoopOverView,
  kAppendProductInputToVector,
  kLoopOverProductInputVector,
  kAppendProductOutputToVector,
  kLoopOverProductOutputVector,

  // Loop over a vector of inputs. The format of the vector is based off of
  // the variables in `variables`. The region `body` is executed for each
  // loop iteration.
  kLoopOverImplicitInputVector,

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

template <>
class Node<ProgramOperationRegion> final : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);
  explicit Node(REGION *parent_, ProgramOperation op_);

  Node<ProgramOperationRegion> *AsOperation(void) noexcept override;

  const ProgramOperation op;
  UseList<VAR> variables;
  UseList<TABLE> tables;
  UseList<VIEW> views;
  UseList<INDEX> indices;

  std::optional<ComparisonOperator> compare_operator;
  std::optional<ParsedFunctor> functor;

  // If this operation does something conditional then this is the body it
  // executes.
  UseRef<REGION> body;
};

using OP = Node<ProgramOperationRegion>;

// A procedure region. This represents some entrypoint of data into the program.
template <>
class Node<ProgramProcedureRegion> final : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);

  explicit Node(QueryView view, ProgramImpl *program);

  Node<ProgramProcedureRegion> *AsProcedure(void) noexcept override;

  // Gets or creates a local variable in the procedure.
  VAR *GetOrCreateLocal(QueryColumn col);

  // Local variables that are defined/used in the body of this procedure.
  DefList<VAR> locals;

  // Temporary tables within this procedure.
  DefList<TABLE> tables;

  // Body of this procedure. Initially starts with a loop over an implicit
  // input vector.
  UseRef<REGION> body;

  // Maps `QueryColumn::Id()` values to variables.
  std::unordered_map<unsigned, VAR *> col_id_to_var;

  // Used during building. We are permittied to visit a given `QueryView`
  // multiple times within a procedure, and it's convenient to use the opcode
  // (`OP::op`) as a kind of state tracker to decide how to handle the 2nd, and
  // Nth visits of a given view.
  std::unordered_map<QueryView, REGION *> view_to_region;
};

using PROC = Node<ProgramProcedureRegion>;

// A series region is where the `regions[N]` must finish before `regions[N+1]`
// begins.
template <>
class Node<ProgramSeriesRegion> final : public Node<ProgramRegion> {
 public:
  inline Node(REGION *parent_) : Node<ProgramRegion>(parent_), regions(this) {}

  virtual ~Node(void);
  Node<ProgramSeriesRegion> *AsSeries(void) noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using SERIES = Node<ProgramSeriesRegion>;

// A region where multiple things can happen in parallel. Often when a VIEW's
// columns are needed by two or more other VIEWs, we will have one of these.
template <>
class Node<ProgramParallelRegion> final : public Node<ProgramRegion> {
 public:
  inline Node(REGION *parent_) : Node<ProgramRegion>(parent_), regions(this) {}

  virtual ~Node(void);
  Node<ProgramParallelRegion> *AsParallel(void) noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using PARALLEL = Node<ProgramParallelRegion>;

// An induction is a loop centred on a MERGE node. Some of the views incoming
// to that MERGE are treated as "inputs", as they bring initial data into the
// MERGE. Other nodes are treated as "inductions" as they cycle back to the
// MERGE.
template <>
class Node<ProgramInductionRegion> final : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);
  Node<ProgramInductionRegion> *AsInduction(void) noexcept override;

  inline explicit Node(REGION *parent_) : Node<ProgramRegion>(parent_) {}

  UseRef<REGION> init;
  UseRef<REGION> cycle;
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

  // The dataflow from which this was created.
  const Query query;

  DefList<PROC> procedure_regions;
  DefList<SERIES> series_regions;
  DefList<PARALLEL> parallel_regions;
  DefList<INDUCTION> induction_regions;
  DefList<OP> operation_regions;

  DefList<TABLE> tables;
  DefList<VAR> global_vars;
  unsigned next_global_var_id{~0u};

  std::unordered_map<std::string, TABLE *> col_spec_to_table;
  std::unordered_map<QueryView, PROC *> procedures;
  std::unordered_map<QueryCondition, VAR *> cond_ref_counts;
};

}  // namespace hyde
