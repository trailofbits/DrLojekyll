// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/Node.h>

#include <functional>
#include <memory>
#include <optional>
#include <utility>

namespace hyde {

class ProgramImpl;

class ProgramVisitor;

namespace program {

// A superclass of (most) kinds of IR program nodes.
template <typename T>
class ProgramNode {
 public:
  inline ProgramNode(Node<T> *impl_) : impl(impl_) {}

  inline bool operator==(ProgramNode<T> that) const {
    return impl == that.impl;
  }

  inline bool operator!=(ProgramNode<T> that) const {
    return impl != that.impl;
  }

  inline bool operator<(ProgramNode<T> that) const {
    return impl < that.impl;
  }

  uintptr_t UniqueId(void) const noexcept {
    return reinterpret_cast<uintptr_t>(impl);
  }

 protected:
  friend class ::hyde::ProgramImpl;

  Node<T> *impl;
};

}  // namespace program

class DataColumn;
class DataIndex;
class DataTable;
class DataVariable;
class DataVector;

class Program;
class ProgramCallRegion;
class ProgramReturnRegion;
class ProgramExistenceAssertionRegion;
class ProgramGenerateRegion;
class ProgramInductionRegion;
class ProgramLetBindingRegion;
class ProgramParallelRegion;
class ProgramProcedure;
class ProgramPublishRegion;
class ProgramSeriesRegion;
class ProgramVectorAppendRegion;
class ProgramVectorClearRegion;
class ProgramVectorLoopRegion;
class ProgramVectorSwapRegion;
class ProgramVectorUniqueRegion;
class ProgramTransitionStateRegion;
class ProgramCheckStateRegion;
class ProgramTableJoinRegion;
class ProgramTableProductRegion;
class ProgramTableScanRegion;
class ProgramTupleCompareRegion;

// A generic region of code nested inside of a procedure.
class ProgramRegion : public program::ProgramNode<ProgramRegion> {
 public:

  // Return the region containing `child`, or `std::nullopt` if this is the
  // topmost region in a procedure.
  static std::optional<ProgramRegion> Containing(ProgramRegion &child) noexcept;

  ProgramRegion(const ProgramCallRegion &);
  ProgramRegion(const ProgramReturnRegion &);
  ProgramRegion(const ProgramExistenceAssertionRegion &);
  ProgramRegion(const ProgramGenerateRegion &);
  ProgramRegion(const ProgramInductionRegion &);
  ProgramRegion(const ProgramLetBindingRegion &);
  ProgramRegion(const ProgramParallelRegion &);
  ProgramRegion(const ProgramPublishRegion &);
  ProgramRegion(const ProgramSeriesRegion &);
  ProgramRegion(const ProgramVectorAppendRegion &);
  ProgramRegion(const ProgramVectorClearRegion &);
  ProgramRegion(const ProgramVectorLoopRegion &);
  ProgramRegion(const ProgramVectorSwapRegion &);
  ProgramRegion(const ProgramVectorUniqueRegion &);
  ProgramRegion(const ProgramTransitionStateRegion &);
  ProgramRegion(const ProgramCheckStateRegion &);
  ProgramRegion(const ProgramTableJoinRegion &);
  ProgramRegion(const ProgramTableProductRegion &);
  ProgramRegion(const ProgramTableScanRegion &);
  ProgramRegion(const ProgramTupleCompareRegion &);

  void Accept(ProgramVisitor &visitor) const;

  bool IsCall(void) const noexcept;
  bool IsReturn(void) const noexcept;
  bool IsExistenceAssertion(void) const noexcept;
  bool IsGenerate(void) const noexcept;
  bool IsInduction(void) const noexcept;
  bool IsVectorLoop(void) const noexcept;
  bool IsVectorAppend(void) const noexcept;
  bool IsVectorClear(void) const noexcept;
  bool IsVectorSwap(void) const noexcept;
  bool IsVectorUnique(void) const noexcept;
  bool IsLetBinding(void) const noexcept;
  bool IsTransitionState(void) const noexcept;
  bool IsCheckState(void) const noexcept;
  bool IsTableJoin(void) const noexcept;
  bool IsTableProduct(void) const noexcept;
  bool IsTableScan(void) const noexcept;
  bool IsSeries(void) const noexcept;
  bool IsParallel(void) const noexcept;
  bool IsPublish(void) const noexcept;
  bool IsTupleCompare(void) const noexcept;

  std::string_view Comment(void) const noexcept;

 private:
  friend class ProgramCallRegion;
  friend class ProgramReturnRegion;
  friend class ProgramExistenceAssertionRegion;
  friend class ProgramGenerateRegion;
  friend class ProgramInductionRegion;
  friend class ProgramLetBindingRegion;
  friend class ProgramParallelRegion;
  friend class ProgramProcedure;
  friend class ProgramPublishRegion;
  friend class ProgramSeriesRegion;
  friend class ProgramVectorAppendRegion;
  friend class ProgramVectorClearRegion;
  friend class ProgramVectorLoopRegion;
  friend class ProgramVectorSwapRegion;
  friend class ProgramVectorUniqueRegion;
  friend class ProgramTransitionStateRegion;
  friend class ProgramCheckStateRegion;
  friend class ProgramTableJoinRegion;
  friend class ProgramTableProductRegion;
  friend class ProgramTableScanRegion;
  friend class ProgramTupleCompareRegion;

  using program::ProgramNode<ProgramRegion>::ProgramNode;
};

// A generic sequencing of regions.
class ProgramSeriesRegion : public program::ProgramNode<ProgramSeriesRegion> {
 public:
  static ProgramSeriesRegion From(ProgramRegion) noexcept;

  // The sequence of regions nested inside this series.
  UsedNodeRange<ProgramRegion> Regions(void) const;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramSeriesRegion>::ProgramNode;
};

// A generic representation the two or more regions can execute in parallel.
class ProgramParallelRegion
    : public program::ProgramNode<ProgramParallelRegion> {
 public:
  static ProgramParallelRegion From(ProgramRegion) noexcept;

  // The set of regions nested inside this series.
  UsedNodeRange<ProgramRegion> Regions(void) const;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramParallelRegion>::ProgramNode;
};

enum class VariableRole : int {
  kConditionRefCount,
  kConstant,
  kConstantZero,
  kConstantOne,
  kConstantFalse,
  kConstantTrue,
  kVectorVariable,
  kLetBinding,
  kJoinPivot,
  kJoinNonPivot,
  kProductOutput,
  kScanOutput,
  kFunctorOutput,
  kParameter,
  kInputOutputParameter,
};

// A variable in the program.
class DataVariable : public program::ProgramNode<DataVariable> {
 public:
  VariableRole DefiningRole(void) const noexcept;

  // The region which defined this local variable. If this variable has no
  // defining region then it is a global variable.
  std::optional<ProgramRegion> DefiningRegion(void) const noexcept;

  // Unique ID of this variable.
  unsigned Id(void) const noexcept;

  // Name of this variable, if any. There might not be a name.
  Token Name(void) const noexcept;

  // The literal, constant value of this variable.
  std::optional<ParsedLiteral> Value(void) const noexcept;

  // Type of this variable.
  TypeLoc Type(void) const noexcept;

  // Whether this variable is global.
  bool IsGlobal(void) const noexcept;

  // Whether or not this variable is a constant.
  bool IsConstant(void) const noexcept;

 private:
  using program::ProgramNode<DataVariable>::ProgramNode;
};

enum class VectorKind : unsigned {
  kParameter,
  kInputOutputParameter,
  kInduction,
  kJoinPivots,
  kProductInput,
  kTableScan
};

// A column in a table.
class DataColumn : public program::ProgramNode<DataColumn> {
 public:
  // Unique ID of this column.
  unsigned Id(void) const noexcept;

  // Index of this column within its table.
  unsigned Index(void) const noexcept;

  // Type of this column.
  TypeKind Type(void) const noexcept;

  // Possible names that can be associated with this column.
  //
  // NOTE(pag): Multiple columns of the same table might have intersecting
  //            sets of possible names.
  const std::vector<Token> &PossibleNames(void) const noexcept;

 private:
  friend class DataTable;

  using program::ProgramNode<DataColumn>::ProgramNode;
};

// An index on a table.
class DataIndex : public program::ProgramNode<DataIndex> {
 public:
  // Unique ID of this index.
  unsigned Id(void) const noexcept;

  // Columns from a table that are part of this index.
  UsedNodeRange<DataColumn> KeyColumns(void) const;

  // Columns from a table that are part of this index.
  UsedNodeRange<DataColumn> ValueColumns(void) const;

 private:
  friend class DataTable;

  using program::ProgramNode<DataIndex>::ProgramNode;
};

// A persistent table, backed by some kind of data store / database.
class DataTable : public program::ProgramNode<DataTable> {
 public:
  static DataTable Containing(DataColumn col) noexcept;
  static DataTable Backing(DataIndex index) noexcept;

  unsigned Id(void) const noexcept;

  // Columns in this table. The columns may be from different `QueryView`
  // nodes.
  DefinedNodeRange<DataColumn> Columns(void) const;

  // Indices on this table.
  DefinedNodeRange<DataIndex> Indices(void) const;

  // Visit the users of this table.
  void VisitUsers(ProgramVisitor &visitor);

  // Apply a function to each user.
  void ForEachUser(std::function<void(ProgramRegion)> cb);

 private:
  using program::ProgramNode<DataTable>::ProgramNode;
};

// A vector in the program.
class DataVector : public program::ProgramNode<DataVector> {
 public:
  VectorKind Kind(void) const noexcept;

  unsigned Id(void) const noexcept;

  bool IsInputVector(void) const noexcept;

  const std::vector<TypeKind> ColumnTypes(void) const noexcept;

  // Visit the users of this vector.
  void VisitUsers(ProgramVisitor &visitor);

  // Apply a function to each user.
  void ForEachUser(std::function<void(ProgramRegion)> cb);

 private:
  friend class ProgramVectorClearRegion;
  friend class ProgramVectorSwapRegion;
  friend class ProgramVectorUniqueRegion;

  using program::ProgramNode<DataVector>::ProgramNode;
};

// Increment or decrement a reference counter, asserting that some set of tuples
// exists or does not exist.
//
// This is related to a there-exists clause, and the assertion here is to say
// "something definitely exits" (i.e. increment), or "something may no longer
// exist" (i.e. decrement).
class ProgramExistenceAssertionRegion
    : public program::ProgramNode<ProgramExistenceAssertionRegion> {
 public:
  static ProgramExistenceAssertionRegion From(ProgramRegion) noexcept;

  bool IsIncrement(void) const noexcept;
  bool IsDecrement(void) const noexcept;

  // List of reference count variables that are mutated.
  UsedNodeRange<DataVariable> ReferenceCounts(void) const;

  // Return the body which is conditionally executed if all reference counts
  // just went from `0` to `1` (in the increment case) or `1` to `0` in the
  // decrement case.
  std::optional<ProgramRegion> Body(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramExistenceAssertionRegion>::ProgramNode;
};

// Apply a functor to one or more inputs, yield zero or more outputs. In the
// zero output case, the output is considered to be a boolean value.
class ProgramGenerateRegion
    : public program::ProgramNode<ProgramGenerateRegion> {
 public:
  static ProgramGenerateRegion From(ProgramRegion) noexcept;

  unsigned Id(void) const noexcept;

  // Does this functor application behave like a filter function?
  bool IsFilter(void) const noexcept;

  // Returns `true` if repeated executions of the function given the same
  // inputs generate the same outputs.
  bool IsPure(void) const noexcept;

  // Returns `true` if repeated executions of the function given the same
  // inputs are not guaranteed to generate the same outputs.
  bool IsImpure(void) const noexcept;

  // Returns the functor to be applied.
  ParsedFunctor Functor(void) const noexcept;

  // Returns `true` if it's an application of the functor meant to get results,
  // and `false` if we're testing for the absence of results.
  bool IsPositive(void) const noexcept;

  // List of variables to pass at inputs. The Nth input variable corresponds
  // with the Nth `bound`-attributed variable in the parameter list of
  // `Functor()`.
  UsedNodeRange<DataVariable> InputVariables(void) const;

  // List of variables that are generated by applied this functor. The Nth
  // output variable corresponds with the Nth `free`-attributed variable
  // in the parameter list of `Functor()`. This will be empty if `IsFilter()`
  // is `true`.
  DefinedNodeRange<DataVariable> OutputVariables(void) const;

  // Return the body which is conditionally executed if the filter functor
  // returns true (`IsFilter() == true`) or if at least one tuple is generated
  // (as represented by `OutputVariables()`).
  std::optional<ProgramRegion> Body(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramGenerateRegion>::ProgramNode;
};

// A let binding is an assignment of variables.
class ProgramLetBindingRegion
    : public program::ProgramNode<ProgramLetBindingRegion> {
 public:
  static ProgramLetBindingRegion From(ProgramRegion) noexcept;

  DefinedNodeRange<DataVariable> DefinedVariables(void) const;
  UsedNodeRange<DataVariable> UsedVariables(void) const;

  // Return the body to which the lexical scoping of the variables applies.
  std::optional<ProgramRegion> Body(void) const noexcept;

 private:
  friend class ProgramRegion;
  using program::ProgramNode<ProgramLetBindingRegion>::ProgramNode;
};

enum class VectorUsage : unsigned {
  kInvalid,
  kInductionVector,
  kUnionInputVector,
  kJoinPivots,
  kProductInputVector,
  kProcedureInputVector,
  kTableScan
};

// Loop over a vector.
class ProgramVectorLoopRegion
    : public program::ProgramNode<ProgramVectorLoopRegion> {
 public:
  static ProgramVectorLoopRegion From(ProgramRegion) noexcept;

  // Return the loop body.
  std::optional<ProgramRegion> Body(void) const noexcept;

  VectorUsage Usage(void) const noexcept;
  DataVector Vector(void) const noexcept;

  // Variables extracted from the vector during each iteration of the loop
  DefinedNodeRange<DataVariable> TupleVariables(void) const;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramVectorLoopRegion>::ProgramNode;
};

// Append a tuple to a vector.
class ProgramVectorAppendRegion
    : public program::ProgramNode<ProgramVectorAppendRegion> {
 public:
  static ProgramVectorAppendRegion From(ProgramRegion) noexcept;

  VectorUsage Usage(void) const noexcept;
  DataVector Vector(void) const noexcept;
  UsedNodeRange<DataVariable> TupleVariables(void) const;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramVectorAppendRegion>::ProgramNode;
};

#define VECTOR_OP(name) \
  class name : public program::ProgramNode<name> { \
   public: \
    static name From(ProgramRegion) noexcept; \
    VectorUsage Usage(void) const noexcept; \
    DataVector Vector(void) const noexcept; \
   private: \
    friend class ProgramRegion; \
    using program::ProgramNode<name>::ProgramNode; \
  }

// Clear a vector.
VECTOR_OP(ProgramVectorClearRegion);

// Sort and unique the elements in a vector.
VECTOR_OP(ProgramVectorUniqueRegion);

#undef VECTOR_OP

class ProgramVectorSwapRegion
    : public program::ProgramNode<ProgramVectorSwapRegion> {
 public:
  static ProgramVectorSwapRegion From(ProgramRegion) noexcept;

  DataVector LHS(void) const noexcept;
  DataVector RHS(void) const noexcept;

 private:
  friend class ProgramRegion;
  using program::ProgramNode<ProgramVectorSwapRegion>::ProgramNode;
};

enum class TupleState : unsigned {
  kPresent,
  kAbsent,
  kUnknown,
  kAbsentOrUnknown
};

// Set the state of a tuple in a view. In the simplest case, this behaves like
// a SQL `INSERT` statement: it says that some data exists in a relation. There
// are two other states that can be set: absent, which is like a `DELETE`, and
// unknown, which has no SQL equivalent, but it like a tentative `DELETE`. An
// unknown tuple is one which has been speculatively marked as deleted, and
// needs to be re-proven in order via alternate means in order for it to be
// used.
class ProgramTransitionStateRegion
    : public program::ProgramNode<ProgramTransitionStateRegion> {
 public:
  static ProgramTransitionStateRegion From(ProgramRegion) noexcept;

  // The body that conditionally executes if the insert succeeds.
  std::optional<ProgramRegion> Body(void) const noexcept;

  unsigned Arity(void) const noexcept;

  UsedNodeRange<DataVariable> TupleVariables(void) const;

  DataTable Table(void) const;

  // We check if the tuple's current state is this.
  TupleState FromState(void) const noexcept;

  // If the tuple's prior state matches `FromState()`, then we change the state
  // to `ToState()`.
  TupleState ToState(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTransitionStateRegion>::ProgramNode;
};

// Check the state of a tuple. This is sort of like asking if something exists,
// but has three conditionally executed children, based off of the state.
// One state is that the tuple os missing from a view. The second state is
// that the tuple is present in the view. The final state is that we are
// not sure if the tuple is present or absent, because it has been marked
// as a candidate for deletion, and thus we need to re-prove it.
class ProgramCheckStateRegion
    : public program::ProgramNode<ProgramCheckStateRegion> {
 public:
  static ProgramCheckStateRegion From(ProgramRegion) noexcept;

  std::optional<ProgramRegion> IfPresent(void) const noexcept;
  std::optional<ProgramRegion> IfAbsent(void) const noexcept;
  std::optional<ProgramRegion> IfUnknown(void) const noexcept;

  unsigned Arity(void) const noexcept;

  UsedNodeRange<DataVariable> TupleVariables(void) const;

  DataTable Table(void) const;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramCheckStateRegion>::ProgramNode;
};

// Perform an equi-join between two or more tables, and iterate over the
// results.
class ProgramTableJoinRegion
    : public program::ProgramNode<ProgramTableJoinRegion> {
 public:
  static ProgramTableJoinRegion From(ProgramRegion) noexcept;

  // Unique ID for this join.
  unsigned Id(void) const noexcept;

  // The body that conditionally executes for each joined result. Variable
  // bindings are applied.
  std::optional<ProgramRegion> Body(void) const noexcept;

  // The pivot vector that contains the join pivots. The elements of this
  // pivot vector are in the same order as `OutputPivotVariables()`.
  DataVector PivotVector(void) const noexcept;

  // The tables that are joined together. The same table may appear more than
  // once.
  UsedNodeRange<DataTable> Tables(void) const;

  // The indices on the tables.
  UsedNodeRange<DataIndex> Indices(void) const;

  // The columns used in the scan of the Nth table. These are in the same
  // order as the entries in `PivotVector()` and `OutputPivotVariables()`.
  UsedNodeRange<DataColumn> IndexedColumns(unsigned table_index) const;

  // These are the output columns associated with the Nth table scan. These
  // do NOT include pivot columns.
  UsedNodeRange<DataColumn> SelectedColumns(unsigned table_index) const;

  // The index used by the Nth table scan.
  DataIndex Index(unsigned table_index) const noexcept;

  // These are the output variables for the pivot columns. These are in the same
  // order as the entries in the pivot vector.
  DefinedNodeRange<DataVariable> OutputPivotVariables(void) const;

  // These are the output variables from the Nth table scan. These do NOT include
  // pivot variables.
  DefinedNodeRange<DataVariable> OutputVariables(unsigned table_index) const;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTableJoinRegion>::ProgramNode;
};

// Perform an cross-product between two or more tables, and iterate
// over the results.
class ProgramTableProductRegion
    : public program::ProgramNode<ProgramTableProductRegion> {
 public:
  static ProgramTableProductRegion From(ProgramRegion) noexcept;

  // Unique ID of this region.
  unsigned Id(void) const noexcept;

  // The body that conditionally executes for each produced result. Variable
  // bindings are applied.
  std::optional<ProgramRegion> Body(void) const noexcept;

  // The tables that are joined together. The same table may appear more than
  // once.
  UsedNodeRange<DataTable> Tables(void) const;

  // The input vectors that need to be merged with all entries of the tables
  // that don't correspond to the input vectors themselves.
  UsedNodeRange<DataVector> Vectors(void) const;

  // The table used by the Nth table scan.
  DataTable Table(unsigned table_index) const noexcept;

  // The index used by the Nth table scan.
  DataVector Vector(unsigned table_index) const noexcept;

  // These are the output variables from the Nth table scan.
  DefinedNodeRange<DataVariable> OutputVariables(unsigned table_index) const;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTableProductRegion>::ProgramNode;
};

// Perform a scan over a table, possibly using an index. If an index is being
// used the input variables are provided to perform equality matching against
// column values. The results of the scan fill a vector.
class ProgramTableScanRegion
    : public program::ProgramNode<ProgramTableScanRegion> {
 public:
  static ProgramTableScanRegion From(ProgramRegion) noexcept;

  // The table being scanned.
  DataTable Table(void) const noexcept;

  // Optional index being scanned.
  std::optional<DataIndex> Index(void) const noexcept;

  // The columns used to constrain the scan of the table. These are in the same
  // order as the entries in `InputVariables()`. This is empty if an index isn't
  // being used.
  UsedNodeRange<DataColumn> IndexedColumns(void) const;

  // These are the output columns associated with the table scan. These
  // do NOT include any indexed columns.
  UsedNodeRange<DataColumn> SelectedColumns(void) const;

  // The variables being provided for each of the `IndexedColumns()`, which are
  // used to constrain the scan of the table. This is empty if an index isn't
  // being used.
  UsedNodeRange<DataVariable> InputVariables(void) const;

  // The scanned results are filled into this vector.
  DataVector FilledVector(void) const;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTableScanRegion>::ProgramNode;
};

// An inductive area in a program. An inductive area is split up into three
// regions:
//
//    1)  The initialization region, which is responsible for finding the inputs
//        to kick off the inductive cycle.
//    2)  The cyclic region, which iterates, appending on newly proven tuples
//        to one or more vectors. Iteration continues until a fixpoint is
//        reached.
//    2)  The output region, which iterates over all tuples amassed during the
//        initialization and cyclic regions, and operates on those tuples to
//        push to the next region of the data flow.
class ProgramInductionRegion
    : public program::ProgramNode<ProgramInductionRegion> {
 public:
  static ProgramInductionRegion From(ProgramRegion) noexcept;

  // Set of induction vectors that are filled with initial data in the
  // `Initializer()` region, then accumulate more data during the
  // `FixpointLoop()` region (and are tested), and then are finally iterated
  // and cleared in the `Output()` region.
  UsedNodeRange<DataVector> Vectors(void) const;

  ProgramRegion Initializer(void) const noexcept;
  ProgramRegion FixpointLoop(void) const noexcept;
  std::optional<ProgramRegion> Output(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramInductionRegion>::ProgramNode;
};

// A comparison between two tuples.
class ProgramTupleCompareRegion
    : public program::ProgramNode<ProgramTupleCompareRegion> {
 public:
  static ProgramTupleCompareRegion From(ProgramRegion) noexcept;

  ComparisonOperator Operator(void) const noexcept;

  // Variables in the left-hand side tuple.
  UsedNodeRange<DataVariable> LHS(void) const;

  // Variables in the right-hand side tuple.
  UsedNodeRange<DataVariable> RHS(void) const;

  // Code conditionally executed if the comparison is true.
  std::optional<ProgramRegion> Body(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTupleCompareRegion>::ProgramNode;
};

// Publishes a message to the pub/sub.
class ProgramPublishRegion : public program::ProgramNode<ProgramPublishRegion> {
 public:
  static ProgramPublishRegion From(ProgramRegion) noexcept;

  ParsedMessage Message(void) const noexcept;

  // List of variables being published.
  UsedNodeRange<DataVariable> VariableArguments(void) const;

  // Are we publishing the removal of some tuple?
  bool IsRemoval(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramPublishRegion>::ProgramNode;
};

enum class ProcedureKind : unsigned {

  // Function to initialize all relations. If any relation takes a purely
  // constant tuple as input, then this function initializes those flows.
  kInitializer,

  // Process an input vector of zero-or-more tuples received from the
  // network. This is a kind of bottom-up execution of the dataflow.
  kMessageHandler,

  // Given a tuple as input, return `true` if that tuple is present in the
  // database. This may do top-down execution of the data flows to re-prove
  // the tuple.
  kTupleFinder,

  // Given a tuple as input, this procedure removes it, then tries to prove
  // everything provable from it, and recursively remove those things. Removal
  // in this case is really a form of marking, i.e. marking the discovered
  // tuples as being in an unknown state.
  kTupleRemover,

  // Handles the cycle of an induction.
  kInductionCycleHandler,

  // Handles the outputs of an induction.
  kInductionOutputHandler
};

// A procedure in the program. All procedures return either `true` or `false`.
class ProgramProcedure : public program::ProgramNode<ProgramProcedure> {
 public:
  // Return the procedure containing another region.
  static ProgramProcedure Containing(ProgramRegion region) noexcept;

  // Unique ID of this procedure.
  unsigned Id(void) const noexcept;

  // What type of procedure is this?
  ProcedureKind Kind(void) const noexcept;

  // The message received and handled by this procedure.
  std::optional<ParsedMessage> Message(void) const noexcept;

  // Zero or more input vectors on which this procedure operates.
  DefinedNodeRange<DataVector> VectorParameters(void) const;

  // Zero or more input variables on which this procedure operates.
  DefinedNodeRange<DataVariable> VariableParameters(void) const;

  // Zero or more vectors on which this procedure operates.
  DefinedNodeRange<DataVector> DefinedVectors(void) const;

  // Return the region contained by this procedure.
  ProgramRegion Body(void) const noexcept;

 private:
  friend class ProgramCallRegion;

  using program::ProgramNode<ProgramProcedure>::ProgramNode;
};

// Calls another IR procedure. All IR procedures return `true` or `false`. This
// return value can be tested, and if it is, a body can be conditionally
// executed based off of the result of that test.
class ProgramCallRegion : public program::ProgramNode<ProgramCallRegion> {
 public:
  static ProgramCallRegion From(ProgramRegion) noexcept;

  unsigned Id(void) const noexcept;

  ProgramProcedure CalledProcedure(void) const noexcept;

  // List of variables passed as arguments to the procedure.
  UsedNodeRange<DataVariable> VariableArguments(void) const;

  // List of vectors passed as arguments to the procedure.
  UsedNodeRange<DataVector> VectorArguments(void) const;

  // Conditionally executed body, based on how the return value of the
  // procedure is tested.
  std::optional<ProgramRegion> Body(void) const noexcept;

  // Should we execute the body if the called procedure returns `true`?
  bool ExecuteBodyIfReturnIsTrue(void) const noexcept;

  // Should we execute the body if the called procedure returns `false`?
  bool ExecuteBodyIfReturnIsFalse(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramCallRegion>::ProgramNode;
};

// Returns `true` or `false` from a procedure.
class ProgramReturnRegion : public program::ProgramNode<ProgramReturnRegion> {
 public:
  static ProgramReturnRegion From(ProgramRegion) noexcept;

  bool ReturnsTrue(void) const noexcept;
  bool ReturnsFalse(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramReturnRegion>::ProgramNode;
};

// Specifies a relationship between a `#query` declaration, backing storage and
// various procedures needed to fill or check that storage. This information is
// sufficient for code generators to implement a notion of queries without
// imposing or prescribing a particular implementation in the control-flow IR.
class ProgramQuery {
 public:
  // The specific `#query` declaration associated with this entry point.
  const ParsedQuery query;

  // The backing table storing the data that is being queried.
  const DataTable table;

  // The index that must be scanned using any `bound`-attributed parameters
  // of the query declaration.
  const std::optional<DataIndex> index;

  // If present, a procedure which must be invoked on each scanned tuple from
  // the table / index.
  const std::optional<ProgramProcedure> tuple_checker;

  // If present, a procedure which must be invoked in order to ensure the
  // presence of any backing data. The parameters to this procedure are any
  // `bound`-attributed parameters of the query declaration.
  const std::optional<ProgramProcedure> forcing_function;

  inline explicit ProgramQuery(
      ParsedQuery query_, DataTable table_,
      std::optional<DataIndex> index_,
      std::optional<ProgramProcedure> tuple_checker_,
      std::optional<ProgramProcedure> forcing_function_)
      : query(query_),
        table(table_),
        index(index_),
        tuple_checker(tuple_checker_),
        forcing_function(forcing_function_) {}

  ProgramQuery(const ProgramQuery &) = default;
  ProgramQuery(ProgramQuery &&) noexcept = default;
};

// A program in its entirety.
class Program {
 public:
  // Build a program from a query.
  static std::optional<Program> Build(const Query &query, const ErrorLog &log);

  // All persistent tables needed to store data.
  DefinedNodeRange<DataTable> Tables(void) const;

  // List of all global constants.
  DefinedNodeRange<DataVariable> Constants(void) const;

  // List of all global variables.
  DefinedNodeRange<DataVariable> GlobalVariables(void) const;

  // List of all procedures.
  DefinedNodeRange<ProgramProcedure> Procedures(void) const;

  // List of query entry points.
  const std::vector<ProgramQuery> &Queries(void) const noexcept;

  // Return the query used to build this program.
  ::hyde::Query Query(void) const noexcept;

  // Return the parsed module used to build the query.
  ::hyde::ParsedModule ParsedModule(void) const noexcept;

  ~Program(void);

  Program(const Program &) = default;
  Program(Program &&) noexcept = default;
  Program &operator=(const Program &) = default;
  Program &operator=(Program &&) noexcept = default;

 private:
  Program(std::shared_ptr<ProgramImpl> impl_);

  std::shared_ptr<ProgramImpl> impl;
};

// `ProgramRegion` instances have an `Accept` method that will dispatch to the
// appropriate method in this class.
//
// NOTE(brad): This class only does dispatching, it doesn't do traversal.
class ProgramVisitor {
 public:
  virtual ~ProgramVisitor(void);
  virtual void Visit(DataColumn val);
  virtual void Visit(DataIndex val);
  virtual void Visit(DataTable val);
  virtual void Visit(DataVariable val);
  virtual void Visit(DataVector val);
  virtual void Visit(ProgramCallRegion val);
  virtual void Visit(ProgramReturnRegion val);
  virtual void Visit(ProgramExistenceAssertionRegion val);
  virtual void Visit(ProgramGenerateRegion val);
  virtual void Visit(ProgramInductionRegion val);
  virtual void Visit(ProgramLetBindingRegion val);
  virtual void Visit(ProgramParallelRegion val);
  virtual void Visit(ProgramProcedure val);
  virtual void Visit(ProgramPublishRegion val);
  virtual void Visit(ProgramSeriesRegion val);
  virtual void Visit(ProgramVectorAppendRegion val);
  virtual void Visit(ProgramVectorClearRegion val);
  virtual void Visit(ProgramVectorLoopRegion val);
  virtual void Visit(ProgramVectorSwapRegion val);
  virtual void Visit(ProgramVectorUniqueRegion val);
  virtual void Visit(ProgramTransitionStateRegion val);
  virtual void Visit(ProgramCheckStateRegion val);
  virtual void Visit(ProgramTableJoinRegion val);
  virtual void Visit(ProgramTableProductRegion val);
  virtual void Visit(ProgramTableScanRegion val);
  virtual void Visit(ProgramTupleCompareRegion val);
};

}  // namespace hyde
namespace std {

template <>
struct hash<::hyde::DataVariable> {
  using argument_type = ::hyde::DataVariable;
  using result_type = unsigned;
  inline unsigned operator()(::hyde::DataVariable var) const noexcept {
    return var.Id();
  }
};

template <>
struct hash<::hyde::DataVector> {
  using argument_type = ::hyde::DataVector;
  using result_type = unsigned;
  inline unsigned operator()(::hyde::DataVector vec) const noexcept {
    return vec.Id();
  }
};

template <>
struct hash<::hyde::DataColumn> {
  using argument_type = ::hyde::DataColumn;
  using result_type = unsigned;
  inline unsigned operator()(::hyde::DataColumn col) const noexcept {
    return col.Id();
  }
};

template <>
struct hash<::hyde::DataTable> {
  using argument_type = ::hyde::DataTable;
  using result_type = unsigned;
  inline unsigned operator()(::hyde::DataTable table) const noexcept {
    return table.Id();
  }
};

template <>
struct hash<::hyde::DataIndex> {
  using argument_type = ::hyde::DataIndex;
  using result_type = unsigned;
  inline unsigned operator()(::hyde::DataIndex index) const noexcept {
    return index.Id();
  }
};

}  // namespace std
