// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/Node.h>

#include <functional>
#include <memory>
#include <optional>

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
    return impl == that.impl;
  }

  inline bool operator<(ProgramNode<T> that) const {
    return impl < that.impl;
  }

  uintptr_t UniqueId(void) const noexcept {
    return reinterpret_cast<uintptr_t>(impl);
  }

  // Non-mutating visitor acceptor
  // virtual void Accept(ProgramVisitor &visitor) const = 0;

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
class ProgramExistenceAssertionRegion;
class ProgramExistenceCheckRegion;
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
class ProgramVectorUniqueRegion;
class ProgramTableInsertRegion;
class ProgramTableJoinRegion;
class ProgramTableProductRegion;
class ProgramTupleCompareRegion;

// A depth-first program visitor.
class ProgramVisitor {
 public:
  // virtual void Visit(const DataColumn &val) = 0;
  // virtual void Visit(const DataIndex &val) = 0;
  // virtual void Visit(const DataTable &val) = 0;
  // virtual void Visit(const DataVariable &val) = 0;
  // virtual void Visit(const DataVector &val) = 0;

  // virtual void Visit(const ProgramCallRegion &val) = 0;
  // virtual void Visit(const ProgramExistenceAssertionRegion &val) = 0;
  // virtual void Visit(const ProgramExistenceCheckRegion &val) = 0;
  // virtual void Visit(const ProgramGenerateRegion &val) = 0;
  // virtual void Visit(const ProgramInductionRegion &val) = 0;
  // virtual void Visit(const ProgramLetBindingRegion &val) = 0;
  // virtual void Visit(const ProgramParallelRegion &val) = 0;
  // virtual void Visit(const ProgramProcedure &val) = 0;
  // virtual void Visit(const ProgramPublishRegion &val) = 0;
  // virtual void Visit(const ProgramSeriesRegion &val) = 0;
  // virtual void Visit(const ProgramVectorAppendRegion &val) = 0;
  // virtual void Visit(const ProgramVectorClearRegion &val) = 0;
  // virtual void Visit(const ProgramVectorLoopRegion &val) = 0;
  // virtual void Visit(const ProgramVectorUniqueRegion &val) = 0;
  // virtual void Visit(const ProgramTableInsertRegion &val) = 0;
  // virtual void Visit(const ProgramTableJoinRegion &val) = 0;
  // virtual void Visit(const ProgramTableProductRegion &val) = 0;
  // virtual void Visit(const ProgramTupleCompareRegion &val) = 0;

  virtual void Visit(const Program &val) = 0;
};


// A generic region of code nested inside of a procedure.
class ProgramRegion : public program::ProgramNode<ProgramRegion> {
 public:
  ProgramRegion(const ProgramCallRegion &);
  ProgramRegion(const ProgramExistenceAssertionRegion &);
  ProgramRegion(const ProgramExistenceCheckRegion &);
  ProgramRegion(const ProgramGenerateRegion &);
  ProgramRegion(const ProgramInductionRegion &);
  ProgramRegion(const ProgramLetBindingRegion &);
  ProgramRegion(const ProgramParallelRegion &);
  ProgramRegion(const ProgramPublishRegion &);
  ProgramRegion(const ProgramSeriesRegion &);
  ProgramRegion(const ProgramVectorAppendRegion &);
  ProgramRegion(const ProgramVectorClearRegion &);
  ProgramRegion(const ProgramVectorLoopRegion &);
  ProgramRegion(const ProgramVectorUniqueRegion &);
  ProgramRegion(const ProgramTableInsertRegion &);
  ProgramRegion(const ProgramTableJoinRegion &);
  ProgramRegion(const ProgramTableProductRegion &);
  ProgramRegion(const ProgramTupleCompareRegion &);

  bool IsCall(void) const noexcept;
  bool IsExistenceCheck(void) const noexcept;
  bool IsExistenceAssertion(void) const noexcept;
  bool IsGenerate(void) const noexcept;
  bool IsInduction(void) const noexcept;
  bool IsVectorLoop(void) const noexcept;
  bool IsVectorAppend(void) const noexcept;
  bool IsVectorClear(void) const noexcept;
  bool IsVectorUnique(void) const noexcept;
  bool IsLetBinding(void) const noexcept;
  bool IsTableInsert(void) const noexcept;
  bool IsTableJoin(void) const noexcept;
  bool IsTableProduct(void) const noexcept;
  bool IsSeries(void) const noexcept;
  bool IsParallel(void) const noexcept;
  bool IsPublish(void) const noexcept;
  bool IsTupleCompare(void) const noexcept;

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  friend class ProgramCallRegion;
  friend class ProgramExistenceAssertionRegion;
  friend class ProgramExistenceCheckRegion;
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
  friend class ProgramVectorUniqueRegion;
  friend class ProgramTableInsertRegion;
  friend class ProgramTableJoinRegion;
  friend class ProgramTableProductRegion;
  friend class ProgramTupleCompareRegion;

  using program::ProgramNode<ProgramRegion>::ProgramNode;
};

// A generic sequencing of regions.
class ProgramSeriesRegion : public program::ProgramNode<ProgramSeriesRegion> {
 public:
  static ProgramSeriesRegion From(ProgramRegion) noexcept;

  // The sequence of regions nested inside this series.
  UsedNodeRange<ProgramRegion> Regions(void) const;

  // void Accept(ProgramVisitor &visitor) const override;

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

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramParallelRegion>::ProgramNode;
};

enum class VariableRole : int {
  kConditionRefCount,
  kConstant,
  kVectorVariable,
  kLetBinding,
  kJoinPivot,
  kJoinNonPivot,
  kProductOutput,
  kFunctorOutput
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
  TypeKind Type(void) const noexcept;

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  using program::ProgramNode<DataVariable>::ProgramNode;
};

enum class VectorKind : unsigned {
  kInput,
  kInduction,
  kJoinPivots,
  kProductInput
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

  // void Accept(ProgramVisitor &visitor) const override;

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
  UsedNodeRange<DataColumn> Columns(void) const;

  // void Accept(ProgramVisitor &visitor) const override;

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

  // void Accept(ProgramVisitor &visitor) const override;

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

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  using program::ProgramNode<DataVector>::ProgramNode;
};

// A zero or not-zero check on some reference counters that track whether or
// not some set of tuples exists.
class ProgramExistenceCheckRegion
    : public program::ProgramNode<ProgramExistenceCheckRegion> {
 public:
  static ProgramExistenceCheckRegion From(ProgramRegion) noexcept;

  bool CheckForZero(void) const noexcept;
  bool CheckForNotZero(void) const noexcept;

  // List of reference count variables to check.
  UsedNodeRange<DataVariable> ReferenceCounts(void) const;

  // Return the body which is conditionally executed if all reference count
  // variables are either all zero, or all non-zero.
  std::optional<ProgramRegion> Body(void) const noexcept;

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramExistenceCheckRegion>::ProgramNode;
};

// Increment or decrement a reference counter, asserting that some set of tuples
// exists or does not exist.
class ProgramExistenceAssertionRegion
    : public program::ProgramNode<ProgramExistenceAssertionRegion> {
 public:
  static ProgramExistenceAssertionRegion From(ProgramRegion) noexcept;

  bool IsIncrement(void) const noexcept;
  bool IsDecrement(void) const noexcept;

  // List of reference count variables that are mutated.
  UsedNodeRange<DataVariable> ReferenceCounts(void) const;

  // void Accept(ProgramVisitor &visitor) const override;

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

  // void Accept(ProgramVisitor &visitor) const override;

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

  // void Accept(ProgramVisitor &visitor) const override;

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
  DefinedNodeRange<DataVariable> TupleVariables(void) const;

  // void Accept(ProgramVisitor &visitor) const override;

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

  // void Accept(ProgramVisitor &visitor) const override;

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
\
   private: \
    friend class ProgramRegion; \
    using program::ProgramNode<name>::ProgramNode; \
  }

// Clear a vector.
VECTOR_OP(ProgramVectorClearRegion);

// Sort and unique the elements in a vector.
VECTOR_OP(ProgramVectorUniqueRegion);

#undef VECTOR_OP

// Insert a tuple into a view.
class ProgramTableInsertRegion
    : public program::ProgramNode<ProgramTableInsertRegion> {
 public:
  static ProgramTableInsertRegion From(ProgramRegion) noexcept;

  // The body that conditionally executes if the insert succeeds.
  std::optional<ProgramRegion> Body(void) const noexcept;

  unsigned Arity(void) const noexcept;

  UsedNodeRange<DataVariable> TupleVariables(void) const;

  DataTable Table(void) const;

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTableInsertRegion>::ProgramNode;
};

// Perform an equi-join between two or more tables, and iterate over the
// results.
class ProgramTableJoinRegion
    : public program::ProgramNode<ProgramTableJoinRegion> {
 public:
  static ProgramTableJoinRegion From(ProgramRegion) noexcept;

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

  // void Accept(ProgramVisitor &visitor) const override;

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

  // The body that conditionally executes for each produced result. Variable
  // bindings are applied.
  std::optional<ProgramRegion> Body(void) const noexcept;

  // The tables that are joined together. The same table may appear more than
  // once.
  UsedNodeRange<DataTable> Tables(void) const;

  // The input vectors that need to be merged with all entries of the tables
  // that don't correspond to the input vectors themselves.
  UsedNodeRange<DataVector> Vectors(void) const;

  // The index used by the Nth table scan.
  DataVector Vector(unsigned table_index) const noexcept;

  // These are the output variables from the Nth table scan.
  DefinedNodeRange<DataVariable> OutputVariables(unsigned table_index) const;

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTableProductRegion>::ProgramNode;
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

  // void Accept(ProgramVisitor &visitor) const override;

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

  // void Accept(ProgramVisitor &visitor) const override;

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

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramPublishRegion>::ProgramNode;
};

enum class ProcedureKind : unsigned { kInitializer, kMessageHandler };

// A procedure in the program.
class ProgramProcedure : public program::ProgramNode<ProgramProcedure> {
 public:
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

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  friend class ProgramCallRegion;

  using program::ProgramNode<ProgramProcedure>::ProgramNode;
};

// Calls another IR procedure.
class ProgramCallRegion : public program::ProgramNode<ProgramCallRegion> {
 public:
  static ProgramCallRegion From(ProgramRegion) noexcept;

  ProgramProcedure CalledProcedure(void) const noexcept;

  // List of variables passed as arguments to the procedure.
  UsedNodeRange<DataVariable> VariableArguments(void) const;

  // List of vectors passed as arguments to the procedure.
  UsedNodeRange<DataVector> VectorArguments(void) const;

  // void Accept(ProgramVisitor &visitor) const override;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramCallRegion>::ProgramNode;
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

  ~Program(void);

  Program(const Program &) = default;
  Program(Program &&) noexcept = default;
  Program &operator=(const Program &) = default;
  Program &operator=(Program &&) noexcept = default;

  void Accept(ProgramVisitor &visitor) const;

 private:
  Program(std::shared_ptr<ProgramImpl> impl_);

  std::shared_ptr<ProgramImpl> impl;
};

}  // namespace hyde
