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

class ErrorLog;
class ProgramImpl;
class Query;

enum class TypeKind : int;

namespace program {

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

 protected:
  friend class ::hyde::ProgramImpl;

  Node<T> *impl;
};

}  // namespace program

class DataColumn;
class DataTable;
class DataVariable;
class DataVector;

class ProgramExistenceCheckRegion;
class ProgramInductionRegion;
class ProgramLetBindingRegion;
class ProgramParallelRegion;
class ProgramProcedure;
class ProgramSeriesRegion;
class ProgramVectorAppendRegion;
class ProgramVectorClearRegion;
class ProgramVectorLoopRegion;
class ProgramVectorUniqueRegion;
class ProgramTableInsertRegion;
class ProgramTableJoinRegion;
class ProgramTupleCompareRegion;

// A generic region of code nested inside of a procedure.
class ProgramRegion : public program::ProgramNode<ProgramRegion> {
 public:
  ProgramRegion(const ProgramExistenceCheckRegion &);
  ProgramRegion(const ProgramInductionRegion &);
  ProgramRegion(const ProgramLetBindingRegion &);
  ProgramRegion(const ProgramParallelRegion &);
  ProgramRegion(const ProgramSeriesRegion &);
  ProgramRegion(const ProgramVectorAppendRegion &);
  ProgramRegion(const ProgramVectorClearRegion &);
  ProgramRegion(const ProgramVectorLoopRegion &);
  ProgramRegion(const ProgramVectorUniqueRegion &);
  ProgramRegion(const ProgramTableInsertRegion &);
  ProgramRegion(const ProgramTableJoinRegion &);

  bool IsInduction(void) const noexcept;
  bool IsVectorLoop(void) const noexcept;
  bool IsVectorAppend(void) const noexcept;
  bool IsVectorClear(void) const noexcept;
  bool IsVectorUnique(void) const noexcept;
  bool IsLetBinding(void) const noexcept;
  bool IsTableInsert(void) const noexcept;
  bool IsTableJoin(void) const noexcept;
  bool IsSeries(void) const noexcept;
  bool IsExistenceCheck(void) const noexcept;
  bool IsParallel(void) const noexcept;
  bool IsTupleCompare(void) const noexcept;

 private:
  friend class ProgramExistenceCheckRegion;
  friend class ProgramInductionRegion;
  friend class ProgramLetBindingRegion;
  friend class ProgramParallelRegion;
  friend class ProgramProcedure;
  friend class ProgramSeriesRegion;
  friend class ProgramVectorAppendRegion;
  friend class ProgramVectorClearRegion;
  friend class ProgramVectorLoopRegion;
  friend class ProgramVectorUniqueRegion;
  friend class ProgramTableInsertRegion;
  friend class ProgramTableJoinRegion;
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
  kConditionRefCount, kConstant, kVectorVariable, kLetBinding, kJoinPivot,
  kJoinNonPivot
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

 private:
  using program::ProgramNode<DataVariable>::ProgramNode;
};

enum class VectorKind : unsigned {
  kInput,
  kInduction,
  kJoinPivots,
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
  UsedNodeRange<DataColumn> Columns(void) const;

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

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramExistenceCheckRegion>::ProgramNode;
};

// A let binding is an assignment of variables.
class ProgramLetBindingRegion
    : public program::ProgramNode<ProgramLetBindingRegion> {
 public:
  static ProgramLetBindingRegion From(ProgramRegion) noexcept;

  DefinedNodeRange<DataVariable> DefinedVars(void) const;
  UsedNodeRange<DataVariable> UsedVars(void) const;

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
  kProductOutputVector,
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

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTableInsertRegion>::ProgramNode;
};

// Perform an equi-join between two or more views, and iterate over the results.
class ProgramTableJoinRegion
    : public program::ProgramNode<ProgramTableJoinRegion> {
 public:
  static ProgramTableJoinRegion From(ProgramRegion) noexcept;

  // The body that conditionally executes for each joined result. Variable
  // bindings are applied.
  std::optional<ProgramRegion> Body(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramTableJoinRegion>::ProgramNode;
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

// A procedure in the program.
class ProgramProcedure : public program::ProgramNode<ProgramProcedure> {
 public:
  // Unique ID of this procedure.
  unsigned Id(void) const noexcept;

  // The message received and handled by this procedure.
  std::optional<ParsedMessage> Message(void) const noexcept;

  // Zero or more input vectors on which this procedure operates.
  DefinedNodeRange<DataVector> InputVectors(void) const;

  // Zero or more vectors on which this procedure operates.
  DefinedNodeRange<DataVector> DefinedVectors(void) const;

  // Return the region contained by this procedure.
  ProgramRegion Body(void) const noexcept;

 private:
  using program::ProgramNode<ProgramProcedure>::ProgramNode;
};

class Program {
 public:
  // Build a program from a query.
  static std::optional<Program> Build(const Query &query, const ErrorLog &log);

  // All persistent tables needed to store data.
  DefinedNodeRange<DataTable> Tables(void) const;

  // List of all global constants.
  DefinedNodeRange<DataVariable> Constants(void) const;

  // List of all procedures.
  DefinedNodeRange<ProgramProcedure> Procedures(void) const;

  ~Program(void);

  Program(const Program &) = default;
  Program(Program &&) noexcept = default;
  Program &operator=(const Program &) = default;
  Program &operator=(Program &&) noexcept = default;

 private:
  Program(std::shared_ptr<ProgramImpl> impl_);

  std::shared_ptr<ProgramImpl> impl;
};

}  // namespace hyde
