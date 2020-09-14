// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

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

enum class TableKind : int {
  kPersistent,
  kVector,
  kInputVector
};

enum class VariableRole : int {
  kParameter, kLocal, kFree, kGlobalBoolean, kConditionRefCount
};

class ProgramInductionRegion;
class ProgramLetBindingRegion;
class ProgramParallelRegion;
class ProgramProcedure;
class ProgramSeriesRegion;
class ProgramVectorAppendRegion;
class ProgramVectorLoopRegion;
class ProgramViewInsertRegion;
class ProgramViewJoinRegion;

// A generic region of code nested inside of a procedure.
class ProgramRegion : public program::ProgramNode<ProgramRegion> {
 public:
  ProgramRegion(const ProgramInductionRegion &);
  ProgramRegion(const ProgramLetBindingRegion &);
  ProgramRegion(const ProgramParallelRegion &);
  ProgramRegion(const ProgramSeriesRegion &);
  ProgramRegion(const ProgramVectorAppendRegion &);
  ProgramRegion(const ProgramVectorLoopRegion &);
  ProgramRegion(const ProgramViewInsertRegion &);
  ProgramRegion(const ProgramViewJoinRegion &);

  bool IsInduction(void) const noexcept;
  bool IsVectorLoop(void) const noexcept;
  bool IsVectorAppend(void) const noexcept;
  bool IsLetBinding(void) const noexcept;
  bool IsViewInsert(void) const noexcept;
  bool IsViewJoin(void) const noexcept;
  bool IsSeries(void) const noexcept;
  bool IsParallel(void) const noexcept;

 private:
  friend class ProgramInductionRegion;
  friend class ProgramLetBindingRegion;
  friend class ProgramParallelRegion;
  friend class ProgramProcedure;
  friend class ProgramSeriesRegion;
  friend class ProgramVectorAppendRegion;
  friend class ProgramVectorLoopRegion;
  friend class ProgramViewInsertRegion;
  friend class ProgramViewJoinRegion;

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

class ProgramVariable : public program::ProgramNode<ProgramVariable> {
 public:
};

class GlobalVariable : public program::ProgramNode<GlobalVariable> {
 public:
};

enum class VariableDefinitionRole : int {
  kLetBinding
};

enum class VariableUsageRole : int {
  kLetBinding
};

// Local variable within a function.
class LocalVariable : public program::ProgramNode<LocalVariable> {
 public:
  TypeKind Type(void) const noexcept;

  VariableDefinitionRole Role(void) const noexcept;

  // The region which defined this local variable.
  ProgramRegion DefiningRegion(void) const noexcept;

  // Regions that directly use this local variable.
  void ForEachUsage(std::function<void(ProgramRegion, VariableUsageRole)>);

 private:
  using program::ProgramNode<LocalVariable>::ProgramNode;
};

// A let binding is an assignment of variables.
class ProgramLetBindingRegion
    : public program::ProgramNode<ProgramLetBindingRegion> {
 public:
  static ProgramLetBindingRegion From(ProgramRegion) noexcept;

  // Return the body to which the lexical scoping of the variables applies.
  std::optional<ProgramRegion> Body(void) const noexcept;

 private:
  friend class ProgramRegion;
  using program::ProgramNode<ProgramLetBindingRegion>::ProgramNode;
};

enum class VectorKind {
  kJoinPivots,
};

// Loop over a vector.
class ProgramVectorLoopRegion
    : public program::ProgramNode<ProgramVectorLoopRegion> {
 public:
  static ProgramVectorLoopRegion From(ProgramRegion) noexcept;

  // Return the loop body.
  std::optional<ProgramRegion> Body(void) const noexcept;

  VectorKind Kind(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramVectorLoopRegion>::ProgramNode;
};

// Append a tuple to a vector.
class ProgramVectorAppendRegion
    : public program::ProgramNode<ProgramVectorAppendRegion> {
 public:
  static ProgramVectorAppendRegion From(ProgramRegion) noexcept;

  VectorKind Kind(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramVectorAppendRegion>::ProgramNode;
};

// Insert a tuple into a view.
class ProgramViewInsertRegion
    : public program::ProgramNode<ProgramViewInsertRegion> {
 public:
  static ProgramViewInsertRegion From(ProgramRegion) noexcept;

  // The body that conditionally executes if the insert succeeds.
  std::optional<ProgramRegion> Body(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramViewInsertRegion>::ProgramNode;
};

// Perform an equi-join between two or more views, and iterate over the results.
class ProgramViewJoinRegion
    : public program::ProgramNode<ProgramViewJoinRegion> {
 public:
  static ProgramViewJoinRegion From(ProgramRegion) noexcept;

  // The body that conditionally executes for each joined result. Variable
  // bindings are applied.
  std::optional<ProgramRegion> Body(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramViewJoinRegion>::ProgramNode;
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

  ProgramRegion Initializer(void) const noexcept;
  ProgramRegion FixpointLoop(void) const noexcept;
  ProgramRegion Output(void) const noexcept;

 private:
  friend class ProgramRegion;

  using program::ProgramNode<ProgramInductionRegion>::ProgramNode;
};

class ProgramVectorProcedure;
class ProgramTupleProcedure;

// A procedure in the program. It could be a vector procedure or a tuple
// procedure.
class ProgramProcedure : public program::ProgramNode<ProgramProcedure> {
 public:
  ProgramProcedure(const ProgramVectorProcedure &);
  ProgramProcedure(const ProgramTupleProcedure &);

  bool OperatesOnVector(void) const noexcept;
  bool OperatesOnTuple(void) const noexcept;

  // Return the region contained by this procedure.
  ProgramRegion Body(void) const noexcept;

 private:
  friend class ProgramVectorProcedure;
  friend class ProgramTupleProcedure;

  using program::ProgramNode<ProgramProcedure>::ProgramNode;
};

// A procedure in the program, operating on a vector of tuples. Vector
// procedures correspond with received I/Os in the data flow representation,
// and messages used in clause bodies in the parsed representation.
//
// Vector procedures contain a single region, and are the entrypoints of a
// program. They can call external functions, such as functors (used in
// key/value indices, aggregates, and mapping/filter functions), and they
// can also call tuple procedures.
//
// Vector procedures perform bottom-up derivations of all derivable rules given
// an input set of new facts.
class ProgramVectorProcedure : public program::ProgramNode<ProgramVectorProcedure> {
 public:
  static ProgramVectorProcedure From(ProgramProcedure proc) noexcept;

  // Return the region contained by this procedure.
  ProgramRegion Body(void) const noexcept;

 private:
  friend class ProgramProcedure;

  using program::ProgramNode<ProgramVectorProcedure>::ProgramNode;
};

// A procedure in the program,  operating on a single tuple.
//
// Tuple procedures contain a single region, and are only called from inside of
// vector procedures.
//
// Tuple procedures are highly recursive, and behave like the bottom-up push
// method of Datalog execution, as described by Stefan Brass. However, whereas
// the push method is used for normal Datalog execution (proving rule heads
// from facts in the clause bodies), tuple procedurs are instead used for
// *removing* facts. That is, they recursively apply removals, so as to support
// differential updates.
class ProgramTupleProcedure : public program::ProgramNode<ProgramTupleProcedure> {
 public:
  static ProgramTupleProcedure From(ProgramProcedure proc) noexcept;

  // Return the region contained by this procedure.
  ProgramRegion Body(void) const noexcept;

 private:
  friend class ProgramProcedure;

  using program::ProgramNode<ProgramTupleProcedure>::ProgramNode;
};

class Program {
 public:
  // Build a program from a query.
  static std::optional<Program> Build(const Query &query, const ErrorLog &log);

  // Return the list of all vector procedures.
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
