// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif

namespace hyde {

ProgramImpl::~ProgramImpl(void) {
  for (auto region : series_regions) {
    region->regions.ClearWithoutErasure();
  }
  for (auto region : parallel_regions) {
    region->regions.ClearWithoutErasure();
  }
  for (auto induction : induction_regions) {
    for (auto &entry : induction->view_to_init_appends) {
      entry.second.ClearWithoutErasure();
    }
    for (auto &entry : induction->view_to_cycle_appends) {
      entry.second.ClearWithoutErasure();
    }
    for (auto &entry : induction->view_to_cycle_loop) {
      entry.second.ClearWithoutErasure();
    }
    for (auto &entry : induction->view_to_output_loop) {
      entry.second.ClearWithoutErasure();
    }
    for (auto &entry : induction->view_to_vec) {
      entry.second.ClearWithoutErasure();
    }
    induction->init_region.ClearWithoutErasure();
    induction->cyclic_region.ClearWithoutErasure();
    induction->output_region.ClearWithoutErasure();
    induction->vectors.Clear();
  }

  for (auto op : operation_regions) {
    op->body.ClearWithoutErasure();
    if (auto let = op->AsLetBinding(); let) {
      let->used_vars.ClearWithoutErasure();

    } else if (auto vec_loop = op->AsVectorLoop(); vec_loop) {
      vec_loop->vector.ClearWithoutErasure();

    } else if (auto vec_append = op->AsVectorAppend(); vec_append) {
      vec_append->tuple_vars.ClearWithoutErasure();
      vec_append->vector.ClearWithoutErasure();

    } else if (auto vec_clear = op->AsVectorClear(); vec_clear) {
      vec_clear->vector.ClearWithoutErasure();

    } else if (auto view_insert = op->AsViewInsert(); view_insert) {
      view_insert->view.ClearWithoutErasure();
      view_insert->col_values.ClearWithoutErasure();

    } else if (auto view_join = op->AsViewJoin(); view_join) {
      view_join->views.ClearWithoutErasure();
      view_join->indices.ClearWithoutErasure();
    }
  }
  for (auto proc : procedure_regions) {
    proc->body.ClearWithoutErasure();
  }
}

Program::Program(std::shared_ptr<ProgramImpl> impl_)
    : impl(std::move(impl_)) {}

Program::~Program(void) {}

ProgramRegion::ProgramRegion(const ProgramExistenceCheckRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramInductionRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramLetBindingRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramParallelRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramSeriesRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramVectorAppendRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramVectorClearRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramVectorLoopRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramViewInsertRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramViewJoinRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

bool ProgramRegion::IsInduction(void) const noexcept {
  return impl->AsInduction() != nullptr;
}

bool ProgramRegion::IsVectorLoop(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsVectorLoop() != nullptr;
  } else {
    return false;
  }
}

bool ProgramRegion::IsVectorAppend(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsVectorAppend() != nullptr;
  } else {
    return false;
  }
}

bool ProgramRegion::IsVectorClear(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsVectorClear() != nullptr;
  } else {
    return false;
  }
}

bool ProgramRegion::IsLetBinding(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsLetBinding() != nullptr;
  } else {
    return false;
  }
}

bool ProgramRegion::IsViewInsert(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsViewInsert() != nullptr;
  } else {
    return false;
  }
}

bool ProgramRegion::IsViewJoin(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsViewJoin() != nullptr;
  } else {
    return false;
  }
}

bool ProgramRegion::IsExistenceCheck(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsExistenceCheck() != nullptr;
  } else {
    return false;
  }
}

bool ProgramRegion::IsSeries(void) const noexcept {
  return impl->AsSeries() != nullptr;
}

bool ProgramRegion::IsParallel(void) const noexcept {
  return impl->AsParallel() != nullptr;
}

ProgramSeriesRegion ProgramSeriesRegion::From(
    ProgramRegion region) noexcept {
  const auto derived_impl = region.impl->AsSeries();
  assert(derived_impl != nullptr);
  return ProgramSeriesRegion(derived_impl);
}

// The sequence of regions nested inside this series.
UsedNodeRange<ProgramRegion> ProgramSeriesRegion::Regions(void) const {
  return {impl->regions.begin(), impl->regions.end()};
}

ProgramParallelRegion ProgramParallelRegion::From(
    ProgramRegion region) noexcept {
  const auto derived_impl = region.impl->AsParallel();
  assert(derived_impl != nullptr);
  return ProgramParallelRegion(derived_impl);
}

// The set of regions nested inside this series.
UsedNodeRange<ProgramRegion> ProgramParallelRegion::Regions(void) const {
  return {impl->regions.begin(), impl->regions.end()};
}

ProgramExistenceCheckRegion
ProgramExistenceCheckRegion::From(ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsExistenceCheck();
  assert(derived_impl != nullptr);
  return ProgramExistenceCheckRegion(derived_impl);
}

bool ProgramExistenceCheckRegion::CheckForZero(void) const noexcept {
  return impl->op == ProgramOperation::kTestAllZero;
}

bool ProgramExistenceCheckRegion::CheckForNotZero(void) const noexcept {
  return impl->op == ProgramOperation::kTestAllNonZero;
}

// List of reference count variables to check.
UsedNodeRange<DataVariable>
ProgramExistenceCheckRegion::ReferenceCounts(void) const {
  return {impl->cond_vars.begin(), impl->cond_vars.end()};
}

// Return the body which is conditionally executed if all reference count
// variables are either all zero, or all non-zero.
std::optional<ProgramRegion>
ProgramExistenceCheckRegion::Body(void) const noexcept {
  if (impl->body) {
    return ProgramRegion(impl->body.get());
  } else {
    return std::nullopt;
  }
}

ProgramLetBindingRegion ProgramLetBindingRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsLetBinding();
  assert(derived_impl != nullptr);
  return ProgramLetBindingRegion(derived_impl);
}

// Return the body to which the lexical scoping of the variables applies.
std::optional<ProgramRegion> ProgramLetBindingRegion::Body(void) const noexcept {
  if (impl->body) {
    return ProgramRegion(impl->body.get());
  } else {
    return std::nullopt;
  }
}

ProgramVectorLoopRegion ProgramVectorLoopRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsVectorLoop();
  assert(derived_impl != nullptr);
  return ProgramVectorLoopRegion(derived_impl);
}

// Return the loop body.
std::optional<ProgramRegion> ProgramVectorLoopRegion::Body(void) const noexcept {
  if (impl->body) {
    return ProgramRegion(impl->body.get());
  } else {
    return std::nullopt;
  }
}

VectorUsage ProgramVectorLoopRegion::Usage(void) const noexcept {
  switch (impl->op) {
    case ProgramOperation::kLoopOverInductionInputVector:
      return VectorUsage::kInductionVector;
    case ProgramOperation::kLoopOverUnionInputVector:
      return VectorUsage::kUnionInputVector;
    case ProgramOperation::kLoopOverJoinPivots:
      return VectorUsage::kJoinPivots;
    case ProgramOperation::kLoopOverProductInputVector:
      return VectorUsage::kProductInputVector;
    case ProgramOperation::kLoopOverProductOutputVector:
      return VectorUsage::kProductOutputVector;
    case ProgramOperation::kLoopOverInputVector:
      return VectorUsage::kProcedureInputVector;
    default:
      assert(false);
      return VectorUsage::kInvalid;
  }
}

DataVector ProgramVectorLoopRegion::Vector(void) const noexcept {
  return DataVector(impl->vector.get());
}

DefinedNodeRange<DataVariable> ProgramVectorLoopRegion::TupleVariables(void) const {
  return {DefinedNodeIterator<DataVariable>(impl->defined_vars.begin()),
          DefinedNodeIterator<DataVariable>(impl->defined_vars.end())};
}

ProgramVectorAppendRegion ProgramVectorAppendRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsVectorAppend();
  assert(derived_impl != nullptr);
  return ProgramVectorAppendRegion(derived_impl);
}

VectorUsage ProgramVectorAppendRegion::Usage(void) const noexcept {
  switch (impl->op) {
    case ProgramOperation::kAppendInductionInputToVector:
      return VectorUsage::kInductionVector;
    case ProgramOperation::kAppendUnionInputToVector:
      return VectorUsage::kUnionInputVector;
    case ProgramOperation::kAppendJoinPivotsToVector:
      return VectorUsage::kJoinPivots;
    case ProgramOperation::kAppendProductInputToVector:
      return VectorUsage::kProductInputVector;
    case ProgramOperation::kAppendProductOutputToVector:
      return VectorUsage::kProductOutputVector;
    default:
      assert(false);
      return VectorUsage::kInvalid;
  }
}

DataVector ProgramVectorAppendRegion::Vector(void) const noexcept {
  return DataVector(impl->vector.get());
}

UsedNodeRange<DataVariable> ProgramVectorAppendRegion::TupleVariables(void) const {
  return {impl->tuple_vars.begin(), impl->tuple_vars.end()};
}

ProgramVectorClearRegion ProgramVectorClearRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsVectorClear();
  assert(derived_impl != nullptr);
  return ProgramVectorClearRegion(derived_impl);
}

VectorUsage ProgramVectorClearRegion::Usage(void) const noexcept {
  switch (impl->op) {
    case ProgramOperation::kClearJoinPivotVector:
      return VectorUsage::kJoinPivots;
    case ProgramOperation::kClearInductionInputVector:
      return VectorUsage::kInductionVector;
    default:
      assert(false);
      return VectorUsage::kInvalid;
  }
}

DataVector ProgramVectorClearRegion::Vector(void) const noexcept {
  return DataVector(impl->vector.get());
}

ProgramViewInsertRegion ProgramViewInsertRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsViewInsert();
  assert(derived_impl != nullptr);
  return ProgramViewInsertRegion(derived_impl);
}

// The body that conditionally executes if the insert succeeds.
std::optional<ProgramRegion> ProgramViewInsertRegion::Body(void) const noexcept {
  if (impl->body) {
    return ProgramRegion(impl->body.get());
  } else {
    return std::nullopt;
  }
}

UsedNodeRange<DataVariable> ProgramViewInsertRegion::TupleVariables(void) const {
  return {impl->col_values.begin(), impl->col_values.end()};
}

ProgramViewJoinRegion ProgramViewJoinRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsViewJoin();
  assert(derived_impl != nullptr);
  return ProgramViewJoinRegion(derived_impl);
}

// The body that conditionally executes for each joined result. Variable
// bindings are applied.
std::optional<ProgramRegion> ProgramViewJoinRegion::Body(void) const noexcept {
  if (impl->body) {
    return ProgramRegion(impl->body.get());
  } else {
    return std::nullopt;
  }
}

ProgramProcedure::ProgramProcedure(const ProgramVectorProcedure &proc)
    : program::ProgramNode<ProgramProcedure>(proc.impl) {}

ProgramProcedure::ProgramProcedure(const ProgramTupleProcedure &proc)
    : program::ProgramNode<ProgramProcedure>(proc.impl) {}

bool ProgramProcedure::OperatesOnTuple(void) const noexcept {
  return impl->AsTuple();
}

bool ProgramProcedure::OperatesOnVector(void) const noexcept {
  return impl->AsVector();
}

VariableRole DataVariable::DefiningRole(void) const noexcept {
  return impl->role;
}

// The region which defined this local variable. If this variable has no
// defining region then it is a global variable.
std::optional<ProgramRegion> DataVariable::DefiningRegion(void) const noexcept {
  if (impl->defining_region) {
    return ProgramRegion(impl->defining_region);
  } else {
    return std::nullopt;
  }
}

// Unique ID of this variable.
unsigned DataVariable::Id(void) const noexcept {
  return impl->id;
}

// Name of this variable, if any. There might not be a name.
Token DataVariable::Name(void) const noexcept {
  if (impl->query_column) {
    return impl->query_column->Variable().Name();

  } else if (impl->query_cond) {
    if (auto pred = impl->query_cond->Predicate(); pred) {
      return pred->Name();
    }
  }
  return Token();
}

// The literal, constant value of this variable.
std::optional<ParsedLiteral> DataVariable::Value(void) const noexcept {
  if (impl->query_column) {
    if (impl->query_column->IsConstantOrConstantRef()) {
      return QueryConstant::From(*impl->query_column).Literal();
    }
  } else if (impl->query_const) {
    return impl->query_const->Literal();
  }
  return std::nullopt;
}

// Type of this variable.
TypeKind DataVariable::Type(void) const noexcept {
  switch (impl->role) {
    case VariableRole::kConditionRefCount:
      return TypeKind::kUnsigned64;
    case VariableRole::kConstant:
      if (impl->query_const) {
        return impl->query_const->Literal().Type().Kind();
      }
      [[clang::fallthrough]];
    case VariableRole::kVectorVariable:
    case VariableRole::kLetBinding:
    case VariableRole::kJoinNonPivot:
      assert(!!impl->query_column);
      return impl->query_column->Type().Kind();
  }
}

VectorKind DataVector::Kind(void) const noexcept {
  return impl->kind;
}

unsigned DataVector::Id(void) const noexcept {
  return impl->id;
}

bool DataVector::IsInputVector(void) const noexcept {
  return impl->id == VECTOR::kInputVectorId;
}

const std::vector<TypeKind> DataVector::ColumnTypes(void) const noexcept {
  return impl->col_types;
}

// Return the region contained by this procedure.
ProgramRegion ProgramProcedure::Body(void) const noexcept {
  return ProgramRegion(impl->body.get());
}
ProgramVectorProcedure
ProgramVectorProcedure::From(ProgramProcedure proc) noexcept {
  const auto vec_impl = proc.impl->AsVector();
  assert(vec_impl != nullptr);
  return ProgramVectorProcedure(vec_impl);
}

// The message received and handled by this procedure.
ParsedMessage ProgramVectorProcedure::Message(void) const noexcept {
  return ParsedMessage::From(impl->io.Declaration());
}

// The input vector that this procedure will operate on.
DataVector ProgramVectorProcedure::InputVector(void) const noexcept {
  return DataVector(impl->vectors[0]);
}

// Return the region contained by this procedure.
ProgramRegion ProgramVectorProcedure::Body(void) const noexcept {
  return ProgramRegion(impl->body.get());
}

ProgramTupleProcedure
ProgramTupleProcedure::From(ProgramProcedure proc) noexcept {
  const auto tuple_impl = proc.impl->AsTuple();
  assert(tuple_impl != nullptr);
  return ProgramTupleProcedure(tuple_impl);
}

// Return the region contained by this procedure.
ProgramRegion ProgramTupleProcedure::Body(void) const noexcept {
  return ProgramRegion(impl->body.get());
}

ProgramInductionRegion
ProgramInductionRegion::From(ProgramRegion region) noexcept {
  const auto derived_impl = region.impl->AsInduction();
  assert(derived_impl != nullptr);
  return ProgramInductionRegion(derived_impl);
}

// Set of induction vectors that are filled with initial data in the
// `Initializer()` region, then accumulate more data during the
// `FixpointLoop()` region (and are tested), and then are finally iterated
// and cleared in the `Output()` region.
UsedNodeRange<DataVector> ProgramInductionRegion::Vectors(void) const {
  return {impl->vectors.begin(), impl->vectors.end()};
}

ProgramRegion ProgramInductionRegion::Initializer(void) const noexcept {
  return ProgramRegion(impl->init_region.get());
}

ProgramRegion ProgramInductionRegion::FixpointLoop(void) const noexcept {
  return ProgramRegion(impl->cyclic_region.get());
}

ProgramRegion ProgramInductionRegion::Output(void) const noexcept {
  return ProgramRegion(impl->output_region.get());
}

// List of all global constants.
DefinedNodeRange<DataVariable> Program::Constants(void) const {
  return {DefinedNodeIterator<DataVariable>(impl->const_vars.begin()),
          DefinedNodeIterator<DataVariable>(impl->const_vars.end())};
}

// Return the list of all vector procedures.
DefinedNodeRange<ProgramProcedure> Program::Procedures(void) const {
  return {DefinedNodeIterator<ProgramProcedure>(impl->procedure_regions.begin()),
          DefinedNodeIterator<ProgramProcedure>(impl->procedure_regions.end())};
}

}  // namespace hyde

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif
