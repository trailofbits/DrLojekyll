// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif

namespace hyde {

ProgramImpl::~ProgramImpl(void) {
  for (auto table : tables) {
    for (auto column : table->columns) {
      column->table.ClearWithoutErasure();
    }
    for (auto index : table->indices) {
      index->table.ClearWithoutErasure();
      index->columns.ClearWithoutErasure();
    }
  }

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

    } else if (auto view_insert = op->AsTableInsert(); view_insert) {
      view_insert->table.ClearWithoutErasure();
      view_insert->col_values.ClearWithoutErasure();

    } else if (auto view_join = op->AsTableJoin(); view_join) {
      view_join->tables.ClearWithoutErasure();
      view_join->indices.ClearWithoutErasure();

    } else if (auto exists_check = op->AsExistenceCheck(); exists_check) {
      exists_check->cond_vars.ClearWithoutErasure();

    } else if (auto cmp = op->AsTupleCompare(); cmp) {
      cmp->lhs_vars.ClearWithoutErasure();
      cmp->rhs_vars.ClearWithoutErasure();
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

ProgramRegion::ProgramRegion(const ProgramTableInsertRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramTableJoinRegion &region)
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

bool ProgramRegion::IsTableInsert(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsTableInsert() != nullptr;
  } else {
    return false;
  }
}

bool ProgramRegion::IsTableJoin(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsTableJoin() != nullptr;
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

bool ProgramRegion::IsTupleCompare(void) const noexcept {
  if (auto op = impl->AsOperation(); op) {
    return op->AsTupleCompare() != nullptr;
  } else {
    return false;
  }
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

DefinedNodeRange<DataVariable>
ProgramLetBindingRegion::DefinedVars(void) const {
  return {DefinedNodeIterator<DataVariable>(impl->defined_vars.begin()),
          DefinedNodeIterator<DataVariable>(impl->defined_vars.end())};
}

UsedNodeRange<DataVariable> ProgramLetBindingRegion::UsedVars(void) const {
  return {impl->used_vars.begin(), impl->used_vars.end()};
}

// Return the body to which the lexical scoping of the variables applies.
std::optional<ProgramRegion>
ProgramLetBindingRegion::Body(void) const noexcept {
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
std::optional<ProgramRegion>
ProgramVectorLoopRegion::Body(void) const noexcept {
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

ProgramTableInsertRegion ProgramTableInsertRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsTableInsert();
  assert(derived_impl != nullptr);
  return ProgramTableInsertRegion(derived_impl);
}

// The body that conditionally executes if the insert succeeds.
std::optional<ProgramRegion> ProgramTableInsertRegion::Body(void) const noexcept {
  if (impl->body) {
    return ProgramRegion(impl->body.get());
  } else {
    return std::nullopt;
  }
}

unsigned ProgramTableInsertRegion::Arity(void) const noexcept {
  return impl->col_values.Size();
}

UsedNodeRange<DataVariable> ProgramTableInsertRegion::TupleVariables(void) const {
  return {impl->col_values.begin(), impl->col_values.end()};
}

DataTable ProgramTableInsertRegion::Table(void) const {
  return DataTable(impl->table.get());
}

ProgramTableJoinRegion ProgramTableJoinRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsTableJoin();
  assert(derived_impl != nullptr);
  return ProgramTableJoinRegion(derived_impl);
}

// The body that conditionally executes for each joined result. Variable
// bindings are applied.
std::optional<ProgramRegion> ProgramTableJoinRegion::Body(void) const noexcept {
  if (impl->body) {
    return ProgramRegion(impl->body.get());
  } else {
    return std::nullopt;
  }
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
    case VariableRole::kJoinPivot:
    case VariableRole::kJoinNonPivot:
      assert(!!impl->query_column);
      return impl->query_column->Type().Kind();
  }
}

// Unique ID of this column.
unsigned DataColumn::Id(void) const noexcept {
  return impl->id;
}

// Index of this column within its table.
unsigned DataColumn::Index(void) const noexcept {
  return impl->index;
}

// Type of this column.
TypeKind DataColumn::Type(void) const noexcept {
  return impl->type;
}

// Possible names that can be associated with this column.
//
// NOTE(pag): Multiple columns of the same table might have intersecting
//            sets of possible names.
const std::vector<Token> &DataColumn::PossibleNames(void) const noexcept {
  return impl->names;
}

// Unique ID of this index.
unsigned DataIndex::Id(void) const noexcept {
  return impl->id;
}

// Columns from a table that are part of this index.
UsedNodeRange<DataColumn> DataIndex::Columns(void) const {
  return {impl->columns.begin(), impl->columns.end()};
}

DataTable DataTable::Containing(DataColumn col) noexcept {
  return DataTable(col.impl->table.get());
}

DataTable DataTable::Backing(DataIndex index) noexcept {
  return DataTable(index.impl->table.get());
}

unsigned DataTable::Id(void) const noexcept {
  return impl->id;
}

DefinedNodeRange<DataColumn> DataTable::Columns(void) const {
  return {DefinedNodeIterator<DataColumn>(impl->columns.begin()),
          DefinedNodeIterator<DataColumn>(impl->columns.end())};
}

DefinedNodeRange<DataIndex> DataTable::Indices(void) const {
  return {DefinedNodeIterator<DataIndex>(impl->indices.begin()),
          DefinedNodeIterator<DataIndex>(impl->indices.end())};
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

// Unique ID of this procedure.
unsigned ProgramProcedure::Id(void) const noexcept {
  return impl->id;
}

// The message received and handled by this procedure.
std::optional<ParsedMessage> ProgramProcedure::Message(void) const noexcept {
  if (impl->io) {
    return ParsedMessage::From(impl->io->Declaration());
  } else {
    return std::nullopt;
  }
}

// The input vector that this procedure will operate on.
DefinedNodeRange<DataVector> ProgramProcedure::InputVectors(void) const {
  return {DefinedNodeIterator<DataVector>(impl->input_vectors.begin()),
          DefinedNodeIterator<DataVector>(impl->input_vectors.end())};
}

// Zero or more vectors on which this procedure operates.
DefinedNodeRange<DataVector> ProgramProcedure::DefinedVectors(void) const {
  return {DefinedNodeIterator<DataVector>(impl->vectors.begin()),
          DefinedNodeIterator<DataVector>(impl->vectors.end())};
}

// Return the region contained by this procedure.
ProgramRegion ProgramProcedure::Body(void) const noexcept {
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

std::optional<ProgramRegion>
ProgramInductionRegion::Output(void) const noexcept {
  if (impl->output_region) {
    return ProgramRegion(impl->output_region.get());
  } else {
    return std::nullopt;
  }
}

ProgramTupleCompareRegion
ProgramTupleCompareRegion::From(ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsTupleCompare();
  assert(derived_impl != nullptr);
  return ProgramTupleCompareRegion(derived_impl);
}

ComparisonOperator ProgramTupleCompareRegion::Operator(void) const noexcept {
  return impl->op;
}

// Variables in the left-hand side tuple.
UsedNodeRange<DataVariable> ProgramTupleCompareRegion::LHS(void) const {
  return {impl->lhs_vars.begin(), impl->lhs_vars.end()};
}

// Variables in the right-hand side tuple.
UsedNodeRange<DataVariable> ProgramTupleCompareRegion::RHS(void) const {
  return {impl->rhs_vars.begin(), impl->rhs_vars.end()};
}

// Code conditionally executed if the comparison is true.
std::optional<ProgramRegion>
ProgramTupleCompareRegion::Body(void) const noexcept {
  if (impl->body) {
    return ProgramRegion(impl->body.get());
  } else {
    return std::nullopt;
  }
}

// All persistent tables needed to store data.
DefinedNodeRange<DataTable> Program::Tables(void) const {
  return {DefinedNodeIterator<DataTable>(impl->tables.begin()),
          DefinedNodeIterator<DataTable>(impl->tables.end())};
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
