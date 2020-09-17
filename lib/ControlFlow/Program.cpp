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

ProgramRegion::ProgramRegion(const ProgramVectorUniqueRegion &region)
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
bool ProgramRegion::IsSeries(void) const noexcept {
  return impl->AsSeries() != nullptr;
}

bool ProgramRegion::IsParallel(void) const noexcept {
  return impl->AsParallel() != nullptr;
}

#define IS_OP(kind) \
    bool ProgramRegion::Is ## kind(void) const noexcept { \
      if (auto op = impl->AsOperation(); op) { \
        return op->As ## kind() != nullptr; \
      } else { \
        return false; \
      } \
    }

IS_OP(VectorLoop)
IS_OP(VectorAppend)
IS_OP(VectorClear)
IS_OP(VectorUnique)
IS_OP(LetBinding)
IS_OP(TableInsert)
IS_OP(TableJoin)
IS_OP(ExistenceCheck)
IS_OP(TupleCompare)

#undef IS_OP

ProgramSeriesRegion ProgramSeriesRegion::From(
    ProgramRegion region) noexcept {
  const auto derived_impl = region.impl->AsSeries();
  assert(derived_impl != nullptr);
  return ProgramSeriesRegion(derived_impl);
}

ProgramParallelRegion ProgramParallelRegion::From(
    ProgramRegion region) noexcept {
  const auto derived_impl = region.impl->AsParallel();
  assert(derived_impl != nullptr);
  return ProgramParallelRegion(derived_impl);
}

bool ProgramExistenceCheckRegion::CheckForZero(void) const noexcept {
  return impl->op == ProgramOperation::kTestAllZero;
}

bool ProgramExistenceCheckRegion::CheckForNotZero(void) const noexcept {
  return impl->op == ProgramOperation::kTestAllNonZero;
}

#define OPTIONAL_BODY(name) \
    std::optional<ProgramRegion> name::Body(void) const noexcept { \
      if (impl->body) { \
        return ProgramRegion(impl->body.get()); \
      } else { \
        return std::nullopt; \
      } \
    }

OPTIONAL_BODY(ProgramExistenceCheckRegion)
OPTIONAL_BODY(ProgramLetBindingRegion)
OPTIONAL_BODY(ProgramVectorLoopRegion)
OPTIONAL_BODY(ProgramTableInsertRegion)
OPTIONAL_BODY(ProgramTableJoinRegion)
OPTIONAL_BODY(ProgramTupleCompareRegion)

#undef OPTIONAL_BODY

#define FROM_OP(name, as) \
    name name::From(ProgramRegion region) noexcept { \
      const auto op = region.impl->AsOperation(); \
      assert(op != nullptr); \
      const auto derived_impl = op->as(); \
      assert(derived_impl != nullptr); \
      return name(derived_impl); \
    }

FROM_OP(ProgramLetBindingRegion, AsLetBinding)
FROM_OP(ProgramVectorLoopRegion, AsVectorLoop)
FROM_OP(ProgramVectorAppendRegion, AsVectorAppend)
FROM_OP(ProgramVectorClearRegion, AsVectorClear)
FROM_OP(ProgramVectorUniqueRegion, AsVectorUnique)
FROM_OP(ProgramTableInsertRegion, AsTableInsert)
FROM_OP(ProgramTableJoinRegion, AsTableJoin)
FROM_OP(ProgramExistenceCheckRegion, AsExistenceCheck)

#undef FROM_OP

#define DEFINED_RANGE(name, method, type, access) \
    DefinedNodeRange<type> name::method(void) const { \
      return {DefinedNodeIterator<type>(impl->access.begin()), \
              DefinedNodeIterator<type>(impl->access.end())}; \
    }

#define USED_RANGE(name, method, type, access) \
    UsedNodeRange<type> name::method(void) const { \
      return {impl->access.begin(), impl->access.end()}; \
    }

DEFINED_RANGE(ProgramLetBindingRegion, DefinedVars, DataVariable, defined_vars)
DEFINED_RANGE(ProgramVectorLoopRegion, TupleVariables, DataVariable, defined_vars)
DEFINED_RANGE(DataTable, Columns, DataColumn, columns)
DEFINED_RANGE(DataTable, Indices, DataIndex, indices)
DEFINED_RANGE(ProgramProcedure, InputVectors, DataVector, input_vectors)
DEFINED_RANGE(ProgramProcedure, DefinedVectors, DataVector, vectors)
DEFINED_RANGE(Program, Tables, DataTable, tables)
DEFINED_RANGE(Program, Constants, DataVariable, const_vars)
DEFINED_RANGE(Program, Procedures, ProgramProcedure, procedure_regions)

USED_RANGE(ProgramLetBindingRegion, UsedVars, DataVariable, used_vars)
USED_RANGE(ProgramVectorAppendRegion, TupleVariables, DataVariable, tuple_vars)
USED_RANGE(ProgramTableInsertRegion, TupleVariables, DataVariable, col_values)
USED_RANGE(DataIndex, Columns, DataColumn, columns)
USED_RANGE(ProgramTupleCompareRegion, LHS, DataVariable, lhs_vars)
USED_RANGE(ProgramTupleCompareRegion, RHS, DataVariable, rhs_vars)
USED_RANGE(ProgramSeriesRegion, Regions, ProgramRegion, regions)
USED_RANGE(ProgramParallelRegion, Regions, ProgramRegion, regions)
USED_RANGE(ProgramExistenceCheckRegion, ReferenceCounts, DataVariable, cond_vars)
USED_RANGE(ProgramInductionRegion, Vectors, DataVector, vectors)

#undef DEFINED_RANGE
#undef USED_RANGE

namespace {

static VectorUsage VectorUsageOfOp(ProgramOperation op) {
  switch (op) {
    case ProgramOperation::kLoopOverInputVector:
      return VectorUsage::kProcedureInputVector;
    case ProgramOperation::kAppendInductionInputToVector:
    case ProgramOperation::kClearInductionInputVector:
    case ProgramOperation::kLoopOverInductionInputVector:
      return VectorUsage::kInductionVector;
    case ProgramOperation::kAppendUnionInputToVector:
    case ProgramOperation::kLoopOverUnionInputVector:
      return VectorUsage::kUnionInputVector;
    case ProgramOperation::kAppendJoinPivotsToVector:
    case ProgramOperation::kClearJoinPivotVector:
    case ProgramOperation::kLoopOverJoinPivots:
    case ProgramOperation::kSortAndUniquePivotVector:
      return VectorUsage::kJoinPivots;
    case ProgramOperation::kAppendProductInputToVector:
    case ProgramOperation::kLoopOverProductInputVector:
      return VectorUsage::kProductInputVector;
    case ProgramOperation::kAppendProductOutputToVector:
    case ProgramOperation::kLoopOverProductOutputVector:
      return VectorUsage::kProductOutputVector;
    default:
      assert(false);
      return VectorUsage::kInvalid;
  }
}

}  // namespace

#define VECTOR_OPS(name) \
  VectorUsage name::Usage(void) const noexcept { \
    return VectorUsageOfOp(impl->OP::op); \
  } \
  DataVector name::Vector(void) const noexcept { \
    return DataVector(impl->Node<name>::vector.get()); \
  }

VECTOR_OPS(ProgramVectorLoopRegion)
VECTOR_OPS(ProgramVectorAppendRegion)
VECTOR_OPS(ProgramVectorClearRegion)
VECTOR_OPS(ProgramVectorUniqueRegion)

#undef VECTOR_OPS

unsigned ProgramTableInsertRegion::Arity(void) const noexcept {
  return impl->col_values.Size();
}

DataTable ProgramTableInsertRegion::Table(void) const {
  return DataTable(impl->table.get());
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

DataTable DataTable::Containing(DataColumn col) noexcept {
  return DataTable(col.impl->table.get());
}

DataTable DataTable::Backing(DataIndex index) noexcept {
  return DataTable(index.impl->table.get());
}

unsigned DataTable::Id(void) const noexcept {
  return impl->id;
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

}  // namespace hyde

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif
