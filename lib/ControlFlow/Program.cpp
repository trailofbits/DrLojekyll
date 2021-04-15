// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

ProgramImpl::~ProgramImpl(void) {
  query_checkers.ClearWithoutErasure();

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
    induction->init_region.ClearWithoutErasure();
    induction->cyclic_region.ClearWithoutErasure();
    induction->output_region.ClearWithoutErasure();
    induction->vectors.ClearWithoutErasure();
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

    } else if (auto vec_swap = op->AsVectorSwap(); vec_swap) {
      vec_swap->lhs.ClearWithoutErasure();
      vec_swap->rhs.ClearWithoutErasure();

    } else if (auto vec_unique = op->AsVectorUnique(); vec_unique) {
      vec_unique->vector.ClearWithoutErasure();

    } else if (auto view_insert = op->AsTransitionState(); view_insert) {
      view_insert->table.ClearWithoutErasure();
      view_insert->col_values.ClearWithoutErasure();
      view_insert->failed_body.ClearWithoutErasure();

    } else if (auto view_join = op->AsTableJoin(); view_join) {
      view_join->tables.ClearWithoutErasure();
      view_join->indices.ClearWithoutErasure();
      view_join->pivot_vec.ClearWithoutErasure();
      for (auto &cols : view_join->pivot_cols) {
        cols.ClearWithoutErasure();
      }

      for (auto &cols : view_join->output_cols) {
        cols.ClearWithoutErasure();
      }

    } else if (auto view_product = op->AsTableProduct(); view_product) {
      view_product->tables.ClearWithoutErasure();
      view_product->input_vecs.ClearWithoutErasure();

    } else if (auto view_scan = op->AsTableScan(); view_scan) {
      view_scan->index.ClearWithoutErasure();
      view_scan->in_cols.ClearWithoutErasure();
      view_scan->in_vars.ClearWithoutErasure();
      view_scan->output_vector.ClearWithoutErasure();
      view_scan->table.ClearWithoutErasure();

    } else if (auto exists_assert = op->AsTestAndSet(); exists_assert) {
      exists_assert->accumulator.ClearWithoutErasure();
      exists_assert->displacement.ClearWithoutErasure();
      exists_assert->comparator.ClearWithoutErasure();

    } else if (auto cmp = op->AsTupleCompare(); cmp) {
      cmp->lhs_vars.ClearWithoutErasure();
      cmp->rhs_vars.ClearWithoutErasure();
      cmp->false_body.ClearWithoutErasure();

    } else if (auto gen = op->AsGenerate(); gen) {
      gen->used_vars.ClearWithoutErasure();
      gen->empty_body.ClearWithoutErasure();

    } else if (auto call = op->AsCall(); call) {
      call->arg_vars.ClearWithoutErasure();
      call->arg_vecs.ClearWithoutErasure();
      call->called_proc.ClearWithoutErasure();
      call->false_body.ClearWithoutErasure();

    } else if (auto check = op->AsCheckState(); check) {
      check->col_values.ClearWithoutErasure();
      check->table.ClearWithoutErasure();
      check->absent_body.ClearWithoutErasure();
      check->unknown_body.ClearWithoutErasure();

    } else if (auto pub = op->AsPublish(); pub) {
      pub->arg_vars.ClearWithoutErasure();

    } else if (auto wid = op->AsWorkerId(); wid) {
      wid->hashed_vars.ClearWithoutErasure();
    }
  }

  for (auto proc : procedure_regions) {
    proc->body.ClearWithoutErasure();
  }
}

Program::Program(std::shared_ptr<ProgramImpl> impl_) : impl(std::move(impl_)) {}

Program::~Program(void) {}

// List of query entry points.
const std::vector<ProgramQuery> &Program::Queries(void) const noexcept {
  return impl->queries;
}

// Return the query used to build this program.
::hyde::Query Program::Query(void) const noexcept {
  return impl->query;
}

// Return the parsed module used to build the query.
::hyde::ParsedModule Program::ParsedModule(void) const noexcept {
  return impl->query.ParsedModule();
}

ProgramRegion::ProgramRegion(const ProgramInductionRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramParallelRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

ProgramRegion::ProgramRegion(const ProgramSeriesRegion &region)
    : program::ProgramNode<ProgramRegion>(region.impl) {}

std::optional<ProgramRegion>
ProgramRegion::Containing(ProgramRegion &self) noexcept {
  if (self.impl->parent != self.impl->containing_procedure) {
    return ProgramRegion(self.impl->parent);
  } else {
    return std::nullopt;
  }
}

void ProgramRegion::Accept(ProgramVisitor &visitor) const {
  impl->Accept(visitor);
}

std::string_view ProgramRegion::Comment(void) const noexcept {
  return impl->comment;
}

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
  ProgramRegion::ProgramRegion(const Program##kind##Region &region) \
      : ProgramRegion(region.impl) {} \
\
  bool ProgramRegion::Is##kind(void) const noexcept { \
    if (auto op = impl->AsOperation(); op) { \
      return op->As##kind() != nullptr; \
    } else { \
      return false; \
    } \
  }

IS_OP(Call)
IS_OP(Return)
IS_OP(TestAndSet)
IS_OP(Generate)
IS_OP(LetBinding)
IS_OP(Publish)
IS_OP(TransitionState)
IS_OP(CheckState)
IS_OP(TableJoin)
IS_OP(TableProduct)
IS_OP(TableScan)
IS_OP(TupleCompare)
IS_OP(WorkerId)
IS_OP(VectorLoop)
IS_OP(VectorAppend)
IS_OP(VectorClear)
IS_OP(VectorSwap)
IS_OP(VectorUnique)

#undef IS_OP

ProgramSeriesRegion ProgramSeriesRegion::From(ProgramRegion region) noexcept {
  const auto derived_impl = region.impl->AsSeries();
  assert(derived_impl != nullptr);
  return ProgramSeriesRegion(derived_impl);
}

ProgramParallelRegion
ProgramParallelRegion::From(ProgramRegion region) noexcept {
  const auto derived_impl = region.impl->AsParallel();
  assert(derived_impl != nullptr);
  return ProgramParallelRegion(derived_impl);
}

bool ProgramTestAndSetRegion::IsAdd(void) const noexcept {
  return impl->op == ProgramOperation::kTestAndAdd;
}

bool ProgramTestAndSetRegion::IsSubtract(void) const noexcept {
  return impl->op == ProgramOperation::kTestAndSub;
}

// The source/destination variable. This is `A` in `(A += D) == C`.
DataVariable ProgramTestAndSetRegion::Accumulator(void) const {
  return DataVariable(impl->accumulator.get());
}

// The amount by which the accumulator is displacement. This is `D` in
// `(A += D) == C`.
DataVariable ProgramTestAndSetRegion::Displacement(void) const {
  return DataVariable(impl->displacement.get());
}

// The value which must match the accumulated result for `Body` to execute.
// This is `C` in `(A += D) == C`.
DataVariable ProgramTestAndSetRegion::Comparator(void) const {
  return DataVariable(impl->comparator.get());
}

#define OPTIONAL_BODY(method_name, name, field) \
  std::optional<ProgramRegion> name::method_name(void) const noexcept { \
    if (impl->field) { \
      return ProgramRegion(impl->field.get()); \
    } else { \
      return std::nullopt; \
    } \
  }

OPTIONAL_BODY(Body, ProgramTestAndSetRegion, body)
OPTIONAL_BODY(BodyIfResults, ProgramGenerateRegion, body)
OPTIONAL_BODY(BodyIfEmpty, ProgramGenerateRegion, empty_body)
OPTIONAL_BODY(Body, ProgramLetBindingRegion, body)
OPTIONAL_BODY(Body, ProgramVectorLoopRegion, body)
OPTIONAL_BODY(BodyIfSucceeded, ProgramTransitionStateRegion, body)
OPTIONAL_BODY(BodyIfFailed, ProgramTransitionStateRegion, failed_body)
OPTIONAL_BODY(Body, ProgramTableJoinRegion, body)
OPTIONAL_BODY(Body, ProgramTableProductRegion, body)
OPTIONAL_BODY(BodyIfTrue, ProgramTupleCompareRegion, body)
OPTIONAL_BODY(BodyIfFalse, ProgramTupleCompareRegion, false_body)
OPTIONAL_BODY(Body, ProgramWorkerIdRegion, body)
OPTIONAL_BODY(BodyIfTrue, ProgramCallRegion, body)
OPTIONAL_BODY(BodyIfFalse, ProgramCallRegion, false_body)

#undef OPTIONAL_BODY

#define FROM_OP(name, as) \
  name name::From(ProgramRegion region) noexcept { \
    const auto op = region.impl->AsOperation(); \
    assert(op != nullptr); \
    const auto derived_impl = op->as(); \
    assert(derived_impl != nullptr); \
    return name(derived_impl); \
  }

FROM_OP(ProgramCallRegion, AsCall)
FROM_OP(ProgramReturnRegion, AsReturn)
FROM_OP(ProgramTestAndSetRegion, AsTestAndSet)
FROM_OP(ProgramGenerateRegion, AsGenerate)
FROM_OP(ProgramLetBindingRegion, AsLetBinding)
FROM_OP(ProgramPublishRegion, AsPublish)
FROM_OP(ProgramTransitionStateRegion, AsTransitionState)
FROM_OP(ProgramCheckStateRegion, AsCheckState)
FROM_OP(ProgramTableJoinRegion, AsTableJoin)
FROM_OP(ProgramTableProductRegion, AsTableProduct)
FROM_OP(ProgramTableScanRegion, AsTableScan)
FROM_OP(ProgramVectorLoopRegion, AsVectorLoop)
FROM_OP(ProgramVectorAppendRegion, AsVectorAppend)
FROM_OP(ProgramVectorClearRegion, AsVectorClear)
FROM_OP(ProgramVectorSwapRegion, AsVectorSwap)
FROM_OP(ProgramVectorUniqueRegion, AsVectorUnique)
FROM_OP(ProgramWorkerIdRegion, AsWorkerId)

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

// clang-format off
DEFINED_RANGE(ProgramLetBindingRegion, DefinedVariables, DataVariable, defined_vars)
DEFINED_RANGE(ProgramGenerateRegion, OutputVariables, DataVariable, defined_vars)
DEFINED_RANGE(ProgramVectorLoopRegion, TupleVariables, DataVariable, defined_vars)
DEFINED_RANGE(DataTable, Columns, DataColumn, columns)
DEFINED_RANGE(DataTable, Indices, DataIndex, indices)
DEFINED_RANGE(ProgramProcedure, VectorParameters, DataVector, input_vecs)
DEFINED_RANGE(ProgramProcedure, VariableParameters, DataVariable, input_vars)
DEFINED_RANGE(ProgramProcedure, DefinedVectors, DataVector, vectors)
DEFINED_RANGE(Program, Tables, DataTable, tables)
DEFINED_RANGE(Program, Constants, DataVariable, const_vars)
DEFINED_RANGE(Program, GlobalVariables, DataVariable, global_vars)
DEFINED_RANGE(Program, Procedures, ProgramProcedure, procedure_regions)
DEFINED_RANGE(ProgramTableJoinRegion, OutputPivotVariables, DataVariable, pivot_vars)

USED_RANGE(ProgramCallRegion, VariableArguments, DataVariable, arg_vars)
USED_RANGE(ProgramCallRegion, VectorArguments, DataVector, arg_vecs)
USED_RANGE(ProgramPublishRegion, VariableArguments, DataVariable, arg_vars)
USED_RANGE(ProgramGenerateRegion, InputVariables, DataVariable, used_vars)
USED_RANGE(ProgramLetBindingRegion, UsedVariables, DataVariable, used_vars)
USED_RANGE(ProgramVectorAppendRegion, TupleVariables, DataVariable, tuple_vars)
USED_RANGE(ProgramTransitionStateRegion, TupleVariables, DataVariable, col_values)
USED_RANGE(ProgramCheckStateRegion, TupleVariables, DataVariable, col_values)
USED_RANGE(DataIndex, KeyColumns, DataColumn, columns)
USED_RANGE(DataIndex, ValueColumns, DataColumn, mapped_columns)
USED_RANGE(ProgramTupleCompareRegion, LHS, DataVariable, lhs_vars)
USED_RANGE(ProgramTupleCompareRegion, RHS, DataVariable, rhs_vars)
USED_RANGE(ProgramSeriesRegion, Regions, ProgramRegion, regions)
USED_RANGE(ProgramParallelRegion, Regions, ProgramRegion, regions)
USED_RANGE(ProgramInductionRegion, Vectors, DataVector, vectors)
USED_RANGE(ProgramTableJoinRegion, Tables, DataTable, tables)
USED_RANGE(ProgramTableJoinRegion, Indices, DataIndex, indices)
USED_RANGE(ProgramTableProductRegion, Tables, DataTable, tables)
USED_RANGE(ProgramTableProductRegion, Vectors, DataVector, input_vecs)
USED_RANGE(ProgramTableScanRegion, IndexedColumns, DataColumn, in_cols)
USED_RANGE(ProgramTableScanRegion, SelectedColumns, DataColumn, out_cols)
USED_RANGE(ProgramTableScanRegion, InputVariables, DataVariable, in_vars)
USED_RANGE(ProgramWorkerIdRegion, HashedVariables, DataVariable, hashed_vars)

// clang-format on

#undef DEFINED_RANGE
#undef USED_RANGE

// The format of the code in this program.
IRFormat Program::Format(void) const {
  return impl->format;
}

namespace {

static VectorUsage VectorUsageOfOp(ProgramOperation op) {
  switch (op) {
    case ProgramOperation::kLoopOverInputVector:
      return VectorUsage::kProcedureInputVector;
    case ProgramOperation::kAppendToInductionVector:
    case ProgramOperation::kClearInductionVector:
    case ProgramOperation::kLoopOverInductionVector:
    case ProgramOperation::kSwapInductionVector:
    case ProgramOperation::kSortAndUniqueInductionVector:
      return VectorUsage::kInductionVector;
    case ProgramOperation::kAppendUnionInputToVector:
    case ProgramOperation::kLoopOverUnionInputVector:
      return VectorUsage::kUnionInputVector;
    case ProgramOperation::kAppendJoinPivotsToVector:
    case ProgramOperation::kClearJoinPivotVector:
    case ProgramOperation::kSortAndUniquePivotVector:
      return VectorUsage::kJoinPivots;
    case ProgramOperation::kAppendToProductInputVector:
    case ProgramOperation::kSortAndUniqueProductInputVector:
    case ProgramOperation::kClearProductInputVector:
      return VectorUsage::kProductInputVector;
    case ProgramOperation::kScanTable:
    case ProgramOperation::kLoopOverScanVector:
    case ProgramOperation::kClearScanVector: return VectorUsage::kTableScan;
    case ProgramOperation::kAppendToMessageOutputVector:
    case ProgramOperation::kSortAndUniqueMessageOutputVector:
    case ProgramOperation::kClearMessageOutputVector:
    case ProgramOperation::kLoopOverMessageOutputVector:
      return VectorUsage::kMessageOutputVector;
    default: assert(false); return VectorUsage::kInvalid;
  }
}

}  // namespace

#define VECTOR_OPS(name) \
  VectorUsage name::Usage(void) const noexcept { \
    return VectorUsageOfOp(impl->OP::op); \
  } \
  DataVector name::Vector(void) const noexcept { \
    return DataVector(impl->Node<name>::vector.get()); \
  } \
  std::optional<DataVariable> name::WorkerId(void) const { \
    if (impl->worker_id) { \
      return DataVariable(impl->worker_id.get()); \
    } else { \
      return std::nullopt; \
    } \
  }

VECTOR_OPS(ProgramVectorLoopRegion)
VECTOR_OPS(ProgramVectorAppendRegion)
VECTOR_OPS(ProgramVectorClearRegion)
VECTOR_OPS(ProgramVectorUniqueRegion)

#undef VECTOR_OPS

DataVector ProgramVectorSwapRegion::LHS(void) const noexcept {
  return DataVector(impl->lhs.get());
}

DataVector ProgramVectorSwapRegion::RHS(void) const noexcept {
  return DataVector(impl->rhs.get());
}

unsigned ProgramGenerateRegion::Id(void) const noexcept {
  return impl->id;
}

DataVariable ProgramWorkerIdRegion::WorkerId(void) const {
  return DataVariable(impl->worker_id.get());
}

// Does this functor application behave like a filter function?
bool ProgramGenerateRegion::IsFilter(void) const noexcept {
  return impl->op == ProgramOperation::kCallFilterFunctor;
}

// Returns `true` if repeated executions of the function given the same
// inputs generate the same outputs.
bool ProgramGenerateRegion::IsPure(void) const noexcept {
  return impl->functor.IsPure();
}

// Returns `true` if repeated executions of the function given the same
// inputs are not guaranteed to generate the same outputs.
bool ProgramGenerateRegion::IsImpure(void) const noexcept {
  return !impl->functor.IsPure();
}

// Returns the functor to be applied.
ParsedFunctor ProgramGenerateRegion::Functor(void) const noexcept {
  return impl->functor;
}

unsigned ProgramTransitionStateRegion::Arity(void) const noexcept {
  return impl->col_values.Size();
}

DataTable ProgramTransitionStateRegion::Table(void) const {
  return DataTable(impl->table.get());
}

TupleState ProgramTransitionStateRegion::FromState(void) const noexcept {
  return impl->from_state;
}

TupleState ProgramTransitionStateRegion::ToState(void) const noexcept {
  return impl->to_state;
}

unsigned ProgramCheckStateRegion::Arity(void) const noexcept {
  return impl->col_values.Size();
}

DataTable ProgramCheckStateRegion::Table(void) const {
  return DataTable(impl->table.get());
}

std::optional<ProgramRegion>
ProgramCheckStateRegion::IfPresent(void) const noexcept {
  if (auto body = impl->body.get(); body) {
    return ProgramRegion(body);
  } else {
    return std::nullopt;
  }
}

std::optional<ProgramRegion>
ProgramCheckStateRegion::IfAbsent(void) const noexcept {
  if (auto body = impl->absent_body.get(); body) {
    return ProgramRegion(body);
  } else {
    return std::nullopt;
  }
}

std::optional<ProgramRegion>
ProgramCheckStateRegion::IfUnknown(void) const noexcept {
  if (auto body = impl->unknown_body.get(); body) {
    return ProgramRegion(body);
  } else {
    return std::nullopt;
  }
}

VariableRole DataVariable::DefiningRole(void) const noexcept {
  return impl->role;
}

// The region which defined this local variable.
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
  if (impl->query_const) {
    return impl->query_const->Literal();
  }
  if (impl->query_column && impl->query_column->IsConstantOrConstantRef()) {
    return QueryConstant::From(*impl->query_column).Literal();
  }
  return std::nullopt;
}

// Type of this variable.
TypeLoc DataVariable::Type(void) const noexcept {
  return impl->Type();
}

// Whether this variable is global.
bool DataVariable::IsGlobal(void) const noexcept {
  switch (DefiningRole()) {
    case VariableRole::kConditionRefCount:
    case VariableRole::kConstant:
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue:
      return true;
    default:
      return false;
  }
}

// Whether or not this variable is a constant.
bool DataVariable::IsConstant(void) const noexcept {
  switch (DefiningRole()) {
    case VariableRole::kConstant:
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue:
      return true;
    default:
      return false;
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

// Visit the users of this vector.
void DataTable::VisitUsers(ProgramVisitor &visitor) {
  impl->ForEachUse<Node<ProgramRegion>>(
      [&](Node<ProgramRegion> *region, Node<DataTable> *) {
        region->Accept(visitor);
      });
}

// Apply a function to each user.
void DataTable::ForEachUser(std::function<void(ProgramRegion)> cb) {
  impl->ForEachUse<Node<ProgramRegion>>(
      [&](Node<ProgramRegion> *region, Node<DataTable> *) { cb(region); });
}

VectorKind DataVector::Kind(void) const noexcept {
  return impl->kind;
}

unsigned DataVector::Id(void) const noexcept {
  return impl->id;
}

// Do we need to shard this vector across workers?
bool DataVector::IsSharded(void) const noexcept {
  return impl->is_sharded;
}

const std::vector<TypeKind> DataVector::ColumnTypes(void) const noexcept {
  return impl->col_types;
}

// Visit the users of this vector.
void DataVector::VisitUsers(ProgramVisitor &visitor) {
  impl->ForEachUse<Node<ProgramRegion>>(
      [&](Node<ProgramRegion> *region, Node<DataVector> *) {
        region->Accept(visitor);
      });
}

// Apply a function to each user.
void DataVector::ForEachUser(std::function<void(ProgramRegion)> cb) {
  impl->ForEachUse<Node<ProgramRegion>>(
      [&](Node<ProgramRegion> *region, Node<DataVector> *) { cb(region); });
}

// Return the procedure containing another region.
ProgramProcedure ProgramProcedure::Containing(ProgramRegion region) noexcept {
  return ProgramProcedure(region.impl->containing_procedure);
}

// Unique ID of this procedure.
unsigned ProgramProcedure::Id(void) const noexcept {
  return impl->id;
}

// What type of procedure is this?
ProcedureKind ProgramProcedure::Kind(void) const noexcept {
  return impl->kind;
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

std::optional<ProgramRegion>
ProgramInductionRegion::Initializer(void) const noexcept {
  if (auto region = impl->init_region.get(); region) {
    return ProgramRegion(region);
  } else {
    return std::nullopt;
  }
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
  return impl->cmp_op;
}

// Unique ID for this join.
unsigned ProgramTableJoinRegion::Id(void) const noexcept {
  return impl->id;
}

// The index used by the Nth table scan.
DataIndex ProgramTableJoinRegion::Index(unsigned table_index) const noexcept {
  assert(table_index < impl->indices.Size());
  return DataIndex(impl->indices[table_index]);
}

// The pivot vector that contains the join pivots. The elements of this
// pivot vector are in the same order as `OutputPivotVariables()`.
DataVector ProgramTableJoinRegion::PivotVector(void) const noexcept {
  return DataVector(impl->pivot_vec.get());
}

// The columns used in the scan of the Nth table. These are in the same
// order as the entries in `PivotVector()` and `OutputPivotVariables()`.
UsedNodeRange<DataColumn>
ProgramTableJoinRegion::IndexedColumns(unsigned table_index) const {
  assert(table_index < impl->pivot_cols.size());
  const auto &cols = impl->pivot_cols[table_index];
  return {cols.begin(), cols.end()};
}

// These are the output columns associated with the Nth table scan. These
// do NOT include pivot columns.
UsedNodeRange<DataColumn>
ProgramTableJoinRegion::SelectedColumns(unsigned table_index) const {
  assert(table_index < impl->output_cols.size());
  const auto &cols = impl->output_cols[table_index];
  return {cols.begin(), cols.end()};
}

DefinedNodeRange<DataVariable>
ProgramTableJoinRegion::OutputVariables(unsigned table_index) const {
  assert(table_index < impl->output_vars.size());
  const auto &vars = impl->output_vars[table_index];
  return {DefinedNodeIterator<DataVariable>(vars.begin()),
          DefinedNodeIterator<DataVariable>(vars.end())};
}

// Unique ID of this region.
unsigned ProgramTableProductRegion::Id(void) const noexcept {
  return impl->id;
}

// The table used by the Nth table scan.
DataTable ProgramTableProductRegion::Table(unsigned table_index) const noexcept {
  assert(table_index < impl->tables.Size());
  return DataTable(impl->tables[table_index]);
}

// The index used by the Nth table scan.
DataVector
ProgramTableProductRegion::Vector(unsigned table_index) const noexcept {
  assert(table_index < impl->input_vecs.Size());
  return DataVector(impl->input_vecs[table_index]);
}

DefinedNodeRange<DataVariable>
ProgramTableProductRegion::OutputVariables(unsigned table_index) const {
  assert(table_index < impl->output_vars.size());
  const auto &vars = impl->output_vars[table_index];
  return {DefinedNodeIterator<DataVariable>(vars.begin()),
          DefinedNodeIterator<DataVariable>(vars.end())};
}

// The table being scanned.
DataTable ProgramTableScanRegion::Table(void) const noexcept {
  return DataTable(impl->table.get());
}

// Optional index being scanned.
std::optional<DataIndex> ProgramTableScanRegion::Index(void) const noexcept {
  if (impl->index) {
    return DataIndex(impl->index.get());
  } else {
    return std::nullopt;
  }
}

// The scanned results are filled into this vector.
DataVector ProgramTableScanRegion::FilledVector(void) const {
  return DataVector(impl->output_vector.get());
}

unsigned ProgramCallRegion::Id(void) const noexcept {
  return impl->id;
}

ProgramProcedure ProgramCallRegion::CalledProcedure(void) const noexcept {
  return ProgramProcedure(impl->called_proc.get());
}

bool ProgramReturnRegion::ReturnsTrue(void) const noexcept {
  return impl->op == ProgramOperation::kReturnTrueFromProcedure;
}

bool ProgramReturnRegion::ReturnsFalse(void) const noexcept {
  return impl->op == ProgramOperation::kReturnFalseFromProcedure;
}

ParsedMessage ProgramPublishRegion::Message(void) const noexcept {
  return impl->message;
}

// Are we publishing the removal of some tuple?
bool ProgramPublishRegion::IsRemoval(void) const noexcept {
  return impl->op == ProgramOperation::kPublishMessageRemoval;
}

}  // namespace hyde
