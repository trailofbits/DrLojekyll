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
  }
  for (auto op : operation_regions) {
    op->body.ClearWithoutErasure();
    op->variables.ClearWithoutErasure();
    op->tables.ClearWithoutErasure();
    op->views.ClearWithoutErasure();
    op->indices.ClearWithoutErasure();
  }
  for (auto proc : procedure_regions) {
    proc->body.ClearWithoutErasure();
  }
}

Program::Program(std::shared_ptr<ProgramImpl> impl_)
    : impl(std::move(impl_)) {}

Program::~Program(void) {}

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

ProgramVectorAppendRegion ProgramVectorAppendRegion::From(
    ProgramRegion region) noexcept {
  const auto op = region.impl->AsOperation();
  assert(op != nullptr);
  const auto derived_impl = op->AsVectorAppend();
  assert(derived_impl != nullptr);
  return ProgramVectorAppendRegion(derived_impl);
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

ProgramRegion ProgramInductionRegion::Initializer(void) const noexcept {
  return ProgramRegion(impl->init_region.get());
}

ProgramRegion ProgramInductionRegion::FixpointLoop(void) const noexcept {
  return ProgramRegion(impl->cyclic_region.get());
}

ProgramRegion ProgramInductionRegion::Output(void) const noexcept {
  return ProgramRegion(impl->output_region.get());
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
