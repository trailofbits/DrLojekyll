// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramOperationRegion>::~Node(void) {}

Node<ProgramOperationRegion>::Node(REGION *parent_, ProgramOperation op_)
    : Node<ProgramRegion>(parent_),
      op(op_),
      variables(this),
      tables(this),
      views(this),
      indices(this) {}

Node<ProgramOperationRegion> *
Node<ProgramOperationRegion>::AsOperation(void) noexcept {
  return this;
}

bool Node<ProgramOperationRegion>::IsLoop(void) const noexcept {
  switch (op) {
    case ProgramOperation::kLoopOverUnionInputVector:
    case ProgramOperation::kLoopOverJoinPivots:
    case ProgramOperation::kLoopOverProductInputVector:
    case ProgramOperation::kLoopOverProductOutputVector:
    case ProgramOperation::kLoopOverInputVector:
      return true;
    default:
      return false;
  }
}

}  // namespace hyde
