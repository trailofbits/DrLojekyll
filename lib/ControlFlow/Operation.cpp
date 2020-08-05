// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramOperationRegion>::~Node(void) {}

Node<ProgramOperationRegion> *Node<ProgramOperationRegion>::AsOperation(void) noexcept {
  return this;
}

}  // namespace hyde
