// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramInductionRegion>::~Node(void) {}

Node<ProgramInductionRegion> *
Node<ProgramInductionRegion>::AsInduction(void) noexcept {
  return this;
}

}  // namespace hyde
