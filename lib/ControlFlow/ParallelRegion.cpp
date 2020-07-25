// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramParallelRegion>::~Node(void) {}

Node<ProgramParallelRegion> *Node<ProgramParallelRegion>::AsBasic(
    void) noexcept {
  return this;
}

}  // namespace hyde
