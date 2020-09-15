// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramParallelRegion>::~Node(void) {}

Node<ProgramParallelRegion> *
Node<ProgramParallelRegion>::AsParallel(void) noexcept {
  return this;
}

// Returns true if this region is a no-op.
bool Node<ProgramParallelRegion>::IsNoOp(void) const noexcept {
  for (auto region : regions) {
    if (!region->IsNoOp()) {
      return false;
    }
  }
  return true;
}

}  // namespace hyde
