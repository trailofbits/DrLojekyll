// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramSeriesRegion>::~Node(void) {}

Node<ProgramSeriesRegion> *Node<ProgramSeriesRegion>::AsSeries(void) noexcept {
  return this;
}

// Returns true if this region is a no-op.
bool Node<ProgramSeriesRegion>::IsNoOp(void) const noexcept {
  for (auto region : regions) {
    if (!region->IsNoOp()) {
      return false;
    }
  }
  return true;
}

}  // namespace hyde
