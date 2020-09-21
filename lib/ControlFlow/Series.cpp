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

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming).
bool Node<ProgramSeriesRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that = that_->AsSeries();
  const auto num_regions = regions.Size();
  if (!that || num_regions != that->regions.Size()) {
    return false;
  }

  for (auto i = 0u; i < num_regions; ++i) {
    if (!regions[i]->Equals(eq, that->regions[i])) {
      return false;
    }
  }

  return true;
}

}  // namespace hyde