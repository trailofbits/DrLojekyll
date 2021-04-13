// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/BitManipulation.h>

#include "Program.h"

namespace hyde {

Node<ProgramSeriesRegion>::Node(REGION *parent_)
    : Node<ProgramRegion>(parent_),
      regions(this) {
  assert(parent_->Ancestor()->AsProcedure());
}

Node<ProgramSeriesRegion>::~Node(void) {}

Node<ProgramSeriesRegion> *Node<ProgramSeriesRegion>::AsSeries(void) noexcept {
  return this;
}

uint64_t Node<ProgramSeriesRegion>::Hash(uint32_t depth) const {
  uint64_t hash = 956u;
  if (depth == 0) {
    return hash;
  }
  for (auto region : regions) {
    hash ^= RotateRight64(hash, 13u) * region->Hash(depth - 1u);
  }
  return hash;
}

// Returns true if this region is a no-op.
bool Node<ProgramSeriesRegion>::IsNoOp(void) const noexcept {
  for (auto region : regions) {
    assert(region->parent == this);
    if (!region->IsNoOp()) {
      return false;
    }
  }
  return true;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool Node<ProgramSeriesRegion>::EndsWithReturn(void) const noexcept {
  for (auto region : regions) {
    if (region->EndsWithReturn()) {
      return true;
    }
  }
  return false;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool Node<ProgramSeriesRegion>::Equals(EqualitySet &eq,
                                       Node<ProgramRegion> *that_,
                                       uint32_t depth) const noexcept {
  const auto that = that_->AsSeries();
  const auto num_regions = regions.Size();
  if (!that || num_regions != that->regions.Size()) {
    return false;
  }

  if (depth == 0) {
    return true;
  }

  for (auto i = 0u; i < num_regions; ++i) {
    if (!regions[i]->Equals(eq, that->regions[i], depth - 1u)) {
      return false;
    }
  }

  return true;
}

const bool Node<ProgramSeriesRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {

  // NOTE(ekilmer): Special region that has its own merging code in Optimize.cpp
  return false;
}

}  // namespace hyde
