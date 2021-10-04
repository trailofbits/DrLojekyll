// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/BitManipulation.h>

#include "Program.h"

namespace hyde {

ProgramSeriesRegionImpl::ProgramSeriesRegionImpl(REGION *parent_)
    : ProgramRegionImpl(parent_),
      regions(this) {
  assert(parent_->Ancestor()->AsProcedure());
}

ProgramSeriesRegionImpl::~ProgramSeriesRegionImpl(void) {}

ProgramSeriesRegionImpl *ProgramSeriesRegionImpl::AsSeries(void) noexcept {
  return this;
}

uint64_t ProgramSeriesRegionImpl::Hash(uint32_t depth) const {
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
bool ProgramSeriesRegionImpl::IsNoOp(void) const noexcept {
  for (auto region : regions) {
    assert(region->parent == this);
    if (!region->IsNoOp()) {
      return false;
    }
  }
  return true;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramSeriesRegionImpl::EndsWithReturn(void) const noexcept {
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
bool ProgramSeriesRegionImpl::Equals(EqualitySet &eq,
                                       ProgramRegionImpl *that_,
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

const bool ProgramSeriesRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<ProgramRegionImpl *> &merges) {

  // NOTE(ekilmer): Special region that has its own merging code in Optimize.cpp
  return false;
}

}  // namespace hyde
