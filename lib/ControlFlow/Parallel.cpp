// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

ProgramParallelRegionImpl::ProgramParallelRegionImpl(REGION *parent_)
    : ProgramRegionImpl(parent_),
      regions(this) {
  assert(parent_->Ancestor()->AsProcedure());
}

ProgramParallelRegionImpl::~ProgramParallelRegionImpl(void) {}

ProgramParallelRegionImpl *
ProgramParallelRegionImpl::AsParallel(void) noexcept {
  return this;
}

uint64_t ProgramParallelRegionImpl::Hash(uint32_t depth) const {
  uint64_t hash = 193u;
  if (depth == 0) {
    return hash;
  }

  for (auto region : regions) {
    hash ^= region->Hash(depth - 1u);
  }
  return hash;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool ProgramParallelRegionImpl::Equals(EqualitySet &eq,
                                         ProgramRegionImpl *that_,
                                         uint32_t depth) const noexcept {
  const auto that = that_->AsParallel();
  const auto num_regions = regions.Size();
  if (!that || num_regions != that->regions.Size()) {
    return false;
  }

  std::unordered_map<unsigned, std::vector<REGION *>> grouped_regions;
  for (auto region : regions) {
    unsigned index = 0u;
    if (region->AsSeries()) {
      index = ~0u;
    } else if (region->AsInduction()) {
      index = ~0u - 1u;
    } else if (auto op = region->AsOperation(); op) {
      index = static_cast<unsigned>(op->op);

    // Don't bother trying to compare parallel regions until they've been
    // flattened completely. It is also impossible to put a procedure inside
    // of a parallel region.
    } else {
      return false;
    }

    grouped_regions[index].push_back(region);
  }

  if (depth == 0) {
    return true;
  }

  EqualitySet super_eq(eq, SuperSet());
  std::vector<REGION *> next_candidates;

  for (auto that_region : that->regions) {
    unsigned index = 0u;

    if (that_region->AsSeries()) {
      index = ~0u;
    } else if (that_region->AsInduction()) {
      index = ~0u - 1u;
    } else if (auto op = that_region->AsOperation(); op) {
      index = static_cast<unsigned>(op->op);
    } else {
      return false;
    }

    auto found = false;
    auto &candidate_regions = grouped_regions[index];
    for (auto this_region : candidate_regions) {
      if (found) {
        next_candidates.push_back(this_region);

      } else if (!this_region->Equals(super_eq, that_region, depth - 1)) {
        next_candidates.push_back(this_region);
        super_eq.Clear();

      } else {
        found = true;
      }
    }

    if (!found) {
      return false;
    }

    // We've reduced our candidate size by one.
    candidate_regions.swap(next_candidates);
    next_candidates.clear();
    super_eq.Clear();
  }

  return true;
}

const bool ProgramParallelRegionImpl::MergeEqual(
    ProgramImpl *, std::vector<ProgramRegionImpl *> &merges) {

  for (auto region : merges) {
    auto merge = region->AsParallel();
    assert(merge != nullptr);
    assert(merge != this);
    for (auto child : merge->regions) {
      child->parent = this;
      AddRegion(child);
    }
    merge->regions.Clear();
    merge->parent = nullptr;
  }

  return true;
}

// Returns true if this region is a no-op.
bool ProgramParallelRegionImpl::IsNoOp(void) const noexcept {
  for (auto region : regions) {
    assert(region->parent == this);
    if (!region->IsNoOp()) {
      return false;
    }
  }
  return true;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramParallelRegionImpl::EndsWithReturn(void) const noexcept {
  if (regions.Empty()) {
    return false;
  }

  for (auto region : regions) {
    if (!region->EndsWithReturn()) {
      return false;
    }
  }

  return true;
}

}  // namespace hyde
