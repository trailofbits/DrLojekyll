// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramParallelRegion>::~Node(void) {}

Node<ProgramParallelRegion> *
Node<ProgramParallelRegion>::AsParallel(void) noexcept {
  return this;
}

uint64_t Node<ProgramParallelRegion>::Hash(void) const {
  uint64_t hash = 0u;
  for (auto region : regions) {
    hash ^= region->Hash();
  }
  return hash;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming).
bool Node<ProgramParallelRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that = that_->AsSeries();
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

      } else if (!this_region->Equals(super_eq, that_region)) {
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

// Returns true if this region is a no-op.
bool Node<ProgramParallelRegion>::IsNoOp(void) const noexcept {
  for (auto region : regions) {
    assert(region->parent == this);
    if (!region->IsNoOp()) {
      return false;
    }
  }
  return true;
}

}  // namespace hyde
