// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/BitManipulation.h>

#include "Program.h"

namespace hyde {

Node<ProgramInductionRegion>::~Node(void) {}

Node<ProgramInductionRegion> *
Node<ProgramInductionRegion>::AsInduction(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool Node<ProgramInductionRegion>::EndsWithReturn(void) const noexcept {
  if (auto output = output_region.get(); output) {
    return output->EndsWithReturn();
  } else {
    return false;
  }
}

Node<ProgramInductionRegion>::Node(ProgramImpl *impl, REGION *parent_)
    : Node<ProgramRegion>(parent_->containing_procedure),
      cyclic_region(this, impl->parallel_regions.Create(this)),
      output_region(this, impl->parallel_regions.Create(this)),
      vectors(this) {}

uint64_t Node<ProgramInductionRegion>::Hash(void) const {
  uint64_t hash = 117u;
  if (this->init_region) {
    hash ^= RotateRight64(hash, 13u) * init_region->Hash();
  }
  if (this->cyclic_region) {
    hash ^= RotateRight64(hash, 17u) * cyclic_region->Hash();
  }
  if (this->output_region) {
    hash ^= RotateRight64(hash, 19u) * output_region->Hash();
  }
  return hash;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool Node<ProgramInductionRegion>::Equals(EqualitySet &eq,
                                          Node<ProgramRegion> *that_,
                                          uint32_t depth) const noexcept {
  const auto that = that_->AsInduction();
  const auto num_vectors = vectors.Size();
  if (!that || num_vectors != that->vectors.Size()) {
    return false;
  }

  // One (but not both) of the inductions has a null output region.
  if ((!output_region && that->output_region) ||
      (output_region && !that->output_region)) {
    return false;
  }

  // Their vectors (after possible renaming) are not the same.
  for (auto i = 0u; i < num_vectors; ++i) {
    if (!eq.Contains(vectors[i], that->vectors[i])) {
      return false;
    }
  }

  if (depth == 0) {
    return true;
  }
  auto next_depth = depth - 1;

  if (output_region &&
      !output_region->Equals(eq, that->output_region.get(), next_depth)) {
    return false;
  }

  return init_region->Equals(eq, that->init_region.get(), next_depth) &&
         cyclic_region->Equals(eq, that->cyclic_region.get(), next_depth);
  ;
}

}  // namespace hyde
