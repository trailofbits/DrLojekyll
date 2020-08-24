// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramInductionRegion>::~Node(void) {}

Node<ProgramInductionRegion> *
Node<ProgramInductionRegion>::AsInduction(void) noexcept {
  return this;
}

explicit Node<ProgramInductionRegion>::Node(ProgramImpl *impl, REGION *parent_)
    : Node<ProgramRegion>(parent_->containing_procedure),
      cyclic_region(this, impl->parallel_regions.Create(this)),
      output_region(this, impl->parallel_regions.Create(this)) {}

}  // namespace hyde
