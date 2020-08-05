// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

ProgramImpl::~ProgramImpl(void) {
  for (auto region : series_regions) {
    region->regions.ClearWithoutErasure();
  }
  for (auto region : parallel_regions) {
    region->regions.ClearWithoutErasure();
  }
  for (auto ind : inductions) {
    ind->cycle.ClearWithoutErasure();
  }
}

Program::~Program(void) {}

}  // namespace hyde
