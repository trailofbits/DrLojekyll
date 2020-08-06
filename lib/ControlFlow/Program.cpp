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
  for (auto ind : induction_regions) {
    ind->init.ClearWithoutErasure();
    ind->cycle.ClearWithoutErasure();
  }
  for (auto op : operation_regions) {
    op->body.ClearWithoutErasure();
    op->variables.ClearWithoutErasure();
    op->tables.ClearWithoutErasure();
    op->views.ClearWithoutErasure();
    op->indices.ClearWithoutErasure();
  }
}

Program::~Program(void) {}

}  // namespace hyde
