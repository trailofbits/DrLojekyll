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
  for (auto induction : induction_regions) {
    for (auto &entry : induction->view_to_init_appends) {
      entry.second.ClearWithoutErasure();
    }
    for (auto &entry : induction->view_to_cycle_appends) {
      entry.second.ClearWithoutErasure();
    }
    for (auto &entry : induction->view_to_cycle_loop) {
      entry.second.ClearWithoutErasure();
    }
    for (auto &entry : induction->view_to_output_loop) {
      entry.second.ClearWithoutErasure();
    }
    for (auto &entry : induction->view_to_vec) {
      entry.second.ClearWithoutErasure();
    }
    induction->init_region.ClearWithoutErasure();
    induction->cyclic_region.ClearWithoutErasure();
    induction->output_region.ClearWithoutErasure();
  }
  for (auto op : operation_regions) {
    op->body.ClearWithoutErasure();
    op->variables.ClearWithoutErasure();
    op->tables.ClearWithoutErasure();
    op->views.ClearWithoutErasure();
    op->indices.ClearWithoutErasure();
  }
  for (auto proc : procedure_regions) {
    proc->body.ClearWithoutErasure();
  }
}

Program::Program(std::shared_ptr<ProgramImpl> impl_)
    : impl(std::move(impl_)) {}

Program::~Program(void) {}

}  // namespace hyde
