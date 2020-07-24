// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramRegion>::~Node(void) {}

Node<ProgramBlockRegion> *Node<ProgramRegion>::AsBasic(void) noexcept {
  return nullptr;
}

Node<ProgramSeriesRegion> *Node<ProgramRegion>::AsSeries(void) noexcept {
  return nullptr;
}

Node<ProgramParallelRegion> *Node<ProgramRegion>::AsParallel(void) noexcept {
  return nullptr;
}

}  // namespace hyde
