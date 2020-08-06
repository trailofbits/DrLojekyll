// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramSeriesRegion>::~Node(void) {}

Node<ProgramSeriesRegion> *Node<ProgramSeriesRegion>::AsSeries(void) noexcept {
  return this;
}

}  // namespace hyde
