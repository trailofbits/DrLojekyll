// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramBlockRegion>::~Node(void) {}

Node<ProgramBlockRegion> *Node<ProgramBlockRegion>::AsBasic(void) noexcept {
  return this;
}

}  // namespace hyde
