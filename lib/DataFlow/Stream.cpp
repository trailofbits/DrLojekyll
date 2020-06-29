// Copyright 2020, Trail of Bits. All rights reserved.

#include "../DataFlow/Query.h"

namespace hyde {

Node<QueryStream>::~Node(void) {}

Node<QueryConstant> *Node<QueryStream>::AsConstant(void) noexcept {
  return nullptr;
}

Node<QueryInput> *Node<QueryStream>::AsInput(void) noexcept {
  return nullptr;
}

}  // namespace hyde
