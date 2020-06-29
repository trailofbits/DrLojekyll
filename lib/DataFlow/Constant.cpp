// Copyright 2020, Trail of Bits. All rights reserved.

#include "../DataFlow/Query.h"

namespace hyde {

Node<QueryConstant>::~Node(void) {}

Node<QueryConstant> *Node<QueryConstant>::AsConstant(void) noexcept {
  return this;
}

}  // namespace hyde
