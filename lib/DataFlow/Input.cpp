// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryInput>::~Node(void) {}

Node<QueryInput> *Node<QueryInput>::AsInput(void) noexcept {
  return this;
}

}  // namespace hyde
