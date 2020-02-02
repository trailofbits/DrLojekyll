// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryGenerator>::~Node(void) {}

Node<QueryGenerator> *Node<QueryGenerator>::AsGenerator(void) noexcept {
  return this;
}

}  // namespace hyde
