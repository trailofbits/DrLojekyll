// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryIO>::~Node(void) {}

Node<QueryIO> *Node<QueryIO>::AsIO(void) noexcept {
  return this;
}

const char *Node<QueryIO>::KindName(void) const noexcept {
  return "I/O";
}

}  // namespace hyde
