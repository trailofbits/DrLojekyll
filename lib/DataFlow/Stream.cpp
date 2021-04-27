// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryStream>::~Node(void) {}

Node<QueryConstant> *Node<QueryStream>::AsConstant(void) noexcept {
  return nullptr;
}

Node<QueryTag> *Node<QueryStream>::AsTag(void) noexcept {
  return nullptr;
}

Node<QueryIO> *Node<QueryStream>::AsIO(void) noexcept {
  return nullptr;
}

}  // namespace hyde
