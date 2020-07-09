// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

Node<QueryConstant>::~Node(void) {}

Node<QueryConstant> *Node<QueryConstant>::AsConstant(void) noexcept {
  return this;
}

const char *Node<QueryConstant>::KindName(void) const noexcept {
  return "CONST";
}

}  // namespace hyde
