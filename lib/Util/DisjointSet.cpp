// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/DisjointSet.h>

namespace hyde {

DisjointSet::DisjointSet(unsigned id_)
    : parent(this),
      id(id_) {}

DisjointSet *DisjointSet::Find(void) {
  if (parent == this) {
    return this;
  } else {
    parent = parent->Find();
    return parent;
  }
}

DisjointSet *DisjointSet::Union(DisjointSet *lhs, DisjointSet *rhs) {
  lhs = lhs->Find();
  rhs = rhs->Find();
  if (lhs == rhs) {
    return lhs;

  } else if (lhs->id > rhs->id) {
    lhs->parent = rhs;
    return rhs;

  } else {
    rhs->parent = lhs;
    return lhs;
  }
}

}  // namespace hyde
