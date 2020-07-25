// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

// Implements union-find.
class DisjointSet {
 private:
  DisjointSet *parent;

 public:
  DisjointSet(unsigned id_ = 0);

  static void Reparent(DisjointSet *set) {
    set->parent = set;
  }

  DisjointSet *Find(void);

  template <typename T>
  inline T *FindAs(void) {
    return reinterpret_cast<T *>(Find());
  }

  static DisjointSet *Union(DisjointSet *lhs, DisjointSet *rhs);

  // Union `child` into `parent`, ignoring the default rule of preferring
  // to union by
  static void UnionInto(DisjointSet *child, DisjointSet *parent);

  const unsigned id;
};

}  // namespace hyde
