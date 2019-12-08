// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

// Implements union-find.
class DisjointSet {
 private:
  DisjointSet *parent;

 public:
  DisjointSet(unsigned id_);

  DisjointSet *Find(void);

  static DisjointSet *Union(DisjointSet *lhs, DisjointSet *rhs);

  const unsigned id;
};

}  // namespace hyde
