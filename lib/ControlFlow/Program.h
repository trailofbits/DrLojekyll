// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Query.h>

#include <vector>

namespace hyde {

class Program;
class ProgramRegion;
class ProgramBlockRegion;
class ProgramSeriesRegion;
class ProgramParallelRegion;

class ProgramInduction;

template <>
class Node<ProgramRegion> : public User {
 public:
  virtual ~Node(void);

  inline Node(void) : User(this) {}

  virtual Node<ProgramBlockRegion> *AsBasic(void) noexcept;
  virtual Node<ProgramSeriesRegion> *AsSeries(void) noexcept;
  virtual Node<ProgramParallelRegion> *AsParallel(void) noexcept;
};

// A basic region. This directly represents a query view.
template <>
class Node<ProgramBlockRegion> final : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);

  inline explicit Node(QueryView view_) : view(view_) {}

  Node<ProgramBlockRegion> *AsBasic(void) noexcept override;

  const QueryView view;
};

using BLOCK = Node<ProgramBlockRegion>;

// A series region looks like a "tower" of dataflow: one nodes takes data,
// manipulates it, passes it to something else, etc.
template <>
class Node<ProgramSeriesRegion> final : public Node<ProgramRegion> {
 public:
  inline Node(void) : regions(this) {}

  virtual ~Node(void);
  Node<ProgramSeriesRegion> *AsSeries(void) noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using SERIES = Node<ProgramSeriesRegion>;

// A region where multiple things can happen in parallel. Often when a VIEW's
// columns are needed by two or more other VIEWs, we will have one of these.
template <>
class Node<ProgramParallelRegion> final : public Node<ProgramRegion> {
 public:
  inline Node(void) : regions(this) {}

  virtual ~Node(void);
  Node<ProgramParallelRegion> *AsParallel(void) noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using PARALLEL = Node<ProgramParallelRegion>;

// An induction is a loop centred on a MERGE node. Some of the views incoming
// to that MERGE are treated as "inputs", as they bring initial data into the
// MERGE. Other nodes are treated as "inductions" as they cycle back to the
// MERGE.
template <>
class Node<ProgramInduction> {
 public:
  inline explicit Node(QueryMerge merge_) : merge(merge_) {}

  const QueryMerge merge;
  std::vector<QueryView> input_views;
  std::vector<QueryView> inductive_views;

  // The actual region representing the body of this induction loop.
  UseRef<Node<ProgramRegion>> cycle;
};

using INDUCTION = Node<ProgramInduction>;

class ProgramImpl : public User {
 public:
  ~ProgramImpl(void);

  inline explicit ProgramImpl(Query query_)
      : User(this),
        query(query_),
        block_regions(this),
        series_regions(this),
        parallel_regions(this),
        inductions(this) {}

  const Query query;

  DefList<BLOCK> block_regions;
  DefList<SERIES> series_regions;
  DefList<PARALLEL> parallel_regions;
  DefList<INDUCTION> inductions;
};

}  // namespace hyde
