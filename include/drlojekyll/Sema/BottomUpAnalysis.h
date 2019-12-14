// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

namespace hyde {

class BottomUpVisitor {
 public:
  class State;

  virtual ~BottomUpVisitor(void);

  // Visit a transition, where we're going bottom-up from `from_pred`, into
  // the clause using `from_pred`, then assuming that the clause is proven,
  // and following that through to a use of `to_pred` in a different clause.
  virtual bool VisitState(const State *state);
};

// Bottom-up machine.
class BottomUpAnalysis  {
 public:
  class Impl;
  ~BottomUpAnalysis(void);
  BottomUpAnalysis(void);

  // Start or re-start the analysis.
  void Start(ParsedModule module);

  // Perform a single step. Returns `false` if nothing was done.
  bool Step(BottomUpVisitor &visitor);

 private:
  std::unique_ptr<Impl> impl;
};


class BottomUpVisitor::State {
 public:
  const State * const parent;
  const unsigned depth;
  const unsigned id;
  const ParsedPredicate assumption;

 private:
  friend class BottomUpAnalysis::Impl;

  State(const State *parent_, unsigned id_, ParsedPredicate assumption_)
      : parent(parent_),
        depth(parent ? parent->depth + 1 : 0),
        id(id_),
        assumption(assumption_) {}
};

}  // namespace hyde
