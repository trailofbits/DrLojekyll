// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

namespace hyde {

class BottomUpVisitor {
 public:
  virtual ~BottomUpVisitor(void);

  // Visit a transition, where we're going bottom-up from `from_pred`, into
  // the clause using `from_pred`, then assuming that the clause is proven,
  // and following that through to a use of `to_pred` in a different clause.
  virtual bool VisitTransition(ParsedPredicate from_pred,
                               ParsedPredicate to_pred,
                               unsigned depth);
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

}  // namespace hyde
