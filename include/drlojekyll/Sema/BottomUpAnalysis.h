// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

#include <optional>
#include <vector>

namespace hyde {

class BottomUpVisitor {
 public:
  class State;

  virtual ~BottomUpVisitor(void);

  // Visit a new or newly reachable state `state`. `pred` may be null or
  // non-null. If non-null, it means that `state` was newly reached via
  // `pred`. If null it means that `state->is_start_state` is true.
  //
  // Returns `true` if we want the bottom-up analysis to explore the
  // successors of `state`, and `false` otherwise.
  virtual bool VisitState(const State *state, const State *pred);
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
  bool Step(BottomUpVisitor &visitor=gDefaultVisitor);

  // Run the analysis to completion, generating and returning all states.
  inline const std::vector<BottomUpVisitor::State *> &GenerateStates(
      ParsedModule module, BottomUpVisitor &visitor=gDefaultVisitor) {
    for (Start(module); Step(visitor); ) {}
    return States();
  }

  // Returns a vector of all states. This vector can be indexed by a
  // state's id.
  const std::vector<BottomUpVisitor::State *> &States(void) const;

 private:
  static BottomUpVisitor gDefaultVisitor;

  std::unique_ptr<Impl> impl;
};

// A state produced by a stepping of the bottom-up analysis.
class BottomUpVisitor::State {
 public:
  const unsigned id;
  const bool is_start_state;
  const std::optional<ParsedPredicate> assumption;
  const ParsedClause clause;

  inline const std::vector<State *> &Predecessors(void) const {
    return predecessors;
  }

  inline const std::vector<State *> &Successors(void) const {
    return successors;
  }

 private:
  friend class BottomUpAnalysis::Impl;

  State(unsigned id_, bool is_start_state_, ParsedPredicate assumption_);
  State(unsigned id_, bool is_start_state_, ParsedClause clause_);

  std::vector<State *> predecessors;
  std::vector<State *> successors;
};

}  // namespace hyde
