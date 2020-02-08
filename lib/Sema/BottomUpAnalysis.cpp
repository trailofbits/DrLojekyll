// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Sema/BottomUpAnalysis.h>

#include <unordered_map>

#include <drlojekyll/Sema/ModuleIterator.h>

namespace hyde {

BottomUpVisitor::State::State(unsigned id_, bool is_start_state_,
                              ParsedPredicate assumption_)
    : id(id_),
      is_start_state(is_start_state_),
      assumption(assumption_),
      clause(ParsedClause::Containing(assumption_)) {}

BottomUpVisitor::State::State(unsigned id_, bool is_start_state_,
                              ParsedClause clause_)
    : id(id_),
      is_start_state(is_start_state_),
      clause(clause_) {}

BottomUpVisitor::~BottomUpVisitor(void) {}

bool BottomUpVisitor::VisitState(const State * state, const State *pred) {
  return true;
}

class BottomUpAnalysis::Impl {
 public:
  ~Impl(void);

  // Perform a single step.
  bool Step(BottomUpVisitor &visitor);

  void Start(ParsedModule module);

  unsigned next_state_id{0};

  using Transition = std::pair<BottomUpVisitor::State *,
                               BottomUpVisitor::State *>;

  std::vector<BottomUpVisitor::State *> states;
  std::unordered_map<ParsedPredicate, BottomUpVisitor::State *> pred_to_state;
  std::vector<Transition> work_list;
  std::vector<Transition> next_work_list;
};

BottomUpAnalysis::Impl::~Impl(void) {
  for (auto state : states) {
    delete state;
  }
}

// Initialize for the first step.
void BottomUpAnalysis::Impl::Start(ParsedModule module_) {
  next_state_id = 0;
  for (auto state : states) {
    delete state;
  }
  states.clear();
  next_work_list.clear();
  work_list.clear();
  pred_to_state.clear();

  for (auto module : ParsedModuleIterator(module_)) {

    // A clause without positive predicates.
    //
    // For example:
    //
    //      fib(0, 0).
    //      fib(1, 1).
    //      fib(N, Res)
    //          : sub_i32(N, 1, NMinus1)
    //          , sub_i32(N, 2, NMinus2)
    //          , fib(NMinus1, NMinus1_Res)
    //          , fib(NMinus2, NMinus2_Res)
    //          , add_i32(NMinus1_Res, NMinus2_Res, Res).

    for (auto clause : module.Clauses()) {
      auto pred_range = clause.PositivePredicates();
      if (pred_range.begin() == pred_range.end()) {
        const auto state = new BottomUpVisitor::State(
            next_state_id++, true, clause);
        states.emplace_back(state);
        next_work_list.emplace_back(nullptr, state);
      }
    }

    // A clause containing a message, i.e. an input.
    for (auto message : module.Messages()) {
      for (auto use : message.PositiveUses()) {
        auto &state = pred_to_state[use];
        if (!state) {
          state = new BottomUpVisitor::State(
              next_state_id++, true, use);
          states.emplace_back(state);
          next_work_list.emplace_back(nullptr, state);
        }
      }
    }
  }
}

// Perform a single bottom-up step.
bool BottomUpAnalysis::Impl::Step(BottomUpVisitor &visitor) {
  if (next_work_list.empty()) {
    return false;
  }

  work_list.swap(next_work_list);
  while (!work_list.empty()) {
    const auto transition = work_list.back();
    const auto predecessor = transition.first;
    const auto state = transition.second;
    work_list.pop_back();

    if (!visitor.VisitState(state, predecessor)) {
      continue;
    }

    auto &successors = state->successors;
    const auto clause = state->clause;
    for (auto next_use : ParsedDeclaration::Of(clause).PositiveUses()) {
      auto &next_state = pred_to_state[next_use];
      auto is_new = false;

      // This is a totally new state.
      if (!next_state) {
        is_new = true;
        next_state = new BottomUpVisitor::State(
            next_state_id++, false, next_use);
        states.emplace_back(next_state);

        successors.push_back(next_state);
        next_state->predecessors.push_back(state);

      // If we haven't seen this state transition before, then link the two
      // states together.
      } else if (std::find(successors.begin(), successors.end(), next_state) ==
                 successors.end()) {
        is_new = true;

        successors.push_back(next_state);
        next_state->predecessors.push_back(state);
      }

      if (is_new) {
        next_work_list.emplace_back(state, next_state);
      }
    }
  }

  return true;
}

BottomUpVisitor BottomUpAnalysis::gDefaultVisitor;

BottomUpAnalysis::~BottomUpAnalysis(void) {}

BottomUpAnalysis::BottomUpAnalysis(void)
    : impl(new Impl) {}

// Start or re-start the analysis.
void BottomUpAnalysis::Start(ParsedModule module) {
  impl->Start(module);
}

// Perform a single step.
bool BottomUpAnalysis::Step(BottomUpVisitor &visitor) {
  return impl->Step(visitor);
}

// Returns a vector of all states. This vector can be indexed by a
// state's id.
const std::vector<BottomUpVisitor::State *> &
BottomUpAnalysis::States(void) const {
  return impl->states;
}

}  // namespace hyde

