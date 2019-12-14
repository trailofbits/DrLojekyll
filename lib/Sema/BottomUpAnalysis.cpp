// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Sema/BottomUpAnalysis.h>

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <drlojekyll/Sema/ModuleIterator.h>

namespace hyde {

BottomUpVisitor::~BottomUpVisitor(void) {}

bool BottomUpVisitor::VisitState(const State * state) {
  return false;
}

class BottomUpAnalysis::Impl {
 public:
  // Perform a single step.
  bool Step(BottomUpVisitor &visitor);

  void Start(ParsedModule module);

  unsigned next_state_id{0};
  std::vector<std::unique_ptr<BottomUpVisitor::State>> states;
  std::vector<BottomUpVisitor::State *> work_list;
  std::vector<BottomUpVisitor::State *> next_work_list;
};

// Initialize for the first step.
void BottomUpAnalysis::Impl::Start(ParsedModule module_) {
  next_state_id = 0;
  states.clear();
  next_work_list.clear();
  work_list.clear();

  std::unordered_set<ParsedPredicate> messages;
  for (auto module : ParsedModuleIterator(module_)) {
    for (auto message : module.Messages()) {
      for (auto use : message.PositiveUses()) {
        messages.emplace(use);
      }
    }
  }

  for (auto message : messages) {
    auto state = new BottomUpVisitor::State(
        nullptr, next_state_id++, message);
    states.emplace_back(state);
    next_work_list.push_back(state);
  }
}

// Perform a single bottom-up step.
bool BottomUpAnalysis::Impl::Step(BottomUpVisitor &visitor) {
  if (next_work_list.empty()) {
    return false;
  }

  work_list.swap(next_work_list);
  while (!work_list.empty()) {
    const auto state = work_list.back();
    work_list.pop_back();

    if (!visitor.VisitState(state)) {
      continue;
    }

    auto clause = ParsedClause::Containing(state->assumption);
    for (auto next_use : ParsedDeclaration::Of(clause).PositiveUses()) {
      const auto next_state = new BottomUpVisitor::State(
          state, next_state_id++, next_use);
      next_work_list.push_back(next_state);
      states.emplace_back(next_state);
    }
  }

  return true;
}

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

}  // namespace hyde

