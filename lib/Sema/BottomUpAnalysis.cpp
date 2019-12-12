// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Sema/BottomUpAnalysis.h>

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <drlojekyll/Sema/ModuleIterator.h>

namespace hyde {

BottomUpVisitor::~BottomUpVisitor(void) {}

bool BottomUpVisitor::VisitTransition(
    ParsedPredicate, ParsedPredicate, unsigned) {
  return false;
}

class BottomUpAnalysis::Impl {
 public:
  // Perform a single step.
  bool Step(BottomUpVisitor &visitor);

  void Start(ParsedModule module);

  std::vector<std::pair<ParsedPredicate, unsigned>> work_list;
  std::vector<std::pair<ParsedPredicate, unsigned>> next_work_list;
};

// Initialize for the first step.
void BottomUpAnalysis::Impl::Start(ParsedModule module_) {
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
    next_work_list.emplace_back(message, 0);
  }
}

// Perform a single bottom-up step.
bool BottomUpAnalysis::Impl::Step(BottomUpVisitor &visitor) {
  if (next_work_list.empty()) {
    return false;
  }

  work_list.swap(next_work_list);
  while (!work_list.empty()) {
    auto assumption = work_list.back().first;
    auto depth = work_list.back().second;
    work_list.pop_back();

    auto clause = ParsedClause::Containing(assumption);
    for (auto next_use : ParsedDeclaration::Of(clause).PositiveUses()) {
      if (visitor.VisitTransition(assumption, next_use, depth + 1)) {
        next_work_list.emplace_back(next_use, depth + 1);
      }
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

