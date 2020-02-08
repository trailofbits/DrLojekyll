// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/BAM.h>

#include <bitset>
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Sema/BottomUpAnalysis.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Sema/SIPSScore.h>
#include <drlojekyll/Transforms/CombineModules.h>

namespace hyde {
namespace bam {

static const std::hash<std::string> kStringHasher;

// A binding of a declaration.
struct Index {
 public:
  ParsedDeclaration decl;  // Declaration being bound.
  std::string binding;  // A string of `b`s or `f`s.

  bool operator==(const Index &that) const noexcept {
    return decl == that.decl && binding == that.binding;
  }
};

// A binding for a clause. This includes the clause, and then a bitset of which
// variables in the clause are bound.
struct Binding {
 public:
  ParsedClause clause;
  std::bitset<256> bound_vars;

  bool operator==(const Binding &that) const noexcept {
    return clause == that.clause && bound_vars == that.bound_vars;
  }

  bool operator!=(const Binding &that) const noexcept {
    return clause != that.clause || bound_vars != that.bound_vars;
  }

  bool operator<(const Binding &that) const noexcept {
    if (clause != that.clause) {
      return false;
    }
    if (bound_vars.count() >= that.bound_vars.count()) {
      return false;
    }

    return (bound_vars & that.bound_vars) == bound_vars;
  }
};

class Function;
class FunctionCall;
class Select;

// A code fragment. This can be a function, function call, loop, etc.
class Fragment {
 public:
  virtual ~Fragment(void) = default;

  virtual Select *AsSelect(void) noexcept {
    return nullptr;
  }
  virtual const Select *AsSelect(void) const noexcept {
    return nullptr;
  }

  virtual Function *AsFunction(void) noexcept {
    return nullptr;
  }

  virtual const Function *AsFunction(void) const noexcept {
    return nullptr;
  }

  virtual FunctionCall *AsFunctionCall(void) noexcept {
    return nullptr;
  }

  virtual const FunctionCall *AsFunctionCall(void) const noexcept {
    return nullptr;
  }

  std::vector<Fragment *> children;
};

class Function final : public Fragment {
 public:
  virtual ~Function(void) = default;

  Function *AsFunction(void) noexcept override {
    return this;
  }

  const Function *AsFunction(void) const noexcept override {
    return this;
  }
};

class FunctionCall final : public Fragment {
 public:
  virtual ~FunctionCall(void) = default;

  FunctionCall *AsFunctionCall(void) noexcept override {
    return this;
  }

  const FunctionCall *AsFunctionCall(void) const noexcept override {
    return this;
  }

  Function *called_function{nullptr};
  std::vector<ParsedVariable> arguments;
};

class Select final : public Fragment {
 public:
  virtual ~Select(void) = default;

  Select *AsSelect(void) noexcept override {
    return this;
  }

  const Select *AsSelect(void) const noexcept override {
    return this;
  }

  std::vector<ParsedVariable> selected_vars;
  std::vector<ParsedVariable> indexed_vars;
  Index index;
};

}  // namespace bam
}  // namespace hyde
namespace std {

template<>
struct hash<::hyde::bam::Index> {
  inline uint64_t operator()(::hyde::bam::Index binding) const noexcept {
    return binding.decl.Id() ^ ::hyde::bam::kStringHasher(binding.binding);
  }
};

}  // namespace std

namespace hyde {
namespace bam {

struct Module {
 public:
  std::vector<std::unique_ptr<FunctionCall>> function_calls;
  std::vector<std::unique_ptr<Select>> selects;

  std::unordered_map<ParsedPredicate, std::unique_ptr<Function>> functions;
};

class CodeGenerator final : public SIPSVisitor {
 public:
  virtual ~CodeGenerator(void) = default;


};

}  // namespace

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of datalog.
void CodeGenBAM(
    DisplayManager &display_manager, ParsedModule module,
    std::ostream &cxx_os) {

  if (module != module.RootModule()) {
    module = CombineModules(display_manager, module.RootModule());
  }

  for (auto _ : module.Imports()) {
    (void) _;
    module = CombineModules(display_manager, module.RootModule());
    break;
  }

  bam::CodeGenerator bam_builder;

  BottomUpAnalysis analysis;
  for (auto state : analysis.GenerateStates(module)) {
    FastBindingSIPSScorer scorer;
    if (state->assumption) {
      SIPSGenerator generator(*(state->assumption));
      scorer.VisitBestScoringPermuation(scorer, bam_builder, generator);
    } else {
      SIPSGenerator generator(state->clause);
      scorer.VisitBestScoringPermuation(scorer, bam_builder, generator);
    }
  }
}

}  // namespace hyde
