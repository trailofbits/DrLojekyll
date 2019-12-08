// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Transforms/CombineModules.h>

namespace hyde {
//
//struct Binding {
//  size_t order{std::string::npos};
//  std::variant<ParsedPredicate, ParsedParameter, ParsedAssignment, ParsedComparison> binding;
//};
//
//struct Unification {
//  size_t order{std::string::npos};
//  size_t lhs{std::string::npos};  // Indexes into `BindingEnvironment::bindings`.
//  size_t rhs{std::string::npos};  // Indexes into `BindingEnvironment::bindings`.
//  std::variant<ParsedPredicate, ParsedComparison, ParsedAssignment> constraint;
//};
//
//struct BindingEnvironment {
//  size_t next_order{0};
//  std::vector<Binding> bindings;
//  std::vector<Unification> unifications;
//};
//
//// TODO(pag): have a phase number, so that if we have the messages prove
////            something in one phase, then that thing gets the max of all
////            incoming phases.
//
//// Pulls together the state associated with a single proof step.
//class BottomUpStep : public std::enable_shared_from_this<BottomUpStep> {
// public:
//  explicit BottomUpStep(void)
//      : parent(nullptr),
//        env(std::make_shared<BindingEnvironment>()) {}
//
//  explicit BottomUpStep(const std::shared_ptr<Step> &parent_,
//                        ParsedDeclaration additional_assumption)
//      : parent(parent_),
//        env(parent->env),
//        assumption(additional_assumption),
//        argument_ids(std::move(argument_ids_)) {
//    parent->children.push_back(weak_from_this());
//  }
//
//  const std::shared_ptr<Step> parent;
//  const std::shared_ptr<BindingEnvironment> env;
//  std::vector<std::weak_ptr<Step>> children;
//
//  const ParsedDeclaration assumption;
//  std::vector<size_t> argument_ids;
//
//  std::unordered_set<ParsedDeclaration> positive_depends;
//  std::unordered_set<ParsedDeclaration> negative_depends;
//};
//
//class Stepper final : public SIPSVisitor {
// public:
//  Stepper(const std::shared_ptr<Step> &parent_)
//      : parent(parent_) {}
//
//  void Begin(ParsedPredicate assumption) override {
//    std::make_shared<Step>(parent, )
//  }
//
// private:
//  const std::shared_ptr<Step> parent;
//  std::shared_ptr<Step> step;
//};
#if 0
class BottomUpStepper final : public SIPSVisitor {
 public:
  virtual ~BottomUpStepper(void) = default;

  // Notify the visitor that we're about to begin visiting a clause body.
  void Begin(ParsedPredicate assumption) override {

  }

  // Declares a concrete parameter identified by `id`.
  void DeclareParameter(const Column &col) override {

  }

  // Declares a variable identified by `id`.
  void DeclareVariable(ParsedVariable var, unsigned id) override {

  }

  // Declares a constant identified by `id`.
  void DeclareConstant(ParsedLiteral var, unsigned id) override {

  }

  // Notify the visitor that the clause head has been proven.
  void ProveClauseHead(ParsedDeclaration decl,
                               unsigned *begin_id, unsigned *end_id) override {

  }

  // Asserts that the value of the variable identified by `lhs_id` must match
  // the value of the variable identified by `rhs_id`.
  void AssertEqual(unsigned lhs_id, unsigned rhs_id) override {

  }

  // Asserts that the value of the variable identified by `lhs_id` must NOT
  // match the value of the variable identified by `rhs_id`.
  void AssertNotEqual(unsigned lhs_id, unsigned rhs_id) override {

  }

  // Asserts that the value of the variable identified by `lhs_id` must be less
  // than the value of the variable identified by `rhs_id`.
  void AssertLessThan(unsigned lhs_id, unsigned rhs_id) override {

  }

  // Asserts that the value of the variable identified by `lhs_id` must be
  // greater than the value of the variable identified by `rhs_id`.
  void AssertGreaterThan(unsigned lhs_id, unsigned rhs_id) override {

  }

  // Asserts the presence of some tuple. This is for negative predicates. Uses
  // the variable ids in the range `[begin_id, end_id)`.
  void AssertPresent(
      ParsedPredicate pred, const Column *begin,
      const Column *end) override {

  }

  // Asserts the absence of some tuple. This is for negative predicates. Uses
  // the variable ids in the range `[begin_id, end_id)`.
  void AssertAbsent(
      ParsedPredicate pred, const Column *begin,
      const Column *end) override {

  }

  // Tell the visitor that we're going to insert into a table.
  void Insert(
      ParsedDeclaration decl, const Column *begin,
      const Column *end) override {

  }

  // Selects some columns from a predicate where some of the column values are
  // fixed.
  void EnterFromWhereSelect(
      ParsedPredicate pred, ParsedDeclaration from,
      const Column *where_begin, const Column *where_end,
      const Column *select_begin, const Column *select_end) override {

  }

  // Selects some columns from a predicate where some of the column values are
  // fixed.
  void EnterFromSelect(
      ParsedPredicate pred, ParsedDeclaration from,
      const Column *select_begin, const Column *select_end) override {

  }

  // Exits the a selection.
  void ExitSelect(
      ParsedPredicate pred, ParsedDeclaration from) override {

  }

  // Assigns the value associated with `rhs_id` to `dest_id`.
  void Assign(unsigned dest_id, unsigned rhs_id) override {

  }

  // We've found a SIPS-acceptable ordering, so
  void Commit(void) override {
    advance = false;
  }

  // Notify the visitor that visiting cannot complete/continue due to an
  // invalid comparison `compare` that relates the variable identified by
  // `lhs_id` to the variable identified by `rhs_id`.
  void CancelComparison(
      ParsedComparison compare, unsigned lhs_id, unsigned rhs_id) override {

  }

  // Notify the visitor that visiting cannot complete due to the variable `var`
  // used in the comparison `compare` not being range-restricted.
  void CancelRangeRestriction(
      ParsedComparison compare, ParsedVariable var) override {

  }

  // Notify the visitor that visiting cannot complete due to the variable `var`
  // used in the head of the clause `clause` not being range-restricted, i.e.
  // having a definite value by the end of the clause body.
  void CancelRangeRestriction(
      ParsedClause clause, ParsedVariable var) override {

  }

  // Notify the visitor that visiting cannot complete due to binding
  // restrictions on a particular predicate. A range `[begin, end)` of
  // failed bindings is provided.
  void CancelPredicate(
      const FailedBinding *begin, FailedBinding *end) override {

  }

  // The SIPS generator will ask the visitor if it wants to advance
  // before trying to advance.
  bool Advance(void) override {
    return advance;
  }

 private:
  bool advance{true};
};

static void Simulate(hyde::OutputStream &os, hyde::ParsedModule main_module) {

  auto initial_state = std::make_shared<Step>();

  // Next, go through and initial our states, such that every message is used
  // as a starting declaration. Messages are the sources of inputs. They are
  // basically ephemeral IDB things. Another way of thinking of messages are
  // as the terminals of a grammar.
  std::vector<std::shared_ptr<Step>> states;
  std::vector<std::shared_ptr<Step>> finished_states;
  std::unordered_map<ParsedDeclaration, size_t> phase;
  for (auto module : ParsedModuleIterator(main_module)) {
    for (auto decl : module.Messages()) {
      if (!phase.count(decl)) {
        phase.emplace(decl, 1);
        states.emplace_back(std::make_shared<Step>(decl));
      }
    }
  }

  // Keep going while there are still states to be processed.
  while (!states.empty()) {
    auto state = std::move(states.back());
    states.pop_back();

    auto assumption = state->assumption;

    // We've reached up to a query, which is where this state is finished.
    if (assumption.IsQuery()) {

      auto root_phase = phase[state->root->assumption];
      auto &curr_phase = phase[assumption];

      // If we've never used this particular query as an input, then add a new
      // initial state for it. The key idea here is that everything queryable
      // must also be derivable from messages.
      if (!curr_phase) {
        curr_phase = root_phase + 1;
        states.emplace_back(std::make_shared<Step>(assumption));

      // Push the phase of this derivation to be later.
      } else if (curr_phase <= root_phase) {
        curr_phase = root_phase + 1;
      }

      finished_states.push_back(std::move(state));
      continue;
    }

    // Advance the state by one step. We can only advance things with positive
    // uses, as they add new information.
    for (auto pred : assumption.PositiveUses()) {
      auto clause = ParsedClause::Containing(pred);
      auto next_assumption = ParsedDeclaration::Of(clause);

      SIPSGenerator generator(clause, pred);
      while (generator.Visit(visitor)) {

      }
      std::vector<size_t> argument_ids;  // TODO
      while (generator.TryGetNext()) {
        auto next_state = std::make_shared<Step>(
            state, next_assumption, argument_ids);

        // TODO
        (void) next_state;
      }
    }
  }
}
#endif
}  // namespace hyde

static int ProcessModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log,
                         hyde::ParsedModule module) {

  if (!error_log.IsEmpty()) {
    error_log.Render(std::cerr);
    return EXIT_FAILURE;
  } else {
    module = CombineModules(display_manager, module);
    hyde::OutputStream os(display_manager, std::cerr);
    os << module;
    //Simulate(os, module);
    return EXIT_SUCCESS;
  }
}

int main(int argc, char *argv[]) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log;
  hyde::Parser parser(display_manager, error_log);

  std::string input_path;
  std::string output_path;
  auto num_input_paths = 0;

  std::stringstream linked_module;

  // Parse the command-line arguments.
  for (auto i = 1; i < argc; ++i) {

    // Output file of compiled datalog.
    if (!strcmp(argv[i], "-o")) {
      ++i;
      if (i >= argc) {
        hyde::Error err;
        err << "Command-line argument '-o' must be followed by a file path";
        error_log.Append(std::move(err));
      } else {
        output_path = argv[i];
      }

    // Input file search path.
    } else if (!strcmp(argv[i], "-I")) {
      ++i;
      if (i >= argc) {
        hyde::Error err;
        err << "Command-line argument '-I' must be followed by a directory path";
        error_log.Append(std::move(err));

      } else {
        parser.AddSearchPath(argv[i]);
      }

    // Input datalog file, add it to the list of paths to parse.
    } else {
      linked_module << "#import \"" << argv[i] << "\"\n";
      input_path = argv[i];
      ++num_input_paths;
    }
  }

  if (!num_input_paths) {
    hyde::Error err;
    err << "No input files to parse";
    error_log.Append(std::move(err));

  // Parse a single module.
  } else if (1 == num_input_paths) {
    hyde::DisplayConfiguration config = {
        input_path,  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };
    auto module = parser.ParsePath(input_path, config);
    return ProcessModule(display_manager, error_log, module);

  // Parse multiple modules as a single module including each module to
  // be parsed.
  } else {
    hyde::DisplayConfiguration config = {
        "<amalgamation>",  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };

    auto module = parser.ParseStream(linked_module, config);
    return ProcessModule(display_manager, error_log, module);
  }
}
