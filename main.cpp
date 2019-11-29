// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
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

namespace hyde {

// Wrapper around a `std::ostream` that lets us stream out `Token`s and
// `DisplayRange`s.
class OutputStream {
 public:
  ~OutputStream(void) {
    os.flush();
  }

  OutputStream(DisplayManager &display_manager_, std::ostream &os_)
      : display_manager(display_manager_),
        os(os_) {}

  OutputStream &operator<<(Token tok) {
    std::string_view data;
    (void) display_manager.TryReadData(tok.SpellingRange(), &data);
    os << data;
    return *this;
  }

  OutputStream &operator<<(DisplayRange range) {
    std::string_view data;
    (void) display_manager.TryReadData(range, &data);
    os << data;
    return *this;
  }

  template <typename T>
  OutputStream &operator<<(T val) {
    os << val;
    return *this;
  }

 private:
  DisplayManager display_manager;
  std::ostream &os;
};

static void FormatDecl(OutputStream &os, ParsedDeclaration decl) {
  os << "#" << decl.KindName() << " " << decl.Name() << "(";
  auto comma = "";
  for (auto param : decl.Parameters()) {
    os << comma;

    // Binding specific; optional for some declarations.
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
        break;
      case ParameterBinding::kFree:
        os << "free ";
        break;
      case ParameterBinding::kBound:
        os << "bound ";
        break;
      case ParameterBinding::kAggregate:
        os << "aggregate ";
        break;
      case ParameterBinding::kSummary:
        os << "summary ";
        break;
    }

    os << param.Type() << " " << param.Name();
    comma = ", ";
  }
  os << ")";
}

static void FormatPredicate(OutputStream &os, ParsedPredicate pred) {
  auto decl = ParsedDeclaration::Of(pred);
  os << decl.Name() << "(";
  auto comma = "";
  for (auto arg : pred.Arguments()) {
    os << comma;
    os << arg.Name();
    comma = ", ";
  }
  os << ")";
}

static void FormatClause(OutputStream &os, ParsedClause clause) {
  auto decl = ParsedDeclaration::Of(clause);

  os << decl.Name() << "(";
  auto comma = "";
  for (auto param : clause.Parameters()) {
    os << comma;
    os << param.Name();
    comma = ", ";
  }
  os << ") : ";
  comma = "";
  for (auto assign : clause.Assignments()) {
    os << comma << assign.LHS().Name() << " = " << assign.RHS().SpellingRange();
    comma = ", ";
  }

  for (auto compare : clause.Comparisons()) {
    os << comma << compare.LHS().Name();
    switch (compare.Operator()) {
      case ComparisonOperator::kEqual:
        os << " = ";
        break;
      case ComparisonOperator::kNotEqual:
        os << " != ";
        break;
      case ComparisonOperator::kLessThan:
        os << " < ";
        break;
      case ComparisonOperator::kLessThanEqual:
        os << " <= ";
        break;
      case ComparisonOperator::kGreaterThan:
        os << " > ";
        break;
      case ComparisonOperator::kGreaterThanEqual:
        os << " >= ";
        break;
    }
    os << compare.RHS().Name();
    comma = ", ";
  }

  for (auto pred : clause.PositivePredicates()) {
    os << comma;
    FormatPredicate(os, pred);
    comma = ", ";
  }

  for (auto pred : clause.NegatedPredicates()) {
    os << comma << "!";
    FormatPredicate(os, pred);
    comma = ", ";
  }

  os << ".\n";
}

void FormatModule(OutputStream &os, ParsedModule module) {
  for (auto import : module.Imports()) {
    os << import.SpellingRange() << "\n";
  }

  for (auto decl : module.Queries()) {
    FormatDecl(os, decl);
    os << "\n";
  }

  for (auto decl : module.Messages()) {
    FormatDecl(os, decl);
    os << "\n";
  }

  for (auto decl : module.Functors()) {
    FormatDecl(os, decl);
    if (decl.IsComplex()) {
      os << " complex\n";
    } else {
      os << " trivial\n";
    }
  }

  for (auto decl : module.Exports()) {
    FormatDecl(os, decl);
    os << "\n";
  }

  for (auto decl : module.Locals()) {
    FormatDecl(os, decl);
    os << "\n";
  }

  for (auto clause : module.Clauses()) {
    FormatClause(os, clause);
    os << "\n";
  }
}

struct Binding {
  size_t order{std::string::npos};
  std::variant<ParsedPredicate, ParsedParameter, ParsedAssignment, ParsedComparison> binding;
};

struct Unification {
  size_t order{std::string::npos};
  size_t lhs{std::string::npos};  // Indexes into `BindingEnvironment::bindings`.
  size_t rhs{std::string::npos};  // Indexes into `BindingEnvironment::bindings`.
  std::variant<ParsedPredicate, ParsedComparison, ParsedAssignment> constraint;
};

struct BindingEnvironment {
  size_t next_order{0};
  std::vector<Binding> bindings;
  std::vector<Unification> unifications;
};

// TODO(pag): have a phase number, so that if we have the messages prove
//            something in one phase, then that thing gets the max of all
//            incoming phases.

// Pulls together the state associated with a single proof step.
struct Step {
  explicit Step(ParsedDeclaration initial_assumption)
      : parent(nullptr),
        env(std::make_shared<BindingEnvironment>()),
        assumption(initial_assumption) {

    for (auto param : initial_assumption.Parameters()) {
      argument_ids.push_back(argument_ids.size());
      env->bindings.push_back(
          {env->next_order++, param});
    }
  }

  explicit Step(const std::shared_ptr<Step> &parent_,
                ParsedDeclaration additional_assumption,
                std::vector<size_t> argument_ids_)
      : parent(parent_),
        root(parent->root ? parent->root : parent),
        env(parent->env),
        assumption(additional_assumption),
        argument_ids(std::move(argument_ids_)) {}

  const std::shared_ptr<Step> parent;
  const std::shared_ptr<Step> root;
  const std::shared_ptr<BindingEnvironment> env;
  const ParsedDeclaration assumption;
  std::vector<size_t> argument_ids;

  std::unordered_set<ParsedDeclaration> positive_depends;
  std::unordered_set<ParsedDeclaration> negative_depends;
};

// Generates different orderings of the predicates that result in different
// binding patterns. The idea is that for a given clause, and assuming all
// variables to one of its predicates are bound, we would like to evaluation
// all permutations of predicates in that clause, and score them for their
// ability to take advantage of sideways information passing style (SIPS),
// as well as to perform the bindings and unification recordings that would
// be needed to move our stepper forward.
class SIPSGenerator {
 public:

  // Initialize a SIPS generator. We initialize it with `clause`, whose head
  // must match `assumption`.
  SIPSGenerator(const std::shared_ptr<Step> &state_,
                ParsedClause clause_,
                ParsedPredicate assumption_)
      : state(state_),
        clause(clause_),
        assumption(assumption_) {

    assert(clause == ParsedClause::Containing(assumption));

    // The `assumption` predicate is kind of like a left corner in a parse. We
    // are assuming we have it and that all of its variables are bound, because
    // with the stepper, we're simulating a bottom-up execution. Thus we want
    // to exclude the assumption from the list of tested predicates that will
    // influence its SIP score and binding environment.
    for (auto pred : clause.PositivePredicates()) {
      if (pred == assumption) {
        continue;
      }

      // If any of the predicates are messages, then we will have accounted
      // for them as initial steps in the stepper, due to us starting with
      // messages.
      if (ParsedDeclaration::Of(pred).IsMessage()) {
        next_predicates.clear();
        return;
      }

      next_predicates.push_back(pred);
    }

    auto env = state->env;

    // First, apply renamings to the input variables of the assumption, thus
    // binding them.
    size_t i = 0;
    for (auto var : assumption.Arguments()) {
      const auto var_id = state->argument_ids[i++];

      // `assumption` is something like `foo(A, A)`. There is an implicit
      // unification constraint, where we say the two inputs are the same.
      if (renamings.count(var)) {
        auto prev_var_id = renamings[var];
        auto phi_id = env->bindings.size();
        Rename(prev_var_id, phi_id);

        // Synchronize the order numbers at PHI nodes.
        auto sync_order_num = env->next_order++;
        env->bindings.push_back({sync_order_num, assumption});
        env->unifications.push_back(
            {sync_order_num, prev_var_id, var_id, assumption});

      } else {
        renamings.emplace(var, var_id);
      }
    }

    // Then, go through the assignments, and apply renamings to them, or
    // conditions.
    for (auto assign : clause.Assignments()) {
      if (renamings.count(assign.LHS())) {
        env->unifications.push_back(
            {env->next_order++, renamings[assign.LHS()],
             std::string::npos, assign});
      } else {
        auto var_id = env->bindings.size();
        env->bindings.push_back({env->next_order++, assign});
        renamings[assign.LHS()] = var_id;
      }
    }

    // Now look for comparisons against bound variables. We don't generally
    // care about non-equality comparisons. In theory we should care about
    // inequality, but we will check for that in some other part.
    for (auto compare : clause.Comparisons()) {
      if (ComparisonOperator::kEqual != compare.Operator()) {
        continue;
      }

      auto missing_lhs = !renamings.count(compare.LHS());
      auto missing_rhs = !renamings.count(compare.RHS());
      if (missing_lhs && missing_rhs) {
        pending_compares.push_back(compare);
        continue;

      } else if (missing_rhs) {
        renamings[compare.RHS()] = renamings[compare.LHS()];

      } else if (missing_lhs) {
        renamings[compare.LHS()] = renamings[compare.RHS()];

      // Both are bound, we need to unify. This might mean making a new
      // variable binding representing the unification.
      } else {
        auto lhs_id = renamings[compare.LHS()];
        auto rhs_id = renamings[compare.RHS()];

        // If we're unifying two variables, then create a kind of PHI node
        // binding, and rename everything else in the `renamings` mapping
        // appropriately.
        if (lhs_id == rhs_id) {
          continue;
        }

        const auto phi_id = env->bindings.size();
        Rename(lhs_id, phi_id);
        Rename(rhs_id, phi_id);

        // Synchronize the order numbers at PHI nodes.
        auto sync_order_num = env->next_order++;
        env->bindings.push_back({sync_order_num, compare});
        env->unifications.push_back(
            {sync_order_num, lhs_id, rhs_id, compare});
      }
    }

    std::sort(next_predicates.begin(), next_predicates.end(), Compare);
  }

  // Try to generate a new set of bindings and constraints.
  bool TryGetNext() {
    if (next_predicates.empty()) {
      return false;
    }

    for (auto retry = true; retry; ) {
      retry = false;

      auto prev_renamings = renamings;

      // The complexity score is basically cyclomatic complexity. Imagine that
      // each unbound variable represents a new loop nesting. To increase the
      // score in proportion to this metric, we can simply shift and OR in a 1,
      // with an addition left shift per predicate seen, and a final bit
      // reversal to get the score so that lower is better.
      complexity_score = 0;

      for (auto pred : next_predicates) {
        assert(pred != assumption);
        assert(!pred.IsNegated());

        const auto decl = ParsedDeclaration::Of(pred);
        assert(!decl.IsMessage());

        const auto arity = pred.Arity();

        // For functors and queries, we care about the precise choice in order
        // to meet a binding constraint of one of the redeclarations. For
        // functors, we need to exactly match the usage specification. For
        // queries, we need to match the `bound` arguments, and the `free`
        // ones "cost" us in implicit later binding.
        if (decl.IsFunctor() || decl.IsQuery()) {
          for (auto redecl : decl.Redeclarations()) {
            for (auto i = 0U; i < arity; ++i) {
              auto redecl_param = redecl.NthParameter(i);
              auto pred_arg = pred.NthArgument(i);
              auto pred_arg_is_bound = renamings.count(pred_arg);

              // Make sure we meet the binding constraint for this functor
              // or query.
              if (!pred_arg_is_bound &&
                  (ParameterBinding::kBound == redecl_param.Binding() ||
                   ParameterBinding::kAggregate == redecl_param.Binding())) {
                retry = true;
                goto do_retry;

              } else if (pred_arg_is_bound &&
                         (ParameterBinding::kFree == redecl_param.Binding() ||
                          ParameterBinding::kSummary == redecl_param.Binding())) {
                if (decl.IsFunctor()) {
                  retry = true;
                  goto do_retry;

                // If we have `foo(A, B)`, where `foo(bound @i32 A, free @i32 B)`,
                // then even though we're feeding it a bound `B`, we need to
                // think of this logically as: we're asking for `foo(A, T)` and
                // then doing and implicit `B=T` for each `T`.
                } else {
                  AddComplexity();
                }
              }
            }
          }
        }

        // Go through and update the score before we do any assignments, so
        // that we cleanly handle the case like `foo(A, A)` where `A` is not
        // bound.
        num_uses_in_pred.clear();
        for (auto arg : pred.Arguments()) {
          if (!renamings.count(arg)) {
            num_uses_in_pred[arg] += 1;
            AddComplexity();
          }
        }

        // Now do renamings.
        for (auto arg : pred.Arguments()) {
          auto &num_uses = num_uses_in_pred[arg];
          if (!num_uses) {
            continue;

          // First renaming.
          } else if (!renamings.count(arg)) {
            // TODO

          // Nth renaming. We have something like `foo(A, A)`, where both
          // `A`s are unbound. Thus, the behavior is more like
          // `foo(A, A1), A=A1`.
          } else {
            // TODO
          }

          num_uses -= 1;
        }
      }

      // We shifted as we got deeper into the pemutation, but the deeper we
      // go, logically the deeper the loops go, so we need to
      complexity_score = __builtin_bitreverse64(complexity_score);

      // TODO emit something by ref

    do_retry:
      renamings.swap(prev_renamings);

      if (!std::next_permutation(
          next_predicates.begin(), next_predicates.end(), Compare)) {
        next_predicates.clear();
        retry = false;
      }
    }

    return true;
  }

 private:

  void Rename(size_t from_id, size_t to_id) {
    for (auto &ent : renamings) {
      if (ent.second == from_id) {
        ent.second = to_id;
      }
    }
  }

  void AddComplexity(void) {
    if (~complexity_score) {
      auto next = complexity_score << 1U;
      if (next < complexity_score) {
        complexity_score = 0;
        complexity_score = ~complexity_score;
        return;
      } else {
        complexity_score = next + 1;
      }
    }
  }

  static bool Compare(ParsedPredicate a, ParsedPredicate b) noexcept {
    return a.UniqueId() < b.UniqueId();
  }

  const std::shared_ptr<Step> state;
  const ParsedClause clause;
  const ParsedPredicate assumption;
  std::vector<ParsedPredicate> heads;
  std::vector<ParsedPredicate> next_predicates;
  std::vector<ParsedComparison> pending_compares;
  std::unordered_map<ParsedVariable, uint64_t> renamings;
  std::unordered_map<ParsedVariable, unsigned> num_uses_in_pred;
  size_t complexity_score;
};

static void Simulate(hyde::OutputStream &os, hyde::ParsedModule main_module) {

  // First, go find all modules.
  std::unordered_set<ParsedModule> seen_modules;
  std::vector<ParsedModule> modules;
  modules.push_back(main_module);
  while (!modules.empty()) {
    auto module = modules.back();
    modules.pop_back();
    if (!seen_modules.count(module)) {
      seen_modules.insert(module);
      for (auto import : module.Imports()) {
        modules.push_back(import.ImportedModule());
      }
    }
  }

  // Next, go through and initial our states, such that every message is used
  // as a starting declaration. Messages are the sources of inputs. They are
  // basically ephemeral IDB things.
  std::vector<std::shared_ptr<Step>> states;
  std::vector<std::shared_ptr<Step>> finished_states;
  std::unordered_map<ParsedDeclaration, size_t> phase;
  for (auto module : seen_modules) {
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
    // used, as they add new information.
    for (auto pred : assumption.PositiveUses()) {
      auto clause = ParsedClause::Containing(pred);
      auto next_assumption = ParsedDeclaration::Of(clause);

      SIPSGenerator generator(state, clause, pred);
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

}  // namespace hyde

static int ProcessModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log,
                         hyde::ParsedModule module) {

  if (!error_log.IsEmpty()) {
    error_log.Render(std::cerr);
    return EXIT_FAILURE;
  } else {
    hyde::OutputStream os(display_manager, std::cerr);
    hyde::FormatModule(os, module);
    Simulate(os, module);
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