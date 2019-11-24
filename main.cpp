// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>

namespace hyde {

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
    }

    os << param.Type() << " " << param.Name();
    comma = ", ";
  }
  os << ")\n";
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
  }

  for (auto decl : module.Messages()) {
    FormatDecl(os, decl);
  }

  for (auto decl : module.Functors()) {
    FormatDecl(os, decl);
  }

  for (auto decl : module.Exports()) {
    FormatDecl(os, decl);
  }

  for (auto decl : module.Locals()) {
    FormatDecl(os, decl);
  }

  for (auto clause : module.Clauses()) {
    FormatClause(os, clause);
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