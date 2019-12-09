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

class PseudoCodePrinter final : public SIPSVisitor {
 public:
  PseudoCodePrinter(DisplayManager display_manager_)
      : display_manager(display_manager_),
        os(display_manager_, ss) {}

  virtual ~PseudoCodePrinter(void) = default;

  void Begin(ParsedPredicate assumption) override {
    std::string_view clause_data;
    auto has_new_line = false;
    auto clause = ParsedClause::Containing(assumption);

    // Print out the clause.
    if (display_manager.TryReadData(clause.SpellingRange(), &clause_data)) {
      os << "// ";
      for (auto ch : clause_data) {
        if (has_new_line) {
          os << "\n// ";
          has_new_line = false;
        }
        if (ch == '\n') {
          has_new_line = true;
        } else {
          os << ch;
        }
      }
    }

    os << "\nfor each " << assumption << ":\n";
    indent += "  ";
  }

  void DeclareParameter(const Column &col) override {
    os << indent << "v" << col.id << " = " << col.var.Name()
       << "  // " << col.param.Type() << '\n';
  }

  void DeclareVariable(ParsedVariable var, unsigned id) override {
    os << indent << "v" << id << "  // " << var.Name() << '\n';
  }

  void DeclareConstant(ParsedLiteral val, unsigned id) override {
    os << indent << "v" << id << " = " << val.SpellingRange() << '\n';
  }

  void AssertEqual(unsigned lhs_id, unsigned rhs_id) override {
    os << indent << "if v" << lhs_id << " != " << rhs_id << ":\n"
       << indent << "  continue\n";
  }

  void AssertNotEqual(unsigned lhs_id, unsigned rhs_id) override {
    os << indent << "if v" << lhs_id << " == " << rhs_id << ":\n"
       << indent << "  continue\n";
  }

  void AssertLessThan(unsigned lhs_id, unsigned rhs_id) override {
    os << indent << "if v" << lhs_id << " >= " << rhs_id << ":\n"
       << indent << "  continue\n";
  }

  void AssertGreaterThan(unsigned lhs_id, unsigned rhs_id) override {
    os << indent << "if v" << lhs_id << " <= " << rhs_id << ":\n"
       << indent << "  continue\n";
  }

  void AssertPresent(ParsedPredicate pred, const Column *begin,
                     const Column *end) override {
    os << indent << "if not exists " << ParsedDeclaration::Of(pred).Name();
    auto comma = "(";
    for (auto col = begin; col < end; ++col) {
      os << comma << 'v' << col->id;
      comma = ", ";
    }
    os << "):\n"
       << indent << "  continue\n";
  }

  void AssertAbsent(ParsedPredicate pred, const Column *begin,
                    const Column *end) override {
    os << indent << "if exists " << ParsedDeclaration::Of(pred).Name();
    auto comma = "(";
    for (auto col = begin; col < end; ++col) {
      os << comma << 'v' << col->id;
      comma = ", ";
    }
    os << "):\n"
       << indent << "  continue\n";
  }

  void Insert(ParsedDeclaration decl, const Column *begin,
              const Column *end) override {
    os << indent << "publish " << decl.Name();
    auto comma = "(";
    for (auto col = begin; col < end; ++col) {
      os << comma << col->param.Name() << "=v" << col->id;
      comma = ", ";
    }
    os << ")\n";
  }

  void EnterFromWhereSelect(ParsedPredicate pred,
                            ParsedDeclaration from,
                            const Column *where_begin,
                            const Column *where_end,
                            const Column *select_begin,
                            const Column *select_end) override {
    os << indent << "for each ";
    auto comma = "(";
    for (auto col = select_begin; col < select_end; ++col) {
      os << comma << 'v' << col->id << "=" << col->param.Name();
      comma = ", ";
    }
    os << ") in " << from.Name();
    comma = "(";
    for (auto col = where_begin; col < where_end; ++col) {
      os << comma << col->param.Name() << "=v" << col->id;
      comma = ", ";
    }
    os << "):\n";
    indent += "  ";
  }

  void EnterFromSelect(ParsedPredicate pred,
                       ParsedDeclaration from,
                       const Column *select_begin,
                       const Column *select_end) override {
    os << indent << "for each ";
    auto comma = "(";
    for (auto col = select_begin; col < select_end; ++col) {
      os << comma << 'v' << col->id << "=" << col->param.Name();
      comma = ", ";
    }
    os << ") in ";
    if (from.IsFunctor() && ParsedFunctor::From(from).IsAggregate()) {
      os << "A" << pred.UniqueId();
    } else {
      os << from.Name();
    }
    os << ":\n";
    indent += "  ";
  }

  void ExitSelect(ParsedPredicate pred, ParsedDeclaration from) override {
    indent.pop_back();
    indent.pop_back();
  }

  void EnterAggregation(ParsedPredicate functor,
                        ParsedDeclaration decl,
                        const Column *bound_begin,
                        const Column *bound_end) override {
    os << indent << "A" << functor.UniqueId()
       << " = aggregator " << decl.Name() << "(";
    auto comma = "(";
    for (auto col = bound_begin; col < bound_end; ++col) {
      os << comma << col->param.Name() << " = v" << col->id;
      comma = ", ";
    }
    os << ")\n";
    indent += "  ";
  }
  void Collect(ParsedPredicate agg_pred, ParsedDeclaration agg_decl,
               const Column *begin, const Column *end) override {
    os << indent << "add " << agg_decl.Name();
    auto comma = "(";
    for (auto col = begin; col < end; ++col) {
      os << comma << col->param.Name() << "=v" << col->id;
      comma = ", ";
    }
    os << ") into A" << agg_pred.UniqueId() << "\n";
  }

  void Summarize(ParsedPredicate functor, ParsedDeclaration decl) override {
    indent.pop_back();
    indent.pop_back();
    os << indent << "summarize A" << functor.UniqueId() << '\n';
  }

  void Assign(unsigned dest_id, unsigned rhs_id) override {
    os << indent << "v" << dest_id << " = v" << rhs_id << '\n';
  }

  void Commit(void) override {
    accepted = true;
  }

  bool Advance(void) override {
    return !accepted;
  }

  const DisplayManager display_manager;
  std::stringstream ss;
  OutputStream os;
  std::string indent;
  bool accepted{false};
};

static void CodeDumper(hyde::DisplayManager display_manager,
                       ParsedModule module) {

  std::unordered_set<ParsedPredicate> seen_assumptions;

  std::vector<std::pair<ParsedPredicate, ParsedClause>> work_list;
  std::vector<std::pair<ParsedPredicate, ParsedClause>> next_work_list;

  for (auto message : module.Messages()) {
    for (auto use : message.PositiveUses()) {
      auto clause = ParsedClause::Containing(use);
      next_work_list.emplace_back(use, clause);
    }
  }

  while (!next_work_list.empty()) {
    work_list.swap(next_work_list);
    while (!work_list.empty()) {
      auto assumption = work_list.back().first;
      auto clause = work_list.back().second;
      work_list.pop_back();

      if (seen_assumptions.count(assumption)) {
        continue;
      }
      seen_assumptions.insert(assumption);

      SIPSGenerator generator(clause, assumption);
      do {
        PseudoCodePrinter printer(display_manager);
        if (generator.Visit(printer)) {
          printer.ss.flush();
          std::cerr << printer.ss.str() << '\n';

          for (auto next_use : ParsedDeclaration::Of(clause).PositiveUses()) {
            auto next_clause = ParsedClause::Containing(next_use);
            next_work_list.emplace_back(next_use, next_clause);
          }

          break;
        }
      } while (generator.Advance());
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
    module = CombineModules(display_manager, module);
    hyde::OutputStream os(display_manager, std::cout);
    os << module;
    os << "\n\n";
    CodeDumper(display_manager, module);
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
