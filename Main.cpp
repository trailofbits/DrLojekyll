// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>

#include <drlojekyll/CodeGen/BAM.h>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Rel/Format.h>
#include <drlojekyll/Rel/Builder.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Sema/SIPSScore.h>
#include <drlojekyll/Transforms/CombineModules.h>
#include <drlojekyll/Transforms/ProxyExternalsWithExports.h>

namespace hyde {

OutputStream *gOut = nullptr;

#if 0
class GraphPrinter : public BottomUpVisitor {
 public:
  GraphPrinter(DisplayManager &display_manager_)
      : display_manager(display_manager_),
        os(display_manager, std::cerr) {}

  virtual ~GraphPrinter(void) = default;

  bool VisitState(const State *state) {
    auto to_pred = state->assumption;
    if (!seen.count(to_pred)) {
      os << "p" << to_pred.UniqueId() << " [label=\""
         << to_pred << "\"];\n";
    }

    if (state->parent) {
      auto from_pred = state->parent->assumption;
      if (!seen.count(from_pred)) {
        os << "p" << from_pred.UniqueId() << " [label=\""
           << from_pred << "\"];\n";
      }

      auto &edges = seen[from_pred];
      if (edges.count(to_pred)) {
        return false;
      }

      os << "p" << from_pred.UniqueId() << " -> p" << to_pred.UniqueId() << ";\n";

      edges.insert(to_pred);
    }

    return true;
  }

  DisplayManager display_manager;
  OutputStream os;
  std::unordered_map<ParsedPredicate, std::unordered_set<ParsedPredicate>> seen;
};
#endif

#if 1
static void QueryDumper(DisplayManager display_manager,
                        ParsedModule module) {

  hyde::OutputStream os(display_manager, std::cerr);
  gOut = &os;

  QueryBuilder query_builder;
  for (auto clause : module.Clauses()) {
    FastBindingSIPSScorer scorer;
    SIPSGenerator generator(clause);
    query_builder.VisitClause(scorer, generator);
  }

  auto query = query_builder.BuildQuery();
  os << query;

  hyde::GenerateCode(display_manager, query, std::cerr);
}

#endif
#if 0
static void BottomUpDumper(DisplayManager display_manager,
                           ParsedModule module) {

  BottomUpAnalysis analysis;
  hyde::OutputStream os(display_manager, std::cerr);
  gOut = &os;

  os << "digraph {\n"
     << "node [shape=record margin=0 nojustify=false labeljust=l];\n"
     << "start [shape=none border=none];\n";

  for (auto state : analysis.GenerateStates(module)) {
    os << "s" << state->id << " [label=\"" << state->clause << "\"];\n";

    if (state->is_start_state) {
      os << "start -> s" << state->id;
      if (state->assumption) {
        os << " [label=\"" << *(state->assumption) << "\"]";
      }
      os << ";\n";
    }

    for (auto pred : state->Predecessors()) {
      os << "s" << pred->id << " -> s" << state->id;
      if (state->assumption) {
        os << " [label=\"" << *(state->assumption) << "\"]";
      }
      os << ";\n";
    }
  }
  os << "}\n";
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
    module = ProxyExternalsWithExports(display_manager, module);

    std::stringstream ss;
    do {
      hyde::OutputStream os(display_manager, ss);
      os << module;
    } while (false);

    std::cerr << ss.str();

    hyde::Parser parser(display_manager, error_log);
    auto module2 = parser.ParseStream(ss, hyde::DisplayConfiguration());
    if (!error_log.IsEmpty()) {
      error_log.Render(std::cerr);
      assert(error_log.IsEmpty());
      return EXIT_FAILURE;
    }

    std::stringstream ss2;
    do {
      hyde::OutputStream os(display_manager, ss2);
      os << module2;
    } while (false);

    std::cerr << ss.str() << "\n\n" << ss2.str() << "\n\n";
    assert(ss.str() == ss2.str());

//    auto transformed_module = ConvertQueriesToMessages(
//        display_manager, module);

    QueryDumper(display_manager, module);
//    BottomUpDumper(display_manager, module);
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
