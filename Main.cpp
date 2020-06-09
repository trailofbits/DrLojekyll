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
#include <drlojekyll/Sema/SIPSChecker.h>
#include <drlojekyll/Transforms/CombineModules.h>
#include <drlojekyll/Transforms/ProxyExternalsWithExports.h>

namespace hyde {

OutputStream *gOut = nullptr;

}  // namespace hyde

static void CompileModule(hyde::DisplayManager display_manager,
                          hyde::ParsedModule module) {

  hyde::OutputStream os(display_manager, std::cerr);
  hyde::gOut = &os;

  auto query = hyde::Query::Build(module);
  os << query;

  hyde::GenerateCode(module, query, os);
}

static int ProcessModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log,
                         hyde::ParsedModule module,
                         const std::string &output_path) {

  if (error_log.IsEmpty()) {
    CheckForErrors(display_manager, module, error_log);
  }

  if (!error_log.IsEmpty()) {
    error_log.Render(std::cerr);
    return EXIT_FAILURE;
  }

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

  std::cerr << "\n\n" << ss2.str() << "\n\n";
  assert(ss.str() == ss2.str());

  CompileModule(display_manager, module);
  return EXIT_SUCCESS;
}

int main(int argc, char *argv[]) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log;
  hyde::Parser parser(display_manager, error_log);

  std::string input_path;
  std::string output_path = "/dev/null";
  std::string file_path;
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

    // Datalog module file search path.
    } else if (argv[i] == strstr(argv[i], "-M")) {
      const char *path = nullptr;
      if (strlen(argv[i]) == 2) {
        path = &(argv[i][2]);
      } else {
        if (i >= argc) {
          hyde::Error err;
          err << "Command-line argument '-M' must be followed by a directory path";
          error_log.Append(std::move(err));
          continue;
        }
        path = argv[++i];
      }

      parser.AddModuleSearchPath(path);

    // Include file search path.
    } else if (!strcmp(argv[i], "-isystem")) {
      ++i;
      if (i >= argc) {
        hyde::Error err;
        err << "Command-line argument '-isystem' must be followed by a directory path";
        error_log.Append(std::move(err));

      } else {
        parser.AddIncludeSearchPath(argv[i], hyde::Parser::kSystemInclude);
      }

    // Include file search path.
    } else if (!strcmp(argv[i], "-I")) {
      const char *path = nullptr;
      if (strlen(argv[i]) == 2) {
        path = &(argv[i][2]);
      } else {
        if (i >= argc) {
          hyde::Error err;
          err << "Command-line argument '-I' must be followed by a directory path";
          error_log.Append(std::move(err));
          continue;
        }
        path = argv[++i];
      }

      parser.AddIncludeSearchPath(path, hyde::Parser::kUserInclude);

    // Input datalog file, add it to the list of paths to parse.
    } else {
      file_path.clear();

      for (auto ch : std::string_view(argv[i])) {
        switch (ch) {
          case '\n':
            file_path.push_back('\\');
            file_path.push_back('n');
            break;
          case '\t':
            file_path.push_back('\\');
            file_path.push_back('t');
            break;
          case '"':
            file_path.push_back('\\');
            file_path.push_back('"');
            break;
          default:
            file_path.push_back(ch);
        }
      }

      linked_module << "#import \"" << file_path << "\"\n";
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
    return ProcessModule(display_manager, error_log, module, output_path);

  // Parse multiple modules as a single module including each module to
  // be parsed.
  } else {
    hyde::DisplayConfiguration config = {
        "<amalgamation>",  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };

    auto module = parser.ParseStream(linked_module, config);
    return ProcessModule(display_manager, error_log, module, output_path);
  }
}
