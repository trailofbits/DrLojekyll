// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>

#include <drlojekyll/CodeGen/BAM.h>
#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Sema/SIPSChecker.h>

namespace hyde {

OutputStream *gOut = nullptr;

hyde::OutputStream *gDOTStream = nullptr;
hyde::OutputStream *gCodeStream = nullptr;

}  // namespace hyde

static int CompileModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log,
                         hyde::ParsedModule module) {

  if (auto query_opt = hyde::Query::Build(module, error_log); query_opt) {
    if (hyde::gDOTStream) {
      (*hyde::gDOTStream) << *query_opt;
      hyde::gDOTStream->Flush();
    }

    if (hyde::gCodeStream) {
      hyde::GenerateCode(module, *query_opt, *hyde::gCodeStream);
      hyde::gCodeStream->Flush();
    }

    return EXIT_SUCCESS;

  } else {
    return EXIT_FAILURE;
  }
}

static int ProcessModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log,
                         hyde::ParsedModule module) {

  if (CheckForErrors(display_manager, module, error_log)) {
    return EXIT_FAILURE;
  }

  // Round-trip test of the parser.
#ifndef NDEBUG
  std::stringstream ss;
  do {
    hyde::OutputStream os(display_manager, ss);
    os << module;
  } while (false);

  hyde::Parser parser(display_manager, error_log);
  auto module2_opt = parser.ParseStream(ss, hyde::DisplayConfiguration());
  if (!module2_opt) {
    return EXIT_FAILURE;
  }

  std::stringstream ss2;
  do {
    hyde::OutputStream os(display_manager, ss2);
    os << *module2_opt;
  } while (false);

  assert(ss.str() == ss2.str());
#endif

  return CompileModule(display_manager, error_log, module);
}

struct FileStream {
  FileStream(hyde::DisplayManager &dm_, const char *path_)
      : fs(path_),
        os(dm_, fs) {}

  std::ofstream fs;
  hyde::OutputStream os;
};

int main(int argc, char *argv[]) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log(display_manager);
  hyde::Parser parser(display_manager, error_log);

  std::string input_path;

  std::string file_path;
  auto num_input_paths = 0;


  std::stringstream linked_module;

  hyde::OutputStream os(display_manager, std::cerr);

  std::unique_ptr<FileStream> dot_out;
  std::unique_ptr<FileStream> cpp_out;
  hyde::gOut = &os;

  // Parse the command-line arguments.
  for (auto i = 1; i < argc; ++i) {

    // Output file of compiled datalog.
    if (!strcmp(argv[i], "-o")) {
      ++i;
      if (i >= argc) {
        hyde::Error err(display_manager);
        err << "Command-line argument '-o' must be followed by a file path for "
            << "C++ code output";
        error_log.Append(std::move(err));
      } else {
        cpp_out.reset(new FileStream(display_manager, argv[i]));
        hyde::gCodeStream = &(cpp_out->os);
      }

    } else if (!strcmp(argv[i], "--dot") ||
               !strcmp(argv[i], "-dot")) {
      ++i;
      if (i >= argc) {
        hyde::Error err(display_manager);
        err << "Command-line argument '-o' must be followed by a file path for "
            << "GraphViz DOT digraph output";
        error_log.Append(std::move(err));
      } else {
        dot_out.reset(new FileStream(display_manager, argv[i]));
        hyde::gDOTStream = &(dot_out->os);
      }

    // Datalog module file search path.
    } else if (argv[i] == strstr(argv[i], "-M")) {
      const char *path = nullptr;
      if (strlen(argv[i]) == 2) {
        path = &(argv[i][2]);
      } else {
        if (i >= argc) {
          hyde::Error err(display_manager);
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
        hyde::Error err(display_manager);
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
          hyde::Error err(display_manager);
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

  auto code = EXIT_FAILURE;

  if (!num_input_paths) {
    hyde::Error err(display_manager);
    err << "No input files to parse";
    error_log.Append(std::move(err));

  // Parse a single module.
  } else if (1 == num_input_paths) {
    hyde::DisplayConfiguration config = {
        input_path,  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };

    if (auto module_opt = parser.ParsePath(input_path, config); module_opt) {
      code = ProcessModule(display_manager, error_log, *module_opt);
    }

  // Parse multiple modules as a single module including each module to
  // be parsed.
  } else {
    hyde::DisplayConfiguration config = {
        "<amalgamation>",  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };

    if (auto module_opt = parser.ParseStream(linked_module, config); module_opt) {
      code = ProcessModule(display_manager, error_log, *module_opt);
    }
  }

  if (code) {
    error_log.Render(std::cerr);
  }

  return code;
}
