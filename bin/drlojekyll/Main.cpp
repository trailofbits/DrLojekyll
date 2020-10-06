// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Version/Version.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>

namespace hyde {

OutputStream *gOut = nullptr;

namespace {

OutputStream *gDOTStream = nullptr;
OutputStream *gDRStream = nullptr;
OutputStream *gCodeStream = nullptr;


static int CompileModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log, hyde::ParsedModule module) {

  if (auto query_opt = Query::Build(module, error_log)) {
    if (gDOTStream) {
      (*gDOTStream) << *query_opt;
      gDOTStream->Flush();
    }

    if (auto program_opt = Program::Build(*query_opt, error_log)) {
      if (gCodeStream) {
        (*gCodeStream) << *program_opt;
        gCodeStream->Flush();
      }
    }

    return EXIT_SUCCESS;
  } else {
    return EXIT_FAILURE;
  }
}

static int ProcessModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log, hyde::ParsedModule module) {

  // Output the amalgamation of all files.
  if (gDRStream) {
    gDRStream->SetKeepImports(false);
    gDRStream->SetRenameLocals(true);
    for (auto module : ParsedModuleIterator(module)) {
      (*gDRStream) << module;
    }
    gDRStream->Flush();
  }

  // Round-trip test of the parser.
#ifndef NDEBUG
  std::stringstream ss;
  do {
    OutputStream os(display_manager, ss);
    os << module;
  } while (false);

  Parser parser(display_manager, error_log);

  // FIXME(blarsen): Using ParseStream do re-parse a pretty-printed module
  //                 doesn't work, due to differences in module search paths
  //                 with ParseStream and ParsePath.
  auto module2_opt = parser.ParseStream(ss, hyde::DisplayConfiguration());
  if (!module2_opt) {
    return EXIT_FAILURE;
  }

  std::stringstream ss2;
  do {
    OutputStream os(display_manager, ss2);
    os << *module2_opt;
  } while (false);

  assert(ss.str() == ss2.str());
#endif

  return CompileModule(display_manager, error_log, module);
}

// Our current clang-format configuration reformats the long lines in following
// function into something very hard to read, so explicitly disable
// clang-format for this bit.
//
// clang-format off
static int HelpMessage(const char *argv[]) {
  std::cout
      << "OVERVIEW: Dr. Lojekyll compiler" << std::endl
      << std::endl
      << "USAGE: " << argv[0] << " [options] file..." << std::endl
      << std::endl
      << "OPTIONS:" << std::endl
      << "  -version              Show version number and exit." << std::endl
      << "  -o <PATH>             C++ output file produced as a result of transpiling Datalog to C++." << std::endl
      << "  -amalgamation <PATH>  Datalog output file representing all input and transitively" << std::endl
      << "                        imported modules amalgamated into a single Datalog module." << std::endl
      << "  -dot <PATH>           GraphViz DOT digraph output file of the data flow graph." << std::endl
      << "  -M <PATH>             Directory where import statements can find needed Datalog modules." << std::endl
      << "  -isystem <PATH>       Directory where system C++ include files can be found." << std::endl
      << "  -I <PATH>             Directory where user C++ include files can be found." << std::endl
      << "  <PATH>                Path to an input Datalog module to parse and transpile." << std::endl
      << std::endl;

  return EXIT_SUCCESS;
}
// clang-format on

static int VersionMessage(void) {
  std::stringstream version;

  auto vs = hyde::version::GetVersionString();
  if (0 == vs.size()) {
    vs = "unknown";
  }
  version << "Dr. Lojekyll compiler: " << vs << "\n";
  if (!hyde::version::HasVersionData()) {
    version << "No extended version information found!\n";
  } else {
    version << "Commit Hash: " << hyde::version::GetCommitHash() << "\n";
    version << "Commit Date: " << hyde::version::GetCommitDate() << "\n";
    version << "Last commit by: " << hyde::version::GetAuthorName() << " ["
            << hyde::version::GetAuthorEmail() << "]\n";
    version << "Commit Subject: [" << hyde::version::GetCommitSubject()
            << "]\n";
    version << "\n";
    if (hyde::version::HasUncommittedChanges()) {
      version << "Uncommitted changes were present during build.\n";
    } else {
      version << "All changes were committed prior to building.\n";
    }
  }

  std::cout << version.str();
  return EXIT_SUCCESS;
}

struct FileStream {
  FileStream(hyde::DisplayManager &dm_, const char *path_)
      : fs(path_),
        os(dm_, fs) {}

  std::ofstream fs;
  hyde::OutputStream os;
};

}  // namespace
}  // namespace hyde

extern "C" int main(int argc, const char *argv[]) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log(display_manager);
  hyde::Parser parser(display_manager, error_log);

  std::string input_path;

  std::string file_path;
  auto num_input_paths = 0;

  std::stringstream linked_module;

  hyde::OutputStream os(display_manager, std::cout);
  hyde::gOut = &os;

  std::unique_ptr<hyde::FileStream> dot_out;
  std::unique_ptr<hyde::FileStream> cpp_out;
  std::unique_ptr<hyde::FileStream> dr_out;

  // Parse the command-line arguments.
  for (auto i = 1; i < argc; ++i) {

    // C++ output file of the transpiled from the Dr. Lojekyll source code.
    if (!strcmp(argv[i], "-o")) {
      ++i;
      if (i >= argc) {
        hyde::Error err(display_manager);
        err << "Command-line argument '-o' must be followed by a file path for "
            << "C++ code output";
        error_log.Append(std::move(err));
      } else {
        cpp_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gCodeStream = &(cpp_out->os);
      }

    // Option to output a single Dr. Lojekyll Datalog file that is equivalent
    // to the amalagamation of all input files, and transitively imported files.
    } else if (!strcmp(argv[i], "--amalgamation") ||
               !strcmp(argv[i], "-amalgamation")) {
      ++i;
      if (i >= argc) {
        hyde::Error err(display_manager);
        err << "Command-line argument '" << argv[i - 1]
            << "' must be followed by a file path for "
            << "alamgamated Datalog output";
        error_log.Append(std::move(err));
      } else {
        dr_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gDRStream = &(dr_out->os);
      }

    // GraphViz DOT digraph output, which is useful for debugging the data flow.
    } else if (!strcmp(argv[i], "--dot") || !strcmp(argv[i], "-dot")) {
      ++i;
      if (i >= argc) {
        hyde::Error err(display_manager);
        err << "Command-line argument '" << argv[i - 1]
            << "' must be followed by a file path for "
            << "GraphViz DOT digraph output";
        error_log.Append(std::move(err));
      } else {
        dot_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gDOTStream = &(dot_out->os);
      }

    // Datalog module file search path.
    } else if (strstr(argv[i], "-M")) {
      if (i >= argc) {
        hyde::Error err(display_manager);
        err << "Command-line argument '-M' must be followed by a directory path";
        error_log.Append(std::move(err));
        continue;
      }
      const char *path = argv[++i];

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
    } else if (strstr(argv[i], "-I")) {
      if (i >= argc) {
        hyde::Error err(display_manager);
        err << "Command-line argument '-I' must be followed by a directory path";
        error_log.Append(std::move(err));
        continue;
      }
      const char *path = argv[++i];

      parser.AddIncludeSearchPath(path, hyde::Parser::kUserInclude);

    // Help message :-)
    } else if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-help") ||
               !strcmp(argv[i], "-h")) {
      return hyde::HelpMessage(argv);

    // Version Message
    } else if (!strcmp(argv[i], "--version") || !strcmp(argv[i], "-version") ||
               !strcmp(argv[i], "-v")) {
      return hyde::VersionMessage();

    // Does this look like a command-line option?
    } else if (strstr(argv[i], "--") == argv[i] ||
               strchr(argv[i], '-') == argv[i]) {
      hyde::Error err(display_manager);
      err << "Unrecognized command-line argument '" << argv[i] << "'";
      error_log.Append(std::move(err));
      continue;

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
          default: file_path.push_back(ch);
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

    if (auto module_opt = parser.ParsePath(input_path, config)) {
      code = hyde::ProcessModule(display_manager, error_log, *module_opt);
    }

  // Parse multiple modules as a single module including each module to
  // be parsed.
  } else {
    hyde::DisplayConfiguration config = {
        "<amalgamation>",  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };

    if (auto module_opt = parser.ParseStream(linked_module, config)) {
      code = hyde::ProcessModule(display_manager, error_log, *module_opt);
    }
  }

  if (code) {
    error_log.Render(std::cerr);
  }

  return code;
}
