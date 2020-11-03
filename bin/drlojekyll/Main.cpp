// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/CodeGen/MessageSerialization.h>
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
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>

namespace fs = std::filesystem;

namespace hyde {

OutputStream *gOut = nullptr;

struct FileStream {
  FileStream(hyde::DisplayManager &dm_, const fs::path path_)
      : fs(path_),
        os(dm_, fs) {}

  std::ofstream fs;
  hyde::OutputStream os;
};

namespace {

OutputStream *gDOTStream = nullptr;
OutputStream *gDRStream = nullptr;
OutputStream *gCodeStream = nullptr;
OutputStream *gIRStream = nullptr;
std::optional<fs::path> gMSGDir = std::nullopt;


static int CompileModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log, hyde::ParsedModule module) {

  if (auto query_opt = Query::Build(module, error_log)) {
    if (gDOTStream) {
      (*gDOTStream) << *query_opt;
      gDOTStream->Flush();
    }

    if (auto program_opt = Program::Build(*query_opt, error_log)) {
      if (gIRStream) {
        (*gIRStream) << *program_opt;
        gIRStream->Flush();
      }

      if (gCodeStream) {
        hyde::GenerateCode(*program_opt, *gCodeStream);
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

  // Output all message serializations.
  if (gMSGDir) {
    for (auto module : ParsedModuleIterator(module)) {
      for (auto schema_info :
           GenerateAvroMessageSchemas(display_manager, module, error_log)) {
        FileStream schema_stream(
            display_manager, *gMSGDir / (schema_info.message_name + ".avsc"));
        schema_stream.os << schema_info.schema.dump(2);
      }
    }
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
      << "USAGE: " << argv[0] << " [options] <DATALOG_PATH>..." << std::endl
      << std::endl
      << "OUTPUT OPTIONS:" << std::endl
      << "  -ir-out <PATH>        Emit IR output to PATH." << std::endl
      << "  -cpp-out <PATH>       Emit transpiled C++ output to PATH." << std::endl
      << "  -dr-out <PATH>        Emit an amalgamation of all the input and transitively" << std::endl
      << "                        imported modules to PATH." << std::endl
      << "  -dot-out <PATH>       Emit the data flow graph in GraphViz DOT format to PATH." << std::endl
      << std::endl
      << "COMPILATION OPTIONS:" << std::endl
      << "  -M <PATH>             Directory where import statements can find needed Datalog modules." << std::endl
      << "  -isystem <PATH>       Directory where system C++ include files can be found." << std::endl
      << "  -I <PATH>             Directory where user C++ include files can be found." << std::endl
      << std::endl
      << "OTHER OPTIONS:" << std::endl
      << "  -help, -h             Show help and exit." << std::endl
      << "  -version              Show version number and exit." << std::endl
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

}  // namespace
}  // namespace hyde

extern "C" int main(int argc, const char *argv[]) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log(display_manager);
  hyde::Parser parser(display_manager, error_log);

  std::string input_path;

  std::string file_path;
  int num_input_paths = 0;

  std::stringstream linked_module;

  hyde::OutputStream os(display_manager, std::cout);
  hyde::gOut = &os;

  std::unique_ptr<hyde::FileStream> dot_out;
  std::unique_ptr<hyde::FileStream> cpp_out;
  std::unique_ptr<hyde::FileStream> ir_out;
  std::unique_ptr<hyde::FileStream> dr_out;

  // Parse the command-line arguments.
  for (int i = 1; i < argc; ++i) {

    // C++ output file of the transpiled from the Dr. Lojekyll source code.
    if (!strcmp(argv[i], "-cpp-out") || !strcmp(argv[i], "--cpp-out")) {
      ++i;
      if (i >= argc) {
        error_log.Append()
            << "Command-line argument " << argv[i]
            << " must be followed by a file path for C++ code output";
      } else {
        cpp_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gCodeStream = &(cpp_out->os);
      }

    } else if (!strcmp(argv[i], "-ir-out") || !strcmp(argv[i], "--ir-out")) {
      ++i;
      if (i >= argc) {
        error_log.Append()
            << "Command-line argument " << argv[i]
            << " must be followed by a file path for IR output";
      } else {
        ir_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gIRStream = &(ir_out->os);
      }

    // Option to output a single Dr. Lojekyll Datalog file that is equivalent
    // to the amalagamation of all input files, and transitively imported files.
    } else if (!strcmp(argv[i], "--dr-out") ||
               !strcmp(argv[i], "-dr-out")) {
      ++i;
      if (i >= argc) {
        error_log.Append() << "Command-line argument '" << argv[i - 1]
                           << "' must be followed by a file path for "
                           << "alamgamated Datalog output";
      } else {
        dr_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gDRStream = &(dr_out->os);
      }

    // Write message schemas to an output directory
    } else if (!strcmp(argv[i], "--messages-dir") ||
               !strcmp(argv[i], "-messages-dir")) {
      ++i;
      if (i >= argc) {
        hyde::Error err(display_manager);
        err << "Command-line argument '" << argv[i - 1]
            << "' must be followed by a directory path for "
            << "message serialization output";
        error_log.Append(std::move(err));
      } else {
        hyde::gMSGDir = fs::path(argv[i]);

        // TODO(ek): Error handling
        fs::create_directories(*hyde::gMSGDir);
      }

    // GraphViz DOT digraph output, which is useful for debugging the data flow.
    } else if (!strcmp(argv[i], "--dot-out") || !strcmp(argv[i], "-dot-out")) {
      ++i;
      if (i >= argc) {
        error_log.Append() << "Command-line argument '" << argv[i - 1]
                           << "' must be followed by a file path for "
                           << "GraphViz DOT digraph output";
      } else {
        dot_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gDOTStream = &(dot_out->os);
      }

    // Datalog module file search path.
    } else if (strstr(argv[i], "-M")) {
      if (i >= argc) {
        error_log.Append()
            << "Command-line argument '-M' must be followed by a directory path";
        continue;
      }
      const char *path = argv[++i];

      parser.AddModuleSearchPath(path);

    // Include file search path.
    } else if (!strcmp(argv[i], "-isystem")) {
      ++i;
      if (i >= argc) {
        error_log.Append()
            << "Command-line argument '-isystem' must be followed by a directory path";
      } else {
        parser.AddIncludeSearchPath(argv[i], hyde::Parser::kSystemInclude);
      }

    // Include file search path.
    } else if (strstr(argv[i], "-I")) {
      if (i >= argc) {
        error_log.Append()
            << "Command-line argument '-I' must be followed by a directory path";
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
      error_log.Append() << "Unrecognized command-line argument '" << argv[i]
                         << "'";
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

  int code = EXIT_FAILURE;

  // Exit early if command-line option parsing failed.
  if (!error_log.IsEmpty()) {

  } else if (!num_input_paths) {
    error_log.Append() << "No input files to parse";

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
  } else {
    assert(error_log.IsEmpty());
  }

  return code;
}
