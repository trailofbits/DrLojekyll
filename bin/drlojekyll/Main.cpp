// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Version/Version.h>

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif
#include <flatbuffers/flatc.h>
#include <flatbuffers/util.h>
#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

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

std::string gDatabaseName = "datalog";
bool gHasDatabaseName = false;
const char *gCxxOutDir = nullptr;
const char *gPyOutDir = nullptr;

OutputStream *gDOTStream = nullptr;
OutputStream *gDRStream = nullptr;
OutputStream *gFlatCodeStream = nullptr;
OutputStream *gIRStream = nullptr;

// Generate a flatbuffer schema in memory.
static void GenerateFlatBufferSchema(
    DisplayManager display_manager, Program program,
    std::string *schema) {
  if (!schema->empty()) {
    return;
  }

  std::stringstream fb_ss;
  hyde::OutputStream fb_os(display_manager, fb_ss);
  fb_os.SetIndentSize(2u);
  flat::GenerateInterfaceCode(program, fb_os);
  fb_os.Flush();
  fb_ss.str().swap(*schema);
}

// Run the FlatBuffers parser and compiler to emit C++ and Python code.
static int GenerateFlatBufferOutput(const Parser &dr_parser,
                                    const std::string &schema,
                                    ErrorLog error_log) {

  flatbuffers::IDLOptions opts;
  opts.generate_all = true;
  opts.cpp_static_reflection = true;
  opts.cpp_std = "C++17";
  opts.lang_to_generate = flatbuffers::IDLOptions::kCpp |
                          flatbuffers::IDLOptions::kPython;
  opts.generate_name_strings = true;

  // If we have an include inside of a FlatBuffer `#prologue`, then don't
  // generate code for that.
  opts.generate_all = false;

  flatbuffers::Parser parser(opts);

  std::vector<std::string> include_dirs;
  std::vector<const char *> include_dirs_cstrs;
  for (auto path : dr_parser.SearchPaths()) {
    include_dirs.emplace_back(path.generic_string());
  }
  for (const auto &path : include_dirs) {
    include_dirs_cstrs.emplace_back(path.c_str());
  }
  include_dirs_cstrs.push_back(nullptr);

  std::string source_file_name = gDatabaseName + ":in-memory.fbs";

  auto ret = parser.Parse(schema.c_str(), &(include_dirs_cstrs[0]),
                          source_file_name.c_str());

  if (!ret) {
    error_log.Append()
        << parser.error_;
    return EXIT_FAILURE;
  }

  if (gCxxOutDir) {
    auto out_dir = std::filesystem::path(std::string(gCxxOutDir) + "/").lexically_normal().generic_string();
    auto ret1 = flatbuffers::GenerateCPP(parser, out_dir.c_str(),
                                         gDatabaseName);
    auto ret2 = flatbuffers::GenerateCppGRPC(parser, out_dir.c_str(),
                                             gDatabaseName);
    assert(ret1);
    assert(ret2);
    (void) ret1; (void) ret2;
  }

  if (gPyOutDir) {
    auto out_dir = std::filesystem::path(std::string(gPyOutDir) + "/").lexically_normal();
    auto out_dir_str = out_dir.generic_string();

    // The Python codegen for FlatBuffer gRPC stuff needs to execute in the
    // target working directory.
    const auto current_dir = std::filesystem::current_path();
    std::filesystem::current_path(out_dir);

    // If `#database` is manually specified, then this'll introduce a Python
    // module.
    if (gHasDatabaseName) {
      std::filesystem::create_directories(gDatabaseName);
    }

    parser.opts.generate_object_based_api = true;
    auto ret1 = flatbuffers::GeneratePython(parser, out_dir_str.c_str(),
                                            gDatabaseName);

    auto ret2 = flatbuffers::GeneratePythonGRPC(parser, out_dir_str.c_str(),
                                                gDatabaseName);

    std::filesystem::current_path(current_dir);
    assert(ret1);
    assert(ret2);
    (void) ret1; (void) ret2;
  }

  return EXIT_SUCCESS;
}

static int CompileModule(const Parser &parser, DisplayManager display_manager,
                         ErrorLog error_log, ParsedModule module) {
  auto query_opt = Query::Build(module, error_log);
  if (!query_opt) {
    return EXIT_FAILURE;
  }

  auto ret = EXIT_SUCCESS;
  try {
    std::string fb_schema;

    auto program_opt = Program::Build(*query_opt);
    if (!program_opt) {
      return EXIT_FAILURE;
    }

    if (gCxxOutDir || gPyOutDir || gFlatCodeStream) {
      GenerateFlatBufferSchema(
          display_manager, *program_opt, &fb_schema);

      // FlatBuffer schema output.
      if (gFlatCodeStream) {
        (*gFlatCodeStream) << fb_schema;
        gFlatCodeStream->Flush();
      }
    }

    if (gIRStream) {
      (*gIRStream) << *program_opt;
      gIRStream->Flush();
    }

    if (gCxxOutDir || gPyOutDir) {
      if (auto ret = GenerateFlatBufferOutput(parser, fb_schema, error_log)) {
        return ret;
      }
    }

    if (gCxxOutDir) {
      std::filesystem::path dir = gCxxOutDir;
      hyde::FileStream db_fs(
          display_manager,
          (dir / (gDatabaseName + ".db.h")).generic_string());
      hyde::cxx::GenerateDatabaseCode(*program_opt, db_fs.os);

      hyde::FileStream interface_fs(
          display_manager,
          (dir / (gDatabaseName + ".interface.h")).generic_string());
      hyde::cxx::GenerateInterfaceCode(*program_opt, interface_fs.os);

      hyde::FileStream server_fs(
          display_manager,
          (dir / (gDatabaseName + ".server.cpp")).generic_string());

      hyde::cxx::GenerateServerCode(*program_opt, server_fs.os);

      hyde::FileStream client_h_fs(
          display_manager,
          (dir / (gDatabaseName + ".client.h")).generic_string());

      hyde::FileStream client_cpp_fs(
          display_manager,
          (dir / (gDatabaseName + ".client.cpp")).generic_string());

      hyde::cxx::GenerateClientCode(*program_opt, client_h_fs.os,
                                    client_cpp_fs.os);
    }

    if (gPyOutDir) {
      std::filesystem::path dir = gPyOutDir;
      if (gHasDatabaseName) {
        dir /= gDatabaseName;
      }

      hyde::FileStream interface_fs(
          display_manager,
          (dir / "__init__.py").generic_string());
      hyde::python::GenerateInterfaceCode(*program_opt, interface_fs.os);
    }

//        if (gPyCodeStream) {
//          gPyCodeStream->SetIndentSize(4u);
//          hyde::python::GenerateDatabaseCode(*program_opt, *gPyCodeStream);
//        }
//
//        if (gPyInterfaceCodeStream) {
//          gPyInterfaceCodeStream->SetIndentSize(4u);
//          hyde::python::GenerateInterfaceCode(*program_opt,
//                                              *gPyInterfaceCodeStream);
//        }
  } catch (...) {
    ret = EXIT_FAILURE;
  }

  // NOTE(pag): We do this later because if we produce the control-flow IR
  //            then we break abstraction layers in order to annotate the
  //            data flow IR with table IDs.
  if (gDOTStream) {
    (*gDOTStream) << *query_opt;
    gDOTStream->Flush();
  }

  return ret;
}

// Process a parsed module.
static int ProcessModule(const Parser &parser, DisplayManager display_manager,
                         ErrorLog error_log, ParsedModule module) {

  // Figure out the database name to use. This affects code generation.
  if (auto maybe_name = module.DatabaseName()) {
    gHasDatabaseName = true;
    gDatabaseName = maybe_name->FileName();
  }

  // Output the amalgamation of all files.
  if (gDRStream) {
    gDRStream->SetRenameLocals(true);
    (*gDRStream) << module;
    gDRStream->Flush();
  }

  // Round-trip test of the parser in debug builds.
#ifndef NDEBUG
  std::stringstream ss;
  do {
    OutputStream os(display_manager, ss);
    os << module;
  } while (false);

  Parser parser2(display_manager, error_log);
  auto module2_opt = parser2.ParseStream(ss, hyde::DisplayConfiguration());
  if (!module2_opt) {
    return EXIT_FAILURE;
  }

  std::stringstream ss2;
  do {
    OutputStream os(display_manager, ss2);
    os << *module2_opt;
  } while (false);

  Parser parser3(display_manager, error_log);
  auto module3_opt = parser3.ParseStream(ss2, hyde::DisplayConfiguration());
  if (!module3_opt) {
    return EXIT_FAILURE;
  }

  std::stringstream ss3;
  do {
    OutputStream os(display_manager, ss3);
    os << *module3_opt;
  } while (false);

  assert(ss2.str() == ss3.str());
#endif

  return CompileModule(parser, display_manager, error_log, module);
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
      << "  -ir-out <PATH>            Emit IR output to PATH." << std::endl
      << "  -cpp-out <DIR>            Emit transpiled C++ output files into DIR." << std::endl
      << "  -py-out <DIR>             Emit transpiled Python output files into DIR." << std::endl
      << "  -flat-out <PATH>          Emit a FlatBuffer schema to PATH." << std::endl
      << "  -dr-out <PATH>            Emit an amalgamation of all the input and transitively" << std::endl
      << "                            imported modules to PATH." << std::endl
      << "  -dot-out <PATH>           Emit the data flow graph in GraphViz DOT format to PATH." << std::endl
      << std::endl
      << "COMPILATION OPTIONS:" << std::endl
      << "  -M <PATH>                 Directory where import statements can find needed Datalog modules." << std::endl
      << std::endl
      << "OTHER OPTIONS:" << std::endl
      << "  -help, -h                 Show help and exit." << std::endl
      << "  -version                  Show version number and exit." << std::endl
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
  // Prevent Appveyor-CI hangs.
  flatbuffers::SetupDefaultCRTReportMode();

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
  std::unique_ptr<hyde::FileStream> fb_out;
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
            << " must be followed by a directory path for C++ code output";
      } else {
        hyde::gCxxOutDir = argv[i];
      }

    // Python output file of the transpiled from the Dr. Lojekyll source code.
    } else if (!strcmp(argv[i], "-py-out") || !strcmp(argv[i], "--py-out")) {
      ++i;
      if (i >= argc) {
        error_log.Append()
            << "Command-line argument " << argv[i]
            << " must be followed by a directory path for Python code output";
      } else {
        hyde::gPyOutDir = argv[i];
      }

    } else if (!strcmp(argv[i], "-flat-out") ||
               !strcmp(argv[i], "--flat-out")) {
      ++i;
      if (i >= argc) {
        error_log.Append()
            << "Command-line argument " << argv[i]
            << " must be followed by a file path for FlatBuffer "
            << "schema output";
      } else {
        fb_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gFlatCodeStream = &(fb_out->os);
      }

    } else if (!strcmp(argv[i], "-ir-out") || !strcmp(argv[i], "--ir-out")) {
      ++i;
      if (i >= argc) {
        error_log.Append() << "Command-line argument " << argv[i]
                           << " must be followed by a file path for IR output";
      } else {
        ir_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gIRStream = &(ir_out->os);
      }

    // Option to output a single Dr. Lojekyll Datalog file that is equivalent
    // to the amalagamation of all input files, and transitively imported files.
    } else if (!strcmp(argv[i], "--dr-out") || !strcmp(argv[i], "-dr-out")) {
      ++i;
      if (i >= argc) {
        error_log.Append() << "Command-line argument '" << argv[i - 1]
                           << "' must be followed by a file path for "
                           << "alamgamated Datalog output";
      } else {
        dr_out.reset(new hyde::FileStream(display_manager, argv[i]));
        hyde::gDRStream = &(dr_out->os);
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
    } else if (!strcmp(argv[i], "-M")) {
      if (i >= argc) {
        error_log.Append()
            << "Command-line argument '-M' must be followed by a directory path";

      } else {
        std::filesystem::path path(argv[++i]);
        parser.AddModuleSearchPath(std::move(path));
      }

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

      linked_module << "#import \"" << file_path << "\".\n";
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
  } else if (1 == num_input_paths && !hyde::gHasDatabaseName) {
    hyde::DisplayConfiguration config = {
        input_path,  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };

    if (auto module_opt = parser.ParsePath(input_path, config)) {
      code = hyde::ProcessModule(
          parser, display_manager, error_log, *module_opt);
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
      code = hyde::ProcessModule(parser, display_manager, error_log, *module_opt);
    }
  }

  if (code) {
    error_log.Render(std::cerr);
  } else {
    assert(error_log.IsEmpty());
  }

  return code;
}
