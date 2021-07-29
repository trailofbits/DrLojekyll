// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_set>

#include "UnitTests.h"
#include "drlojekyll/CodeGen/CodeGen.h"
#include "drlojekyll/CodeGen/MessageSerialization.h"
#include "drlojekyll/ControlFlow/Format.h"
#include "drlojekyll/Display/DisplayConfiguration.h"
#include "drlojekyll/Display/DisplayManager.h"
#include "drlojekyll/Display/Format.h"
#include "drlojekyll/Parse/ErrorLog.h"
#include "drlojekyll/Parse/ModuleIterator.h"
#include "drlojekyll/Parse/Parser.h"


namespace fs = std::filesystem;

namespace hyde {

// Needed for Google Test to be able to print hyde::ErrorLog values
std::ostream &operator<<(std::ostream &os, const ErrorLog &log) {
  log.Render(os);
  return os;
}

}  // namespace hyde

// How many kinds of messages are there in the given parsed module?
static size_t NumMessages(const hyde::ParsedModule &module) {
  size_t num_messages = 0;

  // Note: would use std::distance here, but `hyde::ParsedModule` doesn't
  // implement all the necessary APIs.
  for (auto message : module.Messages()) {
    (void) message;
    num_messages += 1;
  }
  return num_messages;
}

// Return a vector of all *.dr files immediately under the directories
template <typename... Path>
static std::vector<fs::path> DrFilesInDir(const Path &... dirs) {
  std::vector<fs::path> all_entries;

  for (const auto &dir : {dirs...}) {
    for (const auto &entry : fs::directory_iterator(dir)) {
      if (entry.path().extension() == ".dr" && fs::is_regular_file(entry)) {
        all_entries.emplace_back(entry.path());
      }
    }
  }

  return all_entries;
}

class PassingExamplesParsingSuite : public testing::TestWithParam<fs::path> {
 protected:
  void SetUp() override {
    TestWithParam<fs::path>::SetUp();  // Sets up the base fixture first.
    fs::create_directories(kGeneratedFilesDir);
    SUCCEED() << "Created directory for generated test files: "
              << kGeneratedFilesDir;
  }
};

// Set of examples that can parse but fail to build
static const std::unordered_set<std::string> kBuildDebugFailExamples{
    "min_block.dr", "pairwise_average_weight.dr", "function_counter.dr",
    "average_weight.dr", "conditions_to_bools.dr"};
static const std::unordered_set<std::string> kBuildReleaseFailExamples{
    "min_block.dr", "function_counter.dr", "conditions_to_bools.dr"};
static const std::unordered_set<std::string> kBuildIRReleaseFailExamples{
    "average_weight.dr", "pairwise_average_weight.dr", "conditions_to_bools.dr"};

// Test that the well-formed example files parse and build.
TEST_P(PassingExamplesParsingSuite, Examples) {
  fs::path path = GetParam();
  fs::path path_filename = path.filename();
  std::string path_filename_str = path_filename.string();
  std::string path_str = path.string();

  hyde::DisplayManager display_mgr;
  hyde::ErrorLog err_log(display_mgr);
  hyde::Parser parser(display_mgr, err_log);
  hyde::DisplayConfiguration display_cfg = {path_str, 2, true};

  // Parse the input
  auto mmod = parser.ParsePath(path_str, display_cfg);
  EXPECT_TRUE(err_log.IsEmpty()) << "Parsing failed:" << std::endl << err_log;
  ASSERT_TRUE(mmod.has_value());

  // Generate code for message schemas
  for (auto module : hyde::ParsedModuleIterator(*mmod)) {
    auto schemas =
        hyde::GenerateAvroMessageSchemas(display_mgr, module, err_log);
    EXPECT_TRUE(err_log.IsEmpty())
        << "Message schema generation failed:" << std::endl
        << err_log;
    EXPECT_TRUE(schemas.size() == NumMessages(module));
  }

  // Build the query
  auto query_opt = hyde::Query::Build(*mmod, err_log);
  ASSERT_TRUE(query_opt.has_value());

  // Build the program
  //
  // Note: Some tests fail to build -- handle those specially
  if (kBuildDebugFailExamples.count(path_filename_str)) {
    ASSERT_DEBUG_DEATH(
        hyde::Program::Build(*query_opt, hyde::IRFormat::kIterative),
        ".*TODO.*");
    return;
  }

  auto program_opt =
      hyde::Program::Build(*query_opt, hyde::IRFormat::kIterative);

  ASSERT_TRUE(program_opt.has_value());

  auto generated_file_base =
      fs::path(kGeneratedFilesDir) / path_filename.stem();

  // Save the IR
  {
    std::ofstream os(generated_file_base.string() + ".ir");
    hyde::OutputStream ir_out(display_mgr, os);
    ir_out << *program_opt;
  }

  // Skip examples that are known to fail
  if (kBuildReleaseFailExamples.count(path_filename_str)) {
    return;
  }

  // Generate Python code
  if (std::string::npos !=
      path_filename_str.find("self_testing_examples/python")) {
    auto py_out_path = generated_file_base.string() + ".py";
    std::ofstream os(py_out_path);
    hyde::OutputStream py_out_fs(display_mgr, os);
    hyde::python::GenerateDatabaseCode(*program_opt, py_out_fs);

    // Type-check the generated Python code with mypy, if available
#ifdef MYPY_PATH

    // Note, mypy can take input from a command line string via '-c STRING'
    // but that sounds unsafe to do from here, so pass the path to the file
    // instead.
    //
    // FIXME: possible command injection here!
    std::string mypy_cmd = std::string(MYPY_PATH) + " " + py_out_path;
    int mypy_ret_code = std::system(mypy_cmd.c_str());
    EXPECT_TRUE(mypy_ret_code == 0)
        << "Python mypy type-checking failed! Saved generated code at "
        << py_out_path << std::endl;
#endif  // MYPY_PATH

    std::string python_cmd = "python3 " + py_out_path;
    int python_ret_code = std::system(python_cmd.c_str());
    EXPECT_TRUE(python_ret_code == 0)
        << "Python correctness testing failed! Saved generated code at "
        << py_out_path << std::endl;
  }

  // Whether or not to compile to object file or main executable for correctness
  // testing
  if (std::string::npos !=
      path_filename_str.find("self_testing_examples/cxx")) {
    // Generate C++ code
    auto cxx_gen_path = generated_file_base.string() + ".cpp";
    {
      std::ofstream os(cxx_gen_path);
      hyde::OutputStream cxx_out_fs(display_mgr, os);
      hyde::cxx::GenerateDatabaseCode(*program_opt, cxx_out_fs);
    }

    auto cxx_out_path = generated_file_base.string() + ".out";

    // Try to compile generated C++ code
    // FIXME: possible command injection here!
    std::stringstream compile_cmd_ss;
    compile_cmd_ss
        << '"' << kCxxCompilerPath
        << "\" " << kCxxFlags
#ifdef _WIN32
        << " /std:c++17 "
        << " /I\"" << kDrlogPublicHeaders << "\" "
        << "/o " << cxx_out_path
#else
        << " -std=c++17 "
        << " -isystem \"" << kDrlogPublicHeaders << "\" "
        << "-o " << cxx_out_path
#endif
        << " \"" << cxx_gen_path + "\"";

    compile_cmd_ss << " \"" << kDrlogRuntimeLib << "\"";

    std::string compile_cmd = compile_cmd_ss.str();
#ifdef _WIN32
    int compile_ret_code = std::system(("\"" + compile_cmd + "\"").c_str());
#else
    int compile_ret_code = std::system(compile_cmd.c_str());
#endif

    EXPECT_TRUE(compile_ret_code == 0)
        << "C++ compilation failed with command:\n\t\"" << compile_cmd
        << "\"\n\tSaved generated code at: \"" << cxx_gen_path << "\"\n";

    // Check if we get a 0 output when trying to run it!
#ifdef _WIN32
    int run_ret_code = std::system(("\"" + cxx_out_path + "\"").c_str());
#else
    int run_ret_code = std::system(cxx_out_path.c_str());
#endif
    EXPECT_TRUE(run_ret_code == 0)
        << "Running the compiled binary failed!\n\t\"" << compile_cmd
        << "\"\n\tSaved binary at: \"" << cxx_out_path << "\"\n";
  }
}

INSTANTIATE_TEST_SUITE_P(
    ValidExampleParsing, PassingExamplesParsingSuite,
    testing::ValuesIn(DrFilesInDir(kExamplesDir, kSelfTestingPythonExamplesDir,
                                   kSelfTestingCxxExamplesDir)));

class FailingExamplesParsingSuite : public testing::TestWithParam<fs::path> {};

// Test that we fail to parse each of the .dr files in the invalid_examples
// directory with an error
TEST_P(FailingExamplesParsingSuite, Examples) {
  std::string path_str = GetParam().string();

  hyde::DisplayManager display_mgr;
  hyde::ErrorLog err_log(display_mgr);
  hyde::Parser parser(display_mgr, err_log);
  hyde::DisplayConfiguration display_cfg = {path_str, 2, true};

  // Parsing is expected to fail for the invalid examples.
  auto mmod = parser.ParsePath(path_str, display_cfg);
  EXPECT_FALSE(mmod.has_value());
  EXPECT_FALSE(err_log.IsEmpty());
}

INSTANTIATE_TEST_SUITE_P(
    InvalidSyntaxParsing, FailingExamplesParsingSuite,
    testing::ValuesIn(DrFilesInDir(kInvalidSyntaxExamplesDir)));
