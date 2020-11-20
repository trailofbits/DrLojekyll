// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
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

struct FileStream {
  FileStream(hyde::DisplayManager &dm_, const fs::path path_)
      : fs(path_),
        os(dm_, fs) {}

  std::ofstream fs;
  hyde::OutputStream os;
};

}  // namespace hyde

// Make sure that we can parse each of the .dr files in the examples directory
// without an error
static auto DrFilesInDir(const fs::path &dir) {
  std::vector<fs::path> all_entries;
  for (const auto &entry : fs::directory_iterator(dir)) {
    if (entry.path().extension() == ".dr" && fs::is_regular_file(entry)) {
      all_entries.emplace_back(entry.path());
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
static const std::unordered_set<std::string> gBuildFailExamples{
    "min_block.dr", "pairwise_average_weight.dr", "function_counter.dr",
    "watched_user_dir.dr", "average_weight.dr"};

TEST_P(PassingExamplesParsingSuite, Examples) {
  auto path = GetParam().string();

  hyde::DisplayManager display_mgr;
  hyde::ErrorLog err_log(display_mgr);
  hyde::Parser parser(display_mgr, err_log);
  hyde::DisplayConfiguration display_cfg = {path, 2, true};

  auto mmod = parser.ParsePath(path, display_cfg);
  EXPECT_TRUE(mmod.has_value());
  EXPECT_TRUE(err_log.IsEmpty()) << "Parsing failed:" << std::endl << err_log;

  // CodeGen for message schemas
  for (auto module : hyde::ParsedModuleIterator(*mmod)) {
    (void) hyde::GenerateAvroMessageSchemas(display_mgr, module, err_log);
    EXPECT_TRUE(err_log.IsEmpty())
        << "Message schema generation failed:" << std::endl
        << err_log;
  }

  // Build
  if (auto query_opt = hyde::Query::Build(*mmod, err_log)) {
    std::optional<class hyde::Program> program_opt;

    // Some tests fail to build
    auto allow_failure = false;
    if (gBuildFailExamples.count(fs::path(path).filename().string())) {
      EXPECT_DEBUG_DEATH(
          program_opt = hyde::Program::Build(*query_opt, err_log), ".*TODO.*");

      // Allow to fail/skip next steps if it fails to build (catches fails in release)
      allow_failure = true;
    } else {
      program_opt = hyde::Program::Build(*query_opt, err_log);
    }

    // Should still produce _some_ IR even during debug death
    ASSERT_TRUE(program_opt);
    if (program_opt) {
      auto generated_file_base = std::string(kGeneratedFilesDir) + "/" +
                                 fs::path(path).filename().stem().string();

      // Save the IR
      {
        auto ir_out =
            hyde::FileStream(display_mgr, generated_file_base + ".ir");
        ir_out.os << *program_opt;
      }

      // CodeGen for Python
      auto py_out_path = generated_file_base + ".py";
      {
        hyde::FileStream py_out_fs = hyde::FileStream(display_mgr, py_out_path);
        hyde::GeneratePythonCode(*program_opt, py_out_fs.os);
      }
#ifdef MYPY_PATH

      // mypy can take input from a command line string via '-c STRING'
      // but that sounds unsafe to do from here
      auto ret_code = std::system(
          std::string(std::string(MYPY_PATH) + " " + py_out_path).c_str());

      if (ret_code != 0 && !allow_failure) {
        FAIL() << "Python mypy type-checking failed! Saved generated code at "
               << py_out_path << "\n";
      }
#else

      // Prevent unused variable errors
      (void) allow_failure;
#endif

      SUCCEED();
    }
  }
}

INSTANTIATE_TEST_SUITE_P(ValidExampleParsing, PassingExamplesParsingSuite,
                         testing::ValuesIn(DrFilesInDir(kExamplesDir)));

class FailingExamplesParsingSuite : public testing::TestWithParam<fs::path> {};

// Make sure that we fail to parse each of the .dr files in the invalid_examples
// directory with an error
TEST_P(FailingExamplesParsingSuite, Examples) {
  auto path = GetParam().string();

  hyde::DisplayManager display_mgr;
  hyde::ErrorLog err_log(display_mgr);
  hyde::Parser parser(display_mgr, err_log);
  hyde::DisplayConfiguration display_cfg = {path, 2, true};

  // Parsing is expected to fail for the invalid examples.
  auto mmod = parser.ParsePath(path, display_cfg);
  EXPECT_FALSE(mmod.has_value());
  EXPECT_FALSE(err_log.IsEmpty());
}

INSTANTIATE_TEST_SUITE_P(
    InvalidSyntaxParsing, FailingExamplesParsingSuite,
    testing::ValuesIn(DrFilesInDir(kInvalidSyntaxExamplesDir)));
