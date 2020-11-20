// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_set>

#include "UnitTests.h"
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
    auto program_build_lambda = [](auto q, auto log) {
      return hyde::Program::Build(q, log);
    };
    std::optional<class hyde::Program> program_opt;

    // Some tests fail this step
    if (gBuildFailExamples.count(fs::path(path).filename().string())) {
      ASSERT_DEBUG_DEATH(
          program_opt = program_build_lambda(*query_opt, err_log), ".*TODO.*");
    } else {
      program_opt = program_build_lambda(*query_opt, err_log);
    }

    if (program_opt) {

      // Save the IR
      {
        auto ir_file = std::string(kGeneratedFilesDir) + "/" +
                       fs::path(path).filename().stem().string() + ".ir";
        auto ir_out = hyde::FileStream(display_mgr, ir_file);
        ir_out.os << *program_opt;
      }

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
