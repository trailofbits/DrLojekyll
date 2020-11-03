// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <filesystem>
#include <string>

#include "UnitTests.h"
#include "drlojekyll/Display/DisplayConfiguration.h"
#include "drlojekyll/Display/DisplayManager.h"
#include "drlojekyll/Parse/ErrorLog.h"
#include "drlojekyll/Parse/Parser.h"

namespace fs = std::filesystem;

namespace hyde {

// Needed for Google Test to be able to print hyde::ErrorLog values
std::ostream &operator<<(std::ostream &os, const ErrorLog &log) {
  log.Render(os);
  return os;
}

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

class PassingExamplesParsingSuite : public testing::TestWithParam<fs::path> {};

TEST_P(PassingExamplesParsingSuite, Examples) {
  auto path = GetParam().string();

  hyde::DisplayManager display_mgr;
  hyde::ErrorLog err_log(display_mgr);
  hyde::Parser parser(display_mgr, err_log);
  hyde::DisplayConfiguration display_cfg = {path, 2, true};

  auto mmod = parser.ParsePath(path, display_cfg);
  EXPECT_TRUE(mmod.has_value());
  EXPECT_TRUE(err_log.IsEmpty()) << "Parsing failed:" << std::endl << err_log;
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
