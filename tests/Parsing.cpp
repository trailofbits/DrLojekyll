#include <filesystem>
#include <string>

#include "drlojekyll/Display.h"
#include "drlojekyll/Parse.h"
#include "UnitTests.h"

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
TEST(Parsing, examples) {
  for (const auto &entry : fs::directory_iterator(kExamplesDir)) {
    if (entry.path().extension() != ".dr" || !fs::is_regular_file(entry)) {
      continue;
    }

    SCOPED_TRACE(entry.path());

    hyde::DisplayManager display_mgr;
    hyde::ErrorLog err_log(display_mgr);
    hyde::Parser parser(display_mgr, err_log);
    std::string path(entry.path().string());
    hyde::DisplayConfiguration display_cfg = {path, 2, true};

    auto mmod = parser.ParsePath(path, display_cfg);
    EXPECT_TRUE(mmod.has_value());
    EXPECT_TRUE(err_log.IsEmpty()) << "Parsing failed:" << std::endl << err_log;
  }
}

// Make sure that we fail to parse each of the .dr files in the invalid_examples
// directory with an error
TEST(Parsing, invalid_syntax_examples) {
  for (const auto &entry : fs::directory_iterator(kInvalidSyntaxExamplesDir)) {
    if (entry.path().extension() != ".dr" || !fs::is_regular_file(entry)) {
      continue;
    }

    SCOPED_TRACE(entry.path());

    // Try parsing the file.
    hyde::DisplayManager display_mgr;
    hyde::ErrorLog err_log(display_mgr);
    hyde::Parser parser(display_mgr, err_log);
    std::string path(entry.path().string());
    hyde::DisplayConfiguration display_cfg = {path, 2, true};

    // Parsing is expected to fail for the invalid examples.
    auto mmod = parser.ParsePath(path, display_cfg);
    EXPECT_FALSE(mmod.has_value());
    EXPECT_FALSE(err_log.IsEmpty());
  }
}
