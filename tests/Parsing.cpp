#include <filesystem>
#include <string>

#include "unittests.h"

#include "drlojekyll/Display/DisplayConfiguration.h"
#include "drlojekyll/Display/DisplayManager.h"
#include "drlojekyll/Parse/ErrorLog.h"
#include "drlojekyll/Parse/Parser.h"

namespace fs = std::filesystem;

namespace hyde {
  // Needed for Google Test to be able to print hyde::ErrorLog values
  std::ostream& operator<<(std::ostream& os, const ErrorLog& log) {
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
    hyde::DisplayConfiguration display_cfg = {entry.path(), 2, true};

    auto mmod = parser.ParsePath(entry.path().native(), display_cfg);
    EXPECT_TRUE(mmod.has_value());
    EXPECT_TRUE(err_log.IsEmpty()) << "Parsing failed:" << std::endl << err_log;
  }
}

// Make sure that we fail to parse each of the .dr files in the invalid_examples
// directory with an error
TEST(Parsing, invalid_examples) {
  for (const auto &entry : fs::directory_iterator(kInvalidExamplesDir)) {
    if (entry.path().extension() != ".dr" || !fs::is_regular_file(entry)) {
      continue;
    }

    SCOPED_TRACE(entry.path());

    // Try parsing the file.
    hyde::DisplayManager display_mgr;
    hyde::ErrorLog err_log(display_mgr);
    hyde::Parser parser(display_mgr, err_log);
    hyde::DisplayConfiguration display_cfg = {entry.path(), 2, true};

    auto mmod = parser.ParsePath(entry.path().native(), display_cfg);
    // NOTE: sometimes, parsing successfully gives back a module, but has
    //       errors for other reasons.  So mmod may have a value.
    //       But either we should get no module value, or we should get errors
    //       in the log.
    EXPECT_TRUE(!mmod.has_value() || !err_log.IsEmpty());
  }
}
