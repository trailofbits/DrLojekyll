#include <string>
#include <filesystem>

#include "unittests.h"

namespace fs = std::filesystem;

// Make sure that `drlojekyll` can parse each of the .dr files in the examples
// directory without an error
TEST(Parsing, examples) {
  for (const auto &entry : fs::directory_iterator(kExamplesDir)) {
    if (entry.path().extension() != ".dr" || !fs::is_regular_file(entry)) {
      continue;
    }
    // TODO(bjl): do some tests with each of the example files
    // EXPECT_EQ(entry.path(), "hello");
  }
}
