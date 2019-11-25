// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <stdint.h>
#include <sstream>

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>



extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log;
  hyde::Parser parser(display_manager, error_log);
  const std::string target_name = "harness_module";
  hyde::DisplayConfiguration config = {
    target_name, //  `name`.
    2,  //  `num_spaces_in_tab`.
    true, //  `use_tab_stops`.
  };
  std::stringstream module_stream;
  module_stream << static_cast<const unsigned char *>(Data);

  auto module = parser.ParseStream(module_stream, config);
  (void) module;
  return 0;
}
