// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <stdint.h>
#include <sstream>
#include <cassert>

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Parse/Format.h>


void ParseAndVerify(std::string_view data) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log;
  hyde::Parser parser(display_manager, error_log);
  const std::string target_name = "harness_module";
  hyde::DisplayConfiguration config = {
          target_name, //  `name`.
          2,  //  `num_spaces_in_tab`.
          true, //  `use_tab_stops`.
  };
  auto module = parser.ParseBuffer(data, config);

  if (error_log.IsEmpty()) {
    std::stringstream format_stream;
    std::stringstream verify_stream;
    hyde::OutputStream os(display_manager, format_stream);
    hyde::FormatModule(os, module);
    const auto format_stream_string = format_stream.str();
    auto module2 = parser.ParseBuffer(format_stream_string, config);

    hyde::OutputStream os2(display_manager, verify_stream);
    hyde::FormatModule(os2, module2);

    assert(error_log.IsEmpty());
    assert(verify_stream.str() == format_stream_string);
  }
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  std::string_view data(reinterpret_cast<const char *>(Data), Size);
  ParseAndVerify(data);
  return 0;
}
