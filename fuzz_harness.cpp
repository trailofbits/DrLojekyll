// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <stdint.h>
#include <sstream>
#include <cassert>
#include <iostream>

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
    config.name = "verified_harness_module";
    hyde::DisplayManager v_display_manager;
    hyde::ErrorLog v_error_log;
    hyde::Parser v_parser(v_display_manager, v_error_log);
    std::stringstream format_stream;
    std::stringstream verify_stream;
    hyde::OutputStream os(display_manager, format_stream);
    os << module;
    const auto format_stream_string = format_stream.str();
    std::cerr << format_stream_string;
    auto module2 = v_parser.ParseBuffer(format_stream_string, config);

    hyde::OutputStream os2(v_display_manager, verify_stream);
    os2 << module2;

    v_error_log.Render(std::cerr);
    assert(v_error_log.IsEmpty());
    assert(verify_stream.str() == format_stream_string);
  }
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  std::string_view data(reinterpret_cast<const char *>(Data), Size);
  ParseAndVerify(data);
  return 0;
}
