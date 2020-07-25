// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parser.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>

static std::string ParsedModuleToString(const hyde::ParsedModule &module) {
  std::stringstream stream;
  hyde::DisplayManager display_manager;
  hyde::OutputStream os(display_manager, stream);  // KeepImports?
  return stream.str();
}

static void ParseAndVerify(std::string_view data) {

  // First, parse the given data.
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log(display_manager);
  hyde::Parser parser(display_manager, error_log);
  hyde::DisplayConfiguration config = {
      "harness_module",  //  `name`.
      2,  //  `num_spaces_in_tab`.
      true,  //  `use_tab_stops`.
  };
  auto opt_module = parser.ParseBuffer(data, config);
  if (!opt_module) {

    // bail out early if no parse
    // error_log.Render(std::cerr);
    return;
  }
  hyde::ParsedModule module = *opt_module;

  // Now, pretty-print the parsed module back to a string.
  const auto module_string = ParsedModuleToString(module);

  // Now, re-parse the pretty-printed string.
  hyde::DisplayManager v_display_manager;
  hyde::ErrorLog v_error_log(v_display_manager);
  hyde::Parser v_parser(v_display_manager, v_error_log);
  hyde::DisplayConfiguration v_config = {
      "verified_harness_module",  //  `name`.
      2,  //  `num_spaces_in_tab`.
      true,  //  `use_tab_stops`.
  };
  auto v_opt_module = v_parser.ParseBuffer(data, v_config);

  // Finally, make sure the parsed result is equal to what we first parsed (the
  // "round-trip" property).
  if (!v_opt_module) {
    std::cerr << "Failed to re-parse module:\n";
    v_error_log.Render(std::cerr);
    abort();
  }
  hyde::ParsedModule v_module = *v_opt_module;
  if (!v_error_log.IsEmpty()) {
    std::cerr << "Error log is non-empty after reparsing:\n";
    v_error_log.Render(std::cerr);
    abort();
  }
  const auto v_module_string = ParsedModuleToString(v_module);
  if (module_string != v_module_string) {
    std::cerr << "Re-parsed module is not equal to original module:\n"
              << "Original module:\n"
              << "----------------------\n"
              << module_string << "----------------------\n"
              << "\n"
              << "Re-parsed module:\n"
              << "----------------------\n"
              << v_module_string << "----------------------\n";
    abort();
  }
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  std::string_view data(reinterpret_cast<const char *>(Data), Size);
  ParseAndVerify(data);
  return 0;
}