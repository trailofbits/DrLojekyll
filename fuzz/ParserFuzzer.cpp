// Copyright 2020, Trail of Bits, Inc. All rights reserved.

// This fuzz target primarily exercises the Dr. Lojekyll parser.
//
// The inputs are mutated at the bytestring level, based on an corpus of Dr.
// Lojekyll input programs.  As a result, the vast majority of inputs tested
// during a fuzzing run will be syntactically invalid inputs, and Dr. Lojekyll
// will not successfully parse them into an AST.  However, it is expected that
// invalid inputs will be handled gracefully, not causing crashes.
//
// When an input _is_ parsed successfully by this target, a round-trip parsing
// and pretty-printing property is checked.

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parser.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace {

// Used to keep track of some coarse fuzzer statistics and print them at
// shutdown.
struct FuzzerStats
{
  uint64_t num_successful_parses{0};
  uint64_t num_failed_parses{0};

  ~FuzzerStats() {
    auto num_total = num_successful_parses + num_failed_parses;
    double success_percent = (double)num_successful_parses / (double)num_total * 100.0;
    double failed_percent = (double)num_failed_parses / (double)num_total * 100.0;

    std::cerr << "### Final fuzzer statistics ###" << std::endl
              << "    Total attempts:    " << std::setw(12) << num_total << std::endl
              << "    Failed parses:     " << std::setw(12) << num_failed_parses << " (" << std::setprecision(4) << failed_percent << "%)"  << std::endl
              << "    Successful parses: " << std::setw(12) << num_successful_parses << " (" << std::setprecision(4) << success_percent << "%)" << std::endl;
  }
};

static FuzzerStats gStats;

// Pretty-print a parsed module as a string.
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
    gStats.num_failed_parses += 1;
    // bail out early if no parse
    // error_log.Render(std::cerr);
    return;
  }

  gStats.num_successful_parses += 1;
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

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  std::string_view data(reinterpret_cast<const char *>(Data), Size);
  ParseAndVerify(data);
  return 0;
}
