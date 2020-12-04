// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parser.h>

#include <cassert>
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

  uint64_t num_custom_calls{0};
  uint64_t num_custom_generated_asts{0};
  uint64_t num_custom_parsed_asts{0};

  ~FuzzerStats() {
    auto num_total = num_successful_parses + num_failed_parses;
    double success_percent = (double)num_successful_parses / (double)num_total * 100.0;
    double failed_percent = (double)num_failed_parses / (double)num_total * 100.0;

    std::cerr << "### Final fuzzer statistics ###" << std::endl
              << std::endl
              << "Custom mutator:" << std::endl
              << "    Total calls:       " << std::setw(12) << num_custom_calls << std::endl
              << "    Parsed ASTs:       " << std::setw(12) << num_custom_parsed_asts << std::endl
              << "    Generated ASTs:    " << std::setw(12) << num_custom_generated_asts << std::endl
              << std::endl
              << "Fuzz target:" << std::endl
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

static std::optional<hyde::ParsedModule> ParseModule(std::string_view input, const std::string &name) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log(display_manager);
  hyde::Parser parser(display_manager, error_log);
  hyde::DisplayConfiguration config = {
      name,  // `name`.
      2,     // `num_spaces_in_tab`.
      true,  // `use_tab_stops`.
  };
  return parser.ParseBuffer(input, config);
}

static hyde::ParsedModule make_ast(unsigned int Seed) {
  // FIXME: actually use the seed!
  // FIXME: do something more interesting here
  std::string input;
  const auto ret = ParseModule(input, "dummy_ast");
  assert(ret && "failed to create dummy AST");
  return *ret;
}

}  // namespace




extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  std::string_view input(reinterpret_cast<const char *>(Data), Size);
  const auto opt_module = ParseModule(input, "harness_module");
  if (!opt_module) {
    gStats.num_failed_parses += 1;
    // Bail out early if no parse.
    // Note: this is expected to be rare for this fuzzer!
    return 0;
  }
  gStats.num_successful_parses += 1;

  return 0;
}





// Forward-declare the libFuzzer's mutator callback.
// FIXME: we only need this if we are going to call the default LibFuzzer mutator ourselves.
extern "C" size_t
LLVMFuzzerMutate(uint8_t *Data, size_t Size, size_t MaxSize);


// The custom mutator does the following:
//
//   1. Parse the data into an AST. If parsing fails, use a dummy AST.
//
//   2. Apply transformations to the AST, controlled by a deterministic random
//      generator initialized with the given seed.
//
//   3. Pretty-print the transformed AST into the given buffer.
extern "C" size_t LLVMFuzzerCustomMutator(uint8_t *Data, size_t Size,
                                          size_t MaxSize, unsigned int Seed) {
  gStats.num_custom_calls += 1;

  // 1. parse the given data.
  std::string_view input(reinterpret_cast<char *>(Data), Size);
  const auto opt_module = ParseModule(input, "harness_module");

  auto module = opt_module ? *opt_module : make_ast(Seed);
  if (opt_module) {
    gStats.num_custom_parsed_asts += 1;
  } else {
    gStats.num_custom_generated_asts += 1;
  }

  // 2. transform the AST
  // FIXME: implement something here, using `Seed` to make it deterministic
  //
  // Ideas:
  //
  //   - consistently rename identifiers
  //   - consistently change parameter types
  //   - reorder rules
  //   - duplicate rules
  //   - add a true clause to an existing rule
  //   - duplicate a rule and add a false clause to it

  // 3. pretty-print the transformed AST
  // FIXME: write the output in-place in `Data` without extra copies
  const auto module_string = ParsedModuleToString(module);
  size_t output_len = std::min(strlen(module_string.c_str()), Size);
  std::memcpy(Data, module_string.c_str(), output_len);
  return output_len;
}





// This isn't well-documented in LibFuzzer, but is an optional API we might
// implement to perform a "combined" mutation of two existing inputs.

#if 0
size_t LLVMFuzzerCustomCrossOver(const uint8_t *Data1,
                                 size_t Size1,
                                 const uint8_t *Data2,
                                 size_t Size2,
                                 uint8_t *Out,
                                 size_t MaxOutSize,
                                 unsigned int Seed) {
}
#endif
