// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Query.h>
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
#include <random>
#include <sstream>

namespace {

// Used to keep track of some coarse fuzzer statistics and print them at
// shutdown.
//
// FIXME: the stats should also be printed even upon abnormal shutdown, like if `abort()` is called
struct FuzzerStats
{
  uint64_t num_attempts{0};
  uint64_t num_parsed{0};
  uint64_t num_compiled{0};
  uint64_t num_generated_python{0};

  uint64_t num_custom_calls{0};
  uint64_t num_custom_fallbacks{0};
  uint64_t num_custom_generated_asts{0};
  uint64_t num_custom_parsed_asts{0};

  ~FuzzerStats() {
    assert(num_attempts >= num_parsed);
    assert(num_parsed >= num_compiled);
    assert(num_custom_calls == num_custom_generated_asts + num_custom_parsed_asts + num_custom_fallbacks);

    // Figure out how wide to make the numeric column in the first section
    int col_width = 1;
    for (uint64_t v = num_custom_calls; v > 0; v /= 10, col_width += 1) {}
    auto set_width = std::setw(col_width);
    std::cerr << "### Final fuzzer statistics ###" << std::endl
              << std::endl
              << "Custom mutator:" << std::endl
              << "    Total calls:          " << set_width << num_custom_calls << std::endl
              << "    Fallbacks to default: " << set_width << num_custom_fallbacks << std::endl
              << "    Parsed ASTs:          " << set_width << num_custom_parsed_asts << std::endl
              << "    Generated ASTs:       " << set_width << num_custom_generated_asts << std::endl
              << std::endl;

    // Figure out how wide to make the numeric column in the second section
    col_width = 1;
    for (uint64_t v = num_attempts; v > 0; v /= 10, col_width += 1) {}
    set_width = std::setw(col_width);

    const auto print_funnel_stats = [set_width](std::string_view name, uint64_t passed, uint64_t total) {
      double percent = (double)passed / (double)total * 100.0;
      std::cerr << name << set_width << passed << "/" << set_width << total << " (" << std::setprecision(4) << percent << "%)" << std::endl;
    };

    std::cerr << "Fuzz target:" << std::endl;
    print_funnel_stats("    Successful parses:   ", num_parsed, num_attempts);
    print_funnel_stats("    Successful compiles: ", num_compiled, num_parsed);
  }
};

static FuzzerStats gStats;

// Pretty-print a parsed module as a string.
static std::string ParsedModuleToString(const hyde::ParsedModule &module) {
  std::stringstream stream;
  hyde::DisplayManager display_manager;
  hyde::OutputStream os(display_manager, stream);
  return stream.str();
}

// Emit Python code from a compiled program to a string.
static std::string ProgramToPython(const hyde::Program &program) {
  std::stringstream stream;
  hyde::DisplayManager display_manager;
  hyde::OutputStream os(display_manager, stream);
  hyde::GeneratePythonCode(program, os);
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

static hyde::ParsedModule generate_ast(std::mt19937_64 &gen) {
  // FIXME: do something more interesting here
  std::string input;
  const auto ret = ParseModule(input, "dummy_ast");
  assert(ret && "failed to generate dummy AST");
  return *ret;
}

}  // namespace




extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  gStats.num_attempts += 1;

  static_assert(sizeof(uint8_t) == sizeof(char));
  static_assert(alignof(uint8_t) == alignof(char));
  std::string_view input(reinterpret_cast<const char *>(Data), Size);

  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log(display_manager);

  // A helper function to check that the error log is empty.
  const auto assert_error_log_empty = [&error_log](std::string_view what) {
    if (!error_log.IsEmpty()) {
      std::cerr << "Error: error log is non-empty after " << what << ":" << std::endl;
      error_log.Render(std::cerr);
      abort();
    }
  };

  // A helper function to check that the error log is _not_ empty.
  const auto assert_error_log_nonempty = [&error_log](std::string_view what) {
    if (error_log.IsEmpty()) {
      std::cerr << "Error: error log is empty after " << what << ":" << std::endl;
      abort();
    }
  };

  // First, parse the input.
  //
  // So long as a starting corpus of parseable inputs is used when fuzzing, we
  // expect parsing to succeed nearly all the time.
  hyde::Parser parser(display_manager, error_log);
  hyde::DisplayConfiguration config = {
      "harness_module",  // `name`.
      2,                 // `num_spaces_in_tab`.
      true,              // `use_tab_stops`.
  };

  const auto module_opt = parser.ParseBuffer(input, config);
  if (!module_opt) {
    // Bail out early if no parse.  Expected to be rare!
    assert_error_log_nonempty("unsuccessful parsing");
    return 0;
  }
  assert_error_log_empty("successful parsing");

  gStats.num_parsed += 1;

  // Second, compile the module into a query, and then into a program.
  //
  // As with parsing, so long as a starting corpus of compileable inputs is
  // used when fuzzing, we expect compilation to succeed nearly all the time.
  const auto query_opt = hyde::Query::Build(*module_opt, error_log);
  if (!query_opt) {
    // Bail out early if query compilation failed.  Expected to be rare!
    assert_error_log_nonempty("unsuccessful query compilation");
    return 0;
  }
  assert_error_log_empty("successful query compilation");

  const auto program_opt = hyde::Program::Build(*query_opt, error_log);
  if (!program_opt) {
    // Bail out early if program compilation failed.  Expected to be rare!
    assert_error_log_nonempty("unsuccessful program compilation");
    return 0;
  }
  assert_error_log_empty("successful program compilation");

  gStats.num_compiled += 1;

  // Third, generate Python code from the program.
  auto python_output = ProgramToPython(*program_opt);
  gStats.num_generated_python += 1;

  return 0;
}





// Forward-declare the libFuzzer's mutator callback.
extern "C" size_t
LLVMFuzzerMutate(uint8_t *Data, size_t Size, size_t MaxSize);



// The custom mutator does the following:
//
//   1. Parse the data into an AST. If parsing fails, generate a dummy AST.
//
//   2. Apply transformations to the AST, controlled by a deterministic random
//      generator initialized with the given seed.
//
//   3. Pretty-print the transformed AST into the given buffer.
extern "C" size_t LLVMFuzzerCustomMutator(uint8_t *Data, size_t Size,
                                          size_t MaxSize, unsigned int Seed) {

  assert(Size <= MaxSize);
  // std::cerr << "!!! LLVMFuzzerCustomMutator Data: " << (void *)Data << " Size: " << Size << " MaxSize: " << MaxSize << " Seed: " << Seed << std::endl;
  gStats.num_custom_calls += 1;

  // We use the given seed for deterministic random number generation, when we
  // need to make random choices here.
  std::mt19937_64 gen(Seed);

  // About 2% of the time, fallback to LLVM's default mutator.
  if (std::uniform_int_distribution(1, 100)(gen) <= 2) {
    gStats.num_custom_fallbacks += 1;
    return LLVMFuzzerMutate(Data, Size, MaxSize);
  }

  // Step 1. Parse the given data.
  std::string_view input(reinterpret_cast<char *>(Data), Size);
  const auto module_opt = ParseModule(input, "harness_module");

  auto module = module_opt ? *module_opt : generate_ast(gen);
  if (module_opt) {
    gStats.num_custom_parsed_asts += 1;
  } else {
    gStats.num_custom_generated_asts += 1;
  }

  // Step 2. Transform the AST.
  //
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

  // Step 3. Pretty-print the transformed AST back into `Data`.
  //
  // FIXME: write the output in-place in `Data` without extra copies
  //
  // Note: it is possible that the new input written into `Data` is not
  //       syntactically valid Dr. Lojekyll input.
  //       It's also possible that it's not null-terminated.
  const auto module_string = ParsedModuleToString(module);
  size_t output_len = std::min(module_string.size(), MaxSize);
  std::memcpy(Data, module_string.data(), output_len);
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
