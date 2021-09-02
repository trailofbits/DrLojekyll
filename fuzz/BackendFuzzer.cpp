// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>
#include <drlojekyll/Parse/Parser.h>
#include <sys/errno.h>

#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <reproc++/drain.hpp>
#include <reproc++/reproc.hpp>
#include <set>
#include <sstream>
#include <string_view>
#include <tuple>

using namespace std::literals::
    string_view_literals;  // for "foo"sv type string view literals

namespace {

// Used to keep track of some coarse fuzzer statistics and print them at
// shutdown.
//
// FIXME(brad): The stats should also be printed even upon abnormal shutdown,
//              like if `abort()` is called.
struct FuzzerStats {
  uint64_t num_attempts{0};
  uint64_t num_parsed{0};
  uint64_t num_compiled{0};
  uint64_t num_generated_python{0};

  uint64_t num_custom_calls{0};
  uint64_t num_custom_fallbacks{0};
  uint64_t num_custom_generated_asts{0};
  uint64_t num_custom_parsed_asts{0};

  ~FuzzerStats(void) {
    PrintStats();
  }

  void PrintStats(void) {
    assert(num_attempts >= num_parsed);
    assert(num_parsed >= num_compiled);
    assert(num_custom_calls == (num_custom_generated_asts +
                                num_custom_parsed_asts + num_custom_fallbacks));

    // Figure out how wide to make the numeric column in the first section
    int col_width = 1;
    for (uint64_t v = num_custom_calls; v > 0; v /= 10, col_width += 1) {
    }
    auto set_width = std::setw(col_width);
    std::cerr << std::endl
              << "### Final fuzzer statistics ###" << std::endl
              << std::endl
              << "Custom mutator:" << std::endl
              << "    Total calls:          " << set_width << num_custom_calls
              << std::endl
              << "    Fallbacks to default: " << set_width
              << num_custom_fallbacks << std::endl
              << "    Parsed ASTs:          " << set_width
              << num_custom_parsed_asts << std::endl
              << "    Generated ASTs:       " << set_width
              << num_custom_generated_asts << std::endl
              << std::endl;

    // Figure out how wide to make the numeric column in the second section
    col_width = 1;
    for (uint64_t v = num_attempts; v > 0; v /= 10, col_width += 1) {
    }
    set_width = std::setw(col_width);

    const auto print_funnel_stats =
        [set_width](std::string_view name, uint64_t passed, uint64_t total) {
          double percent = (double) passed / (double) total * 100.0;
          std::cerr << name << set_width << passed << "/" << set_width << total
                    << " (" << std::setprecision(4) << percent << "%)"
                    << std::endl;
        };

    std::cerr << "Fuzz target:" << std::endl;
    print_funnel_stats("    Successful parses:   ", num_parsed, num_attempts);
    print_funnel_stats("    Successful compiles: ", num_compiled, num_parsed);
  }
};

// A `DrContext` packages up the several objects that cooperate to parse Dr.
// Lojekyll input.
//
// Why does this exist?  Many Dr. Lojekyll APIs, such as Python codegen,
// require some these parameters, and they must all come from the same
// cooperating group, or else you will see baffling results.
struct DrContext {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log;
  hyde::Parser parser;

  DrContext(void)
      : display_manager(),
        error_log(display_manager),
        parser(display_manager, error_log) {}
};

//----------------------------------------------------------------------
// Global variables and fuzzer configuration
//----------------------------------------------------------------------
static FuzzerStats gStats = {};

// Should the fuzzer enable semantics-altering IR mutations?
static bool gAllowSemanticsModifyingMutations = false;

// Should the fuzzer execute each successfully generated Python program?
static bool gExecuteGeneratedPython = true;

//----------------------------------------------------------------------
// Utilities
//----------------------------------------------------------------------

// Emit Python code from a compiled program to a string.
static std::string ProgramToPython(DrContext &cxt,
                                   const hyde::Program &program) {
  assert(cxt.error_log.IsEmpty());
  std::stringstream stream;
  hyde::OutputStream os(cxt.display_manager, stream);
  hyde::python::GenerateDatabaseCode(program, os);
  return stream.str();
}

static std::optional<hyde::ParsedModule>
ParseModule(DrContext &cxt, std::string_view input, const std::string &name) {
  assert(cxt.error_log.IsEmpty());
  hyde::DisplayConfiguration config = {
      name,  // `name`.
      2,  // `num_spaces_in_tab`.
      true,  // `use_tab_stops`.
  };
  return cxt.parser.ParseBuffer(input, config);
}

// Generate a Dr. Lojekyll `ParsedModule` from the given random generator.
// This is referentially transparent: given the same input argument, produces
// the same output.
static hyde::ParsedModule GenerateAST(DrContext &cxt, std::mt19937_64 &gen) {
  assert(cxt.error_log.IsEmpty());

  // FIXME: do something more interesting here than return an empty module
  std::string input = "";
  const auto ret = ParseModule(cxt, input, "dummy_ast");
  assert(ret && "failed to generate dummy AST");
  return *ret;
}

// Execute the given Python script, checking that its exit code is zero.
static void PythonSelfTest(const std::string &gen_python) {
  const reproc::milliseconds timeout(3000);
  reproc::options options;
  options.redirect.parent = false;
  options.redirect.in.type = reproc::redirect::pipe;
  options.redirect.out.type = reproc::redirect::pipe;
  options.redirect.err.type = reproc::redirect::stdout_;
  options.deadline = timeout;

  std::error_code ec;
  const auto assert_ec_ok = [&ec](std::string_view what) {
    if (ec) {
      std::cerr << "Error " << what << ": " << ec.message() << std::endl;
      abort();
    }
  };

  // FIXME: plumb the path to the Python binary through to here
  const std::array cmd{"python"sv};
  reproc::process proc;
  ec = proc.start(cmd, options);
  assert_ec_ok("starting Python process");

  size_t written;
  std::tie(written, ec) = proc.write(
      reinterpret_cast<const uint8_t *>(gen_python.data()), gen_python.size());
  assert_ec_ok("writing Python process stdin");
  if (written != gen_python.size()) {
    std::cerr << "Error writing Python process stdin: tried to write "
              << gen_python.size() << " bytes, but only wrote " << written
              << std::endl;
    abort();
  }
  ec = proc.close(reproc::stream::in);
  assert_ec_ok("closing Python process stdin");

  std::string output;

  // The `reproc++` library doesn't gracefully handle interrupted system calls
  // in its implementation.  So we have to explicitly loop around that here.
  // <Sigh>
  do {
    ec = reproc::drain(proc, reproc::sink::string(output), reproc::sink::null);
  } while (ec.value() == EINTR);
  assert_ec_ok("collecting Python process output");

  int status;
  std::tie(status, ec) = proc.wait(timeout);
  assert_ec_ok("waiting for Python process");

  if (status != 0) {
    std::cerr << "Error: generated Python code exited with code " << status
              << ":" << std::endl
              << output << std::endl;
    abort();
  }
}

// Used to shuffle an array in-place.
//
// Copied from cppreference, where it is listed as one possible implementation
// of `std::shuffle`.  The only change is that here, the random generator is
// passed by reference, instead of by rvalue reference, so that we can continue
// to use it later.
//
// It seems like a mistake in the standard library that `std::shuffle` takes
// the random generator by rvalue reference!
template <class RandomIt, class URBG>
static void Shuffle(RandomIt first, RandomIt last, URBG &g) {
  typedef typename std::iterator_traits<RandomIt>::difference_type diff_t;
  typedef std::uniform_int_distribution<diff_t> distr_t;
  typedef typename distr_t::param_type param_t;

  distr_t D;
  diff_t n = last - first;
  for (diff_t i = n - 1; i > 0; --i) {
    std::swap(first[i], first[D(g, param_t(0, i))]);
  }
}

template <class T, class URBG>
static std::vector<T> Shuffled(hyde::NodeRange<T> c, URBG &g) {

  // Note: we should preallocate the vector for the appropriate size, but
  // NodeRange doesn't implement everything needed to make `std::distance` or
  // similar work.
  std::vector<T> v;
  for (auto t : c) {
    v.push_back(t);
  }
  shuffle(v.begin(), v.end(), g);
  return v;
}

static void ShuffleClause(DrContext &cxt, hyde::OutputStream &os,
                          hyde::ParsedClause clause, std::mt19937_64 &gen);

// Mutates `module` by permuting the order of many of its components using `gen`.
// This ought to be a semantics-preserving transformation.
// The mutated module is returned as a pretty-printed string version of the module.
//
// Note: it would be much better if we didn't have to convert the module to a
// string to mutate it.  However, there is currently no public API for
// _constructing_ `ParsedModule` values.
//
// Note: This conversion-to-string code is adapted from lib/Parse/Format.cpp.
static std::string ShuffleModule(DrContext &cxt, hyde::ParsedModule module,
                                 std::mt19937_64 &gen) {
  std::stringstream stream;
  hyde::OutputStream os(cxt.display_manager, stream);

  module = module.RootModule();

  for (auto type : Shuffled(module.ForeignTypes(), gen)) {
    os << type << "\n";
  }

  std::set<hyde::ParsedDeclaration> seen;

  // NOTE(brad): We do _not_ shuffle the submodules, since the order they are
  //             iterated in is designed to respect interdependencies between
  //             them.
  //
  // TODO(pag): Add special support for ordering declarations with `mutable`-
  //            attributed parameters. These induce a partial order that must
  //            be satisfied, where the referenced merge functor must be
  //            declared prior to the mutable use.
  for (auto sub_module : hyde::ParsedModuleIterator(module)) {

    // We emit all the components of the submodule as individual strings into a
    // vector that we finally shuffle once at the end.  This will result in a
    // greater degree of shuffling than shuffling and emitting each
    // subcomponent type sequentially.
    std::vector<std::string> strings;
    auto AddStringForDecl = [&cxt, &seen,
                             &strings](hyde::ParsedDeclaration decl) {
      if (seen.count(decl)) {
        return;
      }
      seen.insert(decl);
      std::stringstream stream;
      hyde::OutputStream out(cxt.display_manager, stream);
      out << decl;
      strings.push_back(stream.str());
    };

    for (auto decl : sub_module.Queries()) {
      for (auto redecl : decl.Redeclarations()) {
        AddStringForDecl(redecl);
      }
    }

    for (auto decl : sub_module.Messages()) {
      AddStringForDecl(decl);
    }

    for (auto decl : sub_module.Functors()) {
      for (auto redecl : decl.Redeclarations()) {
        AddStringForDecl(redecl);
      }
    }

    for (auto decl : sub_module.Exports()) {
      if (decl.Arity()) {
        AddStringForDecl(decl);
      }
    }

    for (auto decl : sub_module.Locals()) {
      AddStringForDecl(decl);
    }

    Shuffle(strings.begin(), strings.end(), gen);

    for (auto str : strings) {
      os << str << "\n";
    }
  }

  // Note: not shuffling submodules, again like before
  for (auto sub_module : hyde::ParsedModuleIterator(module)) {
    for (auto code : sub_module.Inlines()) {
      if (code.IsPrologue()) {
        os << code << "\n";
      }
    }

    std::vector<hyde::ParsedClause> all_clauses;
    for (auto clause : sub_module.Clauses()) {
      all_clauses.push_back(clause);
    }
    Shuffle(all_clauses.begin(), all_clauses.end(), gen);
    for (auto clause : all_clauses) {
      ShuffleClause(cxt, os, clause, gen);
    }

    // We also do _not_ shuffle inline code snippets, as there are likely
    // ordering constraints among those.
    for (auto code : sub_module.Inlines()) {
      if (!code.IsPrologue()) {
        os << code << "\n";
      }
    }
  }

  return stream.str();
}

// Print `clause` to `os`, but shuffling the components using the pseudorandom
// generator `gen`.
//
// This is cribbed from lib/Parse/Format.cpp.
static void ShuffleClause(DrContext &cxt, hyde::OutputStream &os,
                          hyde::ParsedClause clause, std::mt19937_64 &gen) {
  os << hyde::ParsedClauseHead(clause);
  if (clause.IsHighlighted()) {
    os << " @highlight";
  }
  os << " : ";

  // We emit all the components as individual strings into a vector that we
  // finally shuffle once at the end.  This will result in a greater degree of
  // shuffling than handling each subcomponent type individually.
  std::vector<std::string> strings;
  auto AddString = [&cxt, &strings](auto val) {
    std::stringstream stream;
    hyde::OutputStream out(cxt.display_manager, stream);
    out << val;
    strings.push_back(stream.str());
  };

  for (auto assign : clause.Assignments()) {
    AddString(assign);
  }

  for (auto compare : clause.Comparisons()) {
    AddString(compare);
  }

  for (auto pred : clause.PositivePredicates()) {
    AddString(pred);
  }

  for (auto pred : clause.NegatedPredicates()) {
    AddString(pred);
  }

  for (auto agg : clause.Aggregates()) {
    AddString(agg);
  }

  Shuffle(strings.begin(), strings.end(), gen);

  auto delim = "";
  for (auto str : strings) {
    os << delim << str;
    delim = ", ";
  }

  os << ".\n";
}

}  // namespace


extern "C" int LLVMFuzzerInitialize(int *argc, char ***argv) {
  char **args = *argv;
  for (int i = 1, iMax = *argc; i < iMax; i += 1) {
    const char *arg = args[i];
    if (strcmp("--enable-all-mutators", arg) == 0) {
      gAllowSemanticsModifyingMutations = true;
    } else if (strcmp("--no-execute-generated-python", arg) == 0) {
      gExecuteGeneratedPython = false;

    // if only we were using C++20, we could use string_view.starts_with...
    } else if (strstr(arg, "--") == arg && strcmp(arg, "--") != 0) {
      std::cerr
          << "Error: unknown custom fuzzer argument `" << arg << "`"
          << std::endl
          << std::endl
          << "Available custom fuzzer arguments:" << std::endl
          << "    --enable-all-mutators               enable all mutators, including semantics-altering ones"
          << std::endl
          << "    --no-execute-generated-python       do not execute the generated Python code"
          << std::endl;
      exit(1);
    }
  }

  const char *prog = args[0];
  std::cerr << prog << ": using semantics-altering mutators: "
            << gAllowSemanticsModifyingMutations << std::endl;
  std::cerr << prog << ": executing generated Python code:   "
            << gExecuteGeneratedPython << std::endl;

  return 0;
}


extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  gStats.num_attempts += 1;

  DrContext cxt;

  static_assert(sizeof(uint8_t) == sizeof(char));
  static_assert(alignof(uint8_t) == alignof(char));
  std::string_view input(reinterpret_cast<const char *>(Data), Size);

  // A helper function to check that the error log is empty.
  const auto assert_error_log_empty = [&cxt](std::string_view what) {
    if (!cxt.error_log.IsEmpty()) {
      std::cerr << "Error: error log is non-empty after " << what << ":"
                << std::endl;
      cxt.error_log.Render(std::cerr);
      abort();
    }
  };

  // A helper function to check that the error log is _not_ empty.
  const auto assert_error_log_nonempty = [&cxt](std::string_view what) {
    if (cxt.error_log.IsEmpty()) {
      std::cerr << "Error: error log is empty after " << what << ":"
                << std::endl;
      abort();
    }
  };

  // First, parse the input.
  //
  // So long as a starting corpus of parseable inputs is used when fuzzing, we
  // expect parsing to succeed nearly all the time.
  hyde::DisplayConfiguration config = {
      "harness_module",  // `name`.
      2,  // `num_spaces_in_tab`.
      true,  // `use_tab_stops`.
  };

  const auto module_opt = cxt.parser.ParseBuffer(input, config);
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
  const auto query_opt = hyde::Query::Build(*module_opt, cxt.error_log);
  if (!query_opt) {

    // Bail out early if query compilation failed.  Expected to be rare!
    assert_error_log_nonempty("unsuccessful query compilation");
    return 0;
  }
  assert_error_log_empty("successful query compilation");

  const auto program_opt = hyde::Program::Build(*query_opt);
  if (!program_opt) {

    // Bail out early if program compilation failed.  Expected to be rare!
    assert_error_log_nonempty("unsuccessful program compilation");
    return 0;
  }
  assert_error_log_empty("successful program compilation");

  gStats.num_compiled += 1;

  // Third, generate Python code from the program.
  std::string gen_python = ProgramToPython(cxt, *program_opt);
  std::string gen_python_dup = ProgramToPython(cxt, *program_opt);
  if (gen_python != gen_python_dup) {
    std::cerr
        << "Error: Python code generation multiple times comes out different:"
        << std::endl
        << std::endl
        << "<<<Version 1>>>" << gen_python << std::endl
        << "<<<Version 2>>>" << gen_python_dup << std::endl;
    abort();
  }
  gStats.num_generated_python += 1;

  // FIXME: also, optionally run mypy on the generated Python code

  // Fourth, run the generated Python program.
  //
  // This assumes that the generated program is self-testing -- for example,
  // including a handwritten Python test suite in an `#epilogue` section that
  // executes when directly running the Python module.
  //
  // We probably only run the generated Python program when we are not fuzzing
  // with semantics-modifying mutations.  Otherwise, the fuzzer could break the
  // program's self-tests.  However, control over whether or not to execute the
  // generated Python program is controlled by a separate option from whether
  // semantics-modifying mutations are used.
  if (gExecuteGeneratedPython) {
    PythonSelfTest(gen_python);
  }

  return 0;
}


// Forward-declare the libFuzzer's mutator callback; it is explicitly called
// sometimes within LLVMFuzzerCustomMutator.
extern "C" size_t LLVMFuzzerMutate(uint8_t *Data, size_t Size, size_t MaxSize);


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
  gStats.num_custom_calls += 1;

  // We use the given seed for deterministic random number generation, when we
  // need to make random choices here.
  std::mt19937_64 gen(Seed);

  if (gAllowSemanticsModifyingMutations) {

    // About 1% of the time, fallback to LLVM's default mutator.
    if (std::uniform_int_distribution(1, 100)(gen) <= 1) {
      gStats.num_custom_fallbacks += 1;
      return LLVMFuzzerMutate(Data, Size, MaxSize);
    }
  }

  // Note: We use a unique pointer here for the `DrContext` so that we can
  // effectively re-initialize the object.  The various member variables don't
  // have copy constructors, assignment operators, or the rvalue equivalents,
  // nor do they have methods to re-initialize them.
  //
  // FIXME: Simplify this once the `DrContext` member variables can be re-initialized
  std::unique_ptr<DrContext> cxt(new DrContext);

  // Step 1. Parse the given data into an AST.
  std::string_view input(reinterpret_cast<char *>(Data), Size);
  const auto module_opt = ParseModule(*cxt, input, "harness_module");

  if (!module_opt) {

    // Parsing failed; re-initialize the context so that AST generation doesn't
    // fail.
    cxt.reset(new DrContext);
  }

  auto module = module_opt ? *module_opt : GenerateAST(*cxt, gen);
  if (module_opt) {
    gStats.num_custom_parsed_asts += 1;
  } else {
    gStats.num_custom_generated_asts += 1;
  }

  // Step 2. Transform the AST.
  //
  // Ideas:
  //
  //   - consistently rename identifiers
  //   - consistently change parameter types
  //   - reorder rules
  //   - reordering functors
  //   - consistently reordering parameters
  //   - duplicate rules
  //   - add a true subterm to an existing rule
  //   - duplicate a rule and add a false subterm to it
  //   - weaken an existing rule (i.e., delete subterms), and somehow rephrase
  //     the deleted subterms

  const auto module_string = ShuffleModule(*cxt, module, gen);

  // Step 3. Write the mutated AST back into `Data`.
  //
  // FIXME: write the output in-place in `Data` without making extra copies
  //
  // Note: it is possible that the new input written into `Data` is not
  //       syntactically valid Dr. Lojekyll input.
  //       It's also possible that it's not null-terminated.
  //       However, if the starting corpus comprises only valid Dr. Lojekyll
  //       inputs, these should be rare occurrences!
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
