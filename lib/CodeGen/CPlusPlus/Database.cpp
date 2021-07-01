// Copyright 2021, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include <algorithm>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "Util.h"

namespace hyde {
namespace cxx {
namespace {

static std::vector<ParsedInline> Inlines(ParsedModule module) {
  std::vector<ParsedInline> inlines;
  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedInline code : sub_module.Inlines()) {
      switch (code.Language()) {
        case Language::kUnknown:
        case Language::kCxx:
          inlines.push_back(code);
          break;
        default: break;
      }
    }
  }
  return inlines;
}

static std::vector<ParsedFunctor> Functors(ParsedModule module) {
  std::vector<ParsedFunctor> functors;
  std::unordered_set<std::string> seen;
  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedFunctor first_func : sub_module.Functors()) {
      for (ParsedFunctor func : first_func.Redeclarations()) {
        std::stringstream ss;
        ss << func.Id() << ':' << ParsedDeclaration(func).BindingPattern();
        if (auto [it, inserted] = seen.emplace(ss.str()); inserted) {
          functors.push_back(func);
        }
      }
    }
  }
  return functors;
}

}  // namespace

// Emits C++ code for the given program to `os`.
void GenerateDatabaseCode(const Program &program, OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#pragma once\n\n"
     << "#include <algorithm>\n"
     << "#include <cstdlib>\n"
     << "#include <cstdio>\n"
     << "#include <optional>\n"
     << "#include <tuple>\n"
     << "#include <unordered_map>\n"
     << "#include <vector>\n\n"
     << "#include <drlojekyll/Runtime/Runtime.h>\n\n"
     << "#ifndef __DRLOJEKYLL_PROLOGUE_CODE_" << gClassName << "\n"
     << "#  define __DRLOJEKYLL_PROLOGUE_CODE_" << gClassName << "\n";
  const auto module = program.ParsedModule();

  // Output prologue code.
  auto inlines = Inlines(module);
  for (ParsedInline code : inlines) {
    if (code.IsPrologue()) {
      os << code.CodeToInline() << "\n\n";
    }
  }

  os << "#endif  // __DRLOJEKYLL_PROLOGUE_CODE_" << gClassName << "\n\n";

  (void) Functors;

  // Output epilogue code.
  for (ParsedInline code : inlines) {
    if (code.IsEpilogue()) {
      os << code.CodeToInline() << "\n\n";
    }
  }
}

}  // namespace cxx
}  // namespace hyde
