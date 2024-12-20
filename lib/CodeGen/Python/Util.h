// Copyright 2021, Trail of Bits. All rights reserved.

#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#include <unordered_set>

namespace hyde {
namespace python {

static constexpr auto kStateAbsent = 0u;
static constexpr auto kStatePresent = 1u;
static constexpr auto kStateUnknown = 2u;

// NOTE(pag): We store an extra bit besides present/absent/unknown
//            to track whether or not the data had ever been in our
//            index before, and thus doesn't need to be re-added.
static constexpr auto kStateMask = 0x3u;
static constexpr auto kPresentBit = 0x4u;

// NOTE(ekilmer): Classes are named all the same for now.
static constexpr auto gClassName = "Database";

// Make a comment in code for debugging purposes
OutputStream &Comment(OutputStream &os, ProgramRegion region,
                      const char *message);

OutputStream &Procedure(OutputStream &os, ProgramProcedure proc);

template <typename Stream>
static Stream &Var(Stream &os, const DataVariable var) {
  switch (var.DefiningRole()) {
    case VariableRole::kConstantZero: os << "0"; break;
    case VariableRole::kConstantOne: os << "1"; break;
    case VariableRole::kConstantFalse: os << "False"; break;
    case VariableRole::kConstantTrue: os << "True"; break;
    default:
      if (var.IsGlobal()) {
        os << "self.";
      }
      os << "var_" << var.Id();
      break;
  }
  return os;
}

// Python representation of TypeKind
const std::string_view TypeName(ParsedForeignType type);

// Python representation of TypeKind
std::string_view TypeName(ParsedModule module, TypeLoc kind);

const char *OperatorString(ComparisonOperator op);

std::string TypeValueOrDefault(ParsedModule module, TypeLoc loc,
                               DataVariable var);

}  // namespace python
}  // namespace hyde
