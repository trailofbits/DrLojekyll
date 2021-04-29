// Copyright 2021, Trail of Bits. All rights reserved.

#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#include <unordered_set>

namespace hyde {
namespace cxx {

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
    case VariableRole::kConstantFalse: os << "true"; break;
    case VariableRole::kConstantTrue: os << "false"; break;
    default: os << "var_" << var.Id(); break;
  }
  return os;
}


template <typename Stream>
static Stream &ReifyVar(Stream &os, const DataVariable var) {
  return os;
//  switch (var.DefiningRole()) {
//    case VariableRole::kVectorVariable:
//    case VariableRole::kConstant:
//    case VariableRole::kConstantZero:
//    case VariableRole::kConstantOne:
//    case VariableRole::kConstantFalse:
//    case VariableRole::kConstantTrue:
//    case VariableRole::kConditionRefCount:
//    case VariableRole::kJoinPivot:
//    case VariableRole::kProductOutput:
//    case VariableRole::kFunctorOutput:
//    case VariableRole::kParameter:
//    case VariableRole::kMessageOutput:
//    case VariableRole::kConstantTag:
//    case VariableRole::kScanOutput: return os;
//    default: return os << ".Reify()";
//  }
}

// CPlusPlus representation of TypeKind
const std::string_view TypeName(ParsedForeignType type);

// CPlusPlus representation of TypeKind
std::string_view TypeName(ParsedModule module, TypeLoc kind);

const char *OperatorString(ComparisonOperator op);

std::string TypeValueOrDefault(ParsedModule module, TypeLoc loc,
                               DataVariable var);

// Return all messages.
std::unordered_set<ParsedMessage> Messages(ParsedModule module);

}  // namespace cxx
}  // namespace hyde
