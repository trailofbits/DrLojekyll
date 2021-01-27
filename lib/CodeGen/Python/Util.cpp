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

// Make a comment in code for debugging purposes
OutputStream &Comment(OutputStream &os, ProgramRegion region,
                      const char *message) {
#ifndef NDEBUG
  os << os.Indent() << "# " << message << "\n";
#else
  (void) message;
#endif
  if (!region.Comment().empty()) {
    os << os.Indent() << "# " << region.Comment() << "\n";
  }
  return os;
}

OutputStream &Procedure(OutputStream &os, ProgramProcedure proc) {
  switch (proc.Kind()) {
    case ProcedureKind::kInitializer: return os << "init_" << proc.Id() << "_";
    case ProcedureKind::kMessageHandler:
      return os << proc.Message()->Name() << "_"
                << proc.Message()->Arity();
    case ProcedureKind::kTupleFinder:
    case ProcedureKind::kTupleRemover:
    case ProcedureKind::kInductionCycleHandler:
    case ProcedureKind::kInductionOutputHandler:
    default: return os << "proc_" << proc.Id() << "_";
  }
}

// Python representation of TypeKind
const std::string_view TypeName(ParsedForeignType type) {
  if (auto code = type.CodeToInline(Language::kPython)) {
    return *code;
  }
  assert(false);
  return "Any";
}

// Python representation of TypeKind
std::string_view TypeName(ParsedModule module, TypeLoc kind) {
  switch (kind.UnderlyingKind()) {
    case TypeKind::kBoolean: return "bool";
    case TypeKind::kSigned8:
    case TypeKind::kSigned16:
    case TypeKind::kSigned32:
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned8:
    case TypeKind::kUnsigned16:
    case TypeKind::kUnsigned32:
    case TypeKind::kUnsigned64: return "int";
    case TypeKind::kFloat:
    case TypeKind::kDouble: return "float";
    case TypeKind::kBytes: return "bytes";
    case TypeKind::kASCII:
    case TypeKind::kUTF8:
    case TypeKind::kUUID: return "str";
    case TypeKind::kForeignType:
      if (auto type = module.ForeignType(kind); type) {
        return TypeName(*type);
      }
      [[clang::fallthrough]];
    default: assert(false); return "Any";
  }
}

const char *OperatorString(ComparisonOperator op) {
  switch (op) {
    case ComparisonOperator::kEqual: return "==";
    case ComparisonOperator::kNotEqual: return "!=";
    case ComparisonOperator::kLessThan: return "<";
    case ComparisonOperator::kGreaterThan: return ">";

    // TODO(ekilmer): What's a good default operator?
    default: assert(false); return "None";
  }
}

std::string TypeValueOrDefault(ParsedModule module, TypeLoc loc,
                               DataVariable var) {
  auto val = var.Value();
  std::string_view prefix = "";
  std::string_view suffix = "";

  switch (var.DefiningRole()) {
    case VariableRole::kConstantZero: return "0";
    case VariableRole::kConstantOne: return "1";
    case VariableRole::kConstantFalse: return "False";
    case VariableRole::kConstantTrue: return "True";
    default: break;
  }

  // Default value
  switch (loc.UnderlyingKind()) {
    case TypeKind::kBoolean:
      prefix = "bool(";
      suffix = ")";
      break;
    case TypeKind::kSigned8:
    case TypeKind::kSigned16:
    case TypeKind::kSigned32:
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned8:
    case TypeKind::kUnsigned16:
    case TypeKind::kUnsigned32:
    case TypeKind::kUnsigned64:
      prefix = "int(";
      suffix = ")";
      break;
    case TypeKind::kFloat:
    case TypeKind::kDouble:
      prefix = "float(";
      suffix = ")";
      break;
    case TypeKind::kBytes:
      prefix = "b";
      break;
    case TypeKind::kASCII:
    case TypeKind::kUTF8:
    case TypeKind::kUUID:
      break;
    case TypeKind::kForeignType:
      if (auto type = module.ForeignType(loc); type) {
        if (auto constructor = type->Constructor(Language::kPython);
            constructor) {
          prefix = constructor->first;
          suffix = constructor->second;
        }
        break;
      }
      [[clang::fallthrough]];
    default: assert(false); prefix = "None  #";
  }

  std::stringstream value;
  value << prefix;
  if (val) {
    if (auto spelling = val->Spelling(Language::kPython); spelling) {
      value << *spelling;
    }
  }
  value << suffix;
  return value.str();
}

// Return all messages.
std::unordered_set<ParsedMessage> Messages(ParsedModule module) {
  std::unordered_set<ParsedMessage> seen;

  for (auto module : ParsedModuleIterator(module)) {
    for (auto message : module.Messages()) {
      seen.emplace(message);
    }
  }

  return seen;
}

}  // namespace hyde
