// Copyright 2021, Trail of Bits. All rights reserved.

#include "CPlusPlus/Util.h"

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

namespace hyde {
namespace cxx {

// Make a comment in code for debugging purposes
OutputStream &Comment(OutputStream &os, ProgramRegion region,
                      const char *message) {

  //#ifndef NDEBUG
  //  os << os.Indent() << "// " << message << "\n";
  //#else
  //  (void) message;
  //#endif
  if (!region.Comment().empty()) {
    os << os.Indent() << "// " << region.Comment() << "\n";
  }
  (void) message;
  return os;
}

OutputStream &Procedure(OutputStream &os, ProgramProcedure proc) {
  switch (proc.Kind()) {
    case ProcedureKind::kInitializer: return os << "init_" << proc.Id() << '_';
    case ProcedureKind::kPrimaryDataFlowFunc:
      return os << "flow_" << proc.Id() << '_';
    case ProcedureKind::kMessageHandler:
      return os << proc.Message()->Name() << '_' << proc.Message()->Arity();
    case ProcedureKind::kTupleFinder: return os << "find_" << proc.Id() << '_';
    case ProcedureKind::kTupleRemover:
      return os << "remove_" << proc.Id() << '_';
    case ProcedureKind::kConditionTester:
      return os << "test_" << proc.Id() << '_';
    default: return os << "proc_" << proc.Id() << '_';
  }
}

// CPlusPlus representation of TypeKind
const std::string_view TypeName(ParsedForeignType type) {
  if (auto code = type.CodeToInline(Language::kCxx)) {
    return *code;
  }
  assert(false);
  return "::hyde::rt::Any";
}

// CPlusPlus representation of TypeKind
std::string_view TypeName(ParsedModule module, TypeLoc kind) {
  switch (kind.UnderlyingKind()) {
    case TypeKind::kBoolean: return "bool";
    case TypeKind::kSigned8: return "int8_t";
    case TypeKind::kSigned16: return "int16_t";
    case TypeKind::kSigned32: return "int32_t";
    case TypeKind::kSigned64: return "int64_t";
    case TypeKind::kUnsigned8: return "uint8_t";
    case TypeKind::kUnsigned16: return "uint16_t";
    case TypeKind::kUnsigned32: return "uint32_t";
    case TypeKind::kUnsigned64: return "uint64_t";
    case TypeKind::kFloat: return "float";
    case TypeKind::kDouble: return "double";
    case TypeKind::kBytes: return "::hyde::rt::Bytes";
    case TypeKind::kForeignType:
      if (auto type = module.ForeignType(kind); type) {
        return TypeName(*type);
      }
      [[clang::fallthrough]];
    default: assert(false); return "::hyde::rt::Any";
  }
}

const char *OperatorString(ComparisonOperator op) {
  switch (op) {
    case ComparisonOperator::kEqual: return "==";
    case ComparisonOperator::kNotEqual: return "!=";
    case ComparisonOperator::kLessThan: return "<";
    case ComparisonOperator::kGreaterThan: return ">";

    // TODO(ekilmer): What's a good default operator?
    default: assert(false); return "/* bad operator */";
  }
}

std::string TypeValueOrDefault(ParsedModule module, TypeLoc loc,
                               DataVariable var) {
  auto val = var.Value();
  std::string_view prefix = "(";
  std::string_view suffix = ")";

  if (val && val->IsTag()) {
    std::stringstream ss;
    ss << prefix << QueryTag::From(*val).Value() << suffix;
    return ss.str();
  }

  std::string_view default_val = "";

  switch (var.DefiningRole()) {
    case VariableRole::kConstantZero: return "0";
    case VariableRole::kConstantOne: return "1";
    case VariableRole::kConstantFalse: return "false";
    case VariableRole::kConstantTrue: return "true";
    default: break;
  }

  // Default value
  // TODO(ekilmer): default_val isn't being used anywhere....
  switch (loc.UnderlyingKind()) {
    case TypeKind::kBoolean: default_val = "false"; break;
    case TypeKind::kSigned8:
    case TypeKind::kSigned16:
    case TypeKind::kSigned32:
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned8:
    case TypeKind::kUnsigned16:
    case TypeKind::kUnsigned32:
    case TypeKind::kUnsigned64:
    case TypeKind::kFloat:
    case TypeKind::kDouble: default_val = "0"; break;

    // Default constructors
    case TypeKind::kBytes: break;
    case TypeKind::kForeignType:
      if (auto type = module.ForeignType(loc); type) {
        if (auto constructor = type->Constructor(Language::kCxx); constructor) {
          prefix = constructor->first;
          suffix = constructor->second;
        }
        break;
      }
      [[clang::fallthrough]];
    default: assert(false); prefix = "void  //";
  }

  std::stringstream value;
  value << prefix;
  if (val) {
    if (auto lit = val->Literal()) {
      if (auto spelling = lit->Spelling(Language::kCxx); spelling) {
        value << *spelling;
      }
    }
  }
  value << suffix;
  return value.str();
}

// Return all messages.
std::unordered_set<ParsedMessage> Messages(ParsedModule root_module) {
  std::unordered_set<ParsedMessage> seen;

  for (auto module : ParsedModuleIterator(root_module)) {
    for (auto message : module.Messages()) {
      seen.emplace(message);
    }
  }

  return seen;
}

}  // namespace cxx
}  // namespace hyde
