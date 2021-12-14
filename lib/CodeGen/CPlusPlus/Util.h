// Copyright 2021, Trail of Bits. All rights reserved.

#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#include <unordered_set>

namespace hyde {
namespace cxx {

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

inline static OutputStream &Functor(OutputStream &os,
                                    const ParsedFunctor func) {
  if (auto name = func.InlineName(Language::kCxx); name.has_value()) {
    os << name.value();
  } else {
    os << "functors." << func.Name() << '_'
       << ParsedDeclaration(func).BindingPattern();
  }
  return os;
}

inline static OutputStream &Table(OutputStream &os, const DataTable table) {
  return os << "table_" << table.Id();
}

//inline static OutputStream &Table(OutputStream &os, const DataIndex index) {
//  return Table(os, DataTable::Backing(index));
//}

inline static OutputStream &Vector(OutputStream &os, const DataVector vec) {
  return os << "vec_" << vec.Id();
}

// CPlusPlus representation of TypeKind
std::string TypeName(ParsedForeignType type);

// CPlusPlus representation of TypeKind
std::string TypeName(ParsedModule module, TypeLoc kind);

OutputStream &TypeName(OutputStream &os, ParsedModule module, TypeLoc type);

const char *OperatorString(ComparisonOperator op);

std::string TypeValueOrDefault(ParsedModule module, TypeLoc loc,
                               DataVariable var);

}  // namespace cxx
}  // namespace hyde
