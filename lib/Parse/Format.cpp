// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Parse/Format.h>

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>

namespace hyde {

OutputStream &operator<<(OutputStream &os, ParsedVariable var) {
  auto name = var.Name();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedVariable) {
    os << "V" << var.Id();
  } else {
    os << name;
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, TypeKind type) {
  const char *repr = "";
  switch (type) {
    case TypeKind::kInvalid: break;
    case TypeKind::kSigned8:
      repr = "@i8";
      break;
    case TypeKind::kSigned16:
      repr = "@i16";
      break;
    case TypeKind::kSigned32:
      repr = "@i32";
      break;
    case TypeKind::kSigned64:
      repr = "@i64";
      break;
    case TypeKind::kUnsigned8:
      repr = "@u8";
      break;
    case TypeKind::kUnsigned16:
      repr = "@u16";
      break;
    case TypeKind::kUnsigned32:
      repr = "@u32";
      break;
    case TypeKind::kUnsigned64:
      repr = "@u64";
      break;
    case TypeKind::kFloat:
      repr = "@f32";
      break;
    case TypeKind::kDouble:
      repr = "@f64";
      break;
    case TypeKind::kString:
      repr = "@str";
      break;
    case TypeKind::kUUID:
      repr = "@uuid";
      break;
  }
  os << repr;
  return os;
}

OutputStream &operator<<(OutputStream &os, TypeLoc type) {
  os << type.Kind();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedLiteral val) {
  os << val.SpellingRange();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedParameter param) {
  // Binding specific; optional for some declarations.
  switch (param.Binding()) {
    case ParameterBinding::kImplicit:
      break;

    case ParameterBinding::kMutable:
      os << "mutable(" << ParsedFunctor::MergeOperatorOf(param).Name() << ") "
         << param.Name();
      // NOTE(pag): `param.Type()` is valid but not explicitly printed as it's
      //            taken from the merge functor's parameter types.
      return os;

    case ParameterBinding::kFree:
      os << "free ";
      break;
    case ParameterBinding::kBound:
      os << "bound ";
      break;
    case ParameterBinding::kAggregate:
      os << "aggregate ";
      break;
    case ParameterBinding::kSummary:
      os << "summary ";
      break;
  }

  if (param.Type().IsValid()) {
    os << param.Type() << " ";
  }

  os << param.Name();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedDeclarationName decl_name) {
  auto name = decl_name.decl.Name();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    os << "pred" << decl_name.decl.Id();
  } else {
    os << name;
    if (os.RenameLocals() && decl_name.decl.IsLocal()) {
      os << "_" << decl_name.decl.Id();
    }
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedDeclaration decl) {
  os << "#" << decl.KindName() << " " << ParsedDeclarationName(decl);

  auto comma = "(";
  for (auto param : decl.Parameters()) {
    os << comma << param;
    comma = ", ";
  }

  os << ")";
  if (decl.IsFunctor()) {
    auto functor = ParsedFunctor::From(decl);
    if (functor.IsComplex()) {
      os << " complex";
    } else {
      os << " trivial";
    }
  } else if (decl.IsLocal() && decl.IsInline()) {
    os << " inline";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedAggregate aggregate) {
  os << aggregate.Functor() << " over " << aggregate.Predicate();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedAssignment assign) {
  os << assign.LHS() << " = " << assign.RHS().SpellingRange();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedComparison compare) {
  const char *op = "";
  switch (compare.Operator()) {
    case ComparisonOperator::kEqual:
      op = " = ";
      break;
    case ComparisonOperator::kNotEqual:
      op = " != ";
      break;
    case ComparisonOperator::kLessThan:
      op = " < ";
      break;
    case ComparisonOperator::kGreaterThan:
      op = " > ";
      break;
  }
  os << compare.LHS() << op << compare.RHS();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedClauseHead clause) {
  os << ParsedDeclarationName(ParsedDeclaration::Of(clause.clause));
  auto comma = "(";
  for (auto param : clause.clause.Parameters()) {
    os << comma << param;
    comma = ", ";
  }
  os << ")";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedClauseBody clause) {
  auto comma = "";

  for (auto assign : clause.clause.Assignments()) {
    os << comma << assign;
    comma = ", ";
  }

  for (auto compare : clause.clause.Comparisons()) {
    os << comma << compare;
    comma = ", ";
  }

  for (auto pred : clause.clause.PositivePredicates()) {
    os << comma << pred;
    comma = ", ";
  }

  for (auto pred : clause.clause.NegatedPredicates()) {
    os << comma << pred;
    comma = ", ";
  }

  for (auto agg : clause.clause.Aggregates()) {
    os << comma << agg;
    comma = ", ";
  }

  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedClause clause) {
  os << ParsedClauseHead(clause) << " : "
     << ParsedClauseBody(clause) << ".";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedModule module) {
  if (os.KeepImports()) {
    for (auto import : module.Imports()) {
      os << import.SpellingRange() << "\n";
    }
  }

  for (auto decl : module.Queries()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto decl : module.Messages()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto decl : module.Functors()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto decl : module.Exports()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto decl : module.Locals()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto clause : module.Clauses()) {
    os << clause << "\n";
  }

  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedPredicate pred) {
  if (pred.IsNegated()) {
    os << "!";
  }

  os << ParsedDeclarationName(ParsedDeclaration::Of(pred));

  auto comma = "(";
  for (auto arg : pred.Arguments()) {
    os << comma << arg;
    comma = ", ";
  }
  os << ")";
  return os;
}

}  // namespace hyde
