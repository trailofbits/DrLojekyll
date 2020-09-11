// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#include <cassert>

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
  os << Spelling(type);
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
    case ParameterBinding::kImplicit: break;

    case ParameterBinding::kMutable:
      os << "mutable(" << ParsedFunctor::MergeOperatorOf(param).Name() << ") "
         << param.Name();

      // NOTE(pag): `param.Type()` is valid but not explicitly printed as it's
      //            taken from the merge functor's parameter types.
      return os;

    case ParameterBinding::kFree: os << "free "; break;
    case ParameterBinding::kBound: os << "bound "; break;
    case ParameterBinding::kAggregate: os << "aggregate "; break;
    case ParameterBinding::kSummary: os << "summary "; break;
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
  if (!decl.Arity()) {
    assert(decl.IsExport());
    return os;
  }

  os << "#" << decl.KindName() << " " << ParsedDeclarationName(decl);

  auto comma = "(";
  for (auto param : decl.Parameters()) {
    os << comma << param;
    comma = ", ";
  }

  os << ")";
  if (decl.IsFunctor()) {
    auto functor = ParsedFunctor::From(decl);
    if (!functor.IsPure()) {
      os << " impure";
    }
    os << " range(";
    switch (functor.Range()) {
      case FunctorRange::kOneOrMore: os << "+"; break;
      case FunctorRange::kZeroOrMore: os << "*"; break;
      case FunctorRange::kZeroOrOne: os << "?"; break;
      case FunctorRange::kOneToOne: os << "."; break;
    }
    os << ")";

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
    case ComparisonOperator::kEqual: op = " = "; break;
    case ComparisonOperator::kNotEqual: op = " != "; break;
    case ComparisonOperator::kLessThan: op = " < "; break;
    case ComparisonOperator::kGreaterThan: op = " > "; break;
  }
  os << compare.LHS() << op << compare.RHS();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedClauseHead clause) {
  os << ParsedDeclarationName(ParsedDeclaration::Of(clause.clause));
  if (clause.clause.Arity()) {
    auto comma = "(";
    for (auto param : clause.clause.Parameters()) {
      os << comma << param;
      comma = ", ";
    }
    os << ")";
  }
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
  if (clause.IsDeletion()) {
    os << '!';
  }
  os << ParsedClauseHead(clause) << " : " << ParsedClauseBody(clause) << ".";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedInclude include) {
  if (include.IsSystemInclude()) {
    os << "#include <" << include.IncludedFilePath() << ">";
  } else {
    os << "#include \"" << include.IncludedFilePath() << "\"";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedInline code_) {
  const auto code = code_.CodeToInline();

  if (code.empty()) {
    return os;
  }

  os << "#inline <!";
  if (code.front() == '\n' && code.back() == '\n') {
    os << code;
  } else if (code.front() == '\n') {
    os << code << '\n';
  } else if (code.back() == '\n') {
    os << '\n' << code;
  }
  os << "!>";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedModule module) {
  if (os.KeepImports()) {
    for (auto import : module.Imports()) {
      os << import.SpellingRange() << "\n";
    }
  }

  for (auto include : module.Includes()) {
    if (include.IsSystemInclude()) {
      os << include << "\n";
    }
  }

  for (auto include : module.Includes()) {
    if (!include.IsSystemInclude()) {
      os << include << "\n";
    }
  }

  for (auto code : module.Inlines()) {
    os << code << "\n";
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
    if (decl.Arity()) {
      os << ParsedDeclaration(decl) << "\n";
    }
  }

  for (auto decl : module.Locals()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto clause : module.Clauses()) {
    os << clause << "\n";
  }

  for (auto clause : module.DeletionClauses()) {
    os << clause << "\n";
  }

  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedPredicate pred) {
  if (pred.IsNegated()) {
    os << "!";
  }

  os << ParsedDeclarationName(ParsedDeclaration::Of(pred));

  if (pred.Arity()) {
    auto comma = "(";
    for (auto arg : pred.Arguments()) {
      os << comma << arg;
      comma = ", ";
    }
    os << ")";
  }
  return os;
}

}  // namespace hyde
