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

OutputStream &operator<<(OutputStream &os, ParsedLiteral val) {
  os << val.SpellingRange();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedParameter param) {
  // Binding specific; optional for some declarations.
  switch (param.Binding()) {
    case ParameterBinding::kImplicit:
      break;
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

OutputStream &operator<<(OutputStream &os, ParsedDeclaration decl) {
  os << "#" << decl.KindName() << " ";

  auto name = decl.Name();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    os << "pred" << decl.Id();
  } else {
    os << name;
    if (os.RenameLocals() && decl.IsLocal()) {
      os << "_" << decl.Id();
    }
  }

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

OutputStream &operator<<(OutputStream &os, ParsedClause clause) {
  auto decl = ParsedDeclaration::Of(clause);
  auto name = decl.Name();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    os << "pred" << decl.Id();
  } else {
    os << name;
    if (os.RenameLocals() && decl.IsLocal()) {
      os << "_" << decl.Id();
    }
  }
  auto comma = "(";
  for (auto param : clause.Parameters()) {
    os << comma << param;
    comma = ", ";
  }
  os << ") : ";
  comma = "";

  for (auto assign : clause.Assignments()) {
    os << comma << assign;
    comma = ", ";
  }

  for (auto compare : clause.Comparisons()) {
    os << comma << compare;
    comma = ", ";
  }

  for (auto pred : clause.PositivePredicates()) {
    os << comma << pred;
    comma = ", ";
  }

  for (auto pred : clause.NegatedPredicates()) {
    os << comma << pred;
    comma = ", ";
  }

  for (auto agg : clause.Aggregates()) {
    os << comma << agg;
    comma = ", ";
  }

  os << ".";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedModule module) {
  if (os.KeepImports()) {
    for (auto import : module.Imports()) {
      os << import.SpellingRange() << "\n";
    }
  }

  for (ParsedDeclaration decl : module.Queries()) {
    os << decl << "\n";
  }

  for (ParsedDeclaration decl : module.Messages()) {
    os << decl << "\n";
  }

  for (ParsedDeclaration decl : module.Functors()) {
    os << decl << "\n";
  }

  for (ParsedDeclaration decl : module.Exports()) {
    os << decl << "\n";
  }

  for (ParsedDeclaration decl : module.Locals()) {
    os << decl << "\n";
  }

  for (auto clause : module.Clauses()) {
    os << clause << "\n";
  }

  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedPredicate pred) {
  auto decl = ParsedDeclaration::Of(pred);
  auto name = decl.Name();

  if (pred.IsNegated()) {
    os << "!";
  }

  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    os << "pred" << decl.Id();
  } else {
    os << name;
    if (os.RenameLocals() && decl.IsLocal()) {
      os << "_" << decl.Id();
    }
  }
  auto comma = "(";
  for (auto arg : pred.Arguments()) {
    os << comma << arg;
    comma = ", ";
  }
  os << ")";
  return os;
}

}  // namespace hyde
