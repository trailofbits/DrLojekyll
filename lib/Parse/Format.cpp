// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Parse/Format.h>

namespace hyde {

  OutputStream::~OutputStream(void) {
    os.flush();
  }

  OutputStream::OutputStream(DisplayManager &display_manager_, std::ostream &os_)
      : display_manager(display_manager_),
        os(os_) {}

  OutputStream &OutputStream::operator<<(Token tok) {
    std::string_view data;
    (void) display_manager.TryReadData(tok.SpellingRange(), &data);
    os << data;
    return *this;
  }

  OutputStream &OutputStream::operator<<(DisplayRange range) {
    std::string_view data;
    (void) display_manager.TryReadData(range, &data);
    os << data;
    return *this;
  }

  template <typename T>
  OutputStream &OutputStream::operator<<(T val) {
    os << val;
    return *this;
  }

void FormatDecl(OutputStream &os, ParsedDeclaration decl) {
  os << "#" << decl.KindName() << " ";
  auto name = decl.Name();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    os << "pred" << decl.Id();
  } else {
    os << name;
  }
  auto comma = "(";
  for (auto param : decl.Parameters()) {
    os << comma;

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

    os << param.Type() << " " << param.Name();
    comma = ", ";
  }
  os << ")";
}

void FormatAggregate(OutputStream &os, ParsedAggregate aggregate) {
  FormatPredicate(os, aggregate.Functor());
  os << " over ";
  FormatPredicate(os, aggregate.Predicate());
}

void FormatClause(OutputStream &os, ParsedClause clause) {
  auto decl = ParsedDeclaration::Of(clause);
  auto name = decl.Name();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    os << "pred" << decl.Id();
  } else {
    os << name;
  }
  auto comma = "(";
  for (auto param : clause.Parameters()) {
    os << comma;
    os << param.Name();
    comma = ", ";
  }
  os << ") : ";
  comma = "";
  for (auto assign : clause.Assignments()) {
    os << comma << assign.LHS().Name() << " = " << assign.RHS().SpellingRange();
    comma = ", ";
  }

  for (auto compare : clause.Comparisons()) {
    os << comma << compare.LHS().Name();
    switch (compare.Operator()) {
      case ComparisonOperator::kEqual:
        os << " = ";
        break;
      case ComparisonOperator::kNotEqual:
        os << " != ";
        break;
      case ComparisonOperator::kLessThan:
        os << " < ";
        break;
      case ComparisonOperator::kGreaterThan:
        os << " > ";
        break;
    }
    os << compare.RHS().Name();
    comma = ", ";
  }

  for (auto pred : clause.PositivePredicates()) {
    os << comma;
    FormatPredicate(os, pred);
    comma = ", ";
  }

  for (auto pred : clause.NegatedPredicates()) {
    os << comma << "!";
    FormatPredicate(os, pred);
    comma = ", ";
  }

  for (auto agg : clause.Aggregates()) {
    os << comma;
    FormatAggregate(os, agg);
    comma = ", ";
  }

  os << ".";
}

void FormatModule(OutputStream &os, ParsedModule module) {
  for (auto import : module.Imports()) {
    os << import.SpellingRange() << "\n";
  }

  for (auto decl : module.Queries()) {
    FormatDecl(os, decl);
    os << "\n";
  }

  for (auto decl : module.Messages()) {
    FormatDecl(os, decl);
    os << "\n";
  }

  for (auto decl : module.Functors()) {
    FormatDecl(os, decl);
    if (decl.IsComplex()) {
      os << " complex\n";
    } else {
      os << " trivial\n";
    }
  }

  for (auto decl : module.Exports()) {
    FormatDecl(os, decl);
    os << "\n";
  }

  for (auto decl : module.Locals()) {
    FormatDecl(os, decl);
    os << "\n";
  }

  for (auto clause : module.Clauses()) {
    FormatClause(os, clause);
    os << "\n";
  }
}

void FormatPredicate(OutputStream &os, ParsedPredicate pred) {
  auto decl = ParsedDeclaration::Of(pred);
  auto name = decl.Name();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    os << "pred" << decl.Id();
  } else {
    os << name;
  }
  auto comma = "(";
  for (auto arg : pred.Arguments()) {
    os << comma;
    os << arg.Name();
    comma = ", ";
  }
  os << ")";
}


}  // namespace hyde
