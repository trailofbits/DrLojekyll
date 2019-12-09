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

    OutputStream &OutputStream::operator<<(ParsedDeclaration decl) {
      *this << "#" << decl.KindName() << " ";
      auto name = decl.Name();
      if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
        *this << "pred" << decl.Id();
      } else {
        *this << name;
      }
      auto comma = "(";
      for (auto param : decl.Parameters()) {
        *this << comma;

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

        *this << param.Type() << " " << param.Name();
        comma = ", ";
      }
      *this << ")";
      if (decl.IsFunctor()) {
        auto functor = ParsedFunctor::From(decl);
        if (functor.IsComplex()) {
          *this << " complex\n";
        } else {
          *this << " trivial\n";
        }
      }
      return *this;
    }

    OutputStream &OutputStream::operator<<(ParsedAggregate aggregate) {
      *this << aggregate.Functor() << " over " << aggregate.Predicate();
      return *this;
    }

    OutputStream &OutputStream::operator<<(ParsedClause clause) {
      auto decl = ParsedDeclaration::Of(clause);
      auto name = decl.Name();
      if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
        *this << "pred" << decl.Id();
      } else {
        *this << name;
      }
      auto comma = "(";
      for (auto param : clause.Parameters()) {
        *this << comma;
        *this << param.Name();
        comma = ", ";
      }
      *this << ") : ";
      comma = "";
      for (auto assign : clause.Assignments()) {
        *this << comma << assign.LHS().Name() << " = " << assign.RHS().SpellingRange();
        comma = ", ";
      }

      for (auto compare : clause.Comparisons()) {
        *this << comma << compare.LHS().Name();
        switch (compare.Operator()) {
          case ComparisonOperator::kEqual:
            *this << " = ";
            break;
          case ComparisonOperator::kNotEqual:
            *this << " != ";
            break;
          case ComparisonOperator::kLessThan:
            *this << " < ";
            break;
          case ComparisonOperator::kGreaterThan:
            *this << " > ";
            break;
        }
        *this << compare.RHS().Name();
        comma = ", ";
      }

      for (auto pred : clause.PositivePredicates()) {
        *this << comma << pred;
        comma = ", ";
      }

      for (auto pred : clause.NegatedPredicates()) {
        *this << comma << "!" << pred;
        comma = ", ";
      }

      for (auto agg : clause.Aggregates()) {
        *this << comma << agg;
        comma = ", ";
      }

      *this << ".";
      return *this;
    }

    OutputStream &OutputStream::operator<<(ParsedModule module) {
      for (auto import : module.Imports()) {
        *this << import.SpellingRange() << "\n";
      }

      for (ParsedDeclaration decl : module.Queries()) {
        *this << decl << "\n";
      }

      for (ParsedDeclaration decl : module.Messages()) {
        *this << decl << "\n";
      }

      for (ParsedDeclaration decl : module.Functors()) {
        *this << decl;
      }

      for (ParsedDeclaration decl : module.Exports()) {
        *this << decl << "\n";
      }

      for (ParsedDeclaration decl : module.Locals()) {
        *this << decl << "\n";
      }

      for (auto clause : module.Clauses()) {
        *this << clause << "\n";
      }
      return *this;
    }

    OutputStream &OutputStream::operator<<(ParsedPredicate pred) {
      auto decl = ParsedDeclaration::Of(pred);
      auto name = decl.Name();
      if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
        *this << "pred" << decl.Id();
      } else {
        *this << name;
      }
      auto comma = "(";
      for (auto arg : pred.Arguments()) {
        *this << comma;
        *this << arg.Name();
        comma = ", ";
      }
      *this << ")";
      return *this;
    }


}  // namespace hyde9