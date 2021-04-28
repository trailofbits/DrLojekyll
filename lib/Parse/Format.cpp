// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include <cassert>
#include <unordered_set>

namespace hyde {

OutputStream &operator<<(OutputStream &os, ParsedVariable var) {
  auto name = var.Name();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedVariable) {
    os << 'V' << var.Id();
  } else {
    os << name;
  }
  return os;
}

OutputStream &operator<<(OutputStream &os,
                         const std::optional<ParsedVariable> &var) {
  if (var.has_value()) {
    return os << *var;
  } else {
    return os << "_MissingVar";
  }
}

OutputStream &operator<<(OutputStream &os, TypeKind type) {
  os << Spelling(type);
  return os;
}

OutputStream &operator<<(OutputStream &os, TypeLoc type) {
  auto range = type.SpellingRange();
  if (range.IsValid()) {
    os << range;
  } else {
    os << type.Kind();
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
      os << " @impure";
    }
    if (!functor.IsAggregate() && !functor.IsMerge()) {
      os << " @range(";
      switch (functor.Range()) {
        case FunctorRange::kOneOrMore: os << "+"; break;
        case FunctorRange::kZeroOrMore: os << "*"; break;
        case FunctorRange::kZeroOrOne: os << "?"; break;
        case FunctorRange::kOneToOne: os << "."; break;
      }
      os << ")";
    }

  } else if (decl.IsLocal() && decl.IsInline()) {
    os << " @inline";

  } else if (decl.IsMessage()) {
    auto message = ParsedMessage::From(decl);
    if (message.IsDifferential()) {
      os << " @differential";
    }
  }
  os << ".";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedAggregate aggregate) {
  os << aggregate.Functor() << " over " << aggregate.Predicate();
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedAssignment assign) {
  os << assign.LHS() << " = ";
  if (auto rhs_range = assign.RHS().SpellingRange(); rhs_range.IsValid()) {
    os << rhs_range;
  } else {
    switch (auto tok = assign.RHS().Literal(); tok.Lexeme()) {
      case Lexeme::kLiteralTrue: os << "true"; break;
      case Lexeme::kLiteralFalse: os << "false"; break;
      default:
        if (auto spelling = assign.RHS().Spelling(Language::kUnknown);
            spelling.has_value()) {
          os << (*spelling);
        } else {
          assert(false);
        }
        break;
    }
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ComparisonOperator op) {
  switch (op) {
    case ComparisonOperator::kEqual: os << '='; break;
    case ComparisonOperator::kNotEqual: os << "!="; break;
    case ComparisonOperator::kLessThan: os << '<'; break;
    case ComparisonOperator::kGreaterThan: os << '>'; break;
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedComparison compare) {
  os << compare.LHS() << ' ' << compare.Operator() << ' ' << compare.RHS();
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
  auto comma = " : ";

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

  // If there is something like `!true` or `false` in the clause body, then
  // it gets marked as being disabled.
  if (clause.clause.IsDisabled()) {
    os << comma << "false";
  }

  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedClause clause) {
  os << ParsedClauseHead(clause);
  if (clause.IsHighlighted()) {
    os << " @highlight";
  }
  if (clause.CrossProductsArePermitted()) {
    os << " @product";
  }
  os << ParsedClauseBody(clause) << ".";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedInline code_) {
  const auto code = code_.CodeToInline();
  if (code.empty()) {
    return os;
  }

  if (code_.IsPrologue()) {
    os << "#prologue ```";
  } else {
    os << "#epilogue ```";
  }

  switch (code_.Language()) {
    case Language::kUnknown: os << '\n'; break;
    case Language::kCxx: os << "c++\n"; break;
    case Language::kPython: os << "python\n"; break;
  }

  os << code << "\n```.";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedForeignType type) {

  // Forward declaration.
  if (!type.IsBuiltIn()) {
    os << "#foreign " << type.Name() << '.';
  }

  // Actual definitions, if any.
  for (auto lang : {Language::kUnknown, Language::kCxx, Language::kPython}) {

    if (!type.IsBuiltIn() && type.IsSpecialized(lang)) {
      auto maybe_code = type.CodeToInline(lang);
      if (!maybe_code) {
        continue;
      }

      const auto code = *maybe_code;
      os << "\n#foreign " << type.Name() << " ```";

      switch (lang) {
        case Language::kUnknown: break;
        case Language::kCxx: os << "c++ "; break;
        case Language::kPython: os << "python "; break;
      }

      os << code << "```";

      if (auto constructor = type.Constructor(lang); constructor) {
        os << " ```" << constructor->first << '$' << constructor->second
           << "```";
      }

      if (!type.IsBuiltIn() && type.IsReferentiallyTransparent(lang)) {
        os << " @transparent";
      }
      os << '.';
    }

    for (auto foreign_const : type.Constants(lang)) {
      os << '\n' << foreign_const;
    }
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedForeignConstant constant) {
  os << "#constant " << constant.Type() << ' ' << constant.Name() << " ```";
  switch (constant.Language()) {
    case Language::kUnknown: break;
    case Language::kCxx: os << "c++ "; break;
    case Language::kPython: os << "python "; break;
  }

  os << constant.Constructor() << "```";
  os << ".";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedModule module) {
  module = module.RootModule();

  for (auto type : module.ForeignTypes()) {
    os << type << "\n";
  }

  std::unordered_set<ParsedDeclaration> seen;
  auto do_decl = [&](ParsedDeclaration decl) {
    if (!seen.count(decl)) {
      seen.insert(decl);
      os << decl << '\n';
    }
  };

  for (auto sub_module : ParsedModuleIterator(module)) {
    for (ParsedQuery decl : sub_module.Queries()) {
      for (auto redecl : decl.Redeclarations()) {
        do_decl(redecl);
      }
    }

    for (ParsedMessage decl : sub_module.Messages()) {
      do_decl(decl);
    }

    for (ParsedFunctor decl : sub_module.Functors()) {
      for (auto redecl : decl.Redeclarations()) {
        do_decl(redecl);
      }
    }

    for (ParsedExport decl : sub_module.Exports()) {
      if (decl.Arity()) {
        do_decl(decl);
      }
    }

    for (ParsedLocal decl : sub_module.Locals()) {
      do_decl(decl);
    }
  }

  for (auto sub_module : ParsedModuleIterator(module)) {
    for (auto code : sub_module.Inlines()) {
      if (code.IsPrologue()) {
        os << code << "\n";
      }
    }

    for (auto clause : sub_module.Clauses()) {
      os << clause << "\n";
    }

    for (auto clause : sub_module.DeletionClauses()) {
      os << clause << "\n";
    }

    for (auto code : sub_module.Inlines()) {
      if (!code.IsPrologue()) {
        os << code << "\n";
      }
    }
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
