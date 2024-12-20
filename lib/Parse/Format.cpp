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
    os << "AutoVar_" << var.IdInClause();
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
  switch (name.Lexeme()) {
    case Lexeme::kIdentifierAtom:
      os << name;
      if (os.RenameLocals() && decl_name.decl.IsLocal()) {
        os << "pred_" << decl_name.decl.Id();
      }
      break;
    default:
      os << "pred_" << decl_name.decl.Id();
      break;
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
  if (decl.IsQuery()) {
    auto query = ParsedQuery::From(decl);
    if (query.ReturnsAtMostOneResult()) {
      os << " @first";
    }
  } else if (decl.IsFunctor()) {
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

  // If there's a forcing message, then emit that.
  if (auto pred = clause.clause.ForcingMessage()) {
    os << comma << "@first " << pred.value();
    comma = ", ";
  }

  // If there is something like `!true` or `false` in the clause body, then
  // it gets marked as being disabled.
  if (clause.clause.IsDisabled()) {
    os << comma << "false";
    comma = ", ";
  }

  for (auto g = 0u, num_groups = clause.clause.NumGroups();
       g < num_groups; ++g) {

    for (auto assign : clause.clause.Assignments(g)) {
      os << comma << assign;
      comma = ", ";
    }

    for (auto compare : clause.clause.Comparisons(g)) {
      os << comma << compare;
      comma = ", ";
    }

    for (auto pred : clause.clause.PositivePredicates(g)) {
      os << comma << pred;
      comma = ", ";
    }

    for (auto pred : clause.clause.NegatedPredicates(g)) {
      os << comma << pred;
      comma = ", ";
    }

    for (auto agg : clause.clause.Aggregates(g)) {
      os << comma << agg;
      comma = ", ";
    }

    comma = ", @barrier, ";
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

  os << "#inline(" << code_.Stage() << ") ```";

  switch (code_.Language()) {
    case Language::kUnknown: os << '\n'; break;
    case Language::kCxx: os << "c++\n"; break;
    case Language::kPython: os << "python\n"; break;
    case Language::kFlatBuffer: os << "flat\n"; break;
  }

  os << code << "\n```.";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedEnumType type) {
  os << "#enum " << type.Name();
  if (auto ut = type.UnderlyingType(); ut.IsValid()) {
    os << ' ' << ut;
  }
  os << '.';
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedForeignType type) {

  if (type.IsBuiltIn()) {
    return os;

  } else if (auto maybe_enum = ParsedEnumType::From(type)) {
    os << (*maybe_enum);
    return os;
  }

  // Start with a forward declaration.
  os << "#foreign " << type.Name() << '.';

  // Actual definitions, if any.
  for (auto lang : {Language::kUnknown, Language::kCxx, Language::kPython}) {

    auto maybe_code = type.CodeToInline(lang);
    if (!maybe_code) {
      continue;
    }

    os << "\n#foreign " << type.Name() << " ```";

    switch (lang) {
      case Language::kUnknown: break;
      case Language::kCxx: os << "c++ "; break;
      case Language::kPython: os << "python "; break;
      case Language::kFlatBuffer: os << "flat "; break;
    }

    os << (*maybe_code) << "```";

    if (auto constructor = type.Constructor(lang); constructor) {
      os << " ```" << constructor->first << '$' << constructor->second
         << "```";
    }

    if (type.IsReferentiallyTransparent(lang)) {
      os << " @transparent";
    }

    os << '.';
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedForeignConstant constant) {
  auto is_enum = ParsedForeignType::Of(constant).IsEnum();
  const char *ticks = is_enum ? "" : "```";
  os << "#constant " << constant.Type() << ' ' << constant.Name() << ' '
     << ticks;

  switch (constant.Language()) {
    case Language::kUnknown: break;
    case Language::kCxx: os << "c++ "; break;
    case Language::kPython: os << "python "; break;
    case Language::kFlatBuffer: os << "flat "; break;
  }

  os << constant.Constructor() << ticks;
  if (constant.IsUnique()) {
    os << " @unique";
  }
  os << ".";
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedDatabaseName name) {
  os << "#database " << name.Name() << '.';
  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedModule module) {
  module = module.RootModule();

  if (auto name = module.DatabaseName()) {
    os << (*name) << "\n";
  }

  auto do_decl = [&os](ParsedDeclaration decl) {
    if (decl.IsFirstDeclaration()) {
      for (auto redecl : decl.UniqueRedeclarations()) {
        os << redecl << '\n';
      }
    }
  };

  // First, declare the foreign types. They may be used by the functors.
  for (ParsedForeignType type : module.ForeignTypes()) {
    if (!type.IsBuiltIn()) {
      os << type << "\n";
    }
  }

  // First, declare the foreign types. They may be used by the functors.
  for (ParsedEnumType type : module.EnumTypes()) {
    os << type << "\n";
  }

  for (ParsedForeignConstant cv : module.ForeignConstants()) {
    os << cv << '\n';
  }

  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedFunctor decl : sub_module.Functors()) {
      do_decl(decl);
    }
  }

  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedQuery decl : sub_module.Queries()) {
      do_decl(decl);
    }

    for (ParsedMessage decl : sub_module.Messages()) {
      do_decl(decl);
    }

    for (ParsedExport decl : sub_module.Exports()) {
      if (decl.Arity()) {  // Ignore zero-argument predicates.
        do_decl(decl);
      }
    }

    for (ParsedLocal decl : sub_module.Locals()) {
      do_decl(decl);
    }
  }

  for (ParsedModule sub_module : ParsedModuleIterator(module)) {
    for (ParsedInline code : sub_module.Inlines()) {
      os << code << "\n";
    }

    for (ParsedClause clause : sub_module.Clauses()) {
      os << clause << "\n";
    }
  }

  return os;
}

OutputStream &operator<<(OutputStream &os, ParsedPredicate pred) {
  if (pred.IsNegatedWithNever()) {
    os << "@never ";
  } else if (pred.IsNegated()) {
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
