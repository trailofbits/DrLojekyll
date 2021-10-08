// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Parse an enumeration declaration type.
void ParserImpl::ParseEnum(ParsedModuleImpl *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashEnum);

  const Token directive = tok;

  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, directive.NextPosition())
        << "Expected atom or variable name here for the enum type name, but "
        << "got the end of the file instead";
    return;
  }

  Token name;

  // Should be the first decl.
  if (Lexeme::kIdentifierAtom == tok.Lexeme() ||
      Lexeme::kIdentifierVariable == tok.Lexeme()) {
    name = tok.AsForeignType();

  // Looks like a redecl.
  } else if (Lexeme::kIdentifierType == tok.Lexeme()) {
    name = tok;

  } else {
    context->error_log.Append(scope_range, tok.SpellingRange())
        << "Expected variable or atom here for the name of the enum, but got '"
        << tok << "' instead";
    return;
  }

  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, tok.NextPosition())
        << "Expected a period or an underlying type name to end the "
        << "enum declaration";
    return;

  // Forwarded or default declaration.
  }

  Token dot;
  Token underlying_type;

  switch (tok.Lexeme()) {
    case Lexeme::kPuncPeriod:
      dot = tok;
      break;

    // Underlying type.
    case Lexeme::kTypeIn:
    case Lexeme::kTypeUn:
      underlying_type = tok;
      break;

    default:
      context->error_log.Append(scope_range, tok.SpellingRange())
          << "Expected a period or an underlying type name to end the "
          << "enum declaration, got '" << tok << "' instead";
      return;
  }

  if (dot.IsInvalid()) {
    if (!ReadNextSubToken(tok)) {
      context->error_log.Append(scope_range, underlying_type.NextPosition())
          << "Expected period to end the import statement";
      return;

    } else if (tok.Lexeme() != Lexeme::kPuncPeriod) {
      auto err = context->error_log.Append(
          scope_range, underlying_type.NextPosition());
      err << "Expected period here to end the enum declaration";
      err.Note(scope_range, tok.SpellingRange()) << "Got '" << tok << "' instead";
      return;

    } else {
      dot = tok;
    }
  }

  auto &found_type = context->foreign_types[name.IdentifierId()];
  if (found_type) {

    // Found a conflicting foreign type.
    if (!found_type->is_enum) {
      auto err = context->error_log.Append(scope_range, name.SpellingRange());
      err << "Cannot re-declare foreign type as an enumeration type";

      err.Note(found_type->decls.front(), found_type->name.SpellingRange())
          << "Conflicting foreign type declaration is here";

    // We've now got an underlying type.
    } else if (underlying_type.IsValid()) {

      if (found_type->builtin_type.IsValid() &&
          (found_type->builtin_type.TypeKind() != underlying_type.TypeKind())) {

        auto err = context->error_log.Append(
            scope_range, underlying_type.SpellingRange());
        err << "Cannot re-declare enumeration type with different "
            << "underlying type";

        err.Note(found_type->decls.front(),
                 found_type->builtin_type.SpellingRange())
            << "Conflicting foreign type declaration is here";
      }

      found_type->decls.emplace_back(found_type->decls.front());
      found_type->decls[0] = scope_range;
      found_type->name = name;
      found_type->builtin_type = underlying_type;

    } else {
      found_type->decls.emplace_back(scope_range);
    }

  // First declaration of this enumeration.
  } else {
    ParsedEnumTypeImpl *enum_type = module->root_module->enum_types.Create();
    enum_type->name = name;
    enum_type->builtin_type = underlying_type;
    enum_type->is_enum = true;
    enum_type->decls.emplace_back(scope_range);

    found_type = enum_type;

    context->display_manager.TryReadData(
        name.SpellingRange(), &(found_type->name_view));

    module->root_module->id_to_foreign_type.emplace(
        name.IdentifierId(), found_type);
  }
}

}  // namespace hyde
