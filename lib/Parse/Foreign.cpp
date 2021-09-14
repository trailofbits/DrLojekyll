// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as a foreign type declaration, adding it to
// module if successful.
void ParserImpl::ParseForeignTypeDecl(ParsedModuleImpl *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashForeignTypeDecl);

  int state = 0;
  DisplayPosition next_pos;
  DisplayRange tok_range;
  Token name;
  std::string_view code;

  ParsedForeignTypeImpl *alloc_type = nullptr;
  ParsedForeignTypeImpl *type = nullptr;

  auto set_data = [&](ParsedForeignTypeImpl::Info &info,
                      bool can_override) -> bool {
    if (!info.can_override) {
      auto err = context->error_log.Append(scope_range, tok_range);
      err << "Can't override pre-existing foreign type substitution";

      err.Note(info.range) << "Conflicting previous type substitution is here";
      return false;
    }

    info.can_override = can_override;
    info.is_present = true;
    info.range = scope_range;
    info.code.clear();
    info.code.insert(info.code.end(), code.begin(), code.end());
    return true;
  };

  // Strip out leading and trailing whitespace.
  auto fixup_code = [&code](void) -> bool {
    while (!code.empty() && (code.front() == ' ' || code.front() == '\n')) {
      code = code.substr(1u);
    }

    while (!code.empty() && (code.back() == ' ' || code.back() == '\n')) {
      code = code.substr(0, code.size() - 1u);
    }

    return !code.empty();
  };

  auto report_trailing = true;
  Language last_lang = Language::kUnknown;
  Token transparent;

  auto set_transparent = [&]() {
    type->info[static_cast<unsigned>(last_lang)].is_transparent = true;
    if (last_lang == Language::kUnknown) {
      for (auto &info : type->info) {
        if (info.can_override) {
          info.is_transparent = true;
        }
      }
    }
  };

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    tok_range = tok.SpellingRange();

    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme ||
            Lexeme::kIdentifierVariable == lexeme ||
            Lexeme::kIdentifierType == lexeme) {
          const auto id = tok.IdentifierId();
          name = tok;
          state = 1;
          auto &found_type = context->foreign_types[id];
          if (!found_type) {
            alloc_type = module->foreign_types.Create();
            found_type = alloc_type;
            found_type->name = tok.AsForeignType();

            module->root_module->id_to_foreign_type.emplace(id, found_type);

          } else {
            assert(module->root_module->id_to_foreign_type.count(id));
          }

          type = found_type;
          type->decls.push_back(scope_range);
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom or variable here for the name of "
              << "the foreign type being declared, got '" << tok << "' instead";

          state = 5;
          report_trailing = false;
          continue;
        }

      case 1:
        if (Lexeme::kLiteralCxxCode == lexeme) {
          last_lang = Language::kCxx;
          const auto code_id = tok.CodeId();
          if (!context->string_pool.TryReadCode(code_id, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid C++ code literal in foreign type "
                << "declaration";
            state = 5;
            continue;
          }

          auto &data = type->info[static_cast<unsigned>(Language::kCxx)];
          if (!set_data(data, false)) {
            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kLiteralPythonCode == lexeme) {
          last_lang = Language::kPython;
          const auto code_id = tok.CodeId();
          if (!context->string_pool.TryReadCode(code_id, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid Python code literal in foreign type "
                << "declaration";
            state = 3;
            report_trailing = false;
            continue;
          }

          auto &data = type->info[static_cast<unsigned>(Language::kPython)];
          if (!set_data(data, false)) {
            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kLiteralCode == lexeme) {
          const auto code_id = tok.CodeId();
          if (!context->string_pool.TryReadCode(code_id, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid code literal in foreign type "
                << "declaration";
            state = 3;
            report_trailing = false;
            continue;
          }

          auto &unk_data =
              type->info[static_cast<unsigned>(Language::kUnknown)];
          auto &py_data = type->info[static_cast<unsigned>(Language::kPython)];
          auto &cxx_data = type->info[static_cast<unsigned>(Language::kCxx)];

          if (!set_data(unk_data, false)) {
            state = 3;
            report_trailing = false;
            continue;
          }

          if (py_data.can_override && !set_data(py_data, true)) {
            state = 3;
            report_trailing = false;
            continue;
          }

          if (cxx_data.can_override && !set_data(cxx_data, true)) {
            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kLiteralString == lexeme) {
          const auto str_id = tok.StringId();
          const auto str_len = tok.StringLength();
          if (!context->string_pool.TryReadString(str_id, str_len, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid string literal in foreign type "
                << "declaration";

            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kPuncPeriod == lexeme) {
          state = 4;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected string or code literal here for the foreign type's "
              << "substitution, got '" << tok << "' instead";

          state = 3;
          report_trailing = false;
          continue;
        }

        state = 2;
        continue;

      // We'll just consume these.
      case 2: {
        if (Lexeme::kLiteralString == lexeme) {
          const auto str_id = tok.StringId();
          const auto str_len = tok.StringLength();
          if (!context->string_pool.TryReadString(str_id, str_len, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid constructor literal in foreign type "
                << "declaration";

            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kLiteralCode == lexeme) {
          const auto code_id = tok.CodeId();
          if (!context->string_pool.TryReadCode(code_id, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid constructor literal in foreign type "
                << "declaration";

            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kPragmaPerfTransparent == lexeme) {
          transparent = tok;
          set_transparent();
          state = 3;
          continue;

        } else if (Lexeme::kPuncPeriod == lexeme) {
          state = 4;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected string non-language specific code literal here "
              << "for the foreign type's constructor, got '" << tok
              << "' instead";
          state = 3;
          report_trailing = false;
          continue;
        }

        if (last_lang == Language::kUnknown) {
          context->error_log.Append(scope_range, tok_range)
              << "Not allowed to provide a constructor expression for "
              << "arbitrary languages";
          state = 3;
          report_trailing = false;
          continue;
        }

        const auto dollar_pos = code.find('$');
        if (dollar_pos == std::string::npos) {
          context->error_log.Append(scope_range, tok_range)
              << "Unable to find '$' meta-character in constructor";
          report_trailing = false;

        } else if (code.find('$', dollar_pos + 1u) != std::string::npos) {
          context->error_log.Append(scope_range, tok_range)
              << "Found extra '$' meta-character in constructor; "
              << "there must be only one";
          report_trailing = false;

        } else {
          auto &info = type->info[static_cast<unsigned>(last_lang)];
          if (0 < dollar_pos) {
            info.constructor_prefix = code.substr(0, dollar_pos);
          }
          info.constructor_suffix = code.substr(dollar_pos + 1u, code.size());
        }

        state = 3;
        continue;
      }

      case 3:
        if (Lexeme::kPragmaPerfTransparent == lexeme) {
          if (transparent.IsValid()) {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "The '@transparent' pragma can only be used once";

            err.Note(scope_range, transparent.SpellingRange())
                << "Previous usage of the '@transparent' pragma is here";

            report_trailing = false;

          } else {
            transparent = tok;
            set_transparent();
            continue;
          }

        } else if (Lexeme::kPuncPeriod == lexeme) {
          state = 4;
          continue;
        }

        [[clang::fallthrough]];

      case 4:
        if (report_trailing) {
          context->error_log.Append(scope_range, tok_range)
              << "Unexpected token before/after expected period '" << tok
              << "' at the end foreign type declaration";
          report_trailing = false;
        }

        state = 5;
        continue;

      case 5:

        // absorb any excess tokens
        continue;
    }
  }

  if (state != 4) {
    context->error_log.Append(scope_range, next_pos)
        << "Incomplete foreign type declaration; the foreign type "
        << "declaration must end with a period";
  }
}

// Try to parse `sub_range` as a foreign constant declaration, adding it to
// module if successful.
void ParserImpl::ParseForeignConstantDecl(ParsedModuleImpl *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashForeignConstantDecl);

  int state = 0;
  DisplayPosition next_pos;
  DisplayRange tok_range;
  std::string_view code;

  ParsedForeignTypeImpl *type = nullptr;
  ParsedForeignConstantImpl * const alloc_const =
      module->root_module->foreign_constants.Create();
  alloc_const->range = scope_range;

  // Strip out leading and trailing whitespace.
  auto fixup_code = [&code](void) -> bool {
    while (!code.empty() && (code.front() == ' ' || code.front() == '\n')) {
      code = code.substr(1u);
    }

    while (!code.empty() && (code.back() == ' ' || code.back() == '\n')) {
      code = code.substr(0, code.size() - 1u);
    }

    return !code.empty();
  };

  auto report_trailing = true;

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    tok_range = tok.SpellingRange();

    switch (state) {
      case 0:
        switch (lexeme) {

          // Create a named constant on a foreign type.
          case Lexeme::kIdentifierType: {
            alloc_const->type = TypeLoc(tok);
            state = 1;
            type = context->foreign_types[tok.IdentifierId()];
            assert(type != nullptr);
            alloc_const->parent = type;
            continue;
          }

          // Create a named constant on a built-in type.
          case Lexeme::kTypeBytes:
          case Lexeme::kTypeBoolean:
          case Lexeme::kTypeUn:
          case Lexeme::kTypeIn:
          case Lexeme::kTypeFn: {
            alloc_const->type = TypeLoc(tok);
            const auto id = ~static_cast<uint32_t>(alloc_const->type.Kind());
            auto &found_type = context->foreign_types[id];
            if (!found_type) {
              found_type = module->root_module->builtin_types.Create();
              type->name = tok;
              type->is_built_in = true;
            }

            type = found_type;
            assert(type != nullptr);
            alloc_const->parent = type;

            state = 1;
            continue;
          }

          default:
            context->error_log.Append(scope_range, tok_range)
                << "Expected foreign type name here, got '" << tok
                << "' instead";
            return;
        }

      // Name of the foreign constant.
      case 1:
        if (Lexeme::kIdentifierAtom == lexeme ||
            Lexeme::kIdentifierVariable == lexeme ||
            Lexeme::kIdentifierConstant == lexeme) {

          alloc_const->name = tok.AsForeignConstant(alloc_const->type.Kind());
          state = 2;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom or variable here for the name of "
              << "the foreign constant being declared, got '" << tok
              << "' instead";
          return;
        }

      // Value of the foreign constant.
      case 2:
        if (Lexeme::kLiteralCxxCode == lexeme) {
          alloc_const->lang = Language::kCxx;
          alloc_const->can_overide = false;
          const auto code_id = tok.CodeId();
          if (!context->string_pool.TryReadCode(code_id, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid C++ code literal in foreign constant "
                << "declaration";
            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kLiteralPythonCode == lexeme) {
          alloc_const->lang = Language::kPython;
          alloc_const->can_overide = false;
          const auto code_id = tok.CodeId();
          if (!context->string_pool.TryReadCode(code_id, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid Python code literal in foreign constant "
                << "declaration";
            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kLiteralCode == lexeme) {
          const auto code_id = tok.CodeId();
          if (!context->string_pool.TryReadCode(code_id, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid code literal in foreign constant "
                << "declaration";
            state = 3;
            report_trailing = false;
            continue;
          }

        } else if (Lexeme::kLiteralString == lexeme) {
          switch (alloc_const->type.UnderlyingKind()) {
            case TypeKind::kBytes:
              alloc_const->lang = Language::kUnknown;
              context->display_manager.TryReadData(tok_range, &code);
              break;
            case TypeKind::kForeignType: {
              const auto str_id = tok.StringId();
              const auto str_len = tok.StringLength();
              if (!context->string_pool.TryReadString(str_id, str_len, &code) ||
                  !fixup_code()) {
                context->error_log.Append(scope_range, tok_range)
                    << "Empty or invalid string literal in foreign constant "
                    << "declaration";

                state = 3;
                report_trailing = false;
                continue;
              }
              break;
            }
            default:
              context->error_log.Append(scope_range, tok_range)
                  << "Cannot initialize named constant of built-in type '"
                  << alloc_const->type.SpellingRange() << "' with string literal";
              state = 3;
              report_trailing = false;
              continue;
          }

        // Named number; basically like an enumerator.
        } else if (Lexeme::kLiteralNumber == lexeme) {
          switch (alloc_const->type.UnderlyingKind()) {
            case TypeKind::kBytes:
            case TypeKind::kBoolean:
              context->error_log.Append(scope_range, tok_range)
                  << "Cannot initialize named constant of built-in type '"
                  << alloc_const->type.SpellingRange() << "' with number literal";
              state = 3;
              report_trailing = false;
              continue;

            default:
              alloc_const->lang = Language::kUnknown;
              context->display_manager.TryReadData(tok_range, &code);
              break;
          }

        // Named Boolean; a bit weird, but maybe people want `yes` and `no`.
        } else if (Lexeme::kLiteralTrue == lexeme ||
                   Lexeme::kLiteralFalse == lexeme) {
          switch (alloc_const->type.UnderlyingKind()) {
            default:
              context->error_log.Append(scope_range, tok_range)
                  << "Cannot initialize named constant of built-in type '"
                  << alloc_const->type.SpellingRange()
                  << "' with Boolean literal";
              state = 3;
              report_trailing = false;
              continue;

            case TypeKind::kBoolean:
              alloc_const->lang = Language::kUnknown;
              context->display_manager.TryReadData(tok_range, &code);
              break;
          }

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected string or code literal here for the foreign "
              << "constant's substitution, got '" << tok << "' instead";

          state = 3;
          report_trailing = false;
          continue;
        }

        state = 3;
        continue;

      // We'll just consume these.
      case 3:
        if (Lexeme::kPuncPeriod == lexeme) {
          state = 4;
          continue;
        } else if (Lexeme::kPragmaPerfUnique == lexeme) {
          if (alloc_const->unique.IsValid()) {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Unexpected duplicate '@unique' pragma specified";

            err.Note(scope_range, alloc_const->unique.SpellingRange())
                << "Previous specification is here";
          } else {
            alloc_const->unique = tok;
          }
          state = 3;
          continue;
        }
        [[clang::fallthrough]];

      case 4:
        if (report_trailing) {
          context->error_log.Append(scope_range, tok_range)
              << "Unexpected token before/after expected period '" << tok
              << "' at the end foreign constant declaration";
          report_trailing = false;
        }
        state = 5;
        continue;

      case 5:

        // absorb any excess tokens
        continue;
    }
  }

  if (!report_trailing) {
    return;
  }

  if (state < 2) {
    context->error_log.Append(scope_range, sub_tokens.back().NextPosition())
        << "Expected a variable or atom name here as the name of the "
        << "constant, but got nothing";
    return;
  } else if (state != 4) {
    context->error_log.Append(scope_range, sub_tokens.back().NextPosition())
        << "Incomplete foreign constant declaration; the foreign constant "
        << "declaration must end with a period";

    return;
  }

  if (!type) {
    return;
  }

  if (type->is_built_in) {
    if (code.empty()) {
      context->error_log.Append(scope_range, sub_tokens.back().NextPosition())
          << "Named constants on built-in types must be have an initializer";
      return;
    }
  }

  alloc_const->code.insert(alloc_const->code.end(), code.begin(), code.end());


  // Chain it in to the list of constants with the same name. They might
  // target different languages.
  auto &const_ptr = context->foreign_constants[alloc_const->name.IdentifierId()];
  if (const_ptr) {
    alloc_const->next_with_same_name = const_ptr;
  }
  const_ptr = alloc_const;

//  std::vector<bool> has_definition(kNumLanguages);
//  std::vector<bool> can_override(kNumLanguages);
//  for (auto cp = const_ptr; cp; cp = const_ptr->next_with_same_name) {
//    const auto i = static_cast<unsigned>(cp->lang);
//    if (!has_definition[i]) {
//      has_definition[i] = true;
//      can_override[i] = cp->can_overide;
//
//    } else if (can_override[i]) {
//
//    } else {
//
//    }
//  }

#if 0

  if (const_ptr) {
    for (ParsedForeignConstantImpl *prev_const = const_ptr; prev_const;
         prev_const = prev_const->next_with_same_name) {

      has_definition[static_cast<unsigned>(prev_const->lang)] = true;

      if (prev_const->lang != alloc_const->lang) {
        continue;

      // We're overriding a previous language-agnostic version of the
      // constant with a language-specific version.
      } else if (prev_const->can_overide) {
        prev_const->lang = alloc_const->lang;
        prev_const->range = alloc_const->range;
        prev_const->code = alloc_const->code;
        prev_const->name = alloc_const->name;
        prev_const->unique = alloc_const->unique;
        prev_const->type = alloc_const->type;
        prev_const->can_overide = alloc_const->can_overide;
        return;

      } else {
        auto err = context->error_log.Append(scope_range, tok_range);
        err << "Can't override pre-existing foreign constant";

        err.Note(prev_const->range) << "Conflicting previous constant is here";
        return;
      }
    }
  }

  auto added_def = false;
  for (auto i = 0u; i < kNumLanguages; ++i) {
    if (has_definition[i]) {
      continue;
    }

    const auto lang = static_cast<Language>(i);
    if (lang == alloc_const->lang || alloc_const->lang == Language::kUnknown) {
      auto &info = type->info[i];

      module->root_module->id_to_foreign_constant.emplace(
          alloc_const->name.IdentifierId(), alloc_const);

      info.constants->AddUse(alloc_const);

      assert(alloc_const->type.IsValid());
      assert(alloc_const->parent != nullptr);
      assert(alloc_const->lang == lang);
      alloc_const->lang = lang;
      if (lang == Language::kUnknown) {
        alloc_const->can_overide = false;
      }

      alloc_const->next_with_same_name = const_ptr;
      const_ptr = alloc_const;
      added_def = true;
    }
  }

  (void) added_def;
  assert(added_def);
#endif
}

}  // namespace hyde
