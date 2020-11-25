// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as a foreign type declaration, adding it to
// module if successful.
void ParserImpl::ParseForeignTypeDecl(Node<ParsedModule> *module) {
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

  std::unique_ptr<Node<ParsedForeignType>> alloc_type;
  Node<ParsedForeignType> *type;

  auto set_data =
      [&] (Node<ParsedForeignType>::Info &info, bool can_override) -> bool {
        if (!info.can_override) {
          auto err = context->error_log.Append(scope_range, tok_range);
          err << "Can't override pre-existing foreign type substitution";

          err.Note(info.range)
              << "Conflicting previous type substitution is here";
          return false;
        }

        info.can_override = can_override;
        info.is_present = true;
        info.range = scope_range;
        info.code.insert(info.code.end(), code.begin(), code.end());
        return true;
      };

  // Strip out leading and trailing whitespace.
  auto fixup_code = [&code] (void) -> bool {
    while (code.front() == ' ' || code.front() == '\n') {
      code = code.substr(1u);
    }

    while (code.back() == ' ' || code.back() == '\n') {
      code = code.substr(0, code.size() - 1u);
    }

    return !code.empty();
  };

  auto report_trailing = true;
  Language last_lang = Language::kUnknown;

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    tok_range = tok.SpellingRange();

    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme ||
            Lexeme::kIdentifierVariable == lexeme ||
            Lexeme::kIdentifierType == lexeme) {
          name = tok;
          state = 1;
          auto &found_type = context->foreign_types[tok.IdentifierId()];
          if (!found_type) {
            alloc_type.reset(new Node<ParsedForeignType>);
            found_type = alloc_type.get();
            found_type->name = tok.AsForeignType();

            module->root_module->foreign_types.emplace(
                tok.IdentifierId(), found_type);
          }

          type = found_type;
          type->decls.push_back(scope_range);
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom or variable here for the name of "
              << "the foreign type being declared, got '" << tok << "' instead";
          return;
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
            return;
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

          auto &unk_data = type->info[static_cast<unsigned>(Language::kUnknown)];
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

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected string or non-language specific code literal here "
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
        if (report_trailing) {
          context->error_log.Append(scope_range, tok_range)
              << "Unexpected trailing token '" << tok
              << "' after foreign type declaration";
          report_trailing = false;
        }
        break;
    }
  }

  if (alloc_type) {
    if (!module->root_module->types.empty()) {
      module->root_module->types.back().get()->next = type;
    }
    module->root_module->types.emplace_back(std::move(alloc_type));
  }
}

// Try to parse `sub_range` as a foreign constant declaration, adding it to
// module if successful.
void ParserImpl::ParseForeignConstantDecl(Node<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashForeignConstantDecl);

  int state = 0;
  DisplayPosition next_pos;
  DisplayRange tok_range;
  std::string_view code;

  Node<ParsedForeignType> *type = nullptr;
  Node<ParsedForeignConstant> const_val;
  const_val.range = scope_range;

  // Strip out leading and trailing whitespace.
  auto fixup_code = [&code] (void) -> bool {
    while (code.front() == ' ' || code.front() == '\n') {
      code = code.substr(1u);
    }

    while (code.back() == ' ' || code.back() == '\n') {
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
        if (Lexeme::kIdentifierType == lexeme) {
          const_val.type = TypeLoc(tok);
          state = 1;
          type = context->foreign_types[tok.IdentifierId()];
          assert(type != nullptr);
          const_val.parent = type;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected foreign type name here, got '" << tok << "' instead";
          return;
        }

      case 1:
        if (Lexeme::kIdentifierAtom == lexeme ||
            Lexeme::kIdentifierVariable == lexeme ||
            Lexeme::kIdentifierConstant == lexeme) {

          const_val.name = tok.AsForeignConstant(const_val.type.Kind());
          state = 2;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom or variable here for the name of "
              << "the foreign constant being declared, got '" << tok << "' instead";
          return;
        }

      case 2:
        if (Lexeme::kLiteralCxxCode == lexeme) {
          const_val.lang = Language::kCxx;
          const_val.can_overide = false;
          const auto code_id = tok.CodeId();
          if (!context->string_pool.TryReadCode(code_id, &code) ||
              !fixup_code()) {
            context->error_log.Append(scope_range, tok_range)
                << "Empty or invalid C++ code literal in foreign constant "
                << "declaration";
            return;
          }

        } else if (Lexeme::kLiteralPythonCode == lexeme) {
          const_val.lang = Language::kPython;
          const_val.can_overide = false;
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
        if (report_trailing) {
          context->error_log.Append(scope_range, tok_range)
              << "Unexpected trailing token '" << tok
              << "' after foreign constant declaration";
          report_trailing = false;
        }
        break;
    }
  }

  if (!type) {
    return;
  }

  const_val.code.insert(const_val.code.end(), code.begin(), code.end());

  std::vector<bool> has_definition(kNumLanguages);
  auto &const_ptr = context->foreign_constants[const_val.name.IdentifierId()];
  if (const_ptr) {
    for (auto prev_const = const_ptr; prev_const;
         prev_const = prev_const->next_with_same_name) {

      has_definition[static_cast<unsigned>(prev_const->lang)] = true;

      if (prev_const->lang != const_val.lang) {
        continue;

      // We're overriding a previous language-agnostic version of the
      // constant with a language-specific version.
      } else if (prev_const->can_overide) {
        const_val.next = prev_const->next;
        const_val.next_with_same_name = prev_const->next_with_same_name;
        *prev_const = const_val;
        return;

      } else {
        auto err = context->error_log.Append(scope_range, tok_range);
        err << "Can't override pre-existing foreign constant";

        err.Note(prev_const->range)
            << "Conflicting previous constant is here";
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
    if (lang == const_val.lang || const_val.lang == Language::kUnknown) {
      auto &info = type->info[i];
      auto alloc_const = new Node<ParsedForeignConstant>(const_val);
      module->root_module->foreign_constants.emplace(
          const_val.name.IdentifierId(), alloc_const);
      alloc_const->lang = lang;
      if (lang == Language::kUnknown) {
        alloc_const->can_overide = false;
      }
      if (!info.constants.empty()) {
        info.constants.back()->next = alloc_const;
      }
      info.constants.emplace_back(alloc_const);
      alloc_const->next_with_same_name = const_ptr;
      const_ptr = alloc_const;
      added_def = true;
    }
  }

  (void) added_def;
  assert(added_def);
}

}  // namespace hyde
