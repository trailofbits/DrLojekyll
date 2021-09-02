// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as an inlining of of C/C++/Python code into the Datalog
// module.
void ParserImpl::ParseInlineCode(ParsedModuleImpl *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashInlinePrologueStmt ||
         tok.Lexeme() == Lexeme::kHashInlineEpilogueStmt);

  auto is_prologue = tok.Lexeme() == Lexeme::kHashInlinePrologueStmt;

  auto after_directive = tok.NextPosition();
  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, after_directive)
        << "Expected code literal or string literal for inline statement";
    return;
  }


  std::string_view code;
  auto language = Language::kUnknown;

  const auto tok_range = tok.SpellingRange();

  // Strip out leading newlines, as well as trailing newlines and spaces.
  auto fixup_code = [&code](void) -> bool {
    while (!code.empty() && code.front() == '\n') {
      code = code.substr(1u);
    }

    while (!code.empty() && (code.back() == ' ' || code.back() == '\n')) {
      code = code.substr(0, code.size() - 1u);
    }

    return !code.empty();
  };

  // It's a code literal, e.g. '#prologue ``` ... ```'.
  if (Lexeme::kLiteralCode == tok.Lexeme()) {
    const auto code_id = tok.CodeId();
    if (!context->string_pool.TryReadCode(code_id, &code) || !fixup_code()) {
      context->error_log.Append(scope_range, tok_range)
          << "Empty or invalid code literal in inline statement";
      return;
    }

  } else if (Lexeme::kLiteralCxxCode == tok.Lexeme()) {
    const auto code_id = tok.CodeId();
    if (!context->string_pool.TryReadCode(code_id, &code) || !fixup_code()) {
      context->error_log.Append(scope_range, tok_range)
          << "Empty or invalid C++ code literal in inline statement";
      return;
    }
    language = Language::kCxx;

  } else if (Lexeme::kLiteralPythonCode == tok.Lexeme()) {
    const auto code_id = tok.CodeId();
    if (!context->string_pool.TryReadCode(code_id, &code) || !fixup_code()) {
      context->error_log.Append(scope_range, tok_range)
          << "Empty or invalid Python code literal in inline statement";
      return;
    }
    language = Language::kPython;

  // Parse out a string literal, e.g. `#epilogue "..."`.
  } else if (Lexeme::kLiteralString == tok.Lexeme()) {
    const auto code_id = tok.StringId();
    const auto code_len = tok.StringLength();

    if (!context->string_pool.TryReadString(code_id, code_len, &code) ||
        !code_len || !fixup_code()) {
      context->error_log.Append(scope_range, tok_range)
          << "Empty or invalid string literal in inline statement";
      return;
    }

  // Neither a string nor a code string.
  } else {
    context->error_log.Append(scope_range, tok_range)
        << "Expected string or code literal for "
        << "inline c/c++ code statement, but got '"
        << DisplayRange(tok.Position(), sub_tokens.back().NextPosition())
        << "' instead";
    return;
  }

  (void) module->inlines.Create(scope_range, code, language, is_prologue);
}

}  // namespace hyde
