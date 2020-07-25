// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as an inlining of of C/C++ code into the Datalog
// module.
void ParserImpl::ParseInline(Node<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashInlineStmt);

  auto after_directive = tok.NextPosition();
  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, after_directive)
        << "Expected code literal or string literal for inline statement";
    return;
  }

  std::string_view code;

  const auto tok_range = tok.SpellingRange();

  // It's a code literal, e.g. `#inline <! ... !>`.
  if (Lexeme::kLiteralCode == tok.Lexeme()) {
    const auto code_id = tok.CodeId();
    if (!context->string_pool.TryReadCode(code_id, &code)) {
      context->error_log.Append(scope_range, tok_range)
          << "Empty or invalid code literal in inline statement";
      return;
    }

  // Parse out a string literal, e.g. `#inline "..."`.
  } else if (Lexeme::kLiteralString == tok.Lexeme()) {
    const auto code_id = tok.StringId();
    const auto code_len = tok.StringLength();

    if (!context->string_pool.TryReadString(code_id, code_len, &code) ||
        !code_len) {
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

  const auto inline_node = new Node<ParsedInline>(scope_range, code);
  if (!module->inlines.empty()) {
    module->inlines.back()->next = inline_node;
  }
  module->inlines.emplace_back(inline_node);
}

}  // namespace hyde