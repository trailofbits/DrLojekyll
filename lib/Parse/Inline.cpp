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
    Error err(context->display_manager, SubTokenRange(), after_directive);
    err << "Expected code literal or string literal for inline statement";
    context->error_log.Append(std::move(err));
    return;
  }

  std::string_view code;

  if (Lexeme::kLiteralCode == tok.Lexeme()) {
    const auto code_id = tok.CodeId();
    if (!context->string_pool.TryReadCode(code_id, &code)) {
      Error err(context->display_manager, SubTokenRange(), tok.SpellingRange());
      err << "Empty or invalid code literal in inline statement";
      context->error_log.Append(std::move(err));
      return;
    }

  // Parse out a string literal, e.g. `#include "..."`.
  } else if (Lexeme::kLiteralString == tok.Lexeme()) {
    const auto code_id = tok.StringId();
    const auto code_len = tok.StringLength();

    if (!context->string_pool.TryReadString(code_id, code_len, &code) ||
        !code_len) {
      Error err(context->display_manager, SubTokenRange(), tok.SpellingRange());
      err << "Empty or invalid string literal in inline statement";
      context->error_log.Append(std::move(err));
      return;
    }

  } else {
    Error err(context->display_manager, SubTokenRange(),
              tok.SpellingRange());
    err << "Expected string or code literal for "
        << "inline c/c++ code statement, but got '"
        << DisplayRange(tok.Position(), sub_tokens.back().NextPosition())
        << "' instead";
    context->error_log.Append(std::move(err));
    return;
  }

  const auto inline_node = new Node<ParsedInline>(SubTokenRange(), code);
  if (!module->inlines.empty()) {
    module->inlines.back()->next = inline_node;
  }
  module->inlines.emplace_back(inline_node);
}

}  // namespace hyde
