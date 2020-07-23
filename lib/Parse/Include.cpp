// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as an include of C/C++ code.
void ParserImpl::ParseInclude(Node<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashIncludeStmt);

  auto after_directive = tok.NextPosition();
  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, after_directive)
        << "Expected string literal of file path here for include statement";
    return;
  }

  const auto tok_range = tok.SpellingRange();

  std::string_view path_str;
  DisplayRange path_range;
  bool is_angled = false;

  // Parse out an angled string literal, e.g. `#include <...>`.
  if (Lexeme::kPuncLess == tok.Lexeme() &&
      sub_tokens.back().Lexeme() == Lexeme::kPuncGreater) {
    path_range = DisplayRange(tok.Position(), sub_tokens.back().NextPosition());
    DisplayRange str(tok.NextPosition(), sub_tokens.back().Position());

    if (!context->display_manager.TryReadData(str, &path_str) ||
        path_str.empty()) {
      context->error_log.Append(scope_range, path_range)
          << "Empty or invalid angled string literal in include statement";
      return;
    }

    is_angled = true;

    // Parse out a string literal, e.g. `#include "..."`.
  } else if (Lexeme::kLiteralString == tok.Lexeme()) {
    const auto path_id = tok.StringId();
    const auto path_len = tok.StringLength();
    path_range = tok_range;
    if (!context->string_pool.TryReadString(path_id, path_len, &path_str) ||
        !path_len) {
      context->error_log.Append(scope_range, path_range)
          << "Empty or invalid string literal in include statement";
      return;
    }

  } else {
    context->error_log.Append(scope_range, tok_range)
        << "Expected string or angled string literal of file path here for "
        << "include statement, got '"
        << DisplayRange(tok.Position(), sub_tokens.back().NextPosition())
        << "' instead";
    return;
  }

  std::error_code ec;
  std::string_view full_path;

  auto found = false;
  for (const auto &search_paths : context->include_search_paths) {
    for (auto search_path : search_paths) {
      full_path = std::string_view();

      ec = context->file_manager.PushDirectory(search_path);
      if (ec) {
        continue;
      }

      Path path(context->file_manager, path_str);
      context->file_manager.PopDirectory();

      ec = path.RealPath(&full_path);
      if (ec) {
        continue;
      }

      found = true;
      break;
    }

    if (found) {
      break;
    }
  }

  if (ec || full_path.empty()) {
    context->error_log.Append(scope_range, path_range)
        << "Unable to locate file '" << path_str
        << "' requested by include statement";
    return;
  }

  const auto include =
      new Node<ParsedInclude>(scope_range, full_path, is_angled);
  if (!module->includes.empty()) {
    module->includes.back()->next = include;
  }
  module->includes.emplace_back(include);
}

}  // namespace hyde
