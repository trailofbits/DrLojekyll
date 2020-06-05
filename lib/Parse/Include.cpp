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
    Error err(context->display_manager, SubTokenRange(), after_directive);
    err << "Expected string literal of file path here for include statement";
    context->error_log.Append(std::move(err));
    return;
  }

  std::string_view path_str;
  DisplayRange path_range;
  bool is_angled = false;

  // Parse out an angled string literal, e.g. `#include <...>`.
  if (Lexeme::kPuncLess == tok.Lexeme() &&
      sub_tokens.back().Lexeme() == Lexeme::kPuncGreater) {

    path_range = DisplayRange(tok.Position(), sub_tokens.back().NextPosition());
    DisplayRange str(tok.NextPosition(), sub_tokens.back().Position());

    if (!context->display_manager.TryReadData(str,  &path_str) ||
        path_str.empty()) {
      Error err(context->display_manager, SubTokenRange(), path_range);
      err << "Empty or invalid angled string literal in include statement";
      context->error_log.Append(std::move(err));
      return;
    }

    is_angled = true;

  // Parse out a string literal, e.g. `#include "..."`.
  } else if (Lexeme::kLiteralString == tok.Lexeme()) {
    const auto path_id = tok.StringId();
    const auto path_len = tok.StringLength();
    path_range = tok.SpellingRange();
    if (!context->string_pool.TryReadString(path_id, path_len, &path_str) ||
        !path_len) {
      Error err(context->display_manager, SubTokenRange(), path_range);
      err << "Empty or invalid string literal in include statement";
      context->error_log.Append(std::move(err));
      return;
    }

  } else {
    Error err(context->display_manager, SubTokenRange(),
              tok.SpellingRange());
    err << "Expected string or angled string literal of file path here for "
        << "include statement, got '"
        << DisplayRange(tok.Position(), sub_tokens.back().NextPosition())
        << "' instead";
    context->error_log.Append(std::move(err));
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
    Error err(context->display_manager, SubTokenRange(), path_range);
    err << "Unable to locate file '" << path_str
        << "' requested by include statement";
    context->error_log.Append(std::move(err));
    return;
  }

  const auto include = new Node<ParsedInclude>(
      SubTokenRange(), full_path, is_angled);
  if (!module->includes.empty()) {
    module->includes.back()->next = include;
  }
  module->includes.emplace_back(include);
}

}  // namesace hyde
