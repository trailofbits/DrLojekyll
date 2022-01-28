// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

#include <unordered_set>

namespace hyde {
namespace {

static const std::unordered_set<std::string> kValidStages{
  "c++:client:interface:prologue",
  "c++:client:interface:prologue:namespace",
  "c++:client:interface:epilogue:namespace",
  "c++:client:interface:epilogue",
  "c++:client:database:prologue",
  "c++:client:database:prologue:namespace",
  "c++:client:database:epilogue:namespace",
  "c++:client:database:epilogue",
  "c++:database:descriptors:prologue",
  "c++:database:descriptors:epilogue",
  "c++:database:functors:prologue",
  "c++:database:functors:definition:prologue",
  "c++:database:functors:definition:epilogue",
  "c++:database:functors:epilogue",
  "c++:database:log:prologue",
  "c++:database:log:definition:prologue",
  "c++:database:log:definition:epilogue",
  "c++:database:log:epilogue",
  "c++:database:enums:prologue",
  "c++:database:enums:epilogue",
  "c++:database:prologue",
  "c++:database:prologue:namespace",
  "c++:database:epilogue:namespace",
  "c++:database:epilogue",
  "c++:interface:prologue",
  "c++:interface:prologue:namespace",
  "c++:interface:epilogue:namespace",
  "c++:interface:epilogue",
  "c++:server:prologue",
  "c++:server:prologue:namespace",
  "c++:server:epilogue:namespace",
  "c++:server:epilogue",
  "c++:server:prologue:main",
  "c++:server:epilogue:main",
  "flat:interface:service:prologue",
  "flat:interface:service:epilogue",
  "flat:interface:prologue",
  "flat:interface:prologue:namespace",
  "flat:interface:enums:prologue",
  "flat:interface:enums:epilogue",
  "flat:interface:messages:prologue",
  "flat:interface:messages:epilogue",
  "flat:interface:queries:prologue",
  "flat:interface:queries:epilogue",
  "flat:interface:epilogue:namespace",
  "python:database:prologue",
  "python:database:epilogue",
};

}  // namespace

// Try to parse `sub_range` as an inlining of of C/C++/Python code into the
// Datalog module.
void ParserImpl::ParseInlineCode(ParsedModuleImpl *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashInlineStmt);
  auto after_directive = tok.NextPosition();

  Token l_paren;
  if (!ReadNextSubToken(l_paren)) {
    context->error_log.Append(scope_range, after_directive)
        << "Expected an opening parenthesis here to begin the stage name "
        << "specification of inline statement";
    return;
  } else if (l_paren.Lexeme() != Lexeme::kPuncOpenParen) {
    context->error_log.Append(scope_range, l_paren.SpellingRange())
        << "Expected an opening parenthesis here to begin the stage name "
        << "specification of inline statement";
    return;
  }

  DisplayPosition from_position = l_paren.NextPosition();
  DisplayPosition to_position = from_position;
  Token r_paren;
  while (ReadNextSubToken(tok)) {
    if (tok.Lexeme() == Lexeme::kPuncCloseParen) {
      r_paren = tok;
      break;
    } else {
      to_position = tok.NextPosition();
    }
  }

  if (!r_paren.IsValid()) {
    context->error_log.Append(scope_range, to_position)
        << "Expected a closing parenthesis here to end the stage name "
        << "specification of inline statement";
    return;
  }

  DisplayRange stage_range(from_position, to_position);
  std::string_view stage_name_code;
  if (!context->display_manager.TryReadData(stage_range, &stage_name_code)) {
    context->error_log.Append(scope_range, DisplayRange(l_paren, r_paren))
        << "Unable to read stage name specification of inline statement";
    return;
  }

  std::stringstream stage_ss;
  for (auto ch : stage_name_code) {
    switch (ch) {
      case ' ': case '\t': case '\n':
        continue;
      default:
        stage_ss << ch;
    }
  }

  auto stage_name = stage_ss.str();
  if (!kValidStages.count(stage_name)) {
    context->error_log.Append(scope_range, DisplayRange(l_paren, r_paren))
        << "Invalid stage name '" << stage_name
        << "' in stage specification of inline statement";
    return;
  }

  auto after_stage = tok.NextPosition();
  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, after_stage)
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

  } else if (Lexeme::kLiteralFlatBufferCode == tok.Lexeme()) {
    const auto code_id = tok.CodeId();
    if (!context->string_pool.TryReadCode(code_id, &code) || !fixup_code()) {
      context->error_log.Append(scope_range, tok_range)
          << "Empty or invalid FlatBuffer code literal in inline statement";
      return;
    }
    language = Language::kFlatBuffer;

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

  (void) module->inlines.Create(
      scope_range, code, language, std::move(stage_name));
}

}  // namespace hyde
