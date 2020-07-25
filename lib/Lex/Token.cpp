// Copyright 2019, Trail of Bits. All rights reserved.

#include "Token.h"

namespace hyde {

bool Token::IsValid(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kInvalid:
    case ::hyde::Lexeme::kInvalidDirective:
    case ::hyde::Lexeme::kInvalidNumber:
    case ::hyde::Lexeme::kInvalidNewLineInString:
    case ::hyde::Lexeme::kInvalidEscapeInString:
    case ::hyde::Lexeme::kInvalidUnterminatedString:
    case ::hyde::Lexeme::kInvalidUnterminatedCode:
    case ::hyde::Lexeme::kInvalidStreamOrDisplay:
    case ::hyde::Lexeme::kInvalidTypeName:
    case ::hyde::Lexeme::kInvalidUnknown: return false;
    default: return true;
  }
}

// Return the position of an error associated with this token if it is an
// invalid token.
DisplayPosition Token::ErrorPosition(void) const {
  if (IsValid() || !position.IsValid()) {
    return DisplayPosition();
  }

  const lex::TokenInterpreter token_interpreter = {opaque_data};
  return DisplayPosition(
      position.DisplayId(),
      position.Index() + token_interpreter.error.error_offset,
      position.Line() + token_interpreter.error.disp_lines,
      token_interpreter.error.error_col);
}

// Return the range of characters covered by this token.
DisplayRange Token::SpellingRange(void) const {
  return DisplayRange(position, NextPosition());
}

// Return the position of the first character immediately following
// this token.
DisplayPosition Token::NextPosition(void) const {
  lex::TokenInterpreter token_interpreter = {opaque_data};
  auto index = position.Index();
  auto line = position.Line();
  auto column = position.Column();

  switch (Lexeme()) {
    case ::hyde::Lexeme::kInvalid: return DisplayPosition();

    case ::hyde::Lexeme::kEndOfFile: return position;

    case ::hyde::Lexeme::kWhitespace:
      index += token_interpreter.whitespace.spelling_width;

      if (token_interpreter.whitespace.num_leading_newlines) {
        line += token_interpreter.whitespace.num_leading_newlines;
        column = token_interpreter.whitespace.num_leading_spaces + 1;

      } else {
        column += token_interpreter.whitespace.num_leading_spaces;
      }
      break;

    case ::hyde::Lexeme::kLiteralCode:
      index += token_interpreter.code.num_bytes;
      line += token_interpreter.code.disp_lines;
      column = token_interpreter.code.next_cols;
      break;

    case ::hyde::Lexeme::kInvalidDirective:
    case ::hyde::Lexeme::kInvalidNumber:
    case ::hyde::Lexeme::kInvalidNewLineInString:
    case ::hyde::Lexeme::kInvalidEscapeInString:
    case ::hyde::Lexeme::kInvalidUnterminatedString:
    case ::hyde::Lexeme::kInvalidUnterminatedCode:
    case ::hyde::Lexeme::kInvalidStreamOrDisplay:
    case ::hyde::Lexeme::kInvalidTypeName:
    case ::hyde::Lexeme::kInvalidUnknown: return ErrorPosition();

    default:
      index += token_interpreter.basic.spelling_width;
      column += token_interpreter.basic.spelling_width;
      break;
  }

  return DisplayPosition(position.DisplayId(), index, line, column);
}

// Return the spelling width of this token.
unsigned Token::SpellingWidth(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kInvalid:
    case ::hyde::Lexeme::kInvalidDirective:
    case ::hyde::Lexeme::kInvalidNumber:
    case ::hyde::Lexeme::kInvalidNewLineInString:
    case ::hyde::Lexeme::kInvalidEscapeInString:
    case ::hyde::Lexeme::kInvalidUnterminatedString:
    case ::hyde::Lexeme::kInvalidUnterminatedCode:
    case ::hyde::Lexeme::kInvalidStreamOrDisplay:
    case ::hyde::Lexeme::kInvalidTypeName:
    case ::hyde::Lexeme::kInvalidUnknown:
    case ::hyde::Lexeme::kLiteralCode: {
      int num_lines = 0;
      int num_cols = 0;
      if (Position().TryComputeDistanceTo(NextPosition(), nullptr, &num_lines,
                                          &num_cols) &&
          !num_lines && 0 < num_cols) {
        return static_cast<unsigned>(num_cols);
      }
      return 0;
    }
    default: {
      const lex::TokenInterpreter interpreter = {opaque_data};
      return interpreter.basic.spelling_width;
    }
  }
}

// Lexeme associated with this token.
::hyde::Lexeme Token::Lexeme(void) const {
  const lex::TokenInterpreter interpreter = {opaque_data};
  return static_cast<::hyde::Lexeme>(interpreter.basic.lexeme);
}

// Returns `true` if this token's lexeme corresponds with a type.
bool Token::IsType(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kTypeASCII:
    case ::hyde::Lexeme::kTypeUTF8:
    case ::hyde::Lexeme::kTypeBytes:
    case ::hyde::Lexeme::kTypeUUID:
    case ::hyde::Lexeme::kTypeUn:
    case ::hyde::Lexeme::kTypeIn:
    case ::hyde::Lexeme::kTypeFn: return true;
    default: return false;
  }
}

// Return the ID of the corresponding string, or `0` if not a string.
unsigned Token::CodeId(void) const {
  if (Lexeme() == ::hyde::Lexeme::kLiteralCode) {
    const lex::TokenInterpreter interpreter = {opaque_data};
    return interpreter.code.id;
  } else {
    return 0;
  }
}

// Return the ID of the corresponding string, or `0` if not a string.
unsigned Token::StringId(void) const {
  if (Lexeme() == ::hyde::Lexeme::kLiteralString) {
    const lex::TokenInterpreter interpreter = {opaque_data};
    return interpreter.string.index;
  } else {
    return 0;
  }
}

// Return the length of the corresponding string, or `0` if not a string.
unsigned Token::StringLength(void) const {
  if (Lexeme() == ::hyde::Lexeme::kLiteralString) {
    const lex::TokenInterpreter interpreter = {opaque_data};
    return interpreter.string.num_bytes;
  } else {
    return 0;
  }
}

// Return the ID of the corresponding identifier, or `0` if not a string.
unsigned Token::IdentifierId(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kIdentifierAtom:
    case ::hyde::Lexeme::kIdentifierVariable: {
      const lex::TokenInterpreter interpreter = {opaque_data};
      return interpreter.identifier.index;
    }
    default: return 0;
  }
}

// Return the length of the corresponding string, or `0` if not a string.
unsigned Token::IdentifierLength(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kIdentifierAtom:
    case ::hyde::Lexeme::kIdentifierVariable: {
      const lex::TokenInterpreter interpreter = {opaque_data};
      return interpreter.identifier.spelling_width;
    }
    default: return 0;
  }
}

// Return the size, in bytes, of the corresponding type.
unsigned Token::TypeSizeInBytes(void) const {
  if (IsType()) {
    lex::TokenInterpreter interpreter = {opaque_data};
    return interpreter.type.type_width / 8;
  } else {
    return 0;
  }
}

// Returns the invalid char, or `\0` if not present.
char Token::InvalidChar(void) const {
  if (IsInvalid()) {
    const lex::TokenInterpreter interpreter = {opaque_data};
    return static_cast<char>(interpreter.error.invalid_char);
  } else {
    return '\0';
  }
}

// Return an EOF token at `position`.
Token Token::FakeEndOfFile(DisplayPosition position) {
  lex::TokenInterpreter interpreter = {};
  interpreter.basic.lexeme = static_cast<uint8_t>(::hyde::Lexeme::kEndOfFile);
  Token ret = {};
  ret.opaque_data = interpreter.flat;
  ret.position = position;
  return ret;
}

// Return a number literal token at `position` that occupies `spelling_width`
// columns of text in the display.
Token Token::FakeNumberLiteral(DisplayPosition position,
                               unsigned spelling_width) {
  lex::TokenInterpreter interpreter = {};
  interpreter.basic.lexeme =
      static_cast<uint8_t>(::hyde::Lexeme::kLiteralNumber);
  interpreter.basic.spelling_width = static_cast<uint16_t>(spelling_width);
  if (spelling_width != interpreter.basic.spelling_width) {
    interpreter.basic.spelling_width = static_cast<uint16_t>(~0u);
  }
  interpreter.number.spelling_kind = lex::NumberSpellingKind::kInvalid;

  Token ret = {};
  ret.opaque_data = interpreter.flat;
  ret.position = position;
  return ret;
}
// Return a number literal token at `position` that occupies `spelling_width`
// columns of text in the display.
Token Token::FakeStringLiteral(DisplayPosition position,
                               unsigned spelling_width) {
  lex::TokenInterpreter interpreter = {};
  interpreter.basic.lexeme =
      static_cast<uint8_t>(::hyde::Lexeme::kLiteralString);
  interpreter.basic.spelling_width = static_cast<uint16_t>(spelling_width);
  if (spelling_width != interpreter.basic.spelling_width) {
    interpreter.basic.spelling_width = static_cast<uint16_t>(~0u);
  }

  interpreter.string.index = 0;
  interpreter.string.num_bytes = 0;

  Token ret = {};
  ret.opaque_data = interpreter.flat;
  ret.position = position;
  return ret;
}

// Return a type token at `position` that occupies `spelling_width`
// columns of text in the display.
Token Token::FakeType(DisplayPosition position, unsigned spelling_width) {
  lex::TokenInterpreter interpreter = {};
  interpreter.basic.lexeme = static_cast<uint8_t>(::hyde::Lexeme::kTypeIn);
  interpreter.basic.spelling_width = static_cast<uint16_t>(spelling_width);
  if (spelling_width != interpreter.basic.spelling_width) {
    interpreter.basic.spelling_width = static_cast<uint16_t>(~0u);
  }

  interpreter.type.type_width = 32;

  Token ret = {};
  ret.opaque_data = interpreter.flat;
  ret.position = position;
  return ret;
}

// Return a fake token at `range`.
Token Token::Synthetic(::hyde::Lexeme lexeme, DisplayRange range) {
  lex::TokenInterpreter interpreter = {};
  interpreter.basic.lexeme = static_cast<uint8_t>(lexeme);
  int num_lines = 0;
  int num_cols = 0;
  if (range.TryComputeDistance(nullptr, &num_lines, &num_cols) && !num_lines &&
      0 < num_cols) {
    interpreter.basic.spelling_width = static_cast<uint16_t>(num_cols);
  }

  Token ret = {};
  ret.opaque_data = interpreter.flat;
  ret.position = range.From();
  return ret;
}

}  // namespace hyde
