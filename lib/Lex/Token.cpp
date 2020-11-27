// Copyright 2019, Trail of Bits. All rights reserved.

#include "Token.h"

#include <drlojekyll/Util/BitManipulation.h>

#include <cassert>

namespace hyde {

bool Token::IsValid(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kInvalid:
    case ::hyde::Lexeme::kInvalidDirective:
    case ::hyde::Lexeme::kInvalidNumber:
    case ::hyde::Lexeme::kInvalidOctalNumber:
    case ::hyde::Lexeme::kInvalidHexadecimalNumber:
    case ::hyde::Lexeme::kInvalidBinaryNumber:
    case ::hyde::Lexeme::kInvalidNewLineInString:
    case ::hyde::Lexeme::kInvalidEscapeInString:
    case ::hyde::Lexeme::kInvalidUnterminatedString:
    case ::hyde::Lexeme::kInvalidUnterminatedCode:
    case ::hyde::Lexeme::kInvalidUnterminatedCxxCode:
    case ::hyde::Lexeme::kInvalidUnterminatedPythonCode:
    case ::hyde::Lexeme::kInvalidStreamOrDisplay:
    case ::hyde::Lexeme::kInvalidTypeName:
    case ::hyde::Lexeme::kInvalidUnknown: return false;
    default: return true;
  }
}

// Return the position of an error associated with this token if it is an
// invalid token.
DisplayPosition Token::ErrorPosition(void) const {
  if (IsValid() || position.IsInvalid()) {
    return DisplayPosition();
  }

  const auto error = As<lex::ErrorToken>();
  const auto line = position.Line();
  const auto error_col = error.Load<lex::ErrorColumn>();
  const auto error_line = line + error.Load<lex::ErrorLineDisp>();
  const auto error_index = position.Index() + error.Load<lex::ErrorIndexDisp>();
#ifndef NDEBUG
  const auto col = position.Column();
  const auto next_line = line + error.Load<lex::LineDisp>();
  const auto next_col = error.Load<lex::Column>();
  assert(0u < col);
  assert(0u < error_col);
  assert(0u < next_col);
  if (error_line == next_line) {
    assert(error_col <= next_col);

  } else if (error_line == line) {
    assert(col <= error_col);
    assert(error_line < next_line);

  } else {
    assert(error_line < next_line);
  }
#endif
  return DisplayPosition(position.DisplayId(), error_index,
                         error_line, error_col);
}

// Return the range of characters covered by this token. This is an open range
// of the form `[begin, end)`.
DisplayRange Token::SpellingRange(void) const {
  return DisplayRange(position, NextPosition());
}

// Return the position of the first character immediately following
// this token.
DisplayPosition Token::NextPosition(void) const {
  auto index = position.Index();
  auto line = position.Line();
  auto column = position.Column();

  switch (Lexeme()) {
    case ::hyde::Lexeme::kInvalid: return DisplayPosition();

    case ::hyde::Lexeme::kEndOfFile: return position;

    case ::hyde::Lexeme::kWhitespace: {
      const auto whitespace = As<lex::WhitespaceToken>();
      index += whitespace.Load<lex::IndexDisp>();
      line += whitespace.Load<lex::LineDisp>();
      column = whitespace.Load<lex::Column>();
      break;
    }

    case ::hyde::Lexeme::kLiteralCode:
    case ::hyde::Lexeme::kLiteralCxxCode:
    case ::hyde::Lexeme::kLiteralPythonCode: {
      const auto code = As<lex::CodeLiteralToken>();
      index += code.Load<lex::IndexDisp>();
      line += code.Load<lex::LineDisp>();
      column = code.Load<lex::Column>();
      break;
    }

    case ::hyde::Lexeme::kInvalidDirective:
    case ::hyde::Lexeme::kInvalidNumber:
    case ::hyde::Lexeme::kInvalidOctalNumber:
    case ::hyde::Lexeme::kInvalidHexadecimalNumber:
    case ::hyde::Lexeme::kInvalidBinaryNumber:
    case ::hyde::Lexeme::kInvalidNewLineInString:
    case ::hyde::Lexeme::kInvalidEscapeInString:
    case ::hyde::Lexeme::kInvalidUnterminatedString:
    case ::hyde::Lexeme::kInvalidUnterminatedCode:
    case ::hyde::Lexeme::kInvalidUnterminatedCxxCode:
    case ::hyde::Lexeme::kInvalidUnterminatedPythonCode:
    case ::hyde::Lexeme::kInvalidStreamOrDisplay:
    case ::hyde::Lexeme::kInvalidTypeName:
    case ::hyde::Lexeme::kInvalidUnknown: {
      const auto error = As<lex::ErrorToken>();
      index += error.Load<lex::IndexDisp>();
      line += error.Load<lex::LineDisp>();
      column = error.Load<lex::Column>();
      break;
    }

    default: {
      const auto basic = As<lex::BasicToken>();
      index += basic.Load<lex::SpellingWidth>();
      column += basic.Load<lex::SpellingWidth>();
      break;
    }
  }

  return DisplayPosition(position.DisplayId(), index, line, column);
}

// Return the spelling width of this token.
unsigned Token::SpellingWidth(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kInvalid:
    case ::hyde::Lexeme::kInvalidDirective:
    case ::hyde::Lexeme::kInvalidNumber:
    case ::hyde::Lexeme::kInvalidHexadecimalNumber:
    case ::hyde::Lexeme::kInvalidBinaryNumber:
    case ::hyde::Lexeme::kInvalidOctalNumber:
    case ::hyde::Lexeme::kInvalidNewLineInString:
    case ::hyde::Lexeme::kInvalidEscapeInString:
    case ::hyde::Lexeme::kInvalidUnterminatedString:
    case ::hyde::Lexeme::kInvalidUnterminatedCode:
    case ::hyde::Lexeme::kInvalidUnterminatedCxxCode:
    case ::hyde::Lexeme::kInvalidUnterminatedPythonCode:
    case ::hyde::Lexeme::kInvalidStreamOrDisplay:
    case ::hyde::Lexeme::kInvalidTypeName:
    case ::hyde::Lexeme::kInvalidUnknown:
    case ::hyde::Lexeme::kLiteralCode:
    case ::hyde::Lexeme::kLiteralCxxCode:
    case ::hyde::Lexeme::kLiteralPythonCode: {
      int64_t num_lines = 0;
      int64_t num_cols = 0;
      if (Position().TryComputeDistanceTo(NextPosition(), nullptr, &num_lines,
                                          &num_cols) &&
          !num_lines && 0 < num_cols) {
        return static_cast<unsigned>(num_cols);
      }
      return 0;
    }
    default: {
      return As<lex::BasicToken>().Load<lex::SpellingWidth>();
    }
  }
}

// Lexeme associated with this token.
::hyde::Lexeme Token::Lexeme(void) const {
  return As<lex::BasicToken>().Load<::hyde::Lexeme>();
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
    case ::hyde::Lexeme::kTypeFn:
    case ::hyde::Lexeme::kIdentifierType: return true;
    default: return false;
  }
}

// Returns a hash of this token.
uint64_t Token::Hash(void) const noexcept {
  auto hash = a;
  hash ^= RotateRight64(hash + 1u, 13u) * (b + 17u);
  hash ^= RotateRight64(hash, 15u) * (position.Index() + 3u);
  return hash;
}

// Return the ID of the corresponding string, or `0` if not a string.
unsigned Token::CodeId(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kLiteralCode:
    case ::hyde::Lexeme::kLiteralCxxCode:
    case ::hyde::Lexeme::kLiteralPythonCode: {
      return As<lex::CodeLiteralToken>().Load<lex::Id>();
    }
    default: return 0;
  }
}

// Return the ID of the corresponding string, or `0` if not a string.
unsigned Token::StringId(void) const {
  if (Lexeme() == ::hyde::Lexeme::kLiteralString) {
    return As<lex::StringLiteralToken>().Load<lex::Id>();
  } else {
    return 0;
  }
}

// Return the length of the corresponding string, or `0` if not a string.
unsigned Token::StringLength(void) const {
  if (Lexeme() == ::hyde::Lexeme::kLiteralString) {
    return As<lex::StringLiteralToken>().Load<lex::ReprWidth>();
  } else {
    return 0;
  }
}

// Return the ID of the corresponding identifier, or `0` if not a string.
unsigned Token::IdentifierId(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kIdentifierAtom:
    case ::hyde::Lexeme::kIdentifierVariable:
    case ::hyde::Lexeme::kIdentifierConstant:
    case ::hyde::Lexeme::kIdentifierType:
      return As<lex::IdentifierToken>().Load<lex::Id>();
    default: return 0;
  }
}

// Return the length of the corresponding string, or `0` if not a string.
unsigned Token::IdentifierLength(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kIdentifierAtom:
    case ::hyde::Lexeme::kIdentifierVariable:
    case ::hyde::Lexeme::kIdentifierConstant:
    case ::hyde::Lexeme::kIdentifierType:
      return As<lex::IdentifierToken>().Load<lex::SpellingWidth>();
    default: return 0;
  }
}

// Return the kind of this type. This works for foreign types, as well as
// for foreign constants.
::hyde::TypeKind Token::TypeKind(void) const {
  switch (Lexeme()) {
    case ::hyde::Lexeme::kTypeASCII:
    case ::hyde::Lexeme::kTypeUTF8:
    case ::hyde::Lexeme::kTypeBytes:
    case ::hyde::Lexeme::kTypeUUID:
    case ::hyde::Lexeme::kTypeUn:
    case ::hyde::Lexeme::kTypeIn:
    case ::hyde::Lexeme::kTypeFn:
      return As<lex::TypeToken>().Load<::hyde::TypeKind>();
    case ::hyde::Lexeme::kIdentifierConstant:
    case ::hyde::Lexeme::kIdentifierType:
      return As<lex::IdentifierToken>().Load<::hyde::TypeKind>();
    default: return ::hyde::TypeKind::kInvalid;
  }
}

// Returns the invalid char, or `\0` if not present.
char Token::InvalidChar(void) const {
  if (IsInvalid()) {
    return As<lex::ErrorToken>().Load<char>();
  } else {
    return '\0';
  }
}

// Returns this token, converted to be a foreign type.
Token Token::AsForeignType(void) const {
  Token ret;
  switch (Lexeme()) {
    case ::hyde::Lexeme::kIdentifierType:
      return *this;
    case ::hyde::Lexeme::kIdentifierAtom:
    case ::hyde::Lexeme::kIdentifierVariable: {
      ret = *this;
      const uint32_t high = IdentifierId() << 8u;
      const uint32_t low =
          static_cast<uint32_t>(::hyde::TypeKind::kForeignType);
      auto &ident = ret.As<lex::IdentifierToken>();
      ident.Store<::hyde::Lexeme>(::hyde::Lexeme::kIdentifierType);
      ident.Store<::hyde::TypeKind>(high | low);
      break;
    }
    default: break;
  }

  return ret;
}

// Returns this token, converted to be a foreign constant of a specific type.
Token Token::AsForeignConstant(::hyde::TypeKind kind) const {
  Token ret;
  switch (Lexeme()) {
    case ::hyde::Lexeme::kIdentifierConstant:
      return *this;
    case ::hyde::Lexeme::kIdentifierAtom:
    case ::hyde::Lexeme::kIdentifierVariable: {
      ret = *this;
      auto &ident = ret.As<lex::IdentifierToken>();
      ident.Store<::hyde::Lexeme>(::hyde::Lexeme::kIdentifierConstant);
      ident.Store<::hyde::TypeKind>(kind);
      break;
    }
    default: break;
  }

  return ret;
}

// Return an EOF token at `position`.
Token Token::FakeEndOfFile(DisplayPosition position) {
  Token ret = {};
  ret.position = position;

  auto &basic = ret.As<lex::BasicToken>();
  basic.Store<::hyde::Lexeme>(::hyde::Lexeme::kEndOfFile);
  return ret;
}

// Return a number literal token at `position` that occupies `spelling_width`
// columns of text in the display.
Token Token::FakeNumberLiteral(DisplayPosition position,
                               unsigned spelling_width) {
  Token ret = {};
  ret.position = position;

  auto &literal = ret.As<lex::NumberLiteralToken>();
  literal.Store<::hyde::Lexeme>(::hyde::Lexeme::kLiteralNumber);
  literal.Store<lex::SpellingWidth>(static_cast<uint16_t>(spelling_width));
  literal.Store<lex::NumberSpellingKind>(lex::NumberSpellingKind::kInvalid);

  return ret;
}

// Return a number literal token at `position` that occupies `spelling_width`
// columns of text in the display.
Token Token::FakeStringLiteral(DisplayPosition position,
                               unsigned spelling_width) {
  Token ret = {};
  ret.position = position;

  auto &literal = ret.As<lex::StringLiteralToken>();
  literal.Store<::hyde::Lexeme>(::hyde::Lexeme::kLiteralString);
  literal.Store<lex::SpellingWidth>(static_cast<uint16_t>(spelling_width));
  literal.Store<lex::Id>(0u);
  literal.Store<lex::ReprWidth>(0u);

  return ret;
}

// Return a type token at `position` that occupies `spelling_width`
// columns of text in the display.
Token Token::FakeType(DisplayPosition position, unsigned spelling_width) {
  Token ret = {};
  ret.position = position;

  auto &ident = ret.As<lex::TypeToken>();
  ident.Store<::hyde::Lexeme>(::hyde::Lexeme::kTypeIn);
  ident.Store<lex::SpellingWidth>(3u);
  ident.Store<::hyde::TypeKind>(::hyde::TypeKind::kUnsigned32);

  return ret;
}

// Return a fake token at `range`.
Token Token::Synthetic(::hyde::Lexeme lexeme, DisplayRange range) {
  Token ret = {};
  ret.position = range.From();

  auto &basic = ret.As<lex::BasicToken>();
  basic.Store<::hyde::Lexeme>(lexeme);

  int64_t num_lines = 0;
  int64_t num_cols = 0;
  if (range.TryComputeDistance(nullptr, &num_lines, &num_cols) && !num_lines &&
      0 < num_cols) {
    basic.Store<lex::SpellingWidth>(static_cast<uint64_t>(num_cols));
  }

  return ret;
}

}  // namespace hyde
