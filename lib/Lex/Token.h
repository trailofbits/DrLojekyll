// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Lex/Token.h>

namespace hyde {
namespace lex {

struct BasicToken {
  Lexeme lexeme;  // Common.
  uint8_t unused;
  char invalid_escape_char;
  char invalid_char;
  uint16_t error_offset;
  uint16_t spelling_width;  // Common.
};

struct TypeToken {
  uint64_t lexeme:8;
  uint64_t unused:24;
  uint64_t type_width:16;
  uint64_t spelling_width:16;
};

struct WhitespaceToken {
  uint64_t lexeme:8;
  uint64_t _unused:8;
  uint64_t num_leading_newlines:16;
  uint64_t num_leading_spaces:16;
  uint64_t spelling_width:16;
};

enum class NumberSpellingKind : uint8_t {
  kInvalid,  // When we recover from an invalid number literal.
  kBinary,  // E.g. `0b1010101`.
  kOctal,  // E.g. `0666`.
  kDecimal,  // E.g. `10`.
  kHexadecimal,  // E.g. `0xf00`.
};

struct NumberLiteralToken {
  Lexeme lexeme;
  NumberSpellingKind spelling_kind;
  bool has_decimal_point;
  uint8_t _unused;
  uint16_t prefix_width;
  uint16_t spelling_width;
};

struct StringLiteralToken {
  uint64_t lexeme:8;

  // Unique ID of the represented string. All strings are interned so that
  // they can all share the same representation. This is the index of this
  // string in a global array of all string data.
  uint64_t index:24;

  // Number of bytes in the represented string (after processing things
  // like escape characters).
  uint64_t num_bytes:16;

  // Width of the string literal, as it appears in the display.
  uint64_t spelling_width:16;
};

struct IdentifierToken {
  uint64_t lexeme:8;

  // Unique ID of the identifier. All identifiers are interned along with
  // strings.
  uint64_t index:24;

  uint64_t _unused:16;

  // Maximum length of any identifier is 64 characters. The spelling width
  // is the number of bytes associated with the identifier.
  uint64_t spelling_width:16;
};

union TokenInterpreter {
  uint64_t flat{0};
  BasicToken basic;
  WhitespaceToken whitespace;
  TypeToken type;
  NumberLiteralToken number;
  StringLiteralToken string;
  IdentifierToken identifier;
};

static_assert(sizeof(TokenInterpreter) == 8);

}  // namespace lex
}  // namespace hyde
