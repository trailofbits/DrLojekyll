// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/Type.h>

#include <type_traits>

namespace hyde {
namespace lex {

DEFINE_BOXED_TYPE(SpellingWidth, uint16_t);
DEFINE_BOXED_TYPE(ReprWidth, uint16_t);
DEFINE_BOXED_TYPE(Index, uint32_t);
DEFINE_BOXED_TYPE(ErrorIndexDisp, uint16_t);
DEFINE_BOXED_TYPE(ErrorLineDisp, uint16_t);
DEFINE_BOXED_TYPE(ErrorColumn, uint16_t);
DEFINE_BOXED_TYPE(IndexDisp, uint16_t);
DEFINE_BOXED_TYPE(LineDisp, uint16_t);
DEFINE_BOXED_TYPE(Line, uint16_t);
DEFINE_BOXED_TYPE(Column, uint16_t);
DEFINE_BOXED_TYPE(Id, uint32_t);
DEFINE_BOXED_TYPE(PrefixWidth, uint8_t);

enum class NumberSpellingKind : uint8_t {
  kInvalid,  // When we recover from an invalid number literal.
  kBinary,  // E.g. `0b1010101`.
  kOctal,  // E.g. `0666`.
  kDecimal,  // E.g. `10`.
  kHexadecimal,  // E.g. `0xf00`.
};

struct BasicToken final : public TypedOpaqueData<Lexeme, SpellingWidth> {};

struct ErrorToken final
    : public TypedOpaqueData<
          Lexeme, char /* Character that cause the error */,
          ErrorIndexDisp /* Index of the bad char in the display */,
          ErrorLineDisp /* Line of the bad char in the display */,
          ErrorColumn /* Column of the bad char in the display */,
          IndexDisp /* Index of the first char of the following token */,
          LineDisp /* Line of the first char of the following token */,
          Column /* Column of the first char of the following token */> {};

struct TypeToken final
    : public TypedOpaqueData<Lexeme, SpellingWidth, TypeKind> {};

struct WhitespaceToken final
    : public TypedOpaqueData<
          Lexeme, SpellingWidth /* Number of leading spaces */,
          IndexDisp /* Next index */, LineDisp /* Next line */,
          Column /* Next column */> {};

struct NumberLiteralToken final
    : public TypedOpaqueData<Lexeme, SpellingWidth, NumberSpellingKind,
                             bool /* Has decimal point */,
                             PrefixWidth /* Prefix width, e.g. 2 for `0x`. */> {
};

struct StringLiteralToken final
    : public TypedOpaqueData<Lexeme, SpellingWidth,
                             Id  /* Unique ID of the represented string. All
                                  * strings are interned so that they can all
                                  * share the same representation. This is the
                                  * index of this string in a global array of
                                  * all string data. */,
                             ReprWidth  /* Number of bytes in the represented
                                         * string (after processing things
                                         * like escape characters). */> {};

struct CodeLiteralToken final
    : public TypedOpaqueData<
          Lexeme, Id /* Unique ID of the represented code. The first
                                  * code block has ID `0`, the second has ID
                                  * `1`, etc. */
          ,
          ReprWidth /* Number of bytes in the represented
                                         * string (after processing things
                                         * like escape characters). */
          ,
          IndexDisp /* The next index. */, LineDisp /* The next line ``` */,
          Column /* The next column */> {};

struct IdentifierToken final
    : public TypedOpaqueData<Lexeme, SpellingWidth,
                             Id  /* Unique ID of the identifier. All identifiers
                                  * are interned along with strings. */,
                             TypeKind  /* Foreign type kind, if any */> {};

union TokenInterpreter {
  OpaqueData flat;
  BasicToken basic;
  ErrorToken error;
  WhitespaceToken whitespace;
  TypeToken type;
  NumberLiteralToken number;
  StringLiteralToken string;
  CodeLiteralToken code;
  IdentifierToken identifier;
};
static_assert(sizeof(TokenInterpreter) == sizeof(OpaqueData));

}  // namespace lex
}  // namespace hyde
