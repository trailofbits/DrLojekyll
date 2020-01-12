// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

namespace hyde {

class Lexer;
class ParserImpl;

// The type of a token.
enum class Lexeme : uint8_t {
  kInvalid,
  kInvalidDirective,  // Invalid declaration (starts with a `hash`).
  kInvalidNumber,
  kInvalidNewLineInString,
  kInvalidEscapeInString,
  kInvalidUnterminatedString,
  kInvalidStreamOrDisplay,
  kInvalidTypeName,
  kInvalidUnknown,

  // End of file token; this prevents rules from spanning across files.
  kEndOfFile,

  // Represents one or more spaces and new lines. This tracks and compresses
  // new lines into a format of number of leading newlines, followed by number
  // of trailing spaces on the last non-empty line.
  kWhitespace,

  // A comment, for example:
  //
  //      ; Hello world, this is a comment.
  kComment,

  // Declare a rule that will be defined in this module.
  //
  //    #local helper(@i32 A, @i32 B)
  //
  // Internal rule declarations specify the type of the rule and its
  // parameters, and are not visible outside of the module. Their names
  // must be unique within the module.
  kHashLocalDecl,

  // Declare a rule that will be defined in this module, but visible outside
  // of this module.
  //
  //    #export helper(@i32 A, @i32 B)
  //
  // Exported rule declarations specify the type of the rule and its
  // parameters. Their names must be globally unique.
  kHashExportDecl,

  // Lexemes associated with rules that are defined in this module, but visible
  // outside of this module, and exported to RPC interfaces. These rules are
  // guaranteed to be backed by physical entries in the database.
  //
  //    #query tc(bound @type A, free @type B)
  //
  // Declares `tc` as a 2-tuple, where modules using `tc` must always supply
  // a bound value for `A`, i.e. they can query for `B`s using concrete `A`s.
  //
  // Multiple `.extern` declarations for the same-named rule can be defined, so
  // long as each one specifies a different binding parameters. The binding
  // parameters are hints to the engine as to what indexes should be created,
  // independent of how the rule itself is observed to be used.
  kHashQueryDecl,

  // Lexemes associated with user-defined messages. Messages can be rule heads,
  // where the bottom-up proof of the rule triggers publication of the message,
  // and messages can be subscribed to by rules, with the caveat that a rule
  // can only contain one subscribed message. Messages must have globally unique
  // names that never conflict with defined clauses.
  //
  //    #message is_function(bound @u64 EA)
  //    #local has_symbol_name(@u64 EA, @str Name).
  //    #export entrypoint_function(@u64 EA)
  //    entrypoint_function(EA) : is_function(EA), has_symbol_name(EA, "_start").
  //
  // Any time a `is_function` message is published, we attempt to prove the
  // `entrypoint_function` rule.
  kHashMessageDecl,

  // Lexemes associated with importing modules.
  //
  //    #import "path"
  //
  kHashImportModuleStmt,

  // Declares a user-defined functor. These are functions that are defined
  // by native code modules. They must have globally unique names. When called
  // with all bound arguments, they must be pure, so that two uses can be
  // folded into a single use.
  //
  //    #functor add1(bound Input:i32, free Result:i32)
  //
  // Functors are associated with C++ classes, with methods for each variant
  // of the function parameters (bound, unbound). The methods take in concrete
  // bound arguments, and return a generator that can produce the unbound
  // values.
  //
  // Associating functors with classes enables functors to manage a backing
  // store of state.
  kHashFunctorDecl,

  // Unsigned/signed integral types. `n` must be one of 8, 16, 32, or 64.
  // For example, `@i32` is a signed 32-bit integer, whereas `@u32` is
  // an unsigned 32-bit integer.
  kTypeIn,
  kTypeUn,

  // Floating point integral types. `@f32` is a `float`, and `@f64` is
  // a `double`.
  kTypeFn,

  // Variable-length string type, `str`. Strings are interned, so that they
  // all have a unique ID.
  kTypeString,

  // A universally unique identifier.
  kTypeUUID,

  // Keywords for specifying the binding of parameters.
  kKeywordBound,
  kKeywordFree,
  kKeywordAggregate,
  kKeywordSummary,

  // Binding specifier declaring that a parameter is mutable, and wrap the
  // merge operation of that parameter. For example:
  //
  //    #functor merge_i8(bound @i8 OldVal, bound @i8 ProposedVal,
  //                      free @i8 NewVal) trivial
  //    #local byte_val(@i64 Address, mutable(merge_i8) ByteVal)
  //
  // Proofs of `byte_val` implicitly end with a merge operation, where
  // `merge_i8` in this case is invoked, and the produced value is the `NewVal`
  // output value of `merge_i8`.
  //
  // Mutable-attributed parameters must be used if a parameter value can be
  // derived from an aggregate's summary value.
  kKeywordMutable,

  // Keyword for aggregation over some relation.
  kKeywordOver,

  // Specifiers for the level of complexity of a functor.
  kKeywordTrivial,
  kKeywordComplex,

  // Whether or not a local/export can be inlined.
  kKeywordInline,

  kPuncOpenParen,
  kPuncCloseParen,

  kPuncOpenBrace,
  kPuncCloseBrace,

  kPuncPeriod,
  kPuncComma,
  kPuncColon,

  kPuncEqual,

  // NOTE(pag): We don't supports things like `<=` or `>=` because dealing
  //            with inequalities is much easier.
  kPuncNotEqual,
  kPuncLess,
  kPuncGreater,

  // Used for negation or cut. When in front of a rule, it is negation,
  // but when on its own, it is a Prolog-like cut operator.
  kPuncExclaim,

  kLiteralNumber,
  kLiteralString,

  // Identifiers, e.g. for atoms, functors, messages, etc.
  kIdentifierAtom,
  kIdentifierUnnamedAtom,
  kIdentifierVariable,
  kIdentifierUnnamedVariable  // `_`.
};

// Represents a single token of input.
class Token {
 public:
  bool IsValid(void) const;

  inline bool IsInvalid(void) const {
    return !IsValid();
  }

  // Return the location of this token.
  inline DisplayPosition Position(void) const {
    return position;
  }

  // Return the position of an error associated with this token if it is an
  // invalid token.
  DisplayPosition ErrorPosition(void) const;

  // Return the range of characters covered by this token.
  DisplayRange SpellingRange(void) const;

  // Return the position of the first character immediately following
  // this token.
  DisplayPosition NextPosition(void) const;

  // Return this token's lexeme.
  ::hyde::Lexeme Lexeme(void) const;

  // Return the spelling width of this token.
  unsigned SpellingWidth(void) const;

  // Returns `true` if this token's lexeme corresponds with a type.
  bool IsType(void) const;

  // Return the ID of the corresponding display containing this token, or
  // `~0u` if invalid.
  uint64_t DisplayId(void) const {
    return position.DisplayId();
  }

  // Return the line number on which the first character of this token is
  // located, or `~0u` if invalid.
  uint64_t Line(void) const {
    return position.Line();
  }

  // Return the column number on which the first character of this token is
  // located, or `~0u` if invalid.
  uint64_t Column(void) const {
    return position.Column();
  }

  // Return the ID of the corresponding string, or `0` if not a string.
  unsigned StringId(void) const;

  // Return the length of the corresponding string, or `0` if not a string.
  unsigned StringLength(void) const;

  // Return the ID of the corresponding identifier, or `0` if not a string.
  unsigned IdentifierId(void) const;

  // Return the length of the corresponding identifier, or `0` if not a string.
  unsigned IdentifierLength(void) const;

  // Return the size, in bytes, of the corresponding type.
  unsigned TypeSizeInBytes(void) const;

  // Returns the invalid escape char, or `\0` if not present.
  char InvalidEscapeChar(void) const;

  // Returns the invalid char, or `\0` if not present.
  char InvalidChar(void) const;

 private:
  friend class Lexer;
  friend class ParserImpl;

  // Return an EOF token at `position`.
  static Token FakeEndOfFile(DisplayPosition position);

  // Return a number literal token at `position` that occupies `spelling_width`
  // columns of text in the display.
  static Token FakeNumberLiteral(DisplayPosition position,
                                 unsigned spelling_width);

  // Return a string literal token at `position` that occupies `spelling_width`
  // columns of text in the display.
  static Token FakeStringLiteral(DisplayPosition position,
                                 unsigned spelling_width);

  // Return a type token at `position` that occupies `spelling_width`
  // columns of text in the display.
  static Token FakeType(DisplayPosition position, unsigned spelling_width);

  // Return a fake token at `range`.
  static Token Synthetic(::hyde::Lexeme lexeme, DisplayRange range);

  DisplayPosition position;
  uint64_t opaque_data{0};
};

}  // namespace hyde
