// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display.h>

#include <memory>
#include <string_view>

namespace hyde {

// The type of a token.
enum class Lexeme : uint8_t {
  kInvalid,
  kInvalidDirective,  // Invalid declaration (starts with a `hash`).
  kInvalidNumber,
  kInvalidNewLineInString,
  kInvalidEscapeInString,
  kInvalidUnterminatedString,
  kInvalidUnterminatedCode,
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
  //    #local helper(i32 A, i32 B)
  //
  // Internal rule declarations specify the type of the rule and its
  // parameters, and are not visible outside of the module. Their names
  // must be unique within the module.
  kHashLocalDecl,

  // Declare a rule that will be defined in this module, but visible outside
  // of this module.
  //
  //    #export helper(i32 A, i32 B)
  //
  // Exported rule declarations specify the type of the rule and its
  // parameters. Their names must be globally unique.
  kHashExportDecl,

  // Lexemes associated with rules that are defined in this module, but visible
  // outside of this module, and exported to RPC interfaces. These rules are
  // guaranteed to be backed by physical entries in the database.
  //
  //    #query tc(bound type A, free type B)
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
  //    #message is_function(bound u64 EA)
  //    #local has_symbol_name(u64 EA, ascii Name).
  //    #export entrypoint_function(u64 EA)
  //    entrypoint_function(EA) : is_function(EA), has_symbol_name(EA, "_start").
  //
  // Any time a `is_function` message is published, we attempt to prove the
  // `entrypoint_function` rule.
  kHashMessageDecl,

  // Used to import another module.
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

  // Used to include a C/C++ header our source file. This is translated down
  // into an equivalent include statement in generated C++ code.
  //
  //    #include "path"
  kHashIncludeStmt,

  // Used to insert some C/C++ code "inline" into the Datalog code. This is
  // an alternative to `#include`, and may itself contain `#include`s. The
  // usage looks like:
  //
  //    #inline <!
  //    ... code here ...
  //    !>
  //
  // Inline code is placed into the generated C/C++ code *after* all `#include`
  // statements, regardless of whether or not the `#include`s came before or
  // after the `#inline` statements.
  kHashInlineStmt,

  // Unsigned/signed integral types. `n` must be one of 8, 16, 32, or 64.
  // For example, `i32` is a signed 32-bit integer, whereas `u32` is
  // an unsigned 32-bit integer.
  kTypeIn,
  kTypeUn,

  // Floating point integral types. `f32` is a `float`, and `f64` is
  // a `double`.
  kTypeFn,

  // Variable-length sequence of bytes, encoded as UTF-8. Guaranteed to
  // end in a NUL (`\0`) byte.
  kTypeUTF8,

  // Variable-length sequence of 7-bit bytes, encoded as UTF-8. Guaranteed to
  // end in a NUL (`\0`) byte.
  kTypeASCII,

  // Variable-length sequence of bytes`. No guarantees about a terminating
  // character or the encoding.
  kTypeBytes,

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
  //    #functor merge_i8(bound i8 OldVal, bound i8 ProposedVal,
  //                      free i8 NewVal) trivial
  //    #local byte_val(i64 Address, mutable(merge_i8) ByteVal)
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

  // Used when specifying that some subset of the `bound`-attributed parameters
  // to a functor can be re-ordered for the sake of optimization. For example:
  //
  //      #functor add_i32(
  //          bound i32 LHS,
  //          bound i32 RHS,
  //          free i32 Sum) unordered(LHS, RHS)
  //
  // This lets us say that the optimizer is permitted to reorder the `LHS`
  // and `RHS` parameters.
  kKeywordUnordered,

  // Used with functors to tell the compiler that the outputs (free variables)
  // of the functor, which in this case is like a map, are not pure with respect
  // to the bound parameters. For example, a directory listng functor should be
  // marked as impure, as the file system might have changed since the last
  // invocation of the listing.
  //
  //    #functor foo(...) impure
  //
  // The implication is that impure functors will be "wrapped" with additional
  // state tracking in order to ensure that output values that were previously
  // produced but not produced by the current invocation are subsequently
  // removed from any relations.
  kKeywordImpure,

  // Whether or not a local/export can be inlined. The inline keyword is a hint
  // and the compiler is free the aggressively inline or ignore the hint.
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

  // Literal C/C++ code. Looks like:
  //
  // <! ... stuff here ... !>
  kLiteralCode,

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

  // Return the spelling width of this token. Returns `0` if unknown or if
  // this token spans across more than one line.
  //
  // NOTE(pag): The spelling width is, in practice, not valid for code literals
  //            as they can span multiple lines.
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

  // Return the ID of the corresponding code, or `0` if not a string.
  unsigned CodeId(void) const;

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

  // Returns the invalid char, or `\0` if not present.
  char InvalidChar(void) const;

  // Return a fake token at `range`.
  static Token Synthetic(::hyde::Lexeme lexeme, DisplayRange range);

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

  DisplayPosition position;
  uint64_t opaque_data{0};
};

OutputStream &operator<<(OutputStream &os, Token tok);

// Basic string pool implementation. Used for body_variables, atoms, and strings.
class StringPool {
 public:
  ~StringPool(void);
  StringPool(void);

  // Intern a code block into the pool, returning its ID.
  unsigned InternCode(std::string_view code) const;

  // Read out some code block given its ID.
  bool TryReadCode(unsigned id, std::string_view *code_out) const;

  // Intern a string into the pool, returning its offset in the pool.
  unsigned InternString(std::string_view data, bool force = false) const;

  // Read out some string given its index and length.
  bool TryReadString(unsigned index, unsigned len,
                     std::string_view *data_out) const;

 private:
  class Impl;

  std::shared_ptr<Impl> impl;
};

class LexerImpl;

// Reads one or more characters from a display, and then
class Lexer {
 public:
  ~Lexer(void);

  Lexer(void) = default;

  // Read tokens from `reader`.
  void ReadFromDisplay(const DisplayReader &reader);

  // Try to read the next token from the lexer. If successful, returns `true`
  // and updates `*tok_out`. If no more tokens can be produced, then `false` is
  // returned. Error conditions are signalled via special lexemes.
  bool TryGetNextToken(const StringPool &string_pool, Token *tok_out);

 private:
  std::shared_ptr<LexerImpl> impl;
};

}  // namespace hyde
