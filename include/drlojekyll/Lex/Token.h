// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>
#include <drlojekyll/Util/OpaqueData.h>

#include <functional>
#include <utility>

namespace hyde {

class Lexer;
class ParserImpl;

// The type of a token.
enum class Lexeme : uint8_t {
  kInvalid,
  kInvalidDirective,  // Invalid declaration (starts with a `hash`).
  kInvalidNumber,
  kInvalidOctalNumber,
  kInvalidHexadecimalNumber,
  kInvalidBinaryNumber,
  kInvalidNewLineInString,
  kInvalidEscapeInString,
  kInvalidUnterminatedString,
  kInvalidUnterminatedCode,
  kInvalidUnterminatedCxxCode,
  kInvalidUnterminatedPythonCode,
  kInvalidStreamOrDisplay,
  kInvalidTypeName,
  kInvalidUnknown,
  kInvalidPragma,

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

  // Used to declare a "foreign" type. Foreign types can be "forward declared"
  // with no codegen, e.g.
  //
  //    #foreign std_string
  //
  // And/or be re-declared with concrete implementation types in code, with the
  // syntax:
  //
  //    #foreign <type name> <type name code>
  //
  // For example:
  //
  //    #foreign std_string ```python str```
  //    #foreign std_string ```c++ std::string```
  //
  // Once declared, foreign types are globally visible. There can be at most
  // one concrete implementation declaration per language for each foreign
  // type. If no language specifier is given for the concrete implementation,
  // then it applies to target languages uniformly.
  kHashForeignTypeDecl,

  // Used to declare a foreign constant of a particular foreign type.
  //
  //    #constant <foreign type> <constant name> ```<lang> value```
  //
  // Foreign constants can be used to translate things like `sizeof` expressions
  // from C++, enumeration constants, global variables, etc. Realistically, one
  // could expand a foreign constant to a function call that (should) always
  // return the same value.
  kHashForeignConstantDecl,

  // Used to import another module.
  //
  //    #import "path"
  //
  kHashImportModuleStmt,

  // Used to insert some C/C++/Python code inline into the Datalog code. The
  // usage looks like:
  //
  //    #inline(stage-name) ```<lang>
  //    ... code here ...
  //    ```
  //
  // Inline code names a code generation stage where the code will be placed.
  kHashInlineStmt,

  // Use to name the database. For example:
  //
  //    #database foo.
  //
  // This will name the database `foo` (atom), which is then used to
  // disambiguate different auto-generated code, e.g. via `namespace foo`
  // in C++.
  kHashDatabase,

  // Used to name an enumeration type. For example:
  //
  //    #enum Foo.
  //
  // This declares that there will be an `Foo` enum. A specific underlying
  // integral type can also be specified, e.g.:
  //
  //    #enum Foo u8.
  //
  // To add enumerators to the enum, one uses `#constant`:
  //
  //    #constant Foo BAR 10.
  //    #constant Foo BAZ 20.
  //
  // If no value is provided for `BAR`, then it takes on a default-assigned
  // value.
  kHashEnum,

  // Boolean type.
  kTypeBoolean,

  // Unsigned/signed integral types. `n` must be one of 8, 16, 32, or 64.
  // For example, `i32` is a signed 32-bit integer, whereas `u32` is
  // an unsigned 32-bit integer.
  kTypeIn,
  kTypeUn,

  // Floating point integral types. `f32` is a `float`, and `f64` is
  // a `double`.
  kTypeFn,

  // Variable-length sequence of bytes`. No guarantees about a terminating
  // character or the encoding.
  kTypeBytes,

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

  kPuncOpenParen,
  kPuncCloseParen,

  kPuncOpenBrace,
  kPuncCloseBrace,

  kPuncPeriod,
  kPuncComma,
  kPuncColon,
  kPuncQuestion,
  kPuncPlus,
  kPuncStar,
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

  // Boolean literals, i.e. `true` and `false`. Booleans are nifty for a variety
  // of reasons -- they let one enable/disable rules based on variables, and
  // the `false` literal is also super nifty for testing, as it can let us
  // drill down on which rules are significant to the reproduction of some
  // unexpected behavior.
  kLiteralTrue,
  kLiteralFalse,

  // Literal C/C++, Python, or FlatBuffer code. Looks like:
  //
  //      ```<lang> stuff here```
  //
  // For example:
  //      ```python <code>```
  //      ```c++ <code>```
  //      ```flat <code>```
  kLiteralCode,
  kLiteralCxxCode,
  kLiteralPythonCode,
  kLiteralFlatBufferCode,

  // Identifiers, e.g. for atoms, functors, messages, etc.
  kIdentifierAtom,
  kIdentifierUnnamedAtom,
  kIdentifierVariable,
  kIdentifierUnnamedVariable,  // `_`.
  kIdentifierType,  // Foreign type names.
  kIdentifierConstant,  // Foreign constant name.

  // `@differential` is a pragma used to mark messages that can receive or
  // publish removals.
  kPragmaDifferential,

  // `@highlight` is a debugging pragma, used to mark data flow nodes associated
  // with a particular clause body as "highlighted" so they are easier to
  // spot in the data flow IR visualizations.
  kPragmaDebugHighlight,

  // Used with functors to tell the compiler that the outputs (free variables)
  // of the functor, which in this case is like a map, are not pure with respect
  // to the bound parameters. For example, a directory listng functor should be
  // marked as impure, as the file system might have changed since the last
  // invocation of the listing.
  //
  //    #functor foo(...) @impure
  //
  // The implication is that impure functors will be "wrapped" with additional
  // state tracking in order to ensure that output values that were previously
  // produced but not produced by the current invocation are subsequently
  // removed from any relations.
  kPragmaHintImpure,

  // `@product` is a pragma that tells Dr. Lojekyll that the user is aware that
  // a particular clause is expected to introduce a cross-product, and that this
  // is in fact their intention. It's can be easy to accidentally introduce
  // cross-products, and their performance implications are severe, and so we
  // require cross-products to be opt-in.
  kPragmaPerfProduct,

  // Used to specify the range or amplification of a functor. For example,
  //
  //      #functor add_i32(
  //          bound i32 LHS,
  //          bound i32 RHS,
  //          free i32 Sum) @range(.)
  //
  // Here we say that the range of `add_i32` is one-to-one. That is we will
  // produce one and only one output for each input.
  //
  // Possible variations and their meanings are:
  //
  //      @range(?)      Zero-or-one
  //      @range(*)      Zero-or-more
  //      @range(.)      One-to-one
  //      @range(+)      One-or-more
  //
  // The default range for a functor is conservatively assumed to be
  // zero-or-more. If a functor has no `free` parameters then it implicitly
  // has a zero-or-one range. Finally, an aggregating functor is not allowed
  // to have a range specifier, though you can think of it as many-to-one.
  kPragmaPerfRange,

  // Whether or not a local/export can be inlined. The inline keyword is a hint
  // and the compiler is free the aggressively inline or ignore the hint.
  kPragmaPerfInline,

  // Used to mark a foreign type as having a referentially transparent
  // implementation, such that equality implies identity. For example:
  //
  //      #foreign Address ```python int``` @transparent
  //
  // This is a performance pragma because it reduces the code generation burden
  // because function calls to resolve the earliest identity of an object and
  // merge with that need not be generated.
  kPragmaPerfTransparent,

  // Used to mark a foreign constant as unique. If a constant is marked as
  // unique, and if it's compared to any other constant that isn't identical,
  // then the comparison is assumed to be unequal.
  kPragmaPerfUnique,

  // Used to mark a special form of negation that says that if something
  // satisfies the negation once, then it will always satisfy the negation.
  // This is used as follows:
  //
  //    Instead of:    !foo(A, B)
  //           Use:    @never foo(A, B)
  kPragmaPerfNever,

  // Used to mark a query with `free`-attributed parameters as only returning
  // the first match. This changes the return type from a stream to a possible
  // value.
  //
  //    #query foo(..., free type Val, ...) @first ...
  //
  kPragmaCodeGenFirst,

  // Used to mark that the data flow scheduler should try to schedule everything
  // before a barrier before scheduling everything after a barrier. More than
  // one barrier can be used within a clause body. This is often usedful to
  // ensure that joins are not over-eagerly performed, when a more restricted
  // join could be applied to the output of a functor.
  //
  //    function(ObjId, Arch, OS, EA) @highlight
  //        : json_spec(Arch, OS, Spec)
  //        , json_read_object_key(Spec, "functions", FuncList)
  //        , json_read_list_entry(FuncList, _, FuncInfo)
  //        , json_read_object_key(FuncInfo, "address", IntAddress)
  //        , json_read_address(IntAddress, EA)
  //        , @barrier
  //        , instruction(ObjId, InstArch, EA)
  //        , object_file(ObjId, _ObjArch, OS, _ObjLoadEA, _ObjPath)
  //        , valid_arch_transition(Arch, InstArch).
  //
  // If we didn't have a barrier here, then `instruction` would be joined with
  // `object_file`, then later the the effect of the join against `EA` output
  // from `json_read_address` would be achieved via a comparison. This is not
  // desirable because we really want the result of all the JSON parsing stuff
  // to join against the remaining relations.
  kPragmaPerfBarrier,
};

enum class TypeKind : uint32_t;

// Represents a single token of input.
class Token final : public OpaqueData {
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

  // Return the range of characters covered by this token. This is an open range
  // of the form `[begin, end)`.
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

  // Returns a hash of this token.
  uint64_t Hash(void) const noexcept;

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

  // Return the kind of this type. This works for foreign types, as well as
  // for foreign constants.
  ::hyde::TypeKind TypeKind(void) const;

  // Returns the invalid char, or `\0` if not present.
  char InvalidChar(void) const;

  // Return a fake token at `range`.
  static Token Synthetic(::hyde::Lexeme lexeme, DisplayRange range);

  inline bool operator==(const Token that) const noexcept {
    return this->OpaqueData::operator==(that) && position == that.position;
  }

  inline bool operator!=(const Token that) const noexcept {
    return this->OpaqueData::operator!=(that) || position != that.position;
  }

 private:
  friend class Lexer;
  friend class ParserImpl;

  // Returns this token, converted to be a foreign type.
  Token AsForeignType(void) const;

  // Returns this token, converted to be a foreign constant of a specific type.
  Token AsForeignConstant(::hyde::TypeKind kind) const;

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
};

}  // namespace hyde
namespace std {

template <>
struct hash<::hyde::Token> {
  using argument_type = ::hyde::Token;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::Token tok) const noexcept {
    return tok.Hash();
  }
};

}  // namespace std
